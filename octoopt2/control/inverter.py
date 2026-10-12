"""Send charge/discharge commands to GivEnergy inverter via local Modbus TCP.

Uses the vendored givenergy_modbus_async client (from GivTCP) which supports
firmware 912+ / battery firmware 3015+. Async calls are wrapped synchronously.

Three operating states per slot:
  CHARGE           — force grid charging at desired rate
  DISCHARGE_EXPORT — force discharge at max power, exporting surplus to grid
  ECO              — dynamic self-consumption: battery discharges only to meet
                     load and charges from excess solar; no forced activity.
                     Also used for discharge-to-meet-load decisions, so the
                     battery never force-exports when real load is below the
                     planned discharge (see apply_decision).

Register-write optimisations
─────────────────────────────
1. Skip write entirely when mode and power level are unchanged since the last
   successful write. This avoids hammering flash registers every 5-minute tick
   when the inverter is already in the correct state.

2. Slot-time registers (CHARGE_SLOT_1 / DISCHARGE_SLOT_1) are only written on
   mode transitions — they are set once when entering a mode and then left alone
   until the mode changes. The slot window is always 00:00–23:59 so writing it
   repeatedly is pure waste.
"""
import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timezone

from ..config import BatteryConfig, GivEnergyConfig
from ..db import get_conn
from ..givenergy_modbus_async.client.client import Client
from ..givenergy_modbus_async.model import TimeSlot
from ..givenergy_modbus_async.client import commands
from ..optimizer.model import SlotDecision

logger = logging.getLogger(__name__)

_THRESHOLD_KWH = 0.05
_REGISTER_MAX = 50
_RETRY_ATTEMPTS = 3
_RETRY_DELAY_S = 2


# ── Last-command state ────────────────────────────────────────────────────────

@dataclass
class _LastCommand:
    mode: str           # CHARGE | DISCHARGE_EXPORT | ECO
    power_register: int # 1-50 for CHARGE/DISCHARGE; 0 for ECO


def _load_last_command(db_path: str) -> _LastCommand | None:
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT mode, power_register FROM inverter_last_command WHERE id = 1"
        ).fetchone()
    if row is None:
        return None
    return _LastCommand(mode=row["mode"], power_register=row["power_register"])


def _save_last_command(db_path: str, cmd: _LastCommand, write_count: int) -> int:
    """Persist the last applied command and increment the lifetime write counter.

    Returns the new cumulative total.
    """
    with get_conn(db_path) as conn:
        conn.execute(
            """
            INSERT INTO inverter_last_command (id, applied_at, mode, power_register, total_writes)
            VALUES (1, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                applied_at     = excluded.applied_at,
                mode           = excluded.mode,
                power_register = excluded.power_register,
                total_writes   = inverter_last_command.total_writes + excluded.total_writes
            """,
            (datetime.now(timezone.utc).isoformat(), cmd.mode, cmd.power_register, write_count),
        )
        row = conn.execute(
            "SELECT total_writes FROM inverter_last_command WHERE id = 1"
        ).fetchone()
    return row["total_writes"] if row else write_count


def _log_command(
    db_path: str,
    *,
    mode: str,
    power_register: int,
    n_writes: int,
    ok: bool,
    error: str | None = None,
    slot_start: datetime | None = None,
    trigger: str | None = None,
) -> None:
    """Append one row to inverter_commands (audit trail of every write attempt).

    Best-effort: a bookkeeping failure must never mask the outcome of the
    hardware write itself.
    """
    try:
        with get_conn(db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO inverter_commands
                    (applied_at, mode, power_register, n_writes, ok, error, slot_start, trigger)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now(timezone.utc).isoformat(), mode, power_register, n_writes,
                    1 if ok else 0, error,
                    slot_start.astimezone(timezone.utc).isoformat() if slot_start else None,
                    trigger,
                ),
            )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Could not log inverter command: %s", exc)


# ── Public API ────────────────────────────────────────────────────────────────

def decision_to_mode(decision: SlotDecision, battery: BatteryConfig) -> tuple[str, float]:
    """Map a slot decision to the hardware mode the controller will apply.

    Returns (mode, power_kw) with mode in {"CHARGE", "DISCHARGE_EXPORT", "ECO"}
    and power_kw the requested charge/discharge rate (0 for ECO). Pure: used by
    apply_decision and by the backtest replay so both apply identical rules.

    Only force-discharge when the plan is to EXPORT. A discharge-to-meet-load
    decision is run as dynamic self-consumption (ECO) instead: forcing the
    discharge slot would drain the battery to the grid whenever real load is
    below the planned discharge (high PV, or DHW the inverter cannot serve),
    violating the export=0 intent. Dynamic mode discharges only to meet load.
    """
    wants_charge    = decision.battery_charge_kwh    > _THRESHOLD_KWH
    wants_discharge = decision.battery_discharge_kwh > _THRESHOLD_KWH
    wants_export    = decision.grid_export_kwh       > _THRESHOLD_KWH
    if wants_charge:
        return "CHARGE", decision.battery_charge_kwh / 0.5
    if wants_discharge and wants_export:
        return "DISCHARGE_EXPORT", decision.battery_discharge_kwh / 0.5
    return "ECO", 0.0


def apply_decision(
    decision: SlotDecision,
    config: GivEnergyConfig,
    battery: BatteryConfig,
    db_path: str,
    trigger: str | None = None,
) -> None:
    """Translate a slot decision into inverter register writes.

    Skips the write entirely if mode and power level are unchanged.
    Only writes slot-time registers when the mode actually changes.
    """
    mode, power_kw = decision_to_mode(decision, battery)
    if mode == "CHARGE":
        power_reg = _kw_to_register(power_kw, battery.max_charge_rate_kw)
    elif mode == "DISCHARGE_EXPORT":
        power_reg = _kw_to_register(power_kw, battery.max_discharge_rate_kw)
    else:
        power_reg = 0

    # ── Skip if unchanged ─────────────────────────────────────────────────
    last = _load_last_command(db_path)
    if last is not None and last.mode == mode and last.power_register == power_reg:
        logger.debug(
            "Inverter command unchanged (%s reg=%d) — skipping write", mode, power_reg
        )
        return

    last_mode = last.mode if last is not None else None

    # ── Build command list ────────────────────────────────────────────────
    if mode == "CHARGE":
        reqs = _cmds_charge(power_kw, power_reg, last_mode)
    elif mode == "DISCHARGE_EXPORT":
        reqs = _cmds_discharge(power_kw, power_reg, last_mode)
    else:
        reqs = _cmds_eco(soc_reserve_pct=battery.hw_reserve_pct)

    # ── Send and record ───────────────────────────────────────────────────
    try:
        _run_with_retry(config, reqs)
    except Exception as exc:
        _log_command(db_path, mode=mode, power_register=power_reg, n_writes=len(reqs),
                     ok=False, error=str(exc), slot_start=decision.slot_start, trigger=trigger)
        raise
    total = _save_last_command(db_path, _LastCommand(mode=mode, power_register=power_reg), len(reqs))
    _log_command(db_path, mode=mode, power_register=power_reg, n_writes=len(reqs),
                 ok=True, slot_start=decision.slot_start, trigger=trigger)
    logger.info(
        "Inverter: %d register write(s) sent — lifetime total %d",
        len(reqs), total,
    )


def set_battery_reserve(config: GivEnergyConfig, pct: int) -> None:
    """Set the inverter's hardware minimum-SoC reserve (the hard discharge floor).

    Writes BOTH reserve registers so the floor is enforced regardless of mode:
      • BATTERY_SOC_RESERVE              — the "Battery Reserve %" shown in the app
      • BATTERY_DISCHARGE_MIN_POWER_RESERVE — the discharge-side power reserve

    This is a one-off device setting: the inverter holds it in flash until
    changed again. The optimizer's ``min_soc_pct`` only shapes the *plan*; this
    is the physical limit that actually stops the battery discharging. Keep the
    two in sync (set ``min_soc_pct`` >= this value) so plan and hardware agree.

    Valid range is 4–100% (enforced by the underlying commands).
    """
    if not 4 <= pct <= 100:
        raise ValueError(f"Reserve must be in [4, 100]%, got {pct}")
    logger.info("Inverter → set battery reserve floor to %d%%", pct)
    reqs = [
        *commands.set_battery_soc_reserve(pct),
        *commands.set_battery_power_reserve(pct),
    ]
    _run_with_retry(config, reqs)
    logger.info("Inverter: battery reserve floor set to %d%%", pct)


def set_eco_mode(
    config: GivEnergyConfig,
    db_path: str,
    reserve_pct: int | None = None,
    skip_if_current: bool = False,
) -> int:
    """Put the inverter in ECO (dynamic self-consumption). Returns writes sent.

    reserve_pct: when given, both reserve registers (SoC + discharge power) are
      written to this value in the same one-shot batch as the ECO registers, so
      the SoC reserve is never transiently set to 4% by the ECO command. Used by
      octoopt2-away to leave a persistent battery floor.
    skip_if_current: skip the write when the last-command state already says ECO
      (only honored when reserve_pct is None, since that state doesn't track the
      reserve). The state can be stale if the inverter was changed outside
      octoopt2 — the same assumption apply_decision makes.
    """
    if reserve_pct is not None and not 4 <= reserve_pct <= 100:
        raise ValueError(f"Reserve must be in [4, 100]%, got {reserve_pct}")
    if skip_if_current and reserve_pct is None:
        last = _load_last_command(db_path)
        if last is not None and last.mode == "ECO":
            logger.info("Inverter already in ECO per last-command state — skipping write")
            return 0
    reqs = _cmds_eco(soc_reserve_pct=reserve_pct if reserve_pct is not None else 4)
    try:
        _run_with_retry(config, reqs)
    except Exception as exc:
        _log_command(db_path, mode="ECO", power_register=0, n_writes=len(reqs),
                     ok=False, error=str(exc), trigger="fallback")
        raise
    total = _save_last_command(db_path, _LastCommand(mode="ECO", power_register=0), len(reqs))
    _log_command(db_path, mode="ECO", power_register=0, n_writes=len(reqs), ok=True, trigger="fallback")
    logger.info("Inverter: ECO applied — %d register write(s) sent, lifetime total %d", len(reqs), total)
    return len(reqs)


# ── Command builders ──────────────────────────────────────────────────────────

def _cmds_charge(charge_kw: float, limit: int, last_mode: str | None) -> list:
    """Build charge commands. Slot-time register only written when entering CHARGE."""
    logger.info("Inverter → CHARGE %.2f kW (register %d)", charge_kw, limit)
    cmds = []
    if last_mode != "CHARGE":
        # Set slot window on first entry; it stays until overridden.
        cmds += [*commands.set_charge_slot_1(TimeSlot(start=dt_time(0, 0), end=dt_time(23, 59)))]
    cmds += [
        *commands.set_battery_charge_limit(limit),
        *commands.enable_charge(),
        *commands.disable_discharge(),
    ]
    return cmds


def _cmds_discharge(discharge_kw: float, limit: int, last_mode: str | None) -> list:
    """Build forced-discharge commands for an EXPORT decision: discharge at max
    power up to the rate limit, exporting any surplus to the grid. The slot-time
    register is only written when entering DISCHARGE_EXPORT."""
    logger.info("Inverter → DISCHARGE/EXPORT %.2f kW (register %d)", discharge_kw, limit)
    reqs = []
    if last_mode != "DISCHARGE_EXPORT":
        # Entering forced discharge from another mode — set slot window.
        reqs += [*commands.set_discharge_slot_1(TimeSlot(start=dt_time(0, 0), end=dt_time(23, 59)))]
    reqs += [
        *commands.set_battery_discharge_limit(limit),
        *commands.set_discharge_mode_max_power(),
        *commands.enable_discharge(),
        *commands.disable_charge(),
    ]
    return reqs


def _cmds_eco(soc_reserve_pct: int = 4) -> list:
    """Build ECO (dynamic self-consumption) commands.

    Both schedules are disabled. Disabling the *charge* schedule is essential:
    _cmds_charge arms a 00:00–23:59 charge window, and while that window is
    active the inverter neither discharges to meet load nor stops pulling from
    the grid — so an ECO that left it armed would import every kWh the house
    used until the next forced-export slot cleared it. (Measured Sep–Oct 2026:
    battery served <1% of load in ECO slots following a CHARGE slot.)

    Both reserve registers are written so the hardware floor tracks the
    configured planning floor instead of silently reverting to GivEnergy's 4%
    (SoC reserve) or keeping a stale discharge-power reserve.
    """
    logger.info("Inverter → ECO (dynamic mode), SoC reserve %d%%", soc_reserve_pct)
    return [
        *commands.set_discharge_mode_to_match_demand(),
        *commands.set_battery_soc_reserve(soc_reserve_pct),
        *commands.set_battery_power_reserve(soc_reserve_pct),
        *commands.disable_discharge(),
        *commands.disable_charge(),
    ]


# ── Transport ─────────────────────────────────────────────────────────────────

async def _send_async(config: GivEnergyConfig, reqs: list) -> None:
    client = Client(host=config.host, port=config.port)
    await client.one_shot_command(reqs, timeout=3, retries=3)


def _run_with_retry(config: GivEnergyConfig, reqs: list) -> None:
    """Send commands with retry on failure."""
    last_exc: Exception | None = None
    for attempt in range(1, _RETRY_ATTEMPTS + 1):
        try:
            asyncio.run(_send_async(config, reqs))
            return
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Inverter command attempt %d/%d failed: %s",
                attempt, _RETRY_ATTEMPTS, exc,
            )
            if attempt < _RETRY_ATTEMPTS:
                time.sleep(_RETRY_DELAY_S)

    raise RuntimeError(
        f"Failed to apply inverter command after {_RETRY_ATTEMPTS} attempts"
    ) from last_exc


def _kw_to_register(kw: float, max_kw: float) -> int:
    """Map a desired power (kW) to the 1–50 register range."""
    fraction = min(kw / max_kw, 1.0) if max_kw > 0 else 1.0
    return max(1, min(_REGISTER_MAX, round(fraction * _REGISTER_MAX)))
