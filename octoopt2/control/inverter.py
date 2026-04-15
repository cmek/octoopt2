"""Send charge/discharge commands to GivEnergy inverter via local Modbus TCP.

Uses the vendored givenergy_modbus_async client (from GivTCP) which supports
firmware 912+ / battery firmware 3015+. Async calls are wrapped synchronously.

Three operating states per slot:
  CHARGE    — force grid charging at desired rate
  DISCHARGE — discharge to meet demand, or at max power when exporting
  ECO       — dynamic mode: solar self-consumption, no forced activity

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
    mode: str           # CHARGE | DISCHARGE_DEMAND | DISCHARGE_EXPORT | ECO
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


# ── Public API ────────────────────────────────────────────────────────────────

def apply_decision(
    decision: SlotDecision,
    config: GivEnergyConfig,
    battery: BatteryConfig,
    db_path: str,
) -> None:
    """Translate a slot decision into inverter register writes.

    Skips the write entirely if mode and power level are unchanged.
    Only writes slot-time registers when the mode actually changes.
    """
    wants_charge    = decision.battery_charge_kwh    > _THRESHOLD_KWH
    wants_discharge = decision.battery_discharge_kwh > _THRESHOLD_KWH
    wants_export    = decision.grid_export_kwh       > _THRESHOLD_KWH

    # Determine desired hardware state
    if wants_charge:
        charge_kw  = decision.battery_charge_kwh / 0.5
        power_reg  = _kw_to_register(charge_kw, battery.max_charge_rate_kw)
        mode       = "CHARGE"
    elif wants_discharge:
        discharge_kw = decision.battery_discharge_kwh / 0.5
        power_reg    = _kw_to_register(discharge_kw, battery.max_discharge_rate_kw)
        mode         = "DISCHARGE_EXPORT" if wants_export else "DISCHARGE_DEMAND"
    else:
        power_reg = 0
        mode      = "ECO"

    # ── Skip if unchanged ─────────────────────────────────────────────────
    last = _load_last_command(db_path)
    if last is not None and last.mode == mode and last.power_register == power_reg:
        logger.debug(
            "Inverter command unchanged (%s reg=%d) — skipping write", mode, power_reg
        )
        return

    last_mode = last.mode if last is not None else None

    # ── Build command list ────────────────────────────────────────────────
    if wants_charge:
        reqs = _cmds_charge(charge_kw, power_reg, last_mode)
    elif wants_discharge:
        reqs = _cmds_discharge(discharge_kw, power_reg, wants_export, last_mode)
    else:
        reqs = _cmds_eco()

    # ── Send and record ───────────────────────────────────────────────────
    _run_with_retry(config, reqs)
    total = _save_last_command(db_path, _LastCommand(mode=mode, power_register=power_reg), len(reqs))
    logger.info(
        "Inverter: %d register write(s) sent — lifetime total %d",
        len(reqs), total,
    )


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


def _cmds_discharge(discharge_kw: float, limit: int, export: bool, last_mode: str | None) -> list:
    """Build discharge commands. Slot-time register only written when entering DISCHARGE."""
    mode_label = "EXPORT" if export else "DEMAND"
    logger.info("Inverter → DISCHARGE/%s %.2f kW (register %d)", mode_label, discharge_kw, limit)
    reqs = []
    if last_mode not in ("DISCHARGE_DEMAND", "DISCHARGE_EXPORT"):
        # Entering DISCHARGE from a non-discharge mode — set slot window.
        reqs += [*commands.set_discharge_slot_1(TimeSlot(start=dt_time(0, 0), end=dt_time(23, 59)))]
    reqs += [
        *commands.set_battery_discharge_limit(limit),
    ]
    if export:
        reqs += [*commands.set_discharge_mode_max_power()]
    else:
        reqs += [*commands.set_discharge_mode_to_match_demand()]
    reqs += [
        *commands.enable_discharge(),
        *commands.disable_charge(),
    ]
    return reqs


def _cmds_eco() -> list:
    logger.info("Inverter → ECO (dynamic mode)")
    return [
        *commands.set_discharge_mode_to_match_demand(),
        *commands.set_shallow_charge(4),
        *commands.disable_discharge(),
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
