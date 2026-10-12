"""Read current inverter state from GivEnergy via local Modbus TCP.

Uses the vendored givenergy_modbus_async client from GivTCP, which supports
firmware 912+ / battery firmware 3015+ that broke the archived givenergy-modbus
library. The async client is wrapped synchronously via asyncio.run().
"""
import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from ..config import GivEnergyConfig
from ..db import get_conn, record_fetch
from ..givenergy_modbus_async.client.client import Client

logger = logging.getLogger(__name__)

_RETRY_ATTEMPTS = 3
_RETRY_DELAY_S = 2


@dataclass(frozen=True)
class InverterReading:
    recorded_at: datetime      # UTC
    soc_pct: float             # battery state of charge (%)
    solar_w: float             # total solar generation (W), sum of PV1+PV2
    grid_import_w: float       # power drawn from grid (W, always >= 0)
    grid_export_w: float       # power sent to grid (W, always >= 0)
    battery_charge_w: float    # power flowing into battery (W, always >= 0)
    battery_discharge_w: float # power flowing out of battery (W, always >= 0)
    load_w: float              # home consumption (W)
    # ── Optional extras (None when the register was not read / unsupported) ──
    temp_battery_c: float | None = None
    v_battery: float | None = None
    i_battery: float | None = None             # signed; sign convention as p_battery
    e_pv_total: float | None = None            # lifetime energy counters (kWh)
    e_grid_in_total: float | None = None
    e_grid_out_total: float | None = None
    e_battery_charge_total: float | None = None
    e_battery_discharge_total: float | None = None
    e_inverter_in_total: float | None = None
    e_inverter_out_total: float | None = None
    num_cycles: int | None = None              # battery 0 cycle counter
    # Holding registers as the inverter reports them — the control state we
    # believe we set, so plan-vs-actual gaps can be tied to the real mode.
    hr_enable_charge: int | None = None
    hr_enable_discharge: int | None = None
    hr_soc_reserve: int | None = None
    hr_power_reserve: int | None = None
    hr_charge_limit: int | None = None
    hr_discharge_limit: int | None = None
    hr_eco_mode: int | None = None


# InverterReading fields beyond the original eight, in inverter_readings column
# order. Shared by store_reading / get_latest_reading so the two cannot drift.
_EXTRA_FIELDS = (
    "temp_battery_c", "v_battery", "i_battery",
    "e_pv_total", "e_grid_in_total", "e_grid_out_total",
    "e_battery_charge_total", "e_battery_discharge_total",
    "e_inverter_in_total", "e_inverter_out_total",
    "num_cycles",
    "hr_enable_charge", "hr_enable_discharge", "hr_soc_reserve", "hr_power_reserve",
    "hr_charge_limit", "hr_discharge_limit", "hr_eco_mode",
)


def _opt(obj, name: str, cast=float):
    """getattr(obj, name) cast to `cast`, or None if missing/unconvertible.

    A register absent from this firmware's map must never break a read.
    """
    try:
        v = getattr(obj, name, None)
        return None if v is None else cast(v)
    except Exception:
        return None


async def _read_async(config: GivEnergyConfig) -> InverterReading:
    client = Client(host=config.host, port=config.port)
    await client.connect()
    try:
        await client.refresh_plant(
            full_refresh=True,
            number_batteries=config.number_batteries,
        )
        inv = client.plant.inverter

        p_grid = float(inv.p_grid_out)
        # p_battery (IR 52) is positive when DISCHARGING on this inverter —
        # the opposite convention to p_grid_out. Confirmed by energy balance:
        # solar + battery_discharge + import == load + export (+ losses).
        p_batt = float(inv.p_battery)

        batteries = getattr(client.plant, "batteries", None) or []
        batt0 = batteries[0] if batteries else None

        return InverterReading(
            recorded_at=datetime.now(timezone.utc),
            soc_pct=float(inv.battery_percent),
            solar_w=float(inv.p_pv1) + float(inv.p_pv2),
            grid_import_w=max(0.0, -p_grid),
            grid_export_w=max(0.0, p_grid),
            battery_charge_w=max(0.0, -p_batt),
            battery_discharge_w=max(0.0, p_batt),
            load_w=float(inv.p_load_demand),
            # Extras: all optional, getattr-guarded (see _opt).
            temp_battery_c=_opt(inv, "temp_battery"),
            v_battery=_opt(inv, "v_battery"),
            i_battery=_opt(inv, "i_battery"),
            e_pv_total=_opt(inv, "e_pv_total"),
            e_grid_in_total=_opt(inv, "e_grid_in_total"),
            e_grid_out_total=_opt(inv, "e_grid_out_total"),
            e_battery_charge_total=_opt(inv, "e_battery_charge_total"),
            e_battery_discharge_total=_opt(inv, "e_battery_discharge_total"),
            e_inverter_in_total=_opt(inv, "e_inverter_in_total"),
            e_inverter_out_total=_opt(inv, "e_inverter_out_total"),
            num_cycles=_opt(batt0, "num_cycles", int) if batt0 is not None else None,
            # Enable is an IntEnum (0/1, 3 = unknown) — int() keeps the raw value.
            hr_enable_charge=_opt(inv, "enable_charge", int),
            hr_enable_discharge=_opt(inv, "enable_discharge", int),
            hr_soc_reserve=_opt(inv, "battery_soc_reserve", int),
            hr_power_reserve=_opt(inv, "battery_discharge_min_power_reserve", int),
            hr_charge_limit=_opt(inv, "battery_charge_limit", int),
            hr_discharge_limit=_opt(inv, "battery_discharge_limit", int),
            hr_eco_mode=_opt(inv, "eco_mode", int),
        )
    finally:
        await client.close()


def read_inverter(config: GivEnergyConfig) -> InverterReading:
    """Connect to inverter and return a point-in-time snapshot.

    Retries up to _RETRY_ATTEMPTS times on failure.
    """
    last_exc: Exception | None = None
    for attempt in range(1, _RETRY_ATTEMPTS + 1):
        try:
            return asyncio.run(_read_async(config))
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Inverter read attempt %d/%d failed: %s",
                attempt, _RETRY_ATTEMPTS, exc,
            )
            if attempt < _RETRY_ATTEMPTS:
                time.sleep(_RETRY_DELAY_S)

    raise RuntimeError(
        f"Failed to read inverter after {_RETRY_ATTEMPTS} attempts"
    ) from last_exc


def store_reading(reading: InverterReading, db_path: str) -> None:
    """Persist an inverter reading to the inverter_readings table.

    The extra (optional) fields are written as NULL when unset.
    """
    extras = tuple(getattr(reading, f, None) for f in _EXTRA_FIELDS)
    with get_conn(db_path) as conn:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO inverter_readings
                (recorded_at, soc_pct, solar_w, grid_import_w, grid_export_w,
                 battery_charge_w, battery_discharge_w, load_w,
                 {", ".join(_EXTRA_FIELDS)})
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, {", ".join("?" for _ in _EXTRA_FIELDS)})
            """,  # noqa: S608 - column names are a fixed module constant
            (
                reading.recorded_at.isoformat(),
                reading.soc_pct,
                reading.solar_w,
                reading.grid_import_w,
                reading.grid_export_w,
                reading.battery_charge_w,
                reading.battery_discharge_w,
                reading.load_w,
                *extras,
            ),
        )


def read_and_store(config: GivEnergyConfig, db_path: str) -> InverterReading:
    """Read inverter state and persist it. Returns the reading."""
    reading = read_inverter(config)
    store_reading(reading, db_path)
    # Stamped here and not in read_inverter(): the daemon's 30 s poller stores
    # its readings too (daemon._poll_inverter_loop) but does not stamp, so this
    # feed's age tracks the scheduler's own live read — a scheduler that has
    # been falling back to stale stored readings (see scheduler._read_inverter)
    # still shows up even while the poller keeps the table growing.
    record_fetch(db_path, "inverter")
    logger.info(
        "Inverter: SoC=%.1f%% solar=%.0fW import=%.0fW export=%.0fW "
        "batt_charge=%.0fW batt_discharge=%.0fW load=%.0fW",
        reading.soc_pct, reading.solar_w,
        reading.grid_import_w, reading.grid_export_w,
        reading.battery_charge_w, reading.battery_discharge_w,
        reading.load_w,
    )
    return reading


def get_latest_reading(db_path: str) -> InverterReading | None:
    """Return the most recent stored inverter reading, or None if none exist."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            f"""
            SELECT recorded_at, soc_pct, solar_w, grid_import_w, grid_export_w,
                   battery_charge_w, battery_discharge_w, load_w,
                   {", ".join(_EXTRA_FIELDS)}
            FROM inverter_readings
            ORDER BY recorded_at DESC
            LIMIT 1
            """  # noqa: S608 - column names are a fixed module constant
        ).fetchone()
    if row is None:
        return None
    return InverterReading(
        recorded_at=datetime.fromisoformat(row["recorded_at"]),
        soc_pct=row["soc_pct"],
        solar_w=row["solar_w"],
        grid_import_w=row["grid_import_w"],
        grid_export_w=row["grid_export_w"],
        battery_charge_w=row["battery_charge_w"],
        battery_discharge_w=row["battery_discharge_w"],
        load_w=row["load_w"],
        **{f: row[f] for f in _EXTRA_FIELDS},
    )
