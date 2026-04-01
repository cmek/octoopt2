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
from ..db import get_conn
from ..givenergy_modbus_async.client.client import Client
from ..givenergy_modbus_async.client import commands

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
        p_batt = float(inv.p_battery)

        return InverterReading(
            recorded_at=datetime.now(timezone.utc),
            soc_pct=float(inv.battery_percent),
            solar_w=float(inv.p_pv1) + float(inv.p_pv2),
            grid_import_w=max(0.0, -p_grid),
            grid_export_w=max(0.0, p_grid),
            battery_charge_w=max(0.0, p_batt),
            battery_discharge_w=max(0.0, -p_batt),
            load_w=float(inv.p_load_demand),
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
    """Persist an inverter reading to the inverter_readings table."""
    with get_conn(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO inverter_readings
                (recorded_at, soc_pct, solar_w, grid_import_w, grid_export_w,
                 battery_charge_w, battery_discharge_w, load_w)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                reading.recorded_at.isoformat(),
                reading.soc_pct,
                reading.solar_w,
                reading.grid_import_w,
                reading.grid_export_w,
                reading.battery_charge_w,
                reading.battery_discharge_w,
                reading.load_w,
            ),
        )


def read_and_store(config: GivEnergyConfig, db_path: str) -> InverterReading:
    """Read inverter state and persist it. Returns the reading."""
    reading = read_inverter(config)
    store_reading(reading, db_path)
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
            """
            SELECT recorded_at, soc_pct, solar_w, grid_import_w, grid_export_w,
                   battery_charge_w, battery_discharge_w, load_w
            FROM inverter_readings
            ORDER BY recorded_at DESC
            LIMIT 1
            """
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
    )
