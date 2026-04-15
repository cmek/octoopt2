"""SQLite database setup and schema."""
import sqlite3
from contextlib import contextmanager
from pathlib import Path


SCHEMA = """
-- Half-hourly Octopus Agile prices (buy and sell)
CREATE TABLE IF NOT EXISTS prices (
    slot_start  TEXT NOT NULL,   -- ISO8601 UTC, e.g. "2024-01-15T14:00:00+00:00"
    buy_gbp_kwh  REAL NOT NULL,
    sell_gbp_kwh REAL NOT NULL,
    PRIMARY KEY (slot_start)
);

-- Solcast solar generation forecast (30-min slots)
CREATE TABLE IF NOT EXISTS solar_forecast (
    slot_start       TEXT NOT NULL,
    pv_estimate_kwh  REAL NOT NULL,  -- 50th percentile
    pv_estimate_p10  REAL,           -- 10th percentile (pessimistic)
    pv_estimate_p90  REAL,           -- 90th percentile (optimistic)
    fetched_at       TEXT NOT NULL,
    PRIMARY KEY (slot_start)
);

-- Solcast tuned actuals (retrospective corrected solar generation)
CREATE TABLE IF NOT EXISTS solar_actuals (
    slot_start      TEXT NOT NULL,
    pv_actual_kwh   REAL NOT NULL,
    fetched_at      TEXT NOT NULL,
    PRIMARY KEY (slot_start)
);

-- Open-Meteo weather forecast (15-min intervals)
CREATE TABLE IF NOT EXISTS weather_forecast (
    slot_start          TEXT NOT NULL,
    temperature_c       REAL,
    cloud_cover_pct     REAL,
    wind_speed_ms       REAL,
    humidity_pct        REAL,
    precipitation_mm    REAL,
    fetched_at          TEXT NOT NULL,
    PRIMARY KEY (slot_start)
);

-- Octopus half-hourly consumption (from smart meter)
CREATE TABLE IF NOT EXISTS consumption (
    slot_start      TEXT NOT NULL,
    consumption_kwh REAL NOT NULL,
    PRIMARY KEY (slot_start)
);

-- Inverter state readings (polled every 5 minutes)
CREATE TABLE IF NOT EXISTS inverter_readings (
    recorded_at           TEXT NOT NULL PRIMARY KEY,
    soc_pct               REAL NOT NULL,  -- battery state of charge %
    solar_w               REAL NOT NULL,  -- current solar generation (W)
    grid_import_w         REAL NOT NULL,  -- power imported from grid (W, >= 0)
    grid_export_w         REAL NOT NULL,  -- power exported to grid (W, >= 0)
    battery_charge_w      REAL NOT NULL,  -- power into battery (W, >= 0)
    battery_discharge_w   REAL NOT NULL,  -- power out of battery (W, >= 0)
    load_w                REAL NOT NULL   -- home consumption (W)
);

-- Optimizer schedule: planned actions per half-hour slot
CREATE TABLE IF NOT EXISTS schedule (
    slot_start           TEXT NOT NULL PRIMARY KEY,
    battery_charge_kwh   REAL NOT NULL,   -- planned battery charge this slot (kWh)
    battery_discharge_kwh REAL NOT NULL,  -- planned battery discharge this slot (kWh)
    grid_import_kwh      REAL NOT NULL,   -- planned grid import (kWh)
    grid_export_kwh      REAL NOT NULL,   -- planned grid export (kWh)
    dhw_on               INTEGER NOT NULL, -- 1 = DHW heating enabled this slot
    predicted_load_kwh   REAL NOT NULL,
    predicted_solar_kwh  REAL NOT NULL,
    buy_gbp_kwh          REAL NOT NULL,
    sell_gbp_kwh         REAL NOT NULL,
    optimized_at         TEXT NOT NULL    -- when this schedule was last computed
);

-- Actual outcomes per slot (filled in retrospectively)
CREATE TABLE IF NOT EXISTS actuals (
    slot_start       TEXT NOT NULL PRIMARY KEY,
    grid_import_kwh  REAL,
    grid_export_kwh  REAL,
    solar_kwh        REAL,
    load_kwh         REAL,
    cost_gbp         REAL   -- negative = earned money
);

-- Last inverter command successfully applied (singleton row, id always = 1).
-- Used to skip redundant register writes and to omit slot-time commands when
-- the mode has not changed.
CREATE TABLE IF NOT EXISTS inverter_last_command (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    applied_at     TEXT NOT NULL,
    mode           TEXT NOT NULL,       -- CHARGE | DISCHARGE_DEMAND | DISCHARGE_EXPORT | ECO
    power_register INTEGER NOT NULL,    -- charge/discharge limit register (1-50); 0 for ECO
    total_writes   INTEGER NOT NULL DEFAULT 0  -- cumulative lifetime register writes sent
);
"""


def init_db(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA)
        _migrate(conn)
        conn.commit()


def _migrate(conn: sqlite3.Connection) -> None:
    """Apply schema changes that can't be expressed as CREATE TABLE IF NOT EXISTS."""
    # total_writes added after initial release of inverter_last_command
    try:
        conn.execute(
            "ALTER TABLE inverter_last_command ADD COLUMN total_writes INTEGER NOT NULL DEFAULT 0"
        )
    except sqlite3.OperationalError:
        pass  # column already exists


@contextmanager
def get_conn(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
