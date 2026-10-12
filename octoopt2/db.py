"""SQLite database setup and schema."""
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


SCHEMA = """
-- Half-hourly Octopus Agile prices (buy and sell)
CREATE TABLE IF NOT EXISTS prices (
    slot_start  TEXT NOT NULL,   -- ISO8601 UTC, e.g. "2024-01-15T14:00:00+00:00"
    buy_gbp_kwh  REAL NOT NULL,
    -- NULL = outgoing (export) rate not yet published. Distinct from a genuine
    -- 0.0 rate: callers default a NULL pessimistically (mirror the import price)
    -- rather than treating export as a free energy sink. See data/octopus.py.
    sell_gbp_kwh REAL,
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

-- Ground-truth DHW (Ecodan) tank state, sampled each slot boundary from
-- MELCloud. Lets a future load-model refit replace the planned dhw_on regressor
-- with confirmed heating activity (tank below target / status heating).
CREATE TABLE IF NOT EXISTS dhw_readings (
    recorded_at               TEXT NOT NULL PRIMARY KEY,
    operation_mode            TEXT,   -- force_hot_water | auto | ...
    tank_temperature_c        REAL,
    target_tank_temperature_c REAL,
    status                    TEXT
);

-- Last inverter command successfully applied (singleton row, id always = 1).
-- Used to skip redundant register writes and to omit slot-time commands when
-- the mode has not changed.
CREATE TABLE IF NOT EXISTS inverter_last_command (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    applied_at     TEXT NOT NULL,
    mode           TEXT NOT NULL,       -- CHARGE | DISCHARGE_EXPORT | ECO
    power_register INTEGER NOT NULL,    -- charge/discharge limit register (1-50); 0 for ECO
    total_writes   INTEGER NOT NULL DEFAULT 0  -- cumulative lifetime register writes sent
);

-- Last successful fetch/write per data source (one row per feed, upserted on
-- success). Freshness of a source is *when we last managed to pull it*, which
-- is distinct from how far behind the data itself runs: Octopus publishes smart
-- meter consumption 1-2 days late, so a perfectly healthy fetch still yields
-- day-old slots. Keeping the two apart is what lets a dead fetcher be told from
-- a lagging upstream. See metrics.py for the gauges built on this.
-- Audit trail of every inverter register-write attempt (success or failure).
-- inverter_last_command only keeps the latest state; this table lets the
-- backtest attribute plan-vs-actual gaps to what was actually commanded.
CREATE TABLE IF NOT EXISTS inverter_commands (
    applied_at     TEXT NOT NULL PRIMARY KEY,
    mode           TEXT NOT NULL,       -- CHARGE | DISCHARGE_EXPORT | ECO
    power_register INTEGER NOT NULL,
    n_writes       INTEGER NOT NULL,
    ok             INTEGER NOT NULL,    -- 1 = all writes acknowledged
    error          TEXT,
    slot_start     TEXT,                -- slot the decision was for (NULL for fallback/away)
    trigger        TEXT                 -- fast | slow | fallback | away
);

CREATE TABLE IF NOT EXISTS feed_fetches (
    feed        TEXT NOT NULL,   -- see FEEDS below
    fetched_at  TEXT NOT NULL,   -- ISO8601 UTC of the last successful fetch
    PRIMARY KEY (feed)
);

-- Every schedule the optimizer ever produced, one row per (slot, run). The
-- `schedule` table above is only the *latest* plan per slot; this keeps the
-- superseded ones so plan-vs-plan drift and plan-vs-actual gaps can be studied
-- after the fact. Written in the same transaction as `schedule`.
CREATE TABLE IF NOT EXISTS schedule_history (
    slot_start            TEXT NOT NULL,
    optimized_at          TEXT NOT NULL,
    battery_charge_kwh    REAL NOT NULL,
    battery_discharge_kwh REAL NOT NULL,
    grid_import_kwh       REAL NOT NULL,
    grid_export_kwh       REAL NOT NULL,
    dhw_on                INTEGER NOT NULL,
    predicted_load_kwh    REAL NOT NULL,
    predicted_solar_kwh   REAL NOT NULL,
    buy_gbp_kwh           REAL NOT NULL,
    sell_gbp_kwh          REAL NOT NULL,
    soc_start_kwh         REAL,
    soc_end_kwh           REAL,
    curtailed_kwh         REAL,
    PRIMARY KEY (slot_start, optimized_at)
);

-- One row per optimizer invocation (success, error or fallback), with the
-- solver outcome and the load-model fit it ran against. Lets the solver's
-- health and the model's drift be charted without grepping logs.
CREATE TABLE IF NOT EXISTS optimizer_runs (
    run_at         TEXT NOT NULL PRIMARY KEY,
    status         TEXT,       -- solver status (Optimal, ...) | error | fallback
    objective_gbp  REAL,       -- full objective incl. virtual terms
    grid_cost_gbp  REAL,       -- money-only part of the objective
    solve_seconds  REAL,
    horizon_slots  INTEGER,
    horizon_start  TEXT,
    soc_start_kwh  REAL,
    alpha          REAL,       -- load model temperature coefficient (kWh/°C)
    beta           REAL,       -- load model DHW coefficient (kWh/slot)
    ref_temp       REAL,       -- load model reference temperature (°C)
    n_train_slots  INTEGER,    -- slots the load model was fitted on
    manage_dhw     INTEGER,
    error          TEXT        -- exception text / fallback reason
);

-- Every Solcast forecast ever fetched, keyed by (slot, fetch time). The
-- solar_forecast table keeps only the newest value per slot; this one lets
-- forecast skill be measured as a function of lead time.
CREATE TABLE IF NOT EXISTS solar_forecast_history (
    slot_start       TEXT NOT NULL,
    fetched_at       TEXT NOT NULL,
    pv_estimate_kwh  REAL NOT NULL,
    pv_estimate_p10  REAL,
    pv_estimate_p90  REAL,
    PRIMARY KEY (slot_start, fetched_at)
);

-- Same idea for Open-Meteo: every forecast fetched, by lead time.
CREATE TABLE IF NOT EXISTS weather_forecast_history (
    slot_start          TEXT NOT NULL,
    fetched_at          TEXT NOT NULL,
    temperature_c       REAL,
    cloud_cover_pct     REAL,
    wind_speed_ms       REAL,
    humidity_pct        REAL,
    precipitation_mm    REAL,
    PRIMARY KEY (slot_start, fetched_at)
);

-- Observed (archive/reanalysis) weather, hourly, from the Open-Meteo archive
-- backfill. Kept apart from weather_forecast so the load model can train on
-- what actually happened rather than on whatever forecast was last stored.
CREATE TABLE IF NOT EXISTS weather_observed (
    slot_start          TEXT NOT NULL PRIMARY KEY,
    temperature_c       REAL,
    cloud_cover_pct     REAL,
    wind_speed_ms       REAL,
    humidity_pct        REAL,
    precipitation_mm    REAL,
    fetched_at          TEXT NOT NULL
);

-- Daily standing charge for the import tariff (pence/day inc. VAT), one row
-- per published validity period. Needed to turn per-kWh costs into a bill.
CREATE TABLE IF NOT EXISTS standing_charges (
    tariff_code          TEXT NOT NULL,
    valid_from           TEXT NOT NULL,
    valid_to             TEXT,            -- NULL = open-ended
    value_inc_vat_p_day  REAL NOT NULL,
    fetched_at           TEXT NOT NULL,
    PRIMARY KEY (tariff_code, valid_from)
);
"""


# Every persisted data source, in display order. Both the Prometheus collector
# (metrics.py) and the dashboard status endpoint (status.py) render this list —
# keep it here so the two cannot drift apart.
#
# coverage_table is the table whose slot_start says how far ahead/behind the
# data itself runs, or None where that is the same thing as the fetch time
# (inverter and dhw stamp each row with the moment it was read).
FEEDS: tuple[tuple[str, str | None], ...] = (
    ("solar", "solar_forecast"),
    ("weather", "weather_forecast"),
    ("consumption", "consumption"),
    ("prices", "prices"),
    ("inverter", None),
    ("dhw", None),
    ("standing", None),   # import-tariff standing charge (daily TTL)
)


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

    # prices.sell_gbp_kwh was originally NOT NULL with a 0.0 placeholder for the
    # not-yet-published outgoing rate. That placeholder is indistinguishable from
    # a genuine 0.0 export price, so the optimizer treated unpublished slots as a
    # free export sink. Make the column nullable (NULL = "unknown") so callers
    # can default it pessimistically. SQLite can't drop NOT NULL via ALTER, so
    # rebuild the table. Idempotent: only runs while the column is still NOT NULL.
    cols = conn.execute("PRAGMA table_info(prices)").fetchall()
    sell_col = next((c for c in cols if c[1] == "sell_gbp_kwh"), None)
    if sell_col is not None and sell_col[3] == 1:  # c[3] == notnull flag
        conn.executescript(
            """
            CREATE TABLE prices_new (
                slot_start  TEXT NOT NULL,
                buy_gbp_kwh  REAL NOT NULL,
                sell_gbp_kwh REAL,
                PRIMARY KEY (slot_start)
            );
            INSERT INTO prices_new (slot_start, buy_gbp_kwh, sell_gbp_kwh)
                SELECT slot_start, buy_gbp_kwh, sell_gbp_kwh FROM prices;
            DROP TABLE prices;
            ALTER TABLE prices_new RENAME TO prices;
            """
        )

    # Nullable columns added later. Each ALTER is idempotent via try/except
    # ("duplicate column name"), so init_db can be re-run freely.
    for table, column, decl in _ADDED_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")  # noqa: S608 - fixed names
        except sqlite3.OperationalError:
            pass  # column already exists


# (table, column, type) for every nullable column bolted on after the initial
# CREATE TABLE. All optional: readers must tolerate NULL in older rows.
_ADDED_COLUMNS: tuple[tuple[str, str, str], ...] = (
    # inverter_readings — extra registers captured alongside the power flows
    ("inverter_readings", "temp_battery_c", "REAL"),
    ("inverter_readings", "v_battery", "REAL"),
    ("inverter_readings", "i_battery", "REAL"),
    ("inverter_readings", "e_pv_total", "REAL"),                 # lifetime kWh counters
    ("inverter_readings", "e_grid_in_total", "REAL"),
    ("inverter_readings", "e_grid_out_total", "REAL"),
    ("inverter_readings", "e_battery_charge_total", "REAL"),
    ("inverter_readings", "e_battery_discharge_total", "REAL"),
    ("inverter_readings", "e_inverter_in_total", "REAL"),
    ("inverter_readings", "e_inverter_out_total", "REAL"),
    ("inverter_readings", "num_cycles", "INTEGER"),              # battery 0 cycle count
    ("inverter_readings", "hr_enable_charge", "INTEGER"),        # holding registers as read
    ("inverter_readings", "hr_enable_discharge", "INTEGER"),
    ("inverter_readings", "hr_soc_reserve", "INTEGER"),
    ("inverter_readings", "hr_power_reserve", "INTEGER"),
    ("inverter_readings", "hr_charge_limit", "INTEGER"),
    ("inverter_readings", "hr_discharge_limit", "INTEGER"),
    ("inverter_readings", "hr_eco_mode", "INTEGER"),
    # actuals — battery flows and SoC endpoints per slot
    ("actuals", "battery_charge_kwh", "REAL"),
    ("actuals", "battery_discharge_kwh", "REAL"),
    ("actuals", "soc_start_pct", "REAL"),
    ("actuals", "soc_end_pct", "REAL"),
    ("actuals", "n_samples", "INTEGER"),   # readings aggregated; NULL = pre-upgrade row
    ("actuals", "curtailed_kwh", "REAL"),
    # dhw_readings — wider Ecodan snapshot
    ("dhw_readings", "outside_temperature_c", "REAL"),
    ("dhw_readings", "flow_temperature_c", "REAL"),
    ("dhw_readings", "return_temperature_c", "REAL"),
    ("dhw_readings", "target_flow_temperature_c", "REAL"),
    ("dhw_readings", "room_temperature_c", "REAL"),
    ("dhw_readings", "room_target_c", "REAL"),
    ("dhw_readings", "power", "INTEGER"),
    # prices — ex-VAT rates alongside the inc-VAT ones
    ("prices", "buy_exc_vat_gbp_kwh", "REAL"),
    ("prices", "sell_exc_vat_gbp_kwh", "REAL"),
)


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


def record_fetch(db_path: str, feed: str, now: datetime | None = None) -> None:
    """Stamp a successful fetch of `feed` in feed_fetches.

    Call this only after the fetched data has been written, and never from a
    failure path: the whole point of the stamp is that it stops advancing when
    a source goes quiet. Best-effort — a bookkeeping write must not take down
    the fetch that just succeeded.
    """
    ts = (now or datetime.now(timezone.utc)).isoformat()
    try:
        with get_conn(db_path) as conn:
            conn.execute(
                """
                INSERT INTO feed_fetches (feed, fetched_at)
                VALUES (?, ?)
                ON CONFLICT(feed) DO UPDATE SET fetched_at = excluded.fetched_at
                """,
                (feed, ts),
            )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Could not record fetch for feed %s: %s", feed, exc)


def age_seconds(iso_ts: str | None, now: datetime) -> float | None:
    """Seconds between an ISO8601 timestamp and now. None if unparseable/missing.

    Negative when the timestamp is in the future — which is the normal case for
    the coverage of a forecast feed, so callers must not clamp it.
    """
    if not iso_ts:
        return None
    try:
        ts = datetime.fromisoformat(iso_ts)
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (now - ts).total_seconds()


def feed_freshness(db_path: str, now: datetime) -> dict[str, dict]:
    """Two independent freshness numbers per feed, keyed by feed name.

    fetch_age_s    — seconds since we last successfully refreshed the source.
                     Uniform across feeds, so one alerting threshold fits all;
                     this is the number that says whether a feed has gone quiet.
    coverage_lag_s — seconds between now and the newest slot the feed covers.
                     Negative means coverage runs into the future (forecasts,
                     published prices). Large positive is normal for Octopus
                     consumption, which lands 1-2 days late. None where the feed
                     has no slot dimension.

    Either value is None when unknown. Conflating the two is what made a stalled
    fetcher indistinguishable from an upstream that simply publishes late.
    """
    out: dict[str, dict] = {}
    with get_conn(db_path) as conn:
        try:
            stamps = {
                r["feed"]: r["fetched_at"]
                for r in conn.execute("SELECT feed, fetched_at FROM feed_fetches")
            }
        except sqlite3.Error as exc:  # pragma: no cover - defensive
            logger.debug("feed_fetches unavailable: %s", exc)
            stamps = {}

        for feed, coverage_table in FEEDS:
            coverage = None
            if coverage_table is not None:
                try:
                    row = conn.execute(
                        f"SELECT MAX(slot_start) AS ts FROM {coverage_table}"  # noqa: S608 - fixed table names from FEEDS
                    ).fetchone()
                    coverage = age_seconds(row["ts"] if row else None, now)
                except sqlite3.Error as exc:  # pragma: no cover - defensive
                    logger.debug("coverage for feed %s unavailable: %s", feed, exc)
            out[feed] = {
                "fetch_age_s": age_seconds(stamps.get(feed), now),
                "coverage_lag_s": coverage,
            }
    return out
