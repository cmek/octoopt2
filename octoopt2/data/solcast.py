"""Fetch Solcast solar generation forecasts and estimated actuals.

Rate limit: 10 API calls/day on the free hobbyist tier.
Both fetches are guarded by a staleness check — data is only re-fetched when
older than the configured TTL, so the scheduler can call these safely on every
5-minute cron run without burning the daily quota.
"""
import logging
from datetime import datetime, timedelta, timezone

import requests

from ..config import SolcastConfig
from ..db import get_conn

logger = logging.getLogger(__name__)

SOLCAST_API = "https://api.solcast.com.au"

# How old data can be before we re-fetch. Forecasts update every ~15 min on
# Solcast's end, but given 10 calls/day we refresh at most every 2 hours.
FORECAST_TTL_HOURS = 2
# Estimated actuals cover the past 7 days and change slowly — once per day is fine.
ACTUALS_TTL_HOURS = 24


def _get(url: str, api_key: str, params: dict | None = None) -> dict:
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {api_key}"},
        params=params or {},
        timeout=15,
    )
    resp.raise_for_status()
    remaining = resp.headers.get("x-rate-limit-remaining")
    if remaining is not None:
        logger.info("Solcast rate limit remaining today: %s", remaining)
    return resp.json()


def _period_end_to_slot_start(period_end: str) -> str:
    """Convert Solcast period_end to slot_start (subtract 30 min), return UTC ISO."""
    dt = datetime.fromisoformat(period_end.replace("Z", "+00:00"))
    return (dt - timedelta(minutes=30)).astimezone(timezone.utc).isoformat()


def _forecast_staleness(db_path: str, resource_id: str) -> float | None:
    """Return age in hours of the most recent forecast fetch, or None if no data."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT MAX(fetched_at) FROM solar_forecast",
        ).fetchone()[0]
    if row is None:
        return None
    fetched_at = datetime.fromisoformat(row)
    return (datetime.now(timezone.utc) - fetched_at).total_seconds() / 3600


def _actuals_staleness(db_path: str) -> float | None:
    """Return age in hours of the most recent actuals fetch, or None if no data."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT MAX(fetched_at) FROM solar_actuals",
        ).fetchone()[0]
    if row is None:
        return None
    fetched_at = datetime.fromisoformat(row)
    return (datetime.now(timezone.utc) - fetched_at).total_seconds() / 3600


def fetch_and_store_forecast(
    config: SolcastConfig,
    db_path: str,
    hours: int = 48,
    force: bool = False,
) -> int:
    """Fetch solar generation forecast and upsert into solar_forecast table.

    Skips the API call if existing data is fresher than FORECAST_TTL_HOURS,
    unless force=True. Returns number of slots stored (0 if skipped).

    Solcast returns power in kW — converted to energy (kWh) for 30-min slots
    by multiplying by 0.5.
    """
    staleness = _forecast_staleness(db_path, config.resource_id)
    if not force and staleness is not None and staleness < FORECAST_TTL_HOURS:
        logger.debug(
            "Forecast data is %.1fh old (TTL %dh), skipping fetch",
            staleness,
            FORECAST_TTL_HOURS,
        )
        return 0

    logger.info("Fetching Solcast forecast (%d hours ahead)", hours)
    data = _get(
        f"{SOLCAST_API}/rooftop_sites/{config.resource_id}/forecasts",
        api_key=config.api_key,
        params={"hours": hours, "format": "json"},
    )

    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for item in data.get("forecasts", []):
        slot_start = _period_end_to_slot_start(item["period_end"])
        rows.append((
            slot_start,
            item["pv_estimate"] * 0.5,     # kW → kWh for 30-min slot
            item.get("pv_estimate10", item["pv_estimate"]) * 0.5,
            item.get("pv_estimate90", item["pv_estimate"]) * 0.5,
            fetched_at,
        ))

    if not rows:
        logger.warning("No forecast data returned from Solcast")
        return 0

    with get_conn(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO solar_forecast
                (slot_start, pv_estimate_kwh, pv_estimate_p10, pv_estimate_p90, fetched_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(slot_start) DO UPDATE SET
                pv_estimate_kwh = excluded.pv_estimate_kwh,
                pv_estimate_p10 = excluded.pv_estimate_p10,
                pv_estimate_p90 = excluded.pv_estimate_p90,
                fetched_at      = excluded.fetched_at
            """,
            rows,
        )

    logger.info("Stored %d forecast slots", len(rows))
    return len(rows)


def fetch_and_store_actuals(
    config: SolcastConfig,
    db_path: str,
    force: bool = False,
) -> int:
    """Fetch estimated actuals (past ~7 days) and upsert into solar_actuals table.

    Skips the API call if existing data is fresher than ACTUALS_TTL_HOURS,
    unless force=True. Returns number of slots stored (0 if skipped).
    """
    staleness = _actuals_staleness(db_path)
    if not force and staleness is not None and staleness < ACTUALS_TTL_HOURS:
        logger.debug(
            "Actuals data is %.1fh old (TTL %dh), skipping fetch",
            staleness,
            ACTUALS_TTL_HOURS,
        )
        return 0

    logger.info("Fetching Solcast estimated actuals")
    data = _get(
        f"{SOLCAST_API}/rooftop_sites/{config.resource_id}/estimated_actuals",
        api_key=config.api_key,
        params={"format": "json"},
    )

    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for item in data.get("estimated_actuals", []):
        slot_start = _period_end_to_slot_start(item["period_end"])
        rows.append((
            slot_start,
            item["pv_estimate"] * 0.5,  # kW → kWh
            fetched_at,
        ))

    if not rows:
        logger.warning("No estimated actuals returned from Solcast")
        return 0

    with get_conn(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO solar_actuals (slot_start, pv_actual_kwh, fetched_at)
            VALUES (?, ?, ?)
            ON CONFLICT(slot_start) DO UPDATE SET
                pv_actual_kwh = excluded.pv_actual_kwh,
                fetched_at    = excluded.fetched_at
            """,
            rows,
        )

    logger.info("Stored %d estimated actual slots", len(rows))
    return len(rows)


def get_forecast(
    db_path: str,
    from_dt: datetime,
    to_dt: datetime,
) -> list[dict]:
    """Return stored forecast for slots within [from_dt, to_dt).

    Returns list of dicts with keys: slot_start (datetime, UTC),
    pv_estimate_kwh, pv_estimate_p10, pv_estimate_p90. Sorted ascending.
    """
    from_str = from_dt.astimezone(timezone.utc).isoformat()
    to_str = to_dt.astimezone(timezone.utc).isoformat()
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT slot_start, pv_estimate_kwh, pv_estimate_p10, pv_estimate_p90
            FROM solar_forecast
            WHERE slot_start >= ? AND slot_start < ?
            ORDER BY slot_start
            """,
            (from_str, to_str),
        ).fetchall()
    return [
        {
            "slot_start": datetime.fromisoformat(r["slot_start"]),
            "pv_estimate_kwh": r["pv_estimate_kwh"],
            "pv_estimate_p10": r["pv_estimate_p10"],
            "pv_estimate_p90": r["pv_estimate_p90"],
        }
        for r in rows
    ]
