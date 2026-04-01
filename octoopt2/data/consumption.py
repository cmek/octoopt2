"""Fetch half-hourly electricity consumption from Octopus smart meter API."""
import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from ..config import OctopusConfig
from ..db import get_conn

logger = logging.getLogger(__name__)

OCTOPUS_API = "https://api.octopus.energy/v1"
LONDON = ZoneInfo("Europe/London")


def _fetch_consumption_page(
    mpan: str,
    serial: str,
    api_key: str,
    period_from: datetime,
    period_to: datetime,
) -> list[dict]:
    """Fetch all consumption records for a given window, handling pagination."""
    url = (
        f"{OCTOPUS_API}/electricity-meter-points/{mpan}"
        f"/meters/{serial}/consumption/"
    )
    params = {
        "period_from": period_from.strftime("%Y-%m-%dT%H:%MZ"),
        "period_to": period_to.strftime("%Y-%m-%dT%H:%MZ"),
        "page_size": 1500,
        "order_by": "period",
    }
    results = []
    while url:
        resp = requests.get(url, params=params, auth=(api_key, ""), timeout=10)
        resp.raise_for_status()
        data = resp.json()
        results.extend(data["results"])
        url = data.get("next")
        params = {}
    return results


def fetch_and_store_consumption(
    config: OctopusConfig,
    db_path: str,
    period_from: datetime,
    period_to: datetime,
) -> int:
    """Fetch consumption for a window and upsert into the consumption table.

    Returns the number of slots stored.
    """
    logger.info(
        "Fetching consumption %s → %s",
        period_from.isoformat(),
        period_to.isoformat(),
    )
    records = _fetch_consumption_page(
        mpan=config.mpan,
        serial=config.serial,
        api_key=config.api_key,
        period_from=period_from.astimezone(timezone.utc),
        period_to=period_to.astimezone(timezone.utc),
    )

    if not records:
        logger.warning("No consumption data returned for requested window")
        return 0

    rows = [
        (
            datetime.fromisoformat(
                r["interval_start"].replace("Z", "+00:00")
            ).astimezone(timezone.utc).isoformat(),
            r["consumption"],
        )
        for r in records
    ]

    with get_conn(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO consumption (slot_start, consumption_kwh)
            VALUES (?, ?)
            ON CONFLICT(slot_start) DO UPDATE SET
                consumption_kwh = excluded.consumption_kwh
            """,
            rows,
        )

    logger.info("Stored %d consumption slots", len(rows))
    return len(rows)


def preload_consumption(
    config: OctopusConfig,
    db_path: str,
    days: int = 30,
) -> int:
    """Bulk-load the last N days of consumption. Use once to seed the DB.

    Fetches in weekly chunks to stay well within API limits.
    Returns total slots stored.
    """
    now = datetime.now(timezone.utc)
    period_from = now - timedelta(days=days)
    total = 0
    chunk_start = period_from
    while chunk_start < now:
        chunk_end = min(chunk_start + timedelta(weeks=1), now)
        total += fetch_and_store_consumption(config, db_path, chunk_start, chunk_end)
        chunk_start = chunk_end
    logger.info("Preload complete: %d slots over %d days", total, days)
    return total


def get_consumption(
    db_path: str,
    from_dt: datetime,
    to_dt: datetime,
) -> list[dict]:
    """Return stored consumption for slots within [from_dt, to_dt).

    Returns list of dicts with keys: slot_start (datetime, UTC),
    consumption_kwh. Sorted ascending.
    """
    from_str = from_dt.astimezone(timezone.utc).isoformat()
    to_str = to_dt.astimezone(timezone.utc).isoformat()
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT slot_start, consumption_kwh
            FROM consumption
            WHERE slot_start >= ? AND slot_start < ?
            ORDER BY slot_start
            """,
            (from_str, to_str),
        ).fetchall()
    return [
        {
            "slot_start": datetime.fromisoformat(r["slot_start"]),
            "consumption_kwh": r["consumption_kwh"],
        }
        for r in rows
    ]


def consumption_coverage(db_path: str, days: int = 30) -> dict:
    """Report how many slots are stored vs expected for the last N days.

    Useful for checking preload completeness.
    """
    now = datetime.now(timezone.utc)
    from_dt = now - timedelta(days=days)
    # 48 slots per day
    expected = days * 48
    with get_conn(db_path) as conn:
        stored = conn.execute(
            "SELECT COUNT(*) FROM consumption WHERE slot_start >= ?",
            (from_dt.isoformat(),),
        ).fetchone()[0]
    return {
        "stored": stored,
        "expected": expected,
        "coverage_pct": round(stored / expected * 100, 1) if expected else 0,
    }
