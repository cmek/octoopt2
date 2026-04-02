"""Fetch 15-minute weather forecasts from Open-Meteo (free, no API key required).

Historical weather (for load model training) is backfilled from the Open-Meteo
archive API which provides hourly data going back years. The ±30-minute nearest-
neighbour matching in the load model handles the hourly→30-min resolution gap.
"""
import logging
from datetime import date, datetime, timedelta, timezone

import requests

from ..config import LocationConfig
from ..db import get_conn

logger = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Refresh weather at most once per hour — it changes slowly and there are no
# hard rate limits on the free tier, but there's no point fetching more often.
WEATHER_TTL_HOURS = 1

MINUTELY_VARIABLES = [
    "temperature_2m",
    "cloud_cover",
    "wind_speed_10m",
    "relative_humidity_2m",
    "precipitation",
]


def _staleness(db_path: str) -> float | None:
    """Return age in hours of the most recent weather fetch, or None if no data."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT MAX(fetched_at) FROM weather_forecast"
        ).fetchone()[0]
    if row is None:
        return None
    fetched_at = datetime.fromisoformat(row)
    return (datetime.now(timezone.utc) - fetched_at).total_seconds() / 3600


def fetch_and_store_weather(
    location: LocationConfig,
    db_path: str,
    forecast_days: int = 3,
    force: bool = False,
) -> int:
    """Fetch 15-minute weather forecast and upsert into weather_forecast table.

    Skips the API call if existing data is fresher than WEATHER_TTL_HOURS,
    unless force=True. Returns number of slots stored (0 if skipped).

    Wind speed is converted from km/h (API default) to m/s on ingest.
    All times are stored as UTC ISO8601.
    """
    staleness = _staleness(db_path)
    if not force and staleness is not None and staleness < WEATHER_TTL_HOURS:
        logger.debug(
            "Weather data is %.1fh old (TTL %dh), skipping fetch",
            staleness,
            WEATHER_TTL_HOURS,
        )
        return 0

    logger.info("Fetching Open-Meteo weather forecast (%d days)", forecast_days)
    resp = requests.get(
        OPEN_METEO_URL,
        params={
            "latitude": location.latitude,
            "longitude": location.longitude,
            "minutely_15": ",".join(MINUTELY_VARIABLES),
            "forecast_days": forecast_days,
            "timezone": "UTC",
            "wind_speed_unit": "ms",  # request m/s directly — no conversion needed
        },
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()

    m15 = data.get("minutely_15", {})
    times = m15.get("time", [])
    if not times:
        logger.warning("No 15-minute weather data returned from Open-Meteo")
        return 0

    temperature = m15.get("temperature_2m", [None] * len(times))
    cloud_cover = m15.get("cloud_cover", [None] * len(times))
    wind_speed  = m15.get("wind_speed_10m", [None] * len(times))
    humidity    = m15.get("relative_humidity_2m", [None] * len(times))
    precip      = m15.get("precipitation", [None] * len(times))

    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for i, t in enumerate(times):
        # Open-Meteo returns "YYYY-MM-DDTHH:MM" in the requested timezone (UTC)
        slot_start = datetime.fromisoformat(t).replace(tzinfo=timezone.utc).isoformat()
        rows.append((
            slot_start,
            temperature[i],
            cloud_cover[i],
            wind_speed[i],
            humidity[i],
            precip[i],
            fetched_at,
        ))

    with get_conn(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO weather_forecast
                (slot_start, temperature_c, cloud_cover_pct, wind_speed_ms,
                 humidity_pct, precipitation_mm, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(slot_start) DO UPDATE SET
                temperature_c    = excluded.temperature_c,
                cloud_cover_pct  = excluded.cloud_cover_pct,
                wind_speed_ms    = excluded.wind_speed_ms,
                humidity_pct     = excluded.humidity_pct,
                precipitation_mm = excluded.precipitation_mm,
                fetched_at       = excluded.fetched_at
            """,
            rows,
        )

    logger.info("Stored %d weather slots", len(rows))
    return len(rows)


def backfill_weather_history(
    location: LocationConfig,
    db_path: str,
    days: int = 35,
) -> int:
    """Fetch hourly historical weather from the Open-Meteo archive API.

    Fills the weather_forecast table with past temperature data so the load
    model can be trained against historical consumption. Only fetches slots
    not already present in the DB.

    Returns the number of new slots stored.
    """
    end_date = date.today() - timedelta(days=1)  # archive lags by ~1 day
    start_date = end_date - timedelta(days=days - 1)

    # Check how many historical slots we already have
    start_dt = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
    end_dt = datetime(end_date.year, end_date.month, end_date.day, 23, 0, tzinfo=timezone.utc)
    with get_conn(db_path) as conn:
        existing = conn.execute(
            "SELECT COUNT(*) FROM weather_forecast WHERE slot_start >= ? AND slot_start <= ?",
            (start_dt.isoformat(), end_dt.isoformat()),
        ).fetchone()[0]

    if existing >= days * 20:  # ~24 hourly slots/day, allow some gaps
        logger.debug(
            "Historical weather already present (%d slots from %s), skipping backfill",
            existing, start_date,
        )
        return 0

    logger.info(
        "Backfilling historical weather %s → %s (%d days)",
        start_date, end_date, days,
    )
    resp = requests.get(
        OPEN_METEO_ARCHIVE_URL,
        params={
            "latitude": location.latitude,
            "longitude": location.longitude,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": "temperature_2m,cloud_cover,wind_speed_10m,relative_humidity_2m,precipitation",
            "timezone": "UTC",
            "wind_speed_unit": "ms",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        logger.warning("No historical weather data returned from Open-Meteo archive")
        return 0

    temperature = hourly.get("temperature_2m", [None] * len(times))
    cloud_cover = hourly.get("cloud_cover", [None] * len(times))
    wind_speed  = hourly.get("wind_speed_10m", [None] * len(times))
    humidity    = hourly.get("relative_humidity_2m", [None] * len(times))
    precip      = hourly.get("precipitation", [None] * len(times))

    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for i, t in enumerate(times):
        slot_start = datetime.fromisoformat(t).replace(tzinfo=timezone.utc).isoformat()
        rows.append((
            slot_start,
            temperature[i],
            cloud_cover[i],
            wind_speed[i],
            humidity[i],
            precip[i],
            fetched_at,
        ))

    with get_conn(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO weather_forecast
                (slot_start, temperature_c, cloud_cover_pct, wind_speed_ms,
                 humidity_pct, precipitation_mm, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(slot_start) DO NOTHING
            """,
            rows,
        )

    logger.info("Stored %d historical weather slots (%s → %s)", len(rows), start_date, end_date)
    return len(rows)


def get_weather(
    db_path: str,
    from_dt: datetime,
    to_dt: datetime,
) -> list[dict]:
    """Return stored weather for 15-min slots within [from_dt, to_dt).

    Returns list of dicts sorted ascending by slot_start.
    """
    from_str = from_dt.astimezone(timezone.utc).isoformat()
    to_str = to_dt.astimezone(timezone.utc).isoformat()
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT slot_start, temperature_c, cloud_cover_pct,
                   wind_speed_ms, humidity_pct, precipitation_mm
            FROM weather_forecast
            WHERE slot_start >= ? AND slot_start < ?
            ORDER BY slot_start
            """,
            (from_str, to_str),
        ).fetchall()
    return [
        {
            "slot_start": datetime.fromisoformat(r["slot_start"]),
            "temperature_c": r["temperature_c"],
            "cloud_cover_pct": r["cloud_cover_pct"],
            "wind_speed_ms": r["wind_speed_ms"],
            "humidity_pct": r["humidity_pct"],
            "precipitation_mm": r["precipitation_mm"],
        }
        for r in rows
    ]


def get_temperature_forecast(
    db_path: str,
    from_dt: datetime,
    to_dt: datetime,
) -> dict[datetime, float]:
    """Return temperature forecast as a slot_start → temperature_c mapping.

    Convenience function for the load model, which only needs temperature.
    Covers the requested window at 15-min resolution.
    """
    rows = get_weather(db_path, from_dt, to_dt)
    return {r["slot_start"]: r["temperature_c"] for r in rows if r["temperature_c"] is not None}
