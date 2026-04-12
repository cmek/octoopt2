"""Fetch Octopus Agile half-hourly buy and sell prices."""
import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from ..config import OctopusConfig
from ..db import get_conn

logger = logging.getLogger(__name__)

OCTOPUS_API = "https://api.octopus.energy/v1"
LONDON = ZoneInfo("Europe/London")


def _fetch_unit_rates(
    tariff_code: str,
    product_code: str,
    period_from: datetime,
    period_to: datetime,
    api_key: str,
) -> list[dict]:
    """Fetch all unit rate records for a tariff in a given window.

    Handles pagination automatically. Returns raw result dicts with
    valid_from (str) and value_inc_vat (float, pence/kWh).
    """
    url = (
        f"{OCTOPUS_API}/products/{product_code}"
        f"/electricity-tariffs/{tariff_code}/standard-unit-rates/"
    )
    params = {
        "period_from": period_from.strftime("%Y-%m-%dT%H:%MZ"),
        "period_to": period_to.strftime("%Y-%m-%dT%H:%MZ"),
        "page_size": 1500,
    }
    results = []
    while url:
        resp = requests.get(url, params=params, auth=(api_key, ""), timeout=10)
        resp.raise_for_status()
        data = resp.json()
        results.extend(data["results"])
        url = data.get("next")
        params = {}  # next URL already contains params
    return results


def _agile_window(for_date: date) -> tuple[datetime, datetime]:
    """Return the UTC window covering the Agile day for a given date.

    Agile runs 23:00–23:00 UK time, so we fetch from 23:00 the prior day
    to 23:00 on for_date (all UTC).
    """
    start_london = datetime(
        for_date.year, for_date.month, for_date.day, 23, 0, 0, tzinfo=LONDON
    ) - timedelta(days=1)
    end_london = datetime(
        for_date.year, for_date.month, for_date.day, 23, 0, 0, tzinfo=LONDON
    )
    return start_london.astimezone(timezone.utc), end_london.astimezone(timezone.utc)


def fetch_and_store_prices(
    config: OctopusConfig,
    db_path: str,
    for_date: date | None = None,
) -> int:
    """Fetch Agile buy and sell prices for a given date and upsert into DB.

    Uses the Agile day window (23:00–23:00 UK time). Defaults to today.
    Returns the number of slots stored.
    """
    if for_date is None:
        for_date = datetime.now(LONDON).date()

    period_from, period_to = _agile_window(for_date)
    logger.info(
        "Fetching prices for %s (UTC %s → %s)",
        for_date,
        period_from.isoformat(),
        period_to.isoformat(),
    )

    buy_rates = _fetch_unit_rates(
        tariff_code=config.agile_tariff_code,
        product_code=_product_code_from_tariff(config.agile_tariff_code),
        period_from=period_from,
        period_to=period_to,
        api_key=config.api_key,
    )
    sell_rates = _fetch_unit_rates(
        tariff_code=config.outgoing_tariff_code,
        product_code=_product_code_from_tariff(config.outgoing_tariff_code),
        period_from=period_from,
        period_to=period_to,
        api_key=config.api_key,
    )

    if not sell_rates:
        logger.warning(
            "No sell rates returned for %s — check OCTOPUS_OUTGOING_TARIFF_CODE in .env "
            "(tariff: %s)",
            for_date,
            config.outgoing_tariff_code,
        )

    # Index sell rates by slot start for O(1) lookup
    sell_by_slot: dict[str, float] = {
        _normalise_slot(r["valid_from"]): r["value_inc_vat"] / 100
        for r in sell_rates
    }

    rows = []
    for r in buy_rates:
        slot = _normalise_slot(r["valid_from"])
        buy_gbp = r["value_inc_vat"] / 100
        sell_gbp = sell_by_slot.get(slot, 0.0)
        rows.append((slot, buy_gbp, sell_gbp))

    if not rows:
        logger.warning("No price data returned for %s", for_date)
        return 0

    with get_conn(db_path) as conn:
        if sell_rates:
            # Both buy and sell available — upsert everything
            conn.executemany(
                """
                INSERT INTO prices (slot_start, buy_gbp_kwh, sell_gbp_kwh)
                VALUES (?, ?, ?)
                ON CONFLICT(slot_start) DO UPDATE SET
                    buy_gbp_kwh  = excluded.buy_gbp_kwh,
                    sell_gbp_kwh = excluded.sell_gbp_kwh
                """,
                rows,
            )
        else:
            # Sell rates not yet published — only upsert buy price, preserve
            # any sell price already stored so we don't overwrite with 0.0
            conn.executemany(
                """
                INSERT INTO prices (slot_start, buy_gbp_kwh, sell_gbp_kwh)
                VALUES (?, ?, 0.0)
                ON CONFLICT(slot_start) DO UPDATE SET
                    buy_gbp_kwh = excluded.buy_gbp_kwh
                """,
                [(slot, buy) for slot, buy, _ in rows],
            )

    logger.info("Stored %d price slots for %s", len(rows), for_date)
    return len(rows)


def get_prices_from(
    db_path: str,
    from_dt: datetime,
    to_dt: datetime,
) -> list[dict]:
    """Return stored prices for slots within [from_dt, to_dt).

    Returns list of dicts with keys: slot_start (datetime, UTC),
    buy_gbp_kwh, sell_gbp_kwh. Sorted by slot_start ascending.
    """
    from_str = from_dt.astimezone(timezone.utc).isoformat()
    to_str = to_dt.astimezone(timezone.utc).isoformat()
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT slot_start, buy_gbp_kwh, sell_gbp_kwh
            FROM prices
            WHERE slot_start >= ? AND slot_start < ?
            ORDER BY slot_start
            """,
            (from_str, to_str),
        ).fetchall()
    return [
        {
            "slot_start": datetime.fromisoformat(r["slot_start"]),
            "buy_gbp_kwh": r["buy_gbp_kwh"],
            "sell_gbp_kwh": r["sell_gbp_kwh"],
        }
        for r in rows
    ]


def missing_price_dates(db_path: str, look_ahead_days: int = 2) -> list[date]:
    """Return dates within the next look_ahead_days that need a price fetch.

    A date needs fetching if:
    - it has no buy prices at all, OR
    - it has buy prices but all sell prices are 0.0 (fetched before sell rates
      were published, or before the correct outgoing tariff was configured)
    """
    today = datetime.now(LONDON).date()
    missing = []
    for offset in range(look_ahead_days):
        d = today + timedelta(days=offset)
        period_from, period_to = _agile_window(d)
        with get_conn(db_path) as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN sell_gbp_kwh > 0 THEN 1 ELSE 0 END) AS with_sell
                FROM prices
                WHERE slot_start >= ? AND slot_start < ?
                """,
                (period_from.isoformat(), period_to.isoformat()),
            ).fetchone()
        if row["total"] == 0 or row["with_sell"] == 0:
            missing.append(d)
    return missing


def _product_code_from_tariff(tariff_code: str) -> str:
    """Extract product code from tariff code.

    Tariff codes follow the pattern: E-1R-{PRODUCT_CODE}-{REGION}
    e.g. E-1R-AGILE-24-10-01-C → AGILE-24-10-01
    """
    # Strip leading "E-1R-" and trailing "-{LETTER}"
    parts = tariff_code.split("-")
    # parts: ['E', '1R', ...product parts..., 'C']
    return "-".join(parts[2:-1])


def _normalise_slot(valid_from: str) -> str:
    """Normalise a slot timestamp to a consistent UTC ISO8601 string."""
    dt = datetime.fromisoformat(valid_from.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).isoformat()
