"""Fetch Octopus import/export unit rates into half-hourly ``prices`` rows.

The import tariff may be half-hourly (Agile) or banded (Cosy, Go, Flux): banded
records are expanded to half-hour slots via ``octoopt2.data.rates``. Export is
expected to be Agile Outgoing (half-hourly, published ~16:00 for the next day)
but any product works through the same path.
"""
import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from ..config import OctopusConfig
from ..db import get_conn, record_fetch
from .rates import expand_records, record_from_api, window_slots

logger = logging.getLogger(__name__)

OCTOPUS_API = "https://api.octopus.energy/v1"
LONDON = ZoneInfo("Europe/London")

# Standing charges change a few times a year; once a day is plenty.
STANDING_TTL_HOURS = 24


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


def _pricing_window(for_date: date) -> tuple[datetime, datetime]:
    """Return the UTC window covering the Octopus pricing day for a date.

    Octopus days run 23:00–23:00 UK time, so we fetch from 23:00 the prior day
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
    """Fetch import (buy) and export (sell) prices for a date and upsert into DB.

    Uses the Octopus day window (23:00–23:00 UK time). Defaults to today.
    Banded import tariffs are expanded to one row per half-hour slot, so a
    full day is always 46/48/50 rows. Returns the number of slots stored.
    """
    if for_date is None:
        for_date = datetime.now(LONDON).date()

    period_from, period_to = _pricing_window(for_date)
    logger.info(
        "Fetching prices for %s (UTC %s → %s)",
        for_date,
        period_from.isoformat(),
        period_to.isoformat(),
    )

    buy_rates = _fetch_unit_rates(
        tariff_code=config.import_tariff_code,
        product_code=_product_code_from_tariff(config.import_tariff_code),
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

    # One row per half-hour slot of the window. Agile records are already
    # half-hourly (no-op); Cosy/Go/Flux bands cover several slots each and any
    # band straddling the window edge is clipped by construction.
    slots = window_slots(period_from, period_to)
    buy_recs = expand_records(
        [record_from_api(r) for r in buy_rates], slots, label=config.import_tariff_code
    )
    sell_recs = expand_records(
        [record_from_api(r) for r in sell_rates], slots, label=config.outgoing_tariff_code
    )

    rows = []
    for slot, b, sr in zip(slots, buy_recs, sell_recs):
        if b is None:
            continue
        # None (→ NULL) when the outgoing rate for this slot isn't published yet.
        # Never default to 0.0: a 0 export price is indistinguishable from a free
        # export sink and lets the optimizer round-trip the battery during
        # negative-import slots. Callers default a NULL pessimistically instead.
        sell_gbp = None if sr is None else sr.value_gbp
        sell_exc = None if sr is None else sr.value_exc_vat_gbp
        rows.append((slot.isoformat(), b.value_gbp, sell_gbp, b.value_exc_vat_gbp, sell_exc))

    if not rows:
        logger.warning("No price data returned for %s", for_date)
        return 0

    with get_conn(db_path) as conn:
        if sell_rates:
            # Both buy and sell available — upsert everything
            conn.executemany(
                """
                INSERT INTO prices (slot_start, buy_gbp_kwh, sell_gbp_kwh,
                                    buy_exc_vat_gbp_kwh, sell_exc_vat_gbp_kwh)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(slot_start) DO UPDATE SET
                    buy_gbp_kwh          = excluded.buy_gbp_kwh,
                    sell_gbp_kwh         = excluded.sell_gbp_kwh,
                    buy_exc_vat_gbp_kwh  = excluded.buy_exc_vat_gbp_kwh,
                    sell_exc_vat_gbp_kwh = excluded.sell_exc_vat_gbp_kwh
                """,
                rows,
            )
        else:
            # Sell rates not yet published — upsert buy price with sell = NULL
            # (unknown), preserving any sell price already stored so we don't
            # clobber a real rate fetched earlier.
            conn.executemany(
                """
                INSERT INTO prices (slot_start, buy_gbp_kwh, sell_gbp_kwh,
                                    buy_exc_vat_gbp_kwh)
                VALUES (?, ?, NULL, ?)
                ON CONFLICT(slot_start) DO UPDATE SET
                    buy_gbp_kwh         = excluded.buy_gbp_kwh,
                    buy_exc_vat_gbp_kwh = excluded.buy_exc_vat_gbp_kwh
                """,
                [(slot, buy, buy_exc) for slot, buy, _, buy_exc, _ in rows],
            )

    logger.info("Stored %d half-hour price slots (%d buy records) for %s", len(rows), len(buy_rates), for_date)
    return len(rows)


def fetch_and_store_standing_charges(config: OctopusConfig, db_path: str, force: bool = False) -> int:
    """Fetch the import tariff's standing charge(s) into standing_charges.

    Hits /standing-charges/ for the import tariff. Skipped while the
    "standing" feed stamp in feed_fetches is younger than STANDING_TTL_HOURS
    (a skip still re-stamps the feed — it is up to date). Returns the number
    of validity periods stored (0 if skipped).
    """
    if not force:
        with get_conn(db_path) as conn:
            row = conn.execute(
                "SELECT fetched_at FROM feed_fetches WHERE feed = 'standing'"
            ).fetchone()
        if row is not None:
            age_h = (
                datetime.now(timezone.utc) - datetime.fromisoformat(row["fetched_at"])
            ).total_seconds() / 3600
            if age_h < STANDING_TTL_HOURS:
                logger.debug("Standing charge is %.1fh old (TTL %dh), skipping", age_h, STANDING_TTL_HOURS)
                record_fetch(db_path, "standing")
                return 0

    tariff = config.import_tariff_code
    product = _product_code_from_tariff(tariff)
    url = f"{OCTOPUS_API}/products/{product}/electricity-tariffs/{tariff}/standing-charges/"
    # Only recent/current periods are of interest; one page covers them.
    period_from = datetime.now(timezone.utc) - timedelta(days=30)
    resp = requests.get(
        url,
        params={"period_from": period_from.strftime("%Y-%m-%dT%H:%MZ"), "page_size": 100},
        auth=(config.api_key, ""),
        timeout=10,
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])

    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = [
        (
            tariff,
            _normalise_slot(r["valid_from"]),
            _normalise_slot(r["valid_to"]) if r.get("valid_to") else None,
            r["value_inc_vat"],   # pence/day, as published
            fetched_at,
        )
        for r in results
    ]
    if not rows:
        logger.warning("No standing charges returned for %s", tariff)
        return 0

    with get_conn(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO standing_charges
                (tariff_code, valid_from, valid_to, value_inc_vat_p_day, fetched_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(tariff_code, valid_from) DO UPDATE SET
                valid_to            = excluded.valid_to,
                value_inc_vat_p_day = excluded.value_inc_vat_p_day,
                fetched_at          = excluded.fetched_at
            """,
            rows,
        )
    record_fetch(db_path, "standing")
    logger.info("Stored %d standing charge period(s) for %s", len(rows), tariff)
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
    - any slot is missing its outgoing (sell) rate, i.e. sell_gbp_kwh IS NULL
      (fetched before sell rates were published, or before the correct outgoing
      tariff was configured). A genuine 0.0 or negative rate counts as known and
      is not re-fetched.
    """
    today = datetime.now(LONDON).date()
    missing = []
    for offset in range(look_ahead_days):
        d = today + timedelta(days=offset)
        period_from, period_to = _pricing_window(d)
        with get_conn(db_path) as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN sell_gbp_kwh IS NOT NULL THEN 1 ELSE 0 END) AS with_sell
                FROM prices
                WHERE slot_start >= ? AND slot_start < ?
                """,
                (period_from.isoformat(), period_to.isoformat()),
            ).fetchone()
        if row["total"] == 0 or row["with_sell"] < row["total"]:
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
