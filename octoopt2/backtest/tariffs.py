"""Historic Octopus Energy tariff rates: fetch, cache, expand.

Used by the backtest to re-price half-hourly energy flows under alternative
tariffs.  Octopus publishes unit rates and standing charges per product /
region as time-banded records.  Banded tariffs (Cosy, Flux, ...) are already
expressed in UTC with BST shifts applied by Octopus, so the records are used
as-is and slots are matched purely on UTC instants -- never via local hours.

Cache tables (``tariff_rates``, ``tariff_coverage``) are created lazily here
with ``CREATE TABLE IF NOT EXISTS``; ``octoopt2.db`` is not touched.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Literal
from zoneinfo import ZoneInfo

import requests

from octoopt2.db import get_conn

API_BASE = "https://api.octopus.energy/v1"
LONDON = ZoneInfo("Europe/London")
UTC = timezone.utc
SLOT = timedelta(minutes=30)

Kind = Literal["unit", "standing"]

_KIND_PATH = {"unit": "standard-unit-rates", "standing": "standing-charges"}

# Payment-method preference when a product publishes both variants for the
# same valid_from (lower rank wins).
_PM_RANK = {"DIRECT_DEBIT": 0, None: 1, "": 1, "NON_DIRECT_DEBIT": 2}


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TariffPair:
    name: str
    import_product: str | None  # None = use the DB's own prices for buy
    export_product: str | None  # None = db_sell unless sell_fixed given
    sell_fixed: float | None = None  # GBP/kWh constant export override


@dataclass(frozen=True)
class RateRecord:
    valid_from: datetime  # UTC aware
    valid_to: datetime | None  # UTC aware, None = open-ended
    value_gbp: float  # GBP/kWh (unit) or GBP/day (standing); already /100
    payment_method: str | None


@dataclass
class PriceSeries:
    buy: list[float]
    sell: list[float]
    standing_by_day: dict[date, float]
    notes: list[str] = field(default_factory=list)


# Export products verified live for region G (fetched 2026-10-12, Sept 2026 window):
#   OUTGOING-SEG-FIX-12M-20-07-07    -> 4.10 p/kWh flat, open-ended (SEG floor rate)
#   OUTGOING-SEG-EO-FIX-12M-24-04-05 -> 4.10 p/kWh flat (SEG, non-Octopus import)
#   OUTGOING-VAR-24-10-26            -> "Outgoing Fixed": 15.00 p/kWh 2024-10-25..2026-03-01, 12.00 p/kWh since
#   OUTGOING-PRIME-FIX-12M-26-06-23  -> 9.00 p/kWh, 16.00 p/kWh 15:00-18:00Z (peak-weighted)
#   AGILE-OUTGOING-19-05-13          -> half-hourly, ~7-24 p/kWh
#   FLUX-EXPORT-23-02-14             -> 4.66 / 10.33 / 29.25 p/kWh bands
#   INTELLI-FLUX-EXPORT-23-07-14     -> 404 (product not public); "iflux" needs
#                                       an explicit export via parse_pair_spec
# "fixed" below means the Outgoing Fixed product (OUTGOING-VAR-24-10-26), not the
# 4.1p SEG rate.  Override with e.g. "cosy+seg:COSY-22-12-08/OUTGOING-SEG-FIX-12M-20-07-07"
# or "cosy+15p:COSY-22-12-08/fixed=0.15".
FIXED_EXPORT = "OUTGOING-VAR-24-10-26"

DEFAULT_PAIRS: dict[str, TariffPair] = {
    # current setup: DB prices (Agile import + Agile Outgoing export)
    "agile": TariffPair("agile", None, None),
    # Agile import from DB, Outgoing Fixed export
    "agile+fixed": TariffPair("agile+fixed", None, FIXED_EXPORT),
    "cosy+agileout": TariffPair("cosy+agileout", "COSY-22-12-08", "AGILE-OUTGOING-19-05-13"),
    "cosy+fixed": TariffPair("cosy+fixed", "COSY-22-12-08", FIXED_EXPORT),
    "flux": TariffPair("flux", "FLUX-IMPORT-23-02-14", "FLUX-EXPORT-23-02-14"),
    "flexible+fixed": TariffPair("flexible+fixed", "VAR-22-11-01", FIXED_EXPORT),
    "iflux": TariffPair("iflux", "INTELLI-FLUX-IMPORT-23-07-14", "INTELLI-FLUX-EXPORT-23-07-14"),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def tariff_code(product_code: str, region: str) -> str:
    return f"E-1R-{product_code}-{region}"


def parse_ts(s: str | None) -> datetime | None:
    """Parse an Octopus ISO timestamp (``...Z`` or ``+00:00``) to UTC aware."""
    if s is None:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat()


def _api_period(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%MZ")


def london_day_slots(d: date) -> list[datetime]:
    """UTC-aware half-hour slot starts making up the London calendar day ``d``.

    46 slots on the spring-forward day, 50 on the fall-back day, 48 otherwise.
    """
    start = datetime.combine(d, time(0), tzinfo=LONDON).astimezone(UTC)
    end = datetime.combine(d + timedelta(days=1), time(0), tzinfo=LONDON).astimezone(UTC)
    out: list[datetime] = []
    t = start
    while t < end:
        out.append(t)
        t += SLOT
    return out


def london_days(slot_starts: list[datetime]) -> list[date]:
    seen: dict[date, None] = {}
    for s in slot_starts:
        seen[s.astimezone(LONDON).date()] = None
    return sorted(seen)


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


def fetch_rates(
    product_code: str,
    region: str,
    kind: Kind,
    period_from: datetime,
    period_to: datetime,
    api_key: str | None = None,
    session=None,
) -> list[dict]:
    """Fetch raw rate records for a product/region, following pagination."""
    code = tariff_code(product_code, region)
    url = f"{API_BASE}/products/{product_code}/electricity-tariffs/{code}/{_KIND_PATH[kind]}/"
    params: dict | None = {
        "period_from": _api_period(period_from),
        "period_to": _api_period(period_to),
        "page_size": 1500,
    }
    auth = (api_key, "") if api_key else None
    http = session or requests
    out: list[dict] = []
    while url:
        resp = http.get(url, params=params, auth=auth, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        out.extend(data.get("results", []))
        url = data.get("next")
        params = None  # ``next`` already carries the query string
    return out


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS tariff_rates (
        tariff_code TEXT NOT NULL,
        kind TEXT NOT NULL,
        valid_from TEXT NOT NULL,
        valid_to TEXT,
        value_inc_vat REAL NOT NULL,
        value_exc_vat REAL,
        payment_method TEXT NOT NULL DEFAULT '',
        fetched_at TEXT NOT NULL,
        PRIMARY KEY (tariff_code, kind, valid_from, payment_method)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tariff_coverage (
        tariff_code TEXT NOT NULL,
        kind TEXT NOT NULL,
        period_from TEXT NOT NULL,
        period_to TEXT NOT NULL,
        fetched_at TEXT NOT NULL,
        PRIMARY KEY (tariff_code, kind, period_from, period_to)
    )
    """,
)


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA:
        conn.execute(stmt)


def _is_covered(conn, code: str, kind: str, pf: str, pt: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM tariff_coverage WHERE tariff_code=? AND kind=? "
        "AND period_from<=? AND period_to>=? LIMIT 1",
        (code, kind, pf, pt),
    ).fetchone()
    return row is not None


def ensure_cached(
    db_path: str,
    product_code: str,
    region: str,
    kind: Kind,
    period_from: datetime,
    period_to: datetime,
    *,
    api_key: str | None = None,
    refresh: bool = False,
) -> None:
    """Fetch and store rates unless a coverage row already spans the range."""
    code = tariff_code(product_code, region)
    pf, pt = _iso_utc(period_from), _iso_utc(period_to)
    with get_conn(db_path) as conn:
        _ensure_schema(conn)
        if not refresh and _is_covered(conn, code, kind, pf, pt):
            return
    records = fetch_rates(product_code, region, kind, period_from, period_to, api_key=api_key)
    now = datetime.now(UTC).isoformat()
    with get_conn(db_path) as conn:
        _ensure_schema(conn)
        conn.executemany(
            "INSERT OR REPLACE INTO tariff_rates "
            "(tariff_code, kind, valid_from, valid_to, value_inc_vat, value_exc_vat, "
            "payment_method, fetched_at) VALUES (?,?,?,?,?,?,?,?)",
            [
                (
                    code,
                    kind,
                    _iso_utc(parse_ts(r["valid_from"])),
                    _iso_utc(parse_ts(r["valid_to"])) if r.get("valid_to") else None,
                    float(r["value_inc_vat"]),
                    float(r["value_exc_vat"]) if r.get("value_exc_vat") is not None else None,
                    r.get("payment_method") or "",
                    now,
                )
                for r in records
            ],
        )
        if records:
            conn.execute(
                "INSERT OR REPLACE INTO tariff_coverage "
                "(tariff_code, kind, period_from, period_to, fetched_at) VALUES (?,?,?,?,?)",
                (code, kind, pf, pt, now),
            )


def load_records(db_path: str, tariff_code: str, kind: Kind) -> list[RateRecord]:
    """All cached records for a tariff code / kind, sorted by valid_from."""
    with get_conn(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            "SELECT valid_from, valid_to, value_inc_vat, payment_method FROM tariff_rates "
            "WHERE tariff_code=? AND kind=? ORDER BY valid_from, payment_method",
            (tariff_code, kind),
        ).fetchall()
    return [
        RateRecord(
            valid_from=parse_ts(r["valid_from"]),
            valid_to=parse_ts(r["valid_to"]),
            value_gbp=r["value_inc_vat"] / 100.0,
            payment_method=r["payment_method"] or None,
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Expand
# ---------------------------------------------------------------------------


def _dedupe(records: list[RateRecord]) -> tuple[list[datetime], list[RateRecord]]:
    """Collapse duplicates per valid_from (DIRECT_DEBIT preferred); sorted."""
    best: dict[datetime, RateRecord] = {}
    for r in records:
        vf = r.valid_from.astimezone(UTC)
        cur = best.get(vf)
        if cur is None or _PM_RANK.get(r.payment_method, 1) < _PM_RANK.get(cur.payment_method, 1):
            best[vf] = r
    starts = sorted(best)
    return starts, [best[s] for s in starts]


def _lookup(starts: list[datetime], recs: list[RateRecord], t: datetime) -> RateRecord | None:
    t = t.astimezone(UTC)
    i = bisect.bisect_right(starts, t) - 1
    if i < 0:
        return None
    rec = recs[i]
    if rec.valid_to is not None and t >= rec.valid_to:
        return None
    return rec


def expand_rates(records: list[RateRecord], slot_starts: list[datetime]) -> list[float | None]:
    """GBP/kWh per slot start, or None where no record covers the slot."""
    for r in records:
        vf = r.valid_from.astimezone(UTC)
        assert vf.minute in (0, 30) and vf.second == 0 and vf.microsecond == 0, (
            f"valid_from not on a 30-min boundary: {r.valid_from}"
        )
    starts, recs = _dedupe(records)
    out: list[float | None] = []
    for s in slot_starts:
        rec = _lookup(starts, recs, s)
        out.append(None if rec is None else rec.value_gbp)
    return out


def expand_standing(records: list[RateRecord], london_days: list[date]) -> dict[date, float]:
    """GBP/day keyed by London date (record covering local midnight in UTC).

    Days with no covering record are omitted.
    """
    starts, recs = _dedupe(records)
    out: dict[date, float] = {}
    for d in london_days:
        midnight = datetime.combine(d, time(0), tzinfo=LONDON).astimezone(UTC)
        rec = _lookup(starts, recs, midnight)
        if rec is not None:
            out[d] = rec.value_gbp
    return out


# ---------------------------------------------------------------------------
# Price series
# ---------------------------------------------------------------------------


def _uncovered_error(product: str, code: str, kind: str, values: list, slots: list) -> ValueError:
    idx = [i for i, v in enumerate(values) if v is None]
    first, last = slots[idx[0]], slots[idx[-1]]
    return ValueError(
        f"{product} ({code}) has no {kind} rate for {len(idx)}/{len(slots)} slots: "
        f"first uncovered {first.isoformat()}, last uncovered {last.isoformat()}"
    )


def _product_series(
    db_path: str,
    product: str,
    region: str,
    slot_starts: list[datetime],
    *,
    api_key: str | None,
    refresh: bool,
) -> list[float]:
    period_from = min(slot_starts)
    period_to = max(slot_starts) + SLOT
    code = tariff_code(product, region)
    ensure_cached(db_path, product, region, "unit", period_from, period_to, api_key=api_key, refresh=refresh)
    vals = expand_rates(load_records(db_path, code, "unit"), slot_starts)
    if any(v is None for v in vals):
        raise _uncovered_error(product, code, "unit", vals, slot_starts)
    return [float(v) for v in vals]  # type: ignore[arg-type]


def _is_404(exc: Exception) -> bool:
    resp = getattr(exc, "response", None)
    return isinstance(exc, requests.HTTPError) and resp is not None and resp.status_code == 404


def load_price_series(
    db_path: str,
    pair: TariffPair,
    slot_starts: list[datetime],
    region: str,
    *,
    db_buy: list[float],
    db_sell: list[float],
    api_key: str | None = None,
    refresh: bool = False,
) -> PriceSeries:
    """Buy/sell GBP/kWh per slot plus standing charge per London day."""
    n = len(slot_starts)
    notes: list[str] = []
    standing: dict[date, float] = {}

    if pair.import_product:
        buy = _product_series(db_path, pair.import_product, region, slot_starts, api_key=api_key, refresh=refresh)
        period_from, period_to = min(slot_starts), max(slot_starts) + SLOT
        code = tariff_code(pair.import_product, region)
        ensure_cached(db_path, pair.import_product, region, "standing", period_from, period_to,
                      api_key=api_key, refresh=refresh)
        days = london_days(slot_starts)
        standing = expand_standing(load_records(db_path, code, "standing"), days)
        missing = [d for d in days if d not in standing]
        if missing:
            notes.append(
                f"{pair.import_product}: no standing charge for {len(missing)} day(s) "
                f"({missing[0]} .. {missing[-1]})"
            )
    else:
        if len(db_buy) != n:
            raise ValueError(f"db_buy has {len(db_buy)} values for {n} slots")
        buy = [float(v) for v in db_buy]
        notes.append("buy: DB prices (no import product)")

    if pair.export_product:
        try:
            sell = _product_series(db_path, pair.export_product, region, slot_starts,
                                   api_key=api_key, refresh=refresh)
        except (ValueError, requests.HTTPError) as exc:
            if pair.sell_fixed is None or not (isinstance(exc, ValueError) or _is_404(exc)):
                raise
            sell = [float(pair.sell_fixed)] * n
            notes.append(
                f"{pair.export_product}: unavailable for region {region} ({exc}); "
                f"using sell_fixed={pair.sell_fixed:.4f} GBP/kWh"
            )
    elif pair.sell_fixed is not None:
        sell = [float(pair.sell_fixed)] * n
        notes.append(f"sell: fixed {pair.sell_fixed:.4f} GBP/kWh")
    else:
        if len(db_sell) != n:
            raise ValueError(f"db_sell has {len(db_sell)} values for {n} slots")
        sell = [float(v) for v in db_sell]
        notes.append("sell: DB prices (no export product)")

    return PriceSeries(buy=buy, sell=sell, standing_by_day=standing, notes=notes)


# ---------------------------------------------------------------------------
# CLI spec parsing
# ---------------------------------------------------------------------------


def parse_pair_spec(spec: str) -> TariffPair:
    """``name`` (from DEFAULT_PAIRS), or ``name:IMPORT/EXPORT`` where IMPORT is a
    product code or ``db`` and EXPORT is a product code, ``db`` or ``fixed=0.15``."""
    spec = spec.strip()
    if ":" not in spec:
        try:
            return DEFAULT_PAIRS[spec]
        except KeyError:
            raise ValueError(
                f"unknown tariff pair {spec!r}; known: {', '.join(DEFAULT_PAIRS)} "
                f"or use name:IMPORT/EXPORT"
            ) from None
    name, rest = spec.split(":", 1)
    name = name.strip()
    if "/" not in rest:
        raise ValueError(f"bad tariff spec {spec!r}: expected name:IMPORT/EXPORT")
    imp, exp = (p.strip() for p in rest.split("/", 1))
    if not name or not imp or not exp:
        raise ValueError(f"bad tariff spec {spec!r}: expected name:IMPORT/EXPORT")
    import_product = None if imp.lower() == "db" else imp
    export_product: str | None = None
    sell_fixed: float | None = None
    if exp.lower() == "db":
        pass
    elif exp.lower().startswith("fixed="):
        try:
            sell_fixed = float(exp.split("=", 1)[1])
        except ValueError:
            raise ValueError(f"bad fixed export price in {spec!r}") from None
    else:
        export_product = exp
    return TariffPair(name, import_product, export_product, sell_fixed)
