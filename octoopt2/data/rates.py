"""Octopus tariff rate records: parsing and half-hour expansion.

Pure helpers shared by the live price feed (``data/octopus.py``) and the
backtest (``backtest/tariffs.py``).  Octopus publishes unit rates and standing
charges as time-banded records.  Agile records are already half-hourly; banded
tariffs (Cosy, Flux, Go, ...) return one record per band (2-6 h), already
expressed in UTC with BST shifts applied by Octopus, so slots are matched purely
on UTC instants -- never via local hours.
"""

from __future__ import annotations

import bisect
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

LONDON = ZoneInfo("Europe/London")
UTC = timezone.utc
logger = logging.getLogger(__name__)
SLOT = timedelta(minutes=30)

# Payment-method preference when a product publishes both variants for the
# same valid_from (lower rank wins).
_PM_RANK = {"DIRECT_DEBIT": 0, None: 1, "": 1, "NON_DIRECT_DEBIT": 2}


@dataclass(frozen=True)
class RateRecord:
    valid_from: datetime  # UTC aware
    valid_to: datetime | None  # UTC aware, None = open-ended
    value_gbp: float  # GBP/kWh (unit) or GBP/day (standing); already /100
    payment_method: str | None
    value_exc_vat_gbp: float | None = None  # GBP/kWh exc VAT when the API gave it


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


def record_from_api(r: dict) -> RateRecord:
    """Build a RateRecord from a raw Octopus API result dict (pence)."""
    exc = r.get("value_exc_vat")
    return RateRecord(
        valid_from=parse_ts(r["valid_from"]),
        valid_to=parse_ts(r.get("valid_to")),
        value_gbp=r["value_inc_vat"] / 100.0,
        payment_method=r.get("payment_method") or None,
        value_exc_vat_gbp=None if exc is None else exc / 100.0,
    )


def window_slots(period_from: datetime, period_to: datetime) -> list[datetime]:
    """UTC-aware half-hour slot starts in ``[period_from, period_to)``.

    A 23:00-23:00 London window yields 46 slots on the spring-forward day, 50
    on the fall-back day and 48 otherwise, because the callers convert local
    boundaries to UTC before calling.
    """
    t = period_from.astimezone(UTC)
    end = period_to.astimezone(UTC)
    out: list[datetime] = []
    while t < end:
        out.append(t)
        t += SLOT
    return out


def london_day_slots(d: date) -> list[datetime]:
    """UTC-aware half-hour slot starts making up the London calendar day ``d``.

    46 slots on the spring-forward day, 50 on the fall-back day, 48 otherwise.
    """
    start = datetime.combine(d, time(0), tzinfo=LONDON)
    end = datetime.combine(d + timedelta(days=1), time(0), tzinfo=LONDON)
    return window_slots(start, end)


def london_days(slot_starts: list[datetime]) -> list[date]:
    seen: dict[date, None] = {}
    for s in slot_starts:
        seen[s.astimezone(LONDON).date()] = None
    return sorted(seen)


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


def _on_boundary(r: RateRecord) -> bool:
    vf = r.valid_from.astimezone(UTC)
    return vf.minute in (0, 30) and vf.second == 0 and vf.microsecond == 0


def _check_boundaries(records: list[RateRecord], label: str) -> None:
    for r in records:
        if not _on_boundary(r):
            raise ValueError(f"{label}: valid_from not on a 30-min boundary: {r.valid_from}")


def drop_off_boundary(records: list[RateRecord], label: str = "rates") -> list[RateRecord]:
    """Records whose valid_from is on a half-hour boundary; the rest are logged and dropped.

    The live feed uses this so one malformed record costs its own slots rather
    than the whole day's fetch (expand_records itself raises, which is what the
    backtest wants).
    """
    good = [r for r in records if _on_boundary(r)]
    for r in records:
        if not _on_boundary(r):
            logger.error("%s: dropping rate record not on a 30-min boundary: %s → %s",
                         label, r.valid_from, r.valid_to)
    return good


def expand_records(
    records: list[RateRecord], slot_starts: list[datetime], label: str = "rates"
) -> list[RateRecord | None]:
    """The record covering each slot start, or None where nothing covers it.

    Works for half-hourly and banded records alike: a 2-6 h band simply covers
    several consecutive slots.  Records extending beyond ``slot_starts`` are
    clipped by construction.
    """
    _check_boundaries(records, label)
    starts, recs = _dedupe(records)
    return [_lookup(starts, recs, s) for s in slot_starts]


def expand_rates(records: list[RateRecord], slot_starts: list[datetime]) -> list[float | None]:
    """GBP/kWh per slot start, or None where no record covers the slot."""
    return [None if r is None else r.value_gbp for r in expand_records(records, slot_starts)]


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
