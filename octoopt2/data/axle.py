"""Axle Energy VPP event feed ("Events Only" mode).

Axle publishes the customer's *next* grid event (export or import) a day ahead
at ``https://api.axle.energy/vpp/home-assistant/event`` for a bearer token
generated in the Axle account (Account Settings → Home Assistant; only offered
in Events Only mode). During an event Axle also sends its own export command to
the inverter through the GivEnergy cloud; octoopt2 plans the same thing (full
battery before, max export during) via a per-slot price uplift and holds that
mode through the event so the two never fight.

Only the next event is returned, so after each fetch any *other* future row is
treated as cancelled/rescheduled and removed; past rows are kept as history.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import requests

from ..config import AxleConfig
from ..db import get_conn, record_fetch
from .rates import parse_ts

logger = logging.getLogger(__name__)

AXLE_URL = "https://api.axle.energy/vpp/home-assistant/event"
SLOT = timedelta(minutes=30)


class AxleAuthError(RuntimeError):
    """The token was rejected (HTTP 401/403)."""


@dataclass(frozen=True)
class AxleEvent:
    start: datetime          # UTC aware
    end: datetime            # UTC aware, exclusive
    direction: str           # "export" | "import"
    updated_at: datetime | None = None

    def overlaps(self, slot_start: datetime) -> bool:
        """Does the half-hour slot [slot_start, +30 min) overlap the event?"""
        s = slot_start.astimezone(timezone.utc)
        return s < self.end and s + SLOT > self.start

    def active(self, now: datetime, margin_minutes: int = 0) -> bool:
        m = timedelta(minutes=margin_minutes)
        now = now.astimezone(timezone.utc)
        return self.start - m <= now < self.end + m


def _parse_ts(s) -> datetime | None:
    if not s or not isinstance(s, str):
        return None
    return parse_ts(s)


def parse_event(payload) -> AxleEvent | None:
    """Turn the API payload into an AxleEvent, or None when there is no event.

    Accepts ``None``, ``{}``, ``[]`` and payloads missing the time fields as
    "no event". A list is taken to mean its first element.
    """
    if isinstance(payload, list):
        payload = payload[0] if payload else None
    if not isinstance(payload, dict):
        return None
    start = _parse_ts(payload.get("start_time"))
    end = _parse_ts(payload.get("end_time"))
    if start is None or end is None or end <= start:
        return None
    direction = str(payload.get("import_export") or "export").lower()
    if direction not in ("export", "import"):
        direction = "export"
    return AxleEvent(start=start, end=end, direction=direction,
                     updated_at=_parse_ts(payload.get("updated_at")))


def fetch_event(cfg: AxleConfig, session=None) -> AxleEvent | None:
    http = session or requests
    resp = http.get(AXLE_URL, headers={"Authorization": f"Bearer {cfg.token}"}, timeout=10)
    if resp.status_code in (401, 403):
        raise AxleAuthError(f"Axle rejected the token (HTTP {resp.status_code})")
    resp.raise_for_status()
    try:
        payload = resp.json()
    except ValueError:
        payload = None
    return parse_event(payload)


def store_event(db_path: str, event: AxleEvent | None, now: datetime | None = None) -> None:
    """Upsert the next event and prune other *not yet started* rows.

    The API returns only the next event, so once it moves on to the following
    one the previous row is stale — unless that event is in progress: the feed
    stops returning it before it ends, and dropping it would release the hold
    (and the price uplift) mid-event. Started rows are left alone.
    """
    now = now or datetime.now(timezone.utc)
    with get_conn(db_path) as conn:
        keep = None
        if event is not None:
            keep = event.start.isoformat()
            conn.execute(
                """
                INSERT INTO axle_events (start_utc, end_utc, direction, updated_at, fetched_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(start_utc) DO UPDATE SET
                    end_utc    = excluded.end_utc,
                    direction  = excluded.direction,
                    updated_at = excluded.updated_at,
                    fetched_at = excluded.fetched_at
                """,
                (keep, event.end.isoformat(), event.direction,
                 event.updated_at.isoformat() if event.updated_at else None, now.isoformat()),
            )
        conn.execute(
            "DELETE FROM axle_events WHERE start_utc > ? AND start_utc != ?",
            (now.isoformat(), keep or ""),
        )


def fetch_and_store_axle(cfg: AxleConfig, db_path: str, now: datetime | None = None, session=None) -> int:
    """Poll Axle and store the next event. Returns 1 if an event is known, else 0.

    No-op when the feature is disabled (no token). A rejected token raises
    AxleAuthError and leaves the feed unstamped so the dashboard shows it stale.
    """
    if not cfg.enabled:
        return 0
    now = now or datetime.now(timezone.utc)
    event = fetch_event(cfg, session=session)
    store_event(db_path, event, now=now)
    record_fetch(db_path, "axle", now=now)
    if event is None:
        logger.info("Axle: no upcoming event")
        return 0
    logger.info("Axle: next %s event %s → %s", event.direction,
                event.start.isoformat(timespec="minutes"), event.end.isoformat(timespec="minutes"))
    return 1


def get_events(db_path: str, from_dt: datetime, to_dt: datetime) -> list[AxleEvent]:
    """Events overlapping [from_dt, to_dt), oldest first."""
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT start_utc, end_utc, direction, updated_at FROM axle_events "
            "WHERE end_utc > ? AND start_utc < ? ORDER BY start_utc",
            (from_dt.astimezone(timezone.utc).isoformat(), to_dt.astimezone(timezone.utc).isoformat()),
        ).fetchall()
    return [
        AxleEvent(start=_parse_ts(r["start_utc"]), end=_parse_ts(r["end_utc"]),
                  direction=r["direction"], updated_at=_parse_ts(r["updated_at"]))
        for r in rows
    ]


def next_event(db_path: str, now: datetime) -> AxleEvent | None:
    evs = get_events(db_path, now, now + timedelta(days=14))
    return evs[0] if evs else None


def active_event(db_path: str, now: datetime, margin_minutes: int = 0) -> AxleEvent | None:
    m = timedelta(minutes=margin_minutes)
    for ev in get_events(db_path, now - m, now + m + timedelta(seconds=1)):
        if ev.active(now, margin_minutes):
            return ev
    return None


def slot_uplift(events: list[AxleEvent], slot_starts: list[datetime], cfg: AxleConfig) -> list[float]:
    """Signed £/kWh uplift per slot: +export rate on sell for export events,
    −import rate on buy for import events, 0 elsewhere. Pure."""
    out = []
    for s in slot_starts:
        u = 0.0
        for ev in events:
            if ev.overlaps(s):
                u = cfg.export_gbp_kwh if ev.direction == "export" else -cfg.import_gbp_kwh
                break
        out.append(u)
    return out
