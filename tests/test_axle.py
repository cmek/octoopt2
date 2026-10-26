"""Axle Energy event feed: parsing, storage, uplift (no network)."""
from datetime import datetime, timedelta, timezone

import pytest

from octoopt2.config import AxleConfig
from octoopt2.data import axle as A
from octoopt2.db import feed_freshness, get_conn, init_db

UTC = timezone.utc
CFG = AxleConfig(token="tok")


def utc(*a):
    return datetime(*a, tzinfo=UTC)


@pytest.fixture
def db(tmp_path):
    p = str(tmp_path / "t.db")
    init_db(p)
    return p


class FakeResp:
    def __init__(self, status=200, payload=None):
        self.status_code = status
        self._payload = payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def _session(monkeypatch, *responses):
    calls = []
    it = iter(responses)

    class S:
        @staticmethod
        def get(url, headers=None, timeout=None):
            calls.append((url, headers))
            return next(it)
    return S, calls


FIXTURE = {"start_time": "2026-11-12T17:00:00Z", "end_time": "2026-11-12T18:30:00Z",
           "import_export": "export", "updated_at": "2026-11-11T15:02:11Z"}


def test_parse_event_fixture():
    ev = A.parse_event(FIXTURE)
    assert ev == A.AxleEvent(start=utc(2026, 11, 12, 17), end=utc(2026, 11, 12, 18, 30),
                             direction="export", updated_at=utc(2026, 11, 11, 15, 2, 11))


def test_parse_empty_returns_none():
    for payload in (None, {}, [], {"start_time": None}, {"start_time": "2026-11-12T17:00:00Z"},
                    {"start_time": "2026-11-12T18:00:00Z", "end_time": "2026-11-12T17:00:00Z"}):
        assert A.parse_event(payload) is None
    assert A.parse_event([FIXTURE]).direction == "export"


def test_store_upserts_same_start_and_prunes_other_future_rows(db, monkeypatch):
    now = utc(2026, 11, 11, 16)
    a = {**FIXTURE}
    b = {**FIXTURE, "start_time": "2026-11-13T17:00:00Z", "end_time": "2026-11-13T18:00:00Z"}
    a2 = {**FIXTURE, "end_time": "2026-11-12T19:00:00Z"}
    # a past row survives pruning
    with get_conn(db) as conn:
        conn.execute("INSERT INTO axle_events VALUES (?,?,?,?,?)",
                     ("2026-11-01T17:00:00+00:00", "2026-11-01T18:00:00+00:00", "export", None, "x"))
    S, calls = _session(monkeypatch, FakeResp(200, a), FakeResp(200, a2), FakeResp(200, b))
    A.fetch_and_store_axle(CFG, db, now=now, session=S)
    A.fetch_and_store_axle(CFG, db, now=now, session=S)      # same start → updated end
    rows = A.get_events(db, now, now + timedelta(days=5))
    assert len(rows) == 1 and rows[0].end == utc(2026, 11, 12, 19)
    A.fetch_and_store_axle(CFG, db, now=now, session=S)      # new next event → old future row pruned
    rows = A.get_events(db, now, now + timedelta(days=5))
    assert [r.start for r in rows] == [utc(2026, 11, 13, 17)]
    with get_conn(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM axle_events").fetchone()[0] == 2  # past + next
    assert calls[0][1] == {"Authorization": "Bearer tok"}


def test_401_raises_auth_error_and_no_feed_stamp(db, monkeypatch):
    S, _ = _session(monkeypatch, FakeResp(401))
    with pytest.raises(A.AxleAuthError):
        A.fetch_and_store_axle(CFG, db, now=utc(2026, 11, 11), session=S)
    with get_conn(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM feed_fetches WHERE feed='axle'").fetchone()[0] == 0


def test_no_token_disabled_no_network_no_alarm(db, monkeypatch):
    cfg = AxleConfig(token=None)
    assert cfg.enabled is False

    def boom(*a, **k):
        raise AssertionError("network call with Axle disabled")
    monkeypatch.setattr(A.requests, "get", boom)
    assert A.fetch_and_store_axle(cfg, db) == 0
    assert feed_freshness(db, utc(2026, 11, 11))["axle"]["fetch_age_s"] is None


def test_no_event_response_stamps_feed(db, monkeypatch):
    S, _ = _session(monkeypatch, FakeResp(200, {}))
    now = utc(2026, 11, 11, 16)
    assert A.fetch_and_store_axle(CFG, db, now=now, session=S) == 0
    assert feed_freshness(db, now)["axle"]["fetch_age_s"] == 0


def test_uplift_only_on_overlapping_slots():
    ev = A.AxleEvent(start=utc(2026, 11, 12, 17, 10), end=utc(2026, 11, 12, 18, 20), direction="export")
    slots = [utc(2026, 11, 12, 16) + timedelta(minutes=30 * i) for i in range(8)]
    up = A.slot_uplift([ev], slots, CFG)
    assert up == [0, 0, 1.0, 1.0, 1.0, 0, 0, 0]


def test_uplift_bst_offset_and_fallback_day():
    # 18:00–19:00 BST on 15 June = 17:00–18:00Z
    ev = A.parse_event({"start_time": "2026-06-15T18:00:00+01:00", "end_time": "2026-06-15T19:00:00+01:00",
                        "import_export": "export"})
    slots = [utc(2026, 6, 15, 16) + timedelta(minutes=30 * i) for i in range(6)]
    assert A.slot_uplift([ev], slots, CFG) == [0, 0, 1.0, 1.0, 0, 0]
    # fall-back day: 01:30 GMT on 25 Oct (after the clocks go back) = 01:30Z
    ev2 = A.parse_event({"start_time": "2026-10-25T01:30:00+00:00", "end_time": "2026-10-25T02:30:00+00:00",
                         "import_export": "export"})
    slots2 = [utc(2026, 10, 25, 0) + timedelta(minutes=30 * i) for i in range(6)]
    assert A.slot_uplift([ev2], slots2, CFG) == [0, 0, 0, 1.0, 1.0, 0]


def test_import_event_negative_uplift():
    ev = A.AxleEvent(start=utc(2026, 11, 12, 3), end=utc(2026, 11, 12, 4), direction="import")
    slots = [utc(2026, 11, 12, 2, 30) + timedelta(minutes=30 * i) for i in range(4)]
    assert A.slot_uplift([ev], slots, AxleConfig(token="t", import_gbp_kwh=0.5)) == [0, -0.5, -0.5, 0]


def test_active_event_with_margin(db):
    A.store_event(db, A.AxleEvent(start=utc(2026, 11, 12, 17), end=utc(2026, 11, 12, 18), direction="export"),
                  now=utc(2026, 11, 11))
    assert A.active_event(db, utc(2026, 11, 12, 16, 55)) is None
    assert A.active_event(db, utc(2026, 11, 12, 16, 55), margin_minutes=10) is not None
    assert A.active_event(db, utc(2026, 11, 12, 17, 30)) is not None
    assert A.active_event(db, utc(2026, 11, 12, 18, 0)) is None
