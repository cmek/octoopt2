"""Tests for octoopt2.backtest.tariffs (no network)."""

from datetime import date, datetime, timedelta, timezone

import pytest
import requests

from octoopt2.backtest import tariffs as T
from octoopt2.backtest.tariffs import (
    DEFAULT_PAIRS,
    RateRecord,
    TariffPair,
    ensure_cached,
    expand_rates,
    expand_standing,
    fetch_rates,
    load_price_series,
    load_records,
    london_day_slots,
    parse_pair_spec,
    tariff_code,
)

UTC = timezone.utc


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def rec(vf, vt, pence, pm=None) -> RateRecord:
    return RateRecord(valid_from=vf, valid_to=vt, value_gbp=pence / 100.0, payment_method=pm)


CHEAP, STD = 12.96, 26.42


def cosy_records_across_transition() -> list[RateRecord]:
    """Cosy-style bands across the 2026-10-25 BST->GMT clock change.

    Octopus publishes bands already shifted to UTC: the 04:00-07:00 local cheap
    band is 03:00Z-06:00Z during BST (24th) and 04:00Z-07:00Z during GMT (25th).
    """
    return [
        rec(utc(2026, 10, 23, 23), utc(2026, 10, 24, 3), STD),
        rec(utc(2026, 10, 24, 3), utc(2026, 10, 24, 6), CHEAP),
        rec(utc(2026, 10, 24, 6), utc(2026, 10, 25, 4), STD),
        rec(utc(2026, 10, 25, 4), utc(2026, 10, 25, 7), CHEAP),
        rec(utc(2026, 10, 25, 7), utc(2026, 10, 26, 0), STD),
    ]


# ---------------------------------------------------------------------------
# london_day_slots
# ---------------------------------------------------------------------------


def test_london_day_slots_normal_day():
    slots = london_day_slots(date(2026, 9, 1))
    assert len(slots) == 48
    assert slots[0] == utc(2026, 8, 31, 23)  # BST midnight
    assert slots[-1] == utc(2026, 9, 1, 22, 30)
    assert all(s.tzinfo is not None and s.utcoffset() == timedelta(0) for s in slots)


def test_london_day_slots_fall_back_has_50():
    slots = london_day_slots(date(2026, 10, 25))
    assert len(slots) == 50
    assert slots[0] == utc(2026, 10, 24, 23)
    assert slots[-1] == utc(2026, 10, 25, 23, 30)


def test_london_day_slots_spring_forward_has_46():
    slots = london_day_slots(date(2026, 3, 29))
    assert len(slots) == 46
    assert slots[0] == utc(2026, 3, 29, 0)
    assert slots[-1] == utc(2026, 3, 29, 22, 30)


# ---------------------------------------------------------------------------
# expand_rates
# ---------------------------------------------------------------------------


def test_expand_rates_cosy_bands_across_transition():
    recs = cosy_records_across_transition()
    slots = [utc(2026, 10, 25, 3, 30), utc(2026, 10, 25, 4), utc(2026, 10, 25, 6, 30),
             utc(2026, 10, 25, 7), utc(2026, 10, 24, 3), utc(2026, 10, 24, 5, 30), utc(2026, 10, 24, 6)]
    vals = expand_rates(recs, slots)
    assert vals == pytest.approx([STD / 100, CHEAP / 100, CHEAP / 100, STD / 100,
                                  CHEAP / 100, CHEAP / 100, STD / 100])


def test_expand_rates_whole_london_day_covered_on_transition():
    recs = cosy_records_across_transition()
    slots = london_day_slots(date(2026, 10, 25))
    vals = expand_rates(recs, slots)
    assert len(vals) == 50
    assert None not in vals
    assert sum(1 for v in vals if v == pytest.approx(CHEAP / 100)) == 6  # 3h cheap band


def test_expand_rates_uncovered_is_none():
    recs = [rec(utc(2026, 1, 1), utc(2026, 1, 2), STD)]
    vals = expand_rates(recs, [utc(2025, 12, 31, 23, 30), utc(2026, 1, 1), utc(2026, 1, 1, 23, 30),
                               utc(2026, 1, 2), utc(2026, 1, 5)])
    assert vals == [None, pytest.approx(STD / 100), pytest.approx(STD / 100), None, None]


def test_expand_rates_open_ended_covers_far_future():
    recs = [rec(utc(2022, 2, 1), None, 4.1)]
    vals = expand_rates(recs, [utc(2022, 1, 31, 23, 30), utc(2026, 9, 1, 12), utc(2099, 1, 1)])
    assert vals == [None, pytest.approx(0.041), pytest.approx(0.041)]


def test_expand_rates_prefers_direct_debit():
    vf, vt = utc(2026, 6, 30, 23), utc(2026, 9, 30, 23)
    recs = [
        rec(vf, vt, 27.58, "NON_DIRECT_DEBIT"),
        rec(vf, vt, 26.13, "DIRECT_DEBIT"),
    ]
    assert expand_rates(recs, [utc(2026, 8, 1)]) == [pytest.approx(0.2613)]
    # order independent
    assert expand_rates(list(reversed(recs)), [utc(2026, 8, 1)]) == [pytest.approx(0.2613)]


def test_expand_rates_asserts_half_hour_boundaries():
    with pytest.raises(AssertionError):
        expand_rates([rec(utc(2026, 1, 1, 0, 15), None, 10.0)], [utc(2026, 1, 1)])


def test_expand_rates_empty_records():
    assert expand_rates([], [utc(2026, 1, 1)]) == [None]


# ---------------------------------------------------------------------------
# expand_standing
# ---------------------------------------------------------------------------


def test_expand_standing_keyed_by_london_day_across_transition():
    recs = [
        rec(utc(2026, 10, 24, 23), utc(2026, 10, 26, 0), 42.79),   # covers London 25th midnight (23:00Z 24th)
        rec(utc(2026, 10, 26, 0), None, 50.0),                     # from London 26th midnight (00:00Z)
    ]
    days = [date(2026, 10, 24), date(2026, 10, 25), date(2026, 10, 26), date(2026, 10, 27)]
    out = expand_standing(recs, days)
    assert date(2026, 10, 24) not in out  # 23:00Z on 23rd not covered
    assert out[date(2026, 10, 25)] == pytest.approx(0.4279)
    assert out[date(2026, 10, 26)] == pytest.approx(0.50)
    assert out[date(2026, 10, 27)] == pytest.approx(0.50)


def test_expand_standing_prefers_direct_debit():
    recs = [
        rec(utc(2026, 1, 1), None, 52.08, "NON_DIRECT_DEBIT"),
        rec(utc(2026, 1, 1), None, 44.32, "DIRECT_DEBIT"),
    ]
    assert expand_standing(recs, [date(2026, 9, 1)]) == {date(2026, 9, 1): pytest.approx(0.4432)}


# ---------------------------------------------------------------------------
# fetch_rates (fake session)
# ---------------------------------------------------------------------------


class FakeResp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.HTTPError(f"{self.status_code}")
            err.response = self
            raise err

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, pages: dict):
        self.pages = pages
        self.calls = []

    def get(self, url, params=None, auth=None, timeout=None):
        self.calls.append((url, params, auth))
        if url not in self.pages:
            return FakeResp({"detail": "Not found."}, 404)
        return FakeResp(self.pages[url])


def test_fetch_rates_paginates_and_formats_period():
    code = tariff_code("COSY-22-12-08", "G")
    base = f"{T.API_BASE}/products/COSY-22-12-08/electricity-tariffs/{code}/standard-unit-rates/"
    page2 = base + "?page=2&period_from=x"
    sess = FakeSession({
        base: {"results": [{"value_inc_vat": 1}], "next": page2},
        page2: {"results": [{"value_inc_vat": 2}], "next": None},
    })
    out = fetch_rates("COSY-22-12-08", "G", "unit", utc(2026, 9, 1), utc(2026, 9, 2, 12, 30),
                      api_key="k", session=sess)
    assert [r["value_inc_vat"] for r in out] == [1, 2]
    assert sess.calls[0][1] == {"period_from": "2026-09-01T00:00Z", "period_to": "2026-09-02T12:30Z", "page_size": 1500}
    assert sess.calls[0][2] == ("k", "")
    assert sess.calls[1][1] is None  # next url carries its own query


def test_fetch_rates_standing_path_and_404():
    code = tariff_code("X", "G")
    sess = FakeSession({})
    with pytest.raises(requests.HTTPError):
        fetch_rates("X", "G", "standing", utc(2026, 9, 1), utc(2026, 9, 2), session=sess)
    assert sess.calls[0][0].endswith(f"{code}/standing-charges/")


# ---------------------------------------------------------------------------
# cache
# ---------------------------------------------------------------------------


COSY_RAW = [
    {"value_exc_vat": 25.16, "value_inc_vat": 26.42, "valid_from": "2026-09-01T06:00:00Z",
     "valid_to": "2026-09-01T12:00:00Z"},
    {"value_exc_vat": 12.35, "value_inc_vat": 12.96, "valid_from": "2026-09-01T03:00:00Z",
     "valid_to": "2026-09-01T06:00:00Z"},
    {"value_exc_vat": 25.16, "value_inc_vat": 26.42, "valid_from": "2026-08-31T23:00:00Z",
     "valid_to": "2026-09-01T03:00:00Z", "payment_method": "DIRECT_DEBIT"},
]
SEG_RAW = [
    {"value_exc_vat": 4.1, "value_inc_vat": 4.1, "valid_from": "2022-02-01T00:00:00Z", "valid_to": None},
]
STANDING_RAW = [
    {"value_exc_vat": 40.76, "value_inc_vat": 42.79, "valid_from": "2026-03-31T23:00:00Z", "valid_to": None,
     "payment_method": "DIRECT_DEBIT"},
    {"value_exc_vat": 48.0, "value_inc_vat": 52.08, "valid_from": "2026-03-31T23:00:00Z", "valid_to": None,
     "payment_method": "NON_DIRECT_DEBIT"},
]


def raw_for(product_code, kind):
    if kind == "standing":
        return STANDING_RAW
    if product_code.startswith("OUTGOING"):
        return SEG_RAW
    return COSY_RAW


@pytest.fixture
def fake_fetch(monkeypatch):
    calls = []

    def fake(product_code, region, kind, period_from, period_to, api_key=None, session=None):
        calls.append((product_code, region, kind, period_from, period_to))
        return [dict(r) for r in raw_for(product_code, kind)]

    monkeypatch.setattr(T, "fetch_rates", fake)
    return calls


def test_ensure_cached_fetches_once_then_uses_cache(tmp_path, fake_fetch):
    db = str(tmp_path / "t.db")
    pf, pt = utc(2026, 9, 1), utc(2026, 9, 2)
    ensure_cached(db, "COSY-22-12-08", "G", "unit", pf, pt)
    assert len(fake_fetch) == 1
    ensure_cached(db, "COSY-22-12-08", "G", "unit", pf, pt)
    assert len(fake_fetch) == 1
    # sub-range is covered too
    ensure_cached(db, "COSY-22-12-08", "G", "unit", utc(2026, 9, 1, 6), utc(2026, 9, 1, 12))
    assert len(fake_fetch) == 1
    # wider range is not
    ensure_cached(db, "COSY-22-12-08", "G", "unit", pf, utc(2026, 9, 3))
    assert len(fake_fetch) == 2
    # different kind / region are separate
    ensure_cached(db, "COSY-22-12-08", "G", "standing", pf, pt)
    ensure_cached(db, "COSY-22-12-08", "H", "unit", pf, pt)
    assert len(fake_fetch) == 4
    assert fake_fetch[2][2] == "standing"


def test_ensure_cached_refresh_refetches(tmp_path, fake_fetch):
    db = str(tmp_path / "t.db")
    pf, pt = utc(2026, 9, 1), utc(2026, 9, 2)
    ensure_cached(db, "COSY-22-12-08", "G", "unit", pf, pt)
    ensure_cached(db, "COSY-22-12-08", "G", "unit", pf, pt, refresh=True)
    assert len(fake_fetch) == 2
    # refresh does not duplicate rows
    recs = load_records(db, tariff_code("COSY-22-12-08", "G"), "unit")
    assert len(recs) == 3


def test_records_round_trip_sorted(tmp_path, fake_fetch):
    db = str(tmp_path / "t.db")
    ensure_cached(db, "COSY-22-12-08", "G", "unit", utc(2026, 9, 1), utc(2026, 9, 2))
    recs = load_records(db, tariff_code("COSY-22-12-08", "G"), "unit")
    assert [r.valid_from for r in recs] == sorted(r.valid_from for r in recs)
    assert recs[0].valid_from == utc(2026, 8, 31, 23)
    assert recs[0].payment_method == "DIRECT_DEBIT"
    assert recs[1].valid_from == utc(2026, 9, 1, 3)
    assert recs[1].valid_to == utc(2026, 9, 1, 6)
    assert recs[1].value_gbp == pytest.approx(0.1296)
    assert recs[1].payment_method is None
    assert recs[2].valid_to == utc(2026, 9, 1, 12)
    seg = load_records(db, tariff_code("OUTGOING-SEG-FIX-12M-20-07-07", "G"), "unit")
    assert seg == []
    ensure_cached(db, "OUTGOING-SEG-FIX-12M-20-07-07", "G", "unit", utc(2026, 9, 1), utc(2026, 9, 2))
    seg = load_records(db, tariff_code("OUTGOING-SEG-FIX-12M-20-07-07", "G"), "unit")
    assert len(seg) == 1 and seg[0].valid_to is None and seg[0].value_gbp == pytest.approx(0.041)
    assert all(r.valid_from.tzinfo is not None for r in recs)
    assert load_records(db, tariff_code("OTHER", "G"), "unit") == []


def test_ensure_cached_empty_fetch_records_no_coverage(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(T, "fetch_rates", lambda *a, **k: (calls.append(1), [])[1])
    db = str(tmp_path / "t.db")
    ensure_cached(db, "X", "G", "unit", utc(2026, 9, 1), utc(2026, 9, 2))
    ensure_cached(db, "X", "G", "unit", utc(2026, 9, 1), utc(2026, 9, 2))
    assert len(calls) == 2


# ---------------------------------------------------------------------------
# load_price_series
# ---------------------------------------------------------------------------


def slots_sept(n=4):
    return [utc(2026, 9, 1, 3) + i * timedelta(minutes=30) for i in range(n)]


def test_load_price_series_db_fallbacks(tmp_path, fake_fetch):
    db = str(tmp_path / "t.db")
    slots = slots_sept()
    ps = load_price_series(db, DEFAULT_PAIRS["agile"], slots, "G",
                           db_buy=[0.1, 0.2, 0.3, 0.4], db_sell=[0.01, 0.02, 0.03, 0.04])
    assert ps.buy == [0.1, 0.2, 0.3, 0.4]
    assert ps.sell == [0.01, 0.02, 0.03, 0.04]
    assert ps.standing_by_day == {}
    assert fake_fetch == []


def test_load_price_series_sell_fixed_override(tmp_path, fake_fetch):
    db = str(tmp_path / "t.db")
    slots = slots_sept()
    pair = TariffPair("x", None, None, sell_fixed=0.15)
    ps = load_price_series(db, pair, slots, "G", db_buy=[0.1] * 4, db_sell=[0.01] * 4)
    assert ps.sell == [0.15] * 4
    assert ps.buy == [0.1] * 4


def test_load_price_series_products(tmp_path, fake_fetch):
    db = str(tmp_path / "t.db")
    slots = slots_sept()  # 03:00Z-05:00Z, inside the cheap band in RAW
    pair = TariffPair("p", "COSY-22-12-08", "OUTGOING-SEG-FIX-12M-20-07-07")
    ps = load_price_series(db, pair, slots, "G", db_buy=[], db_sell=[])
    assert ps.buy == pytest.approx([0.1296] * 4)
    assert ps.sell == pytest.approx([0.041] * 4)  # open-ended 4.1p SEG record
    assert ps.standing_by_day == {date(2026, 9, 1): pytest.approx(0.4279)}  # DIRECT_DEBIT preferred
    assert ps.notes == []
    kinds = sorted((c[0], c[2]) for c in fake_fetch)
    assert kinds == [("COSY-22-12-08", "standing"), ("COSY-22-12-08", "unit"),
                     ("OUTGOING-SEG-FIX-12M-20-07-07", "unit")]


def test_load_price_series_uncovered_raises_naming_product(tmp_path, fake_fetch):
    db = str(tmp_path / "t.db")
    slots = [utc(2026, 8, 31, 22), utc(2026, 8, 31, 22, 30), utc(2026, 9, 1, 3), utc(2026, 9, 1, 12)]
    pair = TariffPair("p", "COSY-22-12-08", None, sell_fixed=0.1)
    with pytest.raises(ValueError) as ei:
        load_price_series(db, pair, slots, "G", db_buy=[], db_sell=[])
    msg = str(ei.value)
    assert "COSY-22-12-08" in msg
    assert "first uncovered 2026-08-31T22:00:00+00:00" in msg
    assert "last uncovered 2026-09-01T12:00:00+00:00" in msg
    assert "3/4" in msg


def test_load_price_series_export_404_falls_back_to_sell_fixed(tmp_path, monkeypatch):
    def fake(product_code, region, kind, period_from, period_to, api_key=None, session=None):
        if product_code.startswith("OUTGOING"):
            err = requests.HTTPError("404")
            err.response = FakeResp({}, 404)
            raise err
        return [dict(r) for r in raw_for(product_code, kind)]

    monkeypatch.setattr(T, "fetch_rates", fake)
    db = str(tmp_path / "t.db")
    pair = TariffPair("p", "COSY-22-12-08", "OUTGOING-NOPE", sell_fixed=0.15)
    ps = load_price_series(db, pair, slots_sept(), "G", db_buy=[], db_sell=[])
    assert ps.sell == [0.15] * 4
    assert any("OUTGOING-NOPE" in n and "sell_fixed" in n for n in ps.notes)

    # without sell_fixed the error propagates
    with pytest.raises(requests.HTTPError):
        load_price_series(db, TariffPair("p", "COSY-22-12-08", "OUTGOING-NOPE"), slots_sept(), "G",
                          db_buy=[], db_sell=[])


def test_load_price_series_missing_standing_adds_note(tmp_path, monkeypatch):
    def fake(product_code, region, kind, period_from, period_to, api_key=None, session=None):
        if kind == "standing":
            return [{"value_inc_vat": 42.79, "valid_from": "2026-09-01T23:00:00Z", "valid_to": None}]
        return [{"value_inc_vat": 20.0, "valid_from": "2026-01-01T00:00:00Z", "valid_to": None}]

    monkeypatch.setattr(T, "fetch_rates", fake)
    db = str(tmp_path / "t.db")
    pair = TariffPair("p", "VAR-22-11-01", None, sell_fixed=0.0)
    slots = london_day_slots(date(2026, 9, 1)) + london_day_slots(date(2026, 9, 2))
    ps = load_price_series(db, pair, slots, "G", db_buy=[], db_sell=[])
    assert ps.buy == [0.2] * 96
    assert ps.sell == [0.0] * 96
    assert ps.standing_by_day == {date(2026, 9, 2): pytest.approx(0.4279)}
    assert any("VAR-22-11-01" in n and "standing" in n and "2026-09-01" in n for n in ps.notes)


def test_load_price_series_db_length_mismatch(tmp_path, fake_fetch):
    db = str(tmp_path / "t.db")
    with pytest.raises(ValueError):
        load_price_series(db, DEFAULT_PAIRS["agile"], slots_sept(), "G", db_buy=[0.1], db_sell=[0.1] * 4)


# ---------------------------------------------------------------------------
# parse_pair_spec
# ---------------------------------------------------------------------------


def test_parse_pair_spec_default_names():
    for name, pair in DEFAULT_PAIRS.items():
        assert parse_pair_spec(name) is pair
    assert parse_pair_spec("flux").import_product == "FLUX-IMPORT-23-02-14"


def test_parse_pair_spec_explicit():
    p = parse_pair_spec("mine:COSY-22-12-08/AGILE-OUTGOING-19-05-13")
    assert p == TariffPair("mine", "COSY-22-12-08", "AGILE-OUTGOING-19-05-13", None)
    p = parse_pair_spec("c15:COSY-22-12-08/fixed=0.15")
    assert p == TariffPair("c15", "COSY-22-12-08", None, 0.15)
    p = parse_pair_spec("dbx:db/FLUX-EXPORT-23-02-14")
    assert p == TariffPair("dbx", None, "FLUX-EXPORT-23-02-14", None)
    p = parse_pair_spec("dbdb:db/db")
    assert p == TariffPair("dbdb", None, None, None)


@pytest.mark.parametrize("bad", ["nope", "x:COSY", "x:/A", ":A/B", "x:A/fixed=abc"])
def test_parse_pair_spec_errors(bad):
    with pytest.raises(ValueError):
        parse_pair_spec(bad)


def test_tariff_code():
    assert tariff_code("COSY-22-12-08", "G") == "E-1R-COSY-22-12-08-G"


def test_parse_ts():
    assert T.parse_ts("2026-09-01T15:00:00Z") == utc(2026, 9, 1, 15)
    assert T.parse_ts("2026-09-01T15:00:00+01:00") == utc(2026, 9, 1, 14)
    assert T.parse_ts(None) is None
