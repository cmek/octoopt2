"""Live price feed: banded (Cosy) and half-hourly (Agile) records → half-hour rows."""

from datetime import date, datetime, timedelta, timezone

import pytest

from octoopt2.config import OctopusConfig
from octoopt2.data import octopus as O
from octoopt2.data.rates import drop_off_boundary, expand_records, record_from_api, window_slots
from octoopt2.db import get_conn, init_db

UTC = timezone.utc


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def api(vf: datetime, vt: datetime | None, pence: float) -> dict:
    return {
        "valid_from": vf.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "valid_to": None if vt is None else vt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "value_inc_vat": pence,
        "value_exc_vat": round(pence / 1.05, 4),
        "payment_method": None,
    }


CHEAP, STD, PEAK = 14.0651, 28.6808, 43.0212


def cosy_day(d: date) -> list[dict]:
    """Cosy bands for London day ``d`` in GMT (Nov onwards): the API returns every
    record overlapping the 23:00–23:00 window, including the 22:00–04:00 band
    that starts before it."""
    b = datetime(d.year, d.month, d.day, tzinfo=UTC)
    return [
        api(b - timedelta(hours=2), b + timedelta(hours=4), CHEAP),   # 22:00 prev → 04:00
        api(b + timedelta(hours=4), b + timedelta(hours=7), CHEAP),
        api(b + timedelta(hours=7), b + timedelta(hours=13), STD),
        api(b + timedelta(hours=13), b + timedelta(hours=16), CHEAP),
        api(b + timedelta(hours=16), b + timedelta(hours=19), PEAK),
        api(b + timedelta(hours=19), b + timedelta(hours=22), STD),
        api(b + timedelta(hours=22), b + timedelta(hours=28), CHEAP),  # 22:00 → 04:00 next
    ]


def agile_window(d: date, pence: float = 20.0) -> list[dict]:
    pf, pt = O._pricing_window(d)
    out, t = [], pf
    while t < pt:
        out.append(api(t, t + timedelta(minutes=30), pence))
        t += timedelta(minutes=30)
    return out


def cfg() -> OctopusConfig:
    return OctopusConfig(
        api_key="k", account_number="a", mpan="m", serial="s",
        import_tariff_code="E-1R-COSY-22-12-08-G",
        outgoing_tariff_code="E-1R-AGILE-OUTGOING-19-05-13-G",
        dno_region="G",
    )


@pytest.fixture
def db(tmp_path):
    p = str(tmp_path / "t.db")
    init_db(p)
    return p


def _rows(db):
    with get_conn(db) as conn:
        return conn.execute(
            "SELECT slot_start, buy_gbp_kwh, sell_gbp_kwh, buy_exc_vat_gbp_kwh "
            "FROM prices ORDER BY slot_start"
        ).fetchall()


def _patch_fetch(monkeypatch, by_tariff: dict[str, list[dict]]):
    def fake(tariff_code, product_code, period_from, period_to, api_key):
        assert tariff_code.endswith("-G")
        return by_tariff.get(tariff_code, [])
    monkeypatch.setattr(O, "_fetch_unit_rates", fake)


# ── window_slots ─────────────────────────────────────────────────────────────

def test_window_slots_normal_day_48():
    assert len(window_slots(*O._pricing_window(date(2026, 11, 10)))) == 48


def test_window_slots_fall_back_50():
    assert len(window_slots(*O._pricing_window(date(2026, 10, 25)))) == 50


def test_window_slots_spring_forward_46():
    assert len(window_slots(*O._pricing_window(date(2026, 3, 29)))) == 46


# ── fetch_and_store_prices ───────────────────────────────────────────────────

def test_cosy_bands_expand_to_48_rows(db, monkeypatch):
    d = date(2026, 11, 10)
    _patch_fetch(monkeypatch, {
        "E-1R-COSY-22-12-08-G": cosy_day(d),
        "E-1R-AGILE-OUTGOING-19-05-13-G": agile_window(d, 15.0),
    })
    n = O.fetch_and_store_prices(cfg(), db, for_date=d)
    rows = _rows(db)
    assert n == 48 and len(rows) == 48
    by_hour = {datetime.fromisoformat(r["slot_start"]).hour: r for r in rows}
    assert by_hour[4]["buy_gbp_kwh"] == pytest.approx(CHEAP / 100)
    assert by_hour[6]["buy_gbp_kwh"] == pytest.approx(CHEAP / 100)
    assert by_hour[10]["buy_gbp_kwh"] == pytest.approx(STD / 100)
    assert by_hour[17]["buy_gbp_kwh"] == pytest.approx(PEAK / 100)
    assert by_hour[22]["buy_gbp_kwh"] == pytest.approx(CHEAP / 100)
    assert all(r["sell_gbp_kwh"] == pytest.approx(0.15) for r in rows)
    assert by_hour[17]["buy_exc_vat_gbp_kwh"] == pytest.approx(PEAK / 1.05 / 100, rel=1e-3)


def test_cosy_band_clipped_at_window_edges(db, monkeypatch):
    d = date(2026, 11, 10)
    _patch_fetch(monkeypatch, {"E-1R-COSY-22-12-08-G": cosy_day(d)})
    O.fetch_and_store_prices(cfg(), db, for_date=d)
    rows = _rows(db)
    pf, pt = O._pricing_window(d)
    starts = [datetime.fromisoformat(r["slot_start"]) for r in rows]
    assert starts[0] == pf and starts[-1] == pt - timedelta(minutes=30)
    # the 22:00-prev-day band does not leak rows before the window
    assert all(s >= pf for s in starts)


def test_agile_records_unchanged_by_expansion(db, monkeypatch):
    d = date(2026, 11, 10)
    buy = agile_window(d, 20.0)
    buy[5]["value_inc_vat"] = -1.5  # a negative slot survives as-is
    c = OctopusConfig(api_key="k", account_number="a", mpan="m", serial="s",
                      import_tariff_code="E-1R-AGILE-24-10-01-G",
                      outgoing_tariff_code="E-1R-AGILE-OUTGOING-19-05-13-G", dno_region="G")
    _patch_fetch(monkeypatch, {"E-1R-AGILE-24-10-01-G": buy,
                               "E-1R-AGILE-OUTGOING-19-05-13-G": agile_window(d, 15.0)})
    assert O.fetch_and_store_prices(c, db, for_date=d) == 48
    rows = _rows(db)
    assert [r["buy_gbp_kwh"] for r in rows][5] == pytest.approx(-0.015)
    assert [r["slot_start"] for r in rows] == [O._normalise_slot(r["valid_from"]) for r in buy]


def test_cosy_without_outgoing_stores_null_sell_and_preserves_existing(db, monkeypatch):
    d = date(2026, 11, 10)
    # first fetch has outgoing for the day
    _patch_fetch(monkeypatch, {"E-1R-COSY-22-12-08-G": cosy_day(d),
                               "E-1R-AGILE-OUTGOING-19-05-13-G": agile_window(d, 15.0)})
    O.fetch_and_store_prices(cfg(), db, for_date=d)
    # second fetch: outgoing not published → sell must stay 0.15, not be clobbered
    _patch_fetch(monkeypatch, {"E-1R-COSY-22-12-08-G": cosy_day(d)})
    O.fetch_and_store_prices(cfg(), db, for_date=d)
    assert all(r["sell_gbp_kwh"] == pytest.approx(0.15) for r in _rows(db))
    # a fresh day with no outgoing → NULL sell
    d2 = d + timedelta(days=1)
    _patch_fetch(monkeypatch, {"E-1R-COSY-22-12-08-G": cosy_day(d2)})
    O.fetch_and_store_prices(cfg(), db, for_date=d2)
    pf, _ = O._pricing_window(d2)
    later = [r for r in _rows(db) if datetime.fromisoformat(r["slot_start"]) >= pf]
    assert len(later) == 48 and all(r["sell_gbp_kwh"] is None for r in later)


def test_missing_price_dates_full_cosy_day_not_missing(db, monkeypatch):
    today = datetime.now(O.LONDON).date()
    _patch_fetch(monkeypatch, {"E-1R-COSY-22-12-08-G": cosy_day(today),
                               "E-1R-AGILE-OUTGOING-19-05-13-G": agile_window(today, 15.0)})
    O.fetch_and_store_prices(cfg(), db, for_date=today)
    assert O.missing_price_dates(db, look_ahead_days=2) == [today + timedelta(days=1)]


def test_bad_boundary_record_raises_value_error():
    bad = api(utc(2026, 11, 10, 4, 15), utc(2026, 11, 10, 7), CHEAP)
    with pytest.raises(ValueError, match="COSY"):
        expand_records([record_from_api(bad)], [utc(2026, 11, 10, 4)], label="E-1R-COSY-22-12-08-G")


def test_live_feed_drops_bad_boundary_record_and_keeps_the_rest(db, monkeypatch, caplog):
    d = date(2026, 11, 10)
    recs = cosy_day(d)
    recs[1] = api(utc(2026, 11, 10, 4, 15), utc(2026, 11, 10, 7), CHEAP)  # 04-07 band, off boundary
    _patch_fetch(monkeypatch, {"E-1R-COSY-22-12-08-G": recs,
                               "E-1R-AGILE-OUTGOING-19-05-13-G": agile_window(d, 15.0)})
    with caplog.at_level("ERROR"):
        O.fetch_and_store_prices(cfg(), db, for_date=d)
    assert "not on a 30-min boundary" in caplog.text
    rows = _rows(db)
    assert len(rows) == 48 - 6                                            # the bad band's 6 slots missing
    assert all(r["buy_gbp_kwh"] is not None for r in rows)
    assert len(drop_off_boundary([record_from_api(r) for r in recs], "x")) == len(recs) - 1


# ── config alias ─────────────────────────────────────────────────────────────

def _env(monkeypatch, **extra):
    base = dict(OCTOPUS_API_KEY="k", OCTOPUS_ACCOUNT_NUMBER="a", OCTOPUS_MPAN="m",
                OCTOPUS_SERIAL="s", OCTOPUS_OUTGOING_TARIFF_CODE="E-1R-AGILE-OUTGOING-19-05-13-G",
                OCTOPUS_DNO_REGION="G")
    for k in ("OCTOPUS_IMPORT_TARIFF_CODE", "OCTOPUS_AGILE_TARIFF_CODE"):
        monkeypatch.delenv(k, raising=False)
    for k, v in {**base, **extra}.items():
        monkeypatch.setenv(k, v)


def test_import_tariff_env_new_name(monkeypatch, caplog):
    _env(monkeypatch, OCTOPUS_IMPORT_TARIFF_CODE="E-1R-COSY-22-12-08-G")
    assert OctopusConfig.from_env().import_tariff_code == "E-1R-COSY-22-12-08-G"
    assert "deprecated" not in caplog.text


def test_import_tariff_env_alias_warns(monkeypatch, caplog):
    _env(monkeypatch, OCTOPUS_AGILE_TARIFF_CODE="E-1R-AGILE-24-10-01-G")
    with caplog.at_level("WARNING"):
        assert OctopusConfig.from_env().import_tariff_code == "E-1R-AGILE-24-10-01-G"
    assert "deprecated" in caplog.text


def test_import_tariff_env_missing_names_the_current_variable(monkeypatch):
    _env(monkeypatch)
    with pytest.raises(KeyError, match="OCTOPUS_IMPORT_TARIFF_CODE"):
        OctopusConfig.from_env()


def test_import_tariff_env_new_name_wins(monkeypatch):
    _env(monkeypatch, OCTOPUS_IMPORT_TARIFF_CODE="E-1R-COSY-22-12-08-G",
         OCTOPUS_AGILE_TARIFF_CODE="E-1R-AGILE-24-10-01-G")
    assert OctopusConfig.from_env().import_tariff_code == "E-1R-COSY-22-12-08-G"
