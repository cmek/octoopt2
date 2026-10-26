"""Axle event hold in the scheduler, persistence of the uplift, status block."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from octoopt2 import scheduler, status
from octoopt2.config import AxleConfig, BatteryConfig, DhwConfig, GivEnergyConfig
from octoopt2.control.inverter import decision_to_mode
from octoopt2.data.axle import AxleEvent, store_event
from octoopt2.db import get_conn, init_db
from octoopt2.optimizer.model import OptimizerInput, optimize
from octoopt2.optimizer.schedule import save_schedule
from tests.conftest import synthetic_day

UTC = timezone.utc


def utc(*a):
    return datetime(*a, tzinfo=UTC)


@pytest.fixture
def db(tmp_path):
    p = str(tmp_path / "t.db")
    init_db(p)
    return p


def _config(db, token="tok", margin=0):
    return SimpleNamespace(
        axle=AxleConfig(token=token, hold_margin_minutes=margin), db_path=db,
        battery=BatteryConfig(), givenergy=GivEnergyConfig(host="", max_export_kw=3.6, max_import_kw=11.0),
        dhw=DhwConfig(), melcloud=None,
    )


EVENT = AxleEvent(start=utc(2026, 11, 12, 17), end=utc(2026, 11, 12, 18, 30), direction="export")


def test_hold_forces_discharge_export_during_event(db):
    store_event(db, EVENT, now=utc(2026, 11, 11))
    cfg = _config(db)
    held, ev = scheduler._axle_hold(cfg, None, utc(2026, 11, 12, 17, 40))
    assert ev == EVENT and held is not None
    assert held.slot_start == utc(2026, 11, 12, 17, 30)
    assert decision_to_mode(held, cfg.battery) == ("DISCHARGE_EXPORT", cfg.battery.max_discharge_rate_kw)


def test_hold_import_event_forces_charge(db):
    store_event(db, AxleEvent(start=utc(2026, 11, 12, 3), end=utc(2026, 11, 12, 4), direction="import"),
                now=utc(2026, 11, 11))
    cfg = _config(db)
    held, _ = scheduler._axle_hold(cfg, None, utc(2026, 11, 12, 3, 10))
    assert decision_to_mode(held, cfg.battery) == ("CHARGE", cfg.battery.max_charge_rate_kw)


def test_hold_margin_and_end_return_saved_decision(db):
    store_event(db, EVENT, now=utc(2026, 11, 11))
    saved = object()
    cfg = _config(db)
    assert scheduler._axle_hold(cfg, saved, utc(2026, 11, 12, 16, 50)) == (saved, None)
    assert scheduler._axle_hold(cfg, saved, utc(2026, 11, 12, 18, 30)) == (saved, None)
    held, ev = scheduler._axle_hold(_config(db, margin=15), saved, utc(2026, 11, 12, 16, 50))
    assert ev == EVENT and held is not saved
    assert scheduler._axle_hold(_config(db, token=None), saved, utc(2026, 11, 12, 17, 30)) == (saved, None)


def test_safe_fallback_holds_export_during_event(db, monkeypatch):
    store_event(db, EVENT, now=utc(2026, 11, 11))
    applied, eco = [], []
    monkeypatch.setattr(scheduler, "apply_decision", lambda d, g, b, p, trigger=None, reading=None: applied.append((d, trigger)))
    import octoopt2.control.inverter as ci
    monkeypatch.setattr(ci, "set_eco_mode", lambda *a, **k: eco.append(1))
    monkeypatch.setattr(scheduler, "release_dhw", lambda *a, **k: None)

    class _Now(datetime):
        @classmethod
        def now(cls, tz=None):
            return utc(2026, 11, 12, 17, 20)
    monkeypatch.setattr(scheduler, "datetime", _Now)
    cfg = _config(db)
    scheduler._apply_safe_fallback(cfg, manage_dhw=True)
    assert len(applied) == 1 and applied[0][1] == "axle" and not eco
    scheduler._apply_safe_fallback(cfg, manage_dhw=True, allow_axle_hold=False)   # shutdown path
    assert len(applied) == 1 and eco == [1]


def test_save_schedule_keeps_raw_sell_and_stores_uplift(db, battery, dhw, givenergy):
    slots, buy, sell, solar, load = synthetic_day()
    uplift = [0.0] * 48
    uplift[34] = 1.0
    inp = OptimizerInput(slot_starts=slots, initial_soc_kwh=battery.min_soc_kwh, buy_prices=buy,
                         sell_prices=sell, solar_forecast=[0.0] * 48, load_forecast=load, axle_uplift=uplift)
    res = optimize(inp, battery, dhw, givenergy, manage_dhw=False, terminal_soc_value_gbp_kwh=0.0)
    save_schedule(res, inp, db)
    with get_conn(db) as conn:
        rows = conn.execute("SELECT sell_gbp_kwh, axle_uplift_gbp_kwh FROM schedule ORDER BY slot_start").fetchall()
        hist = conn.execute("SELECT axle_uplift_gbp_kwh FROM schedule_history ORDER BY slot_start").fetchall()
    assert all(r["sell_gbp_kwh"] == 0.15 for r in rows)
    assert [r["axle_uplift_gbp_kwh"] for r in rows][34] == 1.0 and rows[33]["axle_uplift_gbp_kwh"] is None
    assert hist[34]["axle_uplift_gbp_kwh"] == 1.0


def test_status_axle_block_next_active_planned_export(db):
    store_event(db, EVENT, now=utc(2026, 11, 11))
    with get_conn(db) as conn:
        for i, kwh in enumerate([1.4, 1.4, 1.3]):
            conn.execute(
                """INSERT INTO schedule (slot_start, battery_charge_kwh, battery_discharge_kwh, grid_import_kwh,
                   grid_export_kwh, dhw_on, predicted_load_kwh, predicted_solar_kwh, buy_gbp_kwh, sell_gbp_kwh, optimized_at)
                   VALUES (?, 0, 1.4, 0, ?, 0, 0.4, 0, 0.3, 0.2, 'x')""",
                ((utc(2026, 11, 12, 17) + timedelta(minutes=30 * i)).isoformat(), kwh))
    cfg = AxleConfig(token="tok")
    before = status._axle(db, utc(2026, 11, 12, 10), cfg)
    assert before["enabled"] and before["active"] is False
    assert before["next_event"]["start"] == "2026-11-12T17:00:00+00:00"
    assert before["planned_export_kwh"] == pytest.approx(4.1)
    during = status._axle(db, utc(2026, 11, 12, 17, 45), cfg)
    assert during["active"] is True
    assert status._axle(db, utc(2026, 11, 12, 10), AxleConfig()) == {"enabled": False}
