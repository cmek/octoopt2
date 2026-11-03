"""The schedule page must show the plan the daemon is acting on.

web/schedule.json used to be a file written only under ``--output``; the
deployed daemon ran without it and the page served a June plan in November.
The daemon now builds /schedule.json live from the database, and that document
must match what the solver path would have written.
"""
import json
import threading
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from octoopt2 import daemon, webplan
from octoopt2.config import BatteryConfig, DhwConfig, GivEnergyConfig
from octoopt2.db import get_conn, init_db
from octoopt2.optimizer.model import OptimizerInput, optimize
from octoopt2.optimizer.schedule import record_optimizer_run, save_schedule
from tests.conftest import synthetic_day

UTC = timezone.utc


@pytest.fixture
def db(tmp_path):
    p = str(tmp_path / "t.db")
    init_db(p)
    return p


def _config(db):
    return SimpleNamespace(
        db_path=db, battery=BatteryConfig(), dhw=DhwConfig(),
        givenergy=GivEnergyConfig(host="", max_export_kw=3.6, max_import_kw=11.0),
        axle=SimpleNamespace(token=None),
    )


def _solve_and_save(db, battery, dhw, givenergy):
    slots, buy, sell, solar, load = synthetic_day()
    inp = OptimizerInput(slot_starts=slots, initial_soc_kwh=battery.min_soc_kwh, buy_prices=buy,
                         sell_prices=sell, solar_forecast=solar, load_forecast=load)
    res = optimize(inp, battery, dhw, givenergy, manage_dhw=True, terminal_soc_value_gbp_kwh=0.0)
    # Same order as scheduler.run: the run row is stamped after the solve.
    record_optimizer_run(db, run_at=res.optimized_at + timedelta(seconds=1),
                         status=res.solver_status, objective_gbp=res.total_cost_gbp)
    save_schedule(res, inp, db)
    return res, inp


def test_db_payload_matches_solver_payload(db, battery, dhw, givenergy):
    res, inp = _solve_and_save(db, battery, dhw, givenergy)
    cfg = _config(db)
    now = datetime(2026, 6, 15, 10, 7, tzinfo=UTC)

    from_solver = webplan.build_schedule_payload(webplan.plan_from_result(res, inp), now, cfg, True)
    from_db = webplan.build_schedule_from_db(cfg, True, now=now)

    assert from_db["optimized_at"] == from_solver["optimized_at"]
    assert from_db["solver_status"] == res.solver_status
    assert from_db["total_cost_gbp"] == from_solver["total_cost_gbp"]
    assert len(from_db["schedule"]) == 48
    assert from_db["schedule"] == from_solver["schedule"]
    assert from_db["schedule"][20]["is_current"] and from_db["schedule"][20]["time_uk"] == "Mon 11:00"
    assert all(s["soc_pct"] is not None for s in from_db["schedule"])


def test_db_payload_is_newest_plan_only(db, battery, dhw, givenergy):
    """A second optimization supersedes the first; stale rows must not leak in."""
    _solve_and_save(db, battery, dhw, givenergy)
    with get_conn(db) as conn:
        old = conn.execute("SELECT MAX(optimized_at) AS ts FROM schedule_history").fetchone()["ts"]
    res, _ = _solve_and_save(db, battery, dhw, givenergy)
    plan = webplan.plan_from_db(db)
    assert plan.optimized_at == res.optimized_at.astimezone(UTC)
    assert plan.optimized_at.isoformat() != old
    assert len(plan.slots) == 48


def test_failed_rerun_does_not_steal_solver_status(db, battery, dhw, givenergy):
    res, _ = _solve_and_save(db, battery, dhw, givenergy)
    record_optimizer_run(db, run_at=res.optimized_at + timedelta(minutes=30),
                         status="error", error="boom")
    plan = webplan.plan_from_db(db)
    assert plan.solver_status == res.solver_status
    assert plan.total_cost_gbp == pytest.approx(res.total_cost_gbp)


def test_legacy_schedule_table_without_history(db):
    """Databases from before schedule_history: fall back, SoC unknown."""
    with get_conn(db) as conn:
        conn.execute(
            """INSERT INTO schedule (slot_start, battery_charge_kwh, battery_discharge_kwh, grid_import_kwh,
               grid_export_kwh, dhw_on, predicted_load_kwh, predicted_solar_kwh, buy_gbp_kwh, sell_gbp_kwh,
               optimized_at) VALUES ('2026-11-01T10:00:00+00:00', 0, 0.8, 0, 0.5, 1, 0.4, 0.1, 0.30, 0.15,
               '2026-11-01T09:31:00+00:00')""")
    payload = webplan.build_schedule_from_db(_config(db), True, now=datetime(2026, 11, 1, 10, 5, tzinfo=UTC))
    slot = payload["schedule"][0]
    assert slot["soc_pct"] is None and slot["soc_kwh"] is None
    assert slot["mode"].startswith("DISCHARGE/EXPORT")
    assert slot["slot_cost_gbp"] == pytest.approx(-0.075)
    assert payload["solver_status"] == "Unknown"
    assert payload["total_cost_gbp"] == pytest.approx(-0.075)


def test_empty_db_has_no_plan(db):
    assert webplan.build_schedule_from_db(_config(db), True) is None


def _serve(handler):
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


def _get(httpd, path):
    url = f"http://127.0.0.1:{httpd.server_address[1]}{path}"
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return r.status, json.loads(r.read()), r.headers
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read()), e.headers


def test_daemon_serves_schedule_json_live(db, battery, dhw, givenergy, tmp_path, monkeypatch):
    """/schedule.json comes from the DB even when a stale file sits in web/."""
    web = tmp_path / "web"
    web.mkdir()
    (web / "schedule.json").write_text(json.dumps({"generated_at": "2026-06-26T12:00:00+00:00", "schedule": []}))
    monkeypatch.setattr(daemon, "_WEB_DIR", web)
    cfg = _config(db)
    httpd = _serve(daemon._make_handler(daemon.DaemonState(), cfg, manage_dhw=False))
    try:
        code, body, _ = _get(httpd, "/schedule.json?t=1")
        assert code == 404 and "no schedule" in body["error"]

        res, _ = _solve_and_save(db, battery, dhw, givenergy)
        code, body, headers = _get(httpd, "/schedule.json")
        assert code == 200
        assert headers["Cache-Control"] == "no-store"
        assert body["optimized_at"] == res.optimized_at.astimezone(UTC).isoformat()
        assert body["manage_dhw"] is False
        assert len(body["schedule"]) == 48
    finally:
        httpd.shutdown()


def test_db_slot_cost_includes_axle_uplift(db, battery, dhw, givenergy):
    """During an Axle event the page must show the plan's value at the uplifted price."""
    slots, buy, sell, solar, load = synthetic_day()
    uplift = [0.0] * 48
    uplift[34] = 1.0
    inp = OptimizerInput(slot_starts=slots, initial_soc_kwh=battery.min_soc_kwh, buy_prices=buy,
                         sell_prices=sell, solar_forecast=[0.0] * 48, load_forecast=load, axle_uplift=uplift)
    res = optimize(inp, battery, dhw, givenergy, manage_dhw=False, terminal_soc_value_gbp_kwh=0.0)
    save_schedule(res, inp, db)
    cfg = _config(db)
    now = datetime(2026, 6, 15, 17, 0, tzinfo=UTC)
    from_db = webplan.build_schedule_from_db(cfg, False, now=now)["schedule"]
    from_solver = webplan.build_schedule_payload(webplan.plan_from_result(res, inp), now, cfg, False)["schedule"]
    assert res.decisions[34].grid_export_kwh > 0.5           # the event slot is exploited
    assert from_db[34]["slot_cost_gbp"] == from_solver[34]["slot_cost_gbp"]
    assert from_db[34]["slot_cost_gbp"] < -0.5
    assert from_db[34]["sell_p"] == 15.0                     # tariff price shown, uplift in the cost
    assert from_db == from_solver


def test_run_status_not_borrowed_from_a_later_run(db, battery, dhw, givenergy):
    """Plan's own run row missing: report unknown, not the next tick's status."""
    slots, buy, sell, solar, load = synthetic_day()
    inp = OptimizerInput(slot_starts=slots, initial_soc_kwh=battery.min_soc_kwh, buy_prices=buy,
                         sell_prices=sell, solar_forecast=solar, load_forecast=load)
    res = optimize(inp, battery, dhw, givenergy, manage_dhw=True, terminal_soc_value_gbp_kwh=0.0)
    save_schedule(res, inp, db)   # no record_optimizer_run for this plan
    record_optimizer_run(db, run_at=res.optimized_at + timedelta(minutes=30),
                         status="Infeasible", objective_gbp=0.0)
    plan = webplan.plan_from_db(db)
    assert plan.solver_status is None and plan.total_cost_gbp is None
    payload = webplan.build_schedule_payload(plan, res.optimized_at, _config(db), True)
    assert payload["solver_status"] == "Unknown"
    assert payload["total_cost_gbp"] == pytest.approx(sum(d.slot_cost_gbp for d in res.decisions), abs=1e-3)


def test_current_slot_and_age_use_utc(db, battery, dhw, givenergy):
    res, inp = _solve_and_save(db, battery, dhw, givenergy)
    from zoneinfo import ZoneInfo
    # 08:10Z expressed in a half-hour-offset zone: minute=40 locally.
    now = datetime(2026, 6, 15, 13, 40, tzinfo=ZoneInfo("Asia/Kolkata"))
    payload = webplan.build_schedule_payload(webplan.plan_from_result(res, inp), now, _config(db), True)
    current = [s for s in payload["schedule"] if s["is_current"]]
    assert len(current) == 1 and current[0]["slot_start"] == "2026-06-15T08:00:00+00:00"
    assert payload["plan_age_s"] == pytest.approx(
        (now.astimezone(UTC) - res.optimized_at).total_seconds(), abs=0.5)
