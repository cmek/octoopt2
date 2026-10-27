"""Tests for the history/collection tables added in "Part C — collect more data".

Covers: schedule_history append-only writes, optimizer_runs inserts, the
optional InverterReading fields round-tripping through inverter_readings,
_migrate idempotency, and the SQL-bucketed gross-load loader matching the
old per-row Python aggregation.
"""
from datetime import datetime, timedelta, timezone

import pytest

from octoopt2.data.inverter import InverterReading, get_latest_reading, store_reading
from octoopt2.db import get_conn, init_db
from octoopt2.optimizer.forecast import _load_gross_load
from octoopt2.optimizer.model import OptimizerInput, OptimizerResult, SlotDecision
from octoopt2.optimizer.schedule import record_optimizer_run, save_schedule

T0 = datetime(2026, 10, 12, 10, 0, tzinfo=timezone.utc)


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "test.db")
    init_db(path)
    return path


def _result(optimized_at: datetime, n: int = 3, charge: float = 1.0) -> tuple[OptimizerResult, OptimizerInput]:
    slots = [T0 + timedelta(minutes=30 * i) for i in range(n)]
    decisions = [
        SlotDecision(
            slot_start=s,
            battery_charge_kwh=charge,
            battery_discharge_kwh=0.0,
            grid_import_kwh=charge + 0.3,
            grid_export_kwh=0.0,
            dhw_on=(i == 0),
            soc_start_kwh=2.0 + i,
            soc_end_kwh=3.0 + i,
            slot_cost_gbp=0.1,
            curtailed_kwh=0.05 * i,
        )
        for i, s in enumerate(slots)
    ]
    result = OptimizerResult(
        decisions=decisions,
        total_cost_gbp=0.3,
        solver_status="Optimal",
        optimized_at=optimized_at,
    )
    inputs = OptimizerInput(
        slot_starts=slots,
        initial_soc_kwh=2.0,
        buy_prices=[0.2] * n,
        sell_prices=[0.1] * n,
        solar_forecast=[0.0] * n,
        load_forecast=[0.3] * n,
    )
    return result, inputs


def test_save_schedule_appends_history(db_path):
    first_at = T0 - timedelta(minutes=10)
    result, inputs = _result(first_at, charge=1.0)
    save_schedule(result, inputs, db_path)

    with get_conn(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM schedule_history").fetchone()[0] == 3
        row = conn.execute(
            "SELECT * FROM schedule_history WHERE slot_start = ? AND optimized_at = ?",
            (T0.isoformat(), first_at.isoformat()),
        ).fetchone()
    assert row["battery_charge_kwh"] == 1.0
    assert row["soc_start_kwh"] == 2.0
    assert row["soc_end_kwh"] == 3.0
    assert row["curtailed_kwh"] == 0.0
    assert row["dhw_on"] == 1

    # A later run with a different plan: schedule is replaced, history grows.
    second_at = T0 - timedelta(minutes=5)
    result2, inputs2 = _result(second_at, charge=2.0)
    save_schedule(result2, inputs2, db_path)

    with get_conn(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM schedule").fetchone()[0] == 3
        latest = conn.execute(
            "SELECT battery_charge_kwh, optimized_at FROM schedule WHERE slot_start = ?",
            (T0.isoformat(),),
        ).fetchone()
        assert latest["battery_charge_kwh"] == 2.0
        assert latest["optimized_at"] == second_at.isoformat()

        hist = conn.execute(
            "SELECT optimized_at, battery_charge_kwh FROM schedule_history "
            "WHERE slot_start = ? ORDER BY optimized_at",
            (T0.isoformat(),),
        ).fetchall()
    assert [(h["optimized_at"], h["battery_charge_kwh"]) for h in hist] == [
        (first_at.isoformat(), 1.0),
        (second_at.isoformat(), 2.0),
    ]

    # Re-saving the same run is a no-op for history (INSERT OR IGNORE).
    save_schedule(result2, inputs2, db_path)
    with get_conn(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM schedule_history").fetchone()[0] == 6


def test_record_optimizer_run(db_path):
    run_at = T0
    record_optimizer_run(
        db_path,
        run_at=run_at,
        status="Optimal",
        objective_gbp=1.5,
        grid_cost_gbp=1.2,
        solve_seconds=0.42,
        horizon_slots=48,
        horizon_start=T0.isoformat(),
        soc_start_kwh=2.5,
        alpha=0.01,
        beta=0.3,
        ref_temp=12.0,
        n_train_slots=1000,
        manage_dhw=True,
        half_life_days=12.0,
        hdd_base_c=15.5,
        train_days=56,
    )
    record_optimizer_run(db_path, run_at=run_at + timedelta(seconds=1), status="error", error="boom")

    with get_conn(db_path) as conn:
        rows = conn.execute("SELECT * FROM optimizer_runs ORDER BY run_at").fetchall()
    assert len(rows) == 2
    ok, err = rows
    assert ok["run_at"] == run_at.isoformat()
    assert ok["status"] == "Optimal"
    assert (ok["half_life_days"], ok["hdd_base_c"], ok["train_days"]) == (12.0, 15.5, 56)
    assert err["half_life_days"] is None
    assert ok["grid_cost_gbp"] == 1.2
    assert ok["solve_seconds"] == 0.42
    assert ok["manage_dhw"] == 1
    assert ok["error"] is None
    assert err["status"] == "error"
    assert err["error"] == "boom"
    assert err["alpha"] is None

    with pytest.raises(ValueError):
        record_optimizer_run(db_path, not_a_column=1)


def _reading(at: datetime, **extras) -> InverterReading:
    return InverterReading(
        recorded_at=at,
        soc_pct=50.0,
        solar_w=1000.0,
        grid_import_w=0.0,
        grid_export_w=200.0,
        battery_charge_w=300.0,
        battery_discharge_w=0.0,
        load_w=500.0,
        **extras,
    )


def test_store_reading_without_extras(db_path):
    store_reading(_reading(T0), db_path)
    got = get_latest_reading(db_path)
    assert got is not None
    assert got.recorded_at == T0
    assert got.load_w == 500.0
    assert got.temp_battery_c is None
    assert got.hr_enable_charge is None
    assert got.num_cycles is None


def test_store_reading_extras_round_trip(db_path):
    reading = _reading(
        T0,
        temp_battery_c=21.5,
        v_battery=52.1,
        i_battery=-3.2,
        e_pv_total=1234.5,
        e_grid_in_total=100.0,
        e_grid_out_total=50.0,
        e_battery_charge_total=400.0,
        e_battery_discharge_total=380.0,
        e_inverter_in_total=10.0,
        e_inverter_out_total=11.0,
        num_cycles=123,
        hr_enable_charge=1,
        hr_enable_discharge=0,
        hr_soc_reserve=4,
        hr_power_reserve=4,
        hr_charge_limit=50,
        hr_discharge_limit=25,
        hr_eco_mode=1,
    )
    store_reading(reading, db_path)
    # An older reading must not win "latest".
    store_reading(_reading(T0 - timedelta(minutes=5)), db_path)
    got = get_latest_reading(db_path)
    assert got == reading


def test_migrate_is_idempotent(db_path):
    init_db(db_path)  # second call must not raise on existing columns/tables
    init_db(db_path)
    with get_conn(db_path) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(inverter_readings)")}
        assert {"temp_battery_c", "hr_eco_mode", "num_cycles"} <= cols
        cols = {r[1] for r in conn.execute("PRAGMA table_info(actuals)")}
        assert {"battery_charge_kwh", "soc_start_pct", "n_samples", "curtailed_kwh"} <= cols
        cols = {r[1] for r in conn.execute("PRAGMA table_info(dhw_readings)")}
        assert {"outside_temperature_c", "room_target_c", "power"} <= cols
        cols = {r[1] for r in conn.execute("PRAGMA table_info(prices)")}
        assert {"buy_exc_vat_gbp_kwh", "sell_exc_vat_gbp_kwh"} <= cols
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {
        "schedule_history", "optimizer_runs", "solar_forecast_history",
        "weather_forecast_history", "weather_observed", "standing_charges",
    } <= tables


def test_gross_load_sql_bucketing_matches_mean(db_path):
    # Three readings in the :00 half, two in the :30 half, one just outside.
    samples = {
        T0 + timedelta(minutes=0): 400.0,
        T0 + timedelta(minutes=10, seconds=30): 600.0,
        T0 + timedelta(minutes=29, seconds=59): 800.0,
        T0 + timedelta(minutes=30): 1000.0,
        T0 + timedelta(minutes=45): 3000.0,
        T0 + timedelta(minutes=60): 100.0,
    }
    for at, w in samples.items():
        store_reading(InverterReading(
            recorded_at=at, soc_pct=50.0, solar_w=0.0, grid_import_w=0.0,
            grid_export_w=0.0, battery_charge_w=0.0, battery_discharge_w=0.0, load_w=w,
        ), db_path)

    gross = _load_gross_load(db_path, now=T0 + timedelta(hours=2))
    # mean W × 0.5 h / 1000 — same formula as the old per-row aggregation.
    assert gross[T0] == pytest.approx((400 + 600 + 800) / 3 * 0.5 / 1000)
    assert gross[T0 + timedelta(minutes=30)] == pytest.approx((1000 + 3000) / 2 * 0.5 / 1000)
    assert gross[T0 + timedelta(minutes=60)] == pytest.approx(100 * 0.5 / 1000)
    assert all(k.tzinfo is not None for k in gross)

    # Readings older than TRAINING_DAYS are excluded.
    assert _load_gross_load(db_path, now=T0 + timedelta(days=60)) == {}
