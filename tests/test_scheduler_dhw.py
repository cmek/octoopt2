"""Daily DHW quota must survive the 30-min re-plan cycle.

Each re-run only sees the remaining horizon; without carrying over the ON slots
already executed today it would plan a fresh full quota every half hour (the
production DB showed 7–26 planned ON slots per day against a cap of 8).
"""
from datetime import date, datetime, timedelta, timezone

import pytest

from octoopt2 import scheduler
from octoopt2.config import DhwConfig
from octoopt2.db import get_conn, init_db
from octoopt2.optimizer.model import OptimizerInput, optimize
from tests.conftest import synthetic_day


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "test.db")
    init_db(path)
    return path


def _seed(db_path: str, rows: list[tuple[datetime, int]]) -> None:
    with get_conn(db_path) as conn:
        conn.executemany(
            """INSERT INTO schedule (slot_start, battery_charge_kwh, battery_discharge_kwh,
                   grid_import_kwh, grid_export_kwh, dhw_on, predicted_load_kwh,
                   predicted_solar_kwh, buy_gbp_kwh, sell_gbp_kwh, optimized_at)
               VALUES (?, 0, 0, 0, 0, ?, 0.4, 0, 0.2, 0.1, '2026-10-15T00:00:00+00:00')""",
            [(s.isoformat(), on) for s, on in rows],
        )


def test_done_today_counts_only_past_slots_of_the_uk_day(db_path):
    # BST: UK midnight on 15 Oct is 23:00Z on 14 Oct.
    uk_midnight = datetime(2026, 10, 14, 23, 0, tzinfo=timezone.utc)
    now = datetime(2026, 10, 15, 9, 7, tzinfo=timezone.utc)   # current slot 09:00Z
    _seed(db_path, [
        (uk_midnight - timedelta(minutes=30), 1),  # yesterday (UK) — excluded
        (uk_midnight, 1),                           # 00:00 UK — counts
        (uk_midnight + timedelta(hours=3), 1),      # counts
        (uk_midnight + timedelta(hours=4), 0),      # off — ignored
        (datetime(2026, 10, 15, 9, 0, tzinfo=timezone.utc), 1),   # current slot — excluded
        (datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc), 1),  # future — excluded
    ])
    assert scheduler._dhw_slots_done_today(db_path, now) == {date(2026, 10, 15): 2}


def test_done_today_empty_when_nothing_executed(db_path):
    assert scheduler._dhw_slots_done_today(db_path, datetime(2026, 10, 15, 9, 7, tzinfo=timezone.utc)) == {}


def _inputs():
    slots, buy, sell, solar, load = synthetic_day()
    return OptimizerInput(slot_starts=slots, initial_soc_kwh=2.0, buy_prices=buy,
                          sell_prices=sell, solar_forecast=solar, load_forecast=load), slots


def _on_count(res) -> int:
    return sum(d.dhw_on for d in res.decisions)


def test_quota_reduced_by_slots_done_today(battery, givenergy):
    dhw = DhwConfig(min_slots_per_day=4, max_slots_per_day=8)
    inp, slots = _inputs()
    day = slots[0].astimezone(scheduler.LONDON).date()
    full = optimize(inp, battery, dhw, givenergy, manage_dhw=True)
    assert 4 <= _on_count(full) <= 8
    partial = optimize(inp, battery, dhw, givenergy, manage_dhw=True, dhw_done_today={day: 2})
    assert 2 <= _on_count(partial) <= 6
    exhausted = optimize(inp, battery, dhw, givenergy, manage_dhw=True, dhw_done_today={day: 8})
    assert _on_count(exhausted) == 0


def test_done_on_other_day_does_not_affect_horizon(battery, givenergy):
    dhw = DhwConfig(min_slots_per_day=4, max_slots_per_day=8)
    inp, _ = _inputs()
    res = optimize(inp, battery, dhw, givenergy, manage_dhw=True, dhw_done_today={date(2020, 1, 1): 8})
    assert 4 <= _on_count(res) <= 8
