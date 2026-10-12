"""MILP model behaviour: curtailment feasibility and terminal SoC value."""
from datetime import datetime, timedelta, timezone

import pytest

from octoopt2.optimizer.model import OptimizerInput, optimize
from tests.conftest import synthetic_day


def _inputs(soc0, slots, buy, sell, solar, load):
    return OptimizerInput(
        slot_starts=slots, initial_soc_kwh=soc0, buy_prices=buy,
        sell_prices=sell, solar_forecast=solar, load_forecast=load,
    )


def test_curtailment_keeps_full_battery_sunny_slot_feasible(battery, dhw, givenergy):
    slots = [datetime(2026, 6, 15, 12, tzinfo=timezone.utc) + timedelta(minutes=30 * i) for i in range(2)]
    # 2.2 kWh surplus > 1.8 kWh export cap, battery already full
    inp = _inputs(battery.max_soc_kwh, slots, [0.3, 0.3], [0.1, 0.1], [2.6, 2.6], [0.4, 0.4])
    res = optimize(inp, battery, dhw, givenergy, manage_dhw=False, allow_curtailment=True,
                   terminal_soc_value_gbp_kwh=0.0)
    assert res.solver_status == "Optimal"
    assert res.decisions[0].curtailed_kwh > 0.3
    with pytest.raises(RuntimeError):
        optimize(inp, battery, dhw, givenergy, manage_dhw=False, allow_curtailment=False,
                 terminal_soc_value_gbp_kwh=0.0)


def test_terminal_value_stops_draining_to_floor(battery, dhw, givenergy):
    slots, buy, sell, solar, load = synthetic_day()
    # Horizon ends right after the cheap night: without a terminal value the
    # plan has no reason to hold charge; with one it keeps the battery up.
    short = slice(0, 12)
    inp = _inputs(battery.max_soc_kwh, slots[short], buy[short], sell[short], solar[short], load[short])
    drained = optimize(inp, battery, dhw, givenergy, manage_dhw=False, terminal_soc_value_gbp_kwh=0.0)
    held = optimize(inp, battery, dhw, givenergy, manage_dhw=False, terminal_soc_value_gbp_kwh=0.30)
    assert held.decisions[-1].soc_end_kwh > drained.decisions[-1].soc_end_kwh
    assert held.decisions[-1].soc_end_kwh >= battery.max_soc_kwh - 1e-6


def test_min_terminal_soc_is_honoured(battery, dhw, givenergy):
    slots, buy, sell, solar, load = synthetic_day()
    inp = _inputs(battery.min_soc_kwh, slots, buy, sell, solar, load)
    res = optimize(inp, battery, dhw, givenergy, manage_dhw=False,
                   terminal_soc_value_gbp_kwh=0.0, min_terminal_soc_kwh=5.0)
    assert res.decisions[-1].soc_end_kwh >= 5.0 - 1e-6
    assert res.grid_cost_gbp == pytest.approx(sum(d.slot_cost_gbp for d in res.decisions))
    assert res.solve_seconds > 0
