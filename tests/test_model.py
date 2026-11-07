"""MILP model behaviour: curtailment feasibility and terminal SoC value."""
from datetime import datetime, timedelta, timezone

import pytest

from octoopt2.config import DhwConfig
from octoopt2.optimizer.model import OptimizerInput, _equal_price_ranks, optimize
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


# ── Axle event uplift ─────────────────────────────────────────────────────────

def test_axle_uplift_exports_battery_at_cap_in_event_slots(battery, dhw, givenergy):
    slots, buy, sell, solar, load = synthetic_day()
    solar = [0.0] * 48
    uplift = [0.0] * 48
    uplift[34] = uplift[35] = 1.0            # 17:00–18:00 export event
    inp = OptimizerInput(slot_starts=slots, initial_soc_kwh=battery.min_soc_kwh, buy_prices=buy,
                         sell_prices=sell, solar_forecast=solar, load_forecast=load, axle_uplift=uplift)
    res = optimize(inp, battery, dhw, givenergy, manage_dhw=False, terminal_soc_value_gbp_kwh=0.0)
    d = res.decisions
    cap = min(givenergy.max_export_kw, battery.max_discharge_rate_kw) * 0.5
    assert d[34].grid_export_kwh == pytest.approx(cap - 0.4, abs=0.05) or d[34].grid_export_kwh == pytest.approx(cap, abs=0.05)
    assert d[34].grid_export_kwh > 0.9 and d[35].grid_export_kwh > 0.9
    assert d[34].soc_start_kwh > battery.min_soc_kwh + 2.0          # charged beforehand
    assert d[34].slot_cost_gbp < -0.9 * d[34].grid_export_kwh         # priced at ~£1.15/kWh
    # tariff prices on the input are untouched
    assert inp.sell_prices[34] == 0.15


def test_axle_export_event_bypasses_negative_price_cap(battery, dhw, givenergy):
    slots, buy, sell, solar, load = synthetic_day()
    solar = [0.0] * 48
    buy = [-0.02] * 48
    uplift = [0.0] * 48
    uplift[20] = 1.0
    inp = OptimizerInput(slot_starts=slots, initial_soc_kwh=battery.max_soc_kwh, buy_prices=buy,
                         sell_prices=sell, solar_forecast=solar, load_forecast=load, axle_uplift=uplift)
    res = optimize(inp, battery, dhw, givenergy, manage_dhw=False, terminal_soc_value_gbp_kwh=0.0)
    assert res.decisions[20].grid_export_kwh > 0.9          # battery export allowed in the event
    assert all(res.decisions[t].grid_export_kwh < 1e-6 for t in range(48) if t != 20)  # capped elsewhere (no solar)


def test_axle_uplift_length_validated(battery):
    slots, buy, sell, solar, load = synthetic_day()
    with pytest.raises(ValueError):
        OptimizerInput(slot_starts=slots, initial_soc_kwh=1.0, buy_prices=buy, sell_prices=sell,
                       solar_forecast=solar, load_forecast=load, axle_uplift=[0.0])


# ── Tie-breaks between equal-price slots (banded tariffs) ────────────────────

def test_equal_price_ranks_zero_for_unique_prices():
    assert _equal_price_ranks([0.14, 0.14, 0.28, 0.14]) == ([0, 1, 0, 2], [2, 1, 0, 0])
    before, after = _equal_price_ranks([0.1, 0.2, 0.3])
    assert before == [0, 0, 0] and after == [0, 0, 0]
    assert _equal_price_ranks([]) == ([], [])


def _flat(n, buy=0.14, sell=0.05, load=0.4):
    day = datetime(2026, 11, 10, tzinfo=timezone.utc)    # GMT: one UK calendar day
    slots = [day + timedelta(minutes=30 * i) for i in range(n)]
    return slots, [buy] * n, [sell] * n, [0.0] * n, [load] * n


def test_flat_band_charge_lands_in_last_equal_price_slots(battery, dhw, givenergy):
    """Flat 14p band, battery at the floor, a terminal value that makes filling
    it worthwhile: the charge must sit in the LAST slots (ECO drains an idle
    battery, so fill just before the band ends), as one contiguous suffix."""
    slots, buy, sell, solar, load = _flat(12)
    inp = _inputs(battery.min_soc_kwh, slots, buy, sell, solar, load)
    res = optimize(inp, battery, dhw, givenergy, manage_dhw=False, terminal_soc_value_gbp_kwh=0.30)
    charged = [t for t, d in enumerate(res.decisions) if d.battery_charge_kwh > 0.05]
    assert len(charged) >= 5                                   # 8.55 kWh / 1.7 per slot
    assert charged == list(range(12 - len(charged), 12))
    assert res.decisions[-1].soc_end_kwh >= battery.max_soc_kwh - 1e-6


def test_distinct_prices_still_pick_cheapest_slots(battery, dhw, givenergy):
    """Half-hourly prices 0.02p apart: the tie-break must not reorder them."""
    perm = [7, 2, 11, 0, 9, 4, 1, 10, 5, 8, 3, 6]
    slots, _, sell, solar, load = _flat(12)
    buy = [0.14 + 0.0002 * p for p in perm]
    inp = _inputs(battery.min_soc_kwh, slots, buy, sell, solar, load)
    res = optimize(inp, battery, dhw, givenergy, manage_dhw=False, terminal_soc_value_gbp_kwh=0.30)
    charged = sorted(t for t, d in enumerate(res.decisions) if d.battery_charge_kwh > 0.05)
    cheapest = sorted(sorted(range(12), key=lambda t: buy[t])[: len(charged)])
    assert charged == cheapest and len(charged) >= 5


def test_flat_band_dhw_lands_in_first_slots_of_day(battery, givenergy):
    slots, buy, sell, solar, load = _flat(48)
    dhw = DhwConfig(min_slots_per_day=3, max_slots_per_day=8)
    inp = _inputs(2.0, slots, buy, sell, solar, load)
    res = optimize(inp, battery, dhw, givenergy, manage_dhw=True, terminal_soc_value_gbp_kwh=0.0)
    assert [t for t, d in enumerate(res.decisions) if d.dhw_on] == [0, 1, 2]
