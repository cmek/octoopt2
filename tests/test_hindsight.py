
import pytest

from octoopt2.backtest.data import Segment, Slot
from octoopt2.backtest.hindsight import run_hindsight
from octoopt2.backtest.simulate import SimLimits, run_eco, run_no_battery
from tests.conftest import synthetic_day


def _segment(soc0):
    slots, buy, sell, solar, load = synthetic_day()
    sl = [Slot(slot_start=t, buy=b, sell=s, solar_kwh=so, load_kwh=lo, act_import_kwh=0, act_export_kwh=0, source="actuals")
          for t, b, s, so, lo in zip(slots, buy, sell, solar, load)]
    day = sl[0].london_day
    return Segment(days=[day], slots=sl, soc0_kwh=soc0, day_index={day: list(range(48))}), buy, sell, solar, load


def test_hindsight_beats_eco_beats_no_battery(battery, dhw, givenergy):
    lim = SimLimits.from_config(battery, givenergy)
    seg, buy, sell, solar, load = _segment(battery.min_soc_kwh)
    hind = run_hindsight(seg, buy, sell, battery, dhw, givenergy, lookahead_days=0, terminal="end-as-start")
    assert len(hind) == 1 and hind[0].solver_status == "Optimal"
    eco = sum(f.cost(b, s) for f, b, s in zip(run_eco(seg.soc0_kwh, solar, load, lim), buy, sell))
    nob = sum(f.cost(b, s) for f, b, s in zip(run_no_battery(solar, load, lim), buy, sell))
    assert hind[0].cost_gbp <= eco + 1e-6 <= nob + 1e-6
    # end-as-start: battery ends no lower than it started
    assert hind[0].decisions[-1].soc_end_kwh >= seg.soc0_kwh - 1e-6


def test_hindsight_charges_in_cheap_night(battery, dhw, givenergy):
    seg, buy, sell, *_ = _segment(battery.min_soc_kwh)
    hind = run_hindsight(seg, buy, sell, battery, dhw, givenergy, lookahead_days=0, terminal="none")
    night = sum(d.battery_charge_kwh for d in hind[0].decisions[:12])
    assert night > 1.0
    assert hind[0].decisions[-1].soc_end_kwh == pytest.approx(battery.min_soc_kwh, abs=1e-6)
