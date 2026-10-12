import pytest

from octoopt2.backtest.simulate import SimLimits, run_eco, run_no_battery, simulate_slot
from tests.conftest import synthetic_day


@pytest.fixture
def lim(battery, givenergy):
    return SimLimits.from_config(battery, givenergy)


def _balanced(f, solar, load):
    return abs(solar - f.curtail_kwh + f.import_kwh + f.discharge_kwh - load - f.export_kwh - f.charge_kwh) < 1e-9


@pytest.mark.parametrize("mode", ["ECO", "CHARGE", "DISCHARGE_EXPORT", "NO_BATTERY"])
@pytest.mark.parametrize("soc_frac", [0.0, 0.1, 0.5, 1.0])
@pytest.mark.parametrize("solar,load", [(0.0, 0.4), (2.6, 0.4), (0.5, 0.5), (0.3, 3.0)])
def test_balance_and_bounds(lim, mode, soc_frac, solar, load):
    soc = lim.cap_kwh * soc_frac
    f = simulate_slot(mode, 3.0, soc, solar, load, lim)
    assert _balanced(f, solar, load)
    assert f.export_kwh <= lim.max_export_kwh + 1e-9
    assert f.charge_kwh <= lim.max_charge_kwh + 1e-9
    assert f.discharge_kwh <= lim.max_discharge_kwh + 1e-9
    assert 0 <= f.soc_end_kwh <= lim.max_soc_kwh + 1e-9
    if soc >= lim.min_soc_kwh:
        assert f.soc_end_kwh >= lim.min_soc_kwh - 1e-9
    assert not (f.charge_kwh > 0 and f.discharge_kwh > 0)
    assert not (f.import_kwh > 0 and f.export_kwh > 0)


def test_eco_serves_load_from_battery_before_grid(lim):
    f = simulate_slot("ECO", 0.0, lim.cap_kwh, 0.0, 1.0, lim)
    assert f.discharge_kwh == pytest.approx(1.0)
    assert f.import_kwh == 0.0


def test_eco_imports_only_when_floor_reached(lim):
    f = simulate_slot("ECO", 0.0, lim.min_soc_kwh, 0.0, 1.0, lim)
    assert f.discharge_kwh == 0.0
    assert f.import_kwh == pytest.approx(1.0)


def test_eco_curtails_only_when_full_and_export_capped(lim):
    full = simulate_slot("ECO", 0.0, lim.max_soc_kwh, 2.6, 0.4, lim)
    assert full.export_kwh == pytest.approx(lim.max_export_kwh)
    assert full.curtail_kwh == pytest.approx(2.2 - lim.max_export_kwh)
    empty = simulate_slot("ECO", 0.0, lim.min_soc_kwh, 2.6, 0.4, lim)
    assert empty.curtail_kwh == 0.0
    assert empty.charge_kwh == pytest.approx(min(lim.max_charge_kwh, 2.2))


def test_charge_pulls_from_grid_and_never_discharges(lim):
    f = simulate_slot("CHARGE", 3.0, lim.min_soc_kwh, 0.0, 0.4, lim)
    assert f.charge_kwh == pytest.approx(min(1.5, lim.max_charge_kwh))
    assert f.import_kwh == pytest.approx(0.4 + f.charge_kwh)
    assert f.discharge_kwh == 0.0


def test_export_bounded_by_cap_and_availability(lim):
    f = simulate_slot("DISCHARGE_EXPORT", 10.0, lim.max_soc_kwh, 0.0, 0.4, lim)
    assert f.export_kwh == pytest.approx(min(lim.max_export_kwh, lim.max_discharge_kwh - 0.4))
    low = simulate_slot("DISCHARGE_EXPORT", 10.0, lim.min_soc_kwh + 0.2, 0.0, 0.4, lim)
    assert low.discharge_kwh == pytest.approx(0.2 * lim.eff_d)
    assert low.soc_end_kwh == pytest.approx(lim.min_soc_kwh)


def test_no_battery_reference(lim):
    _, _, _, solar, load = synthetic_day()
    flows = run_no_battery(solar, load, lim)
    for f, s, l in zip(flows, solar, load):
        assert f.import_kwh == pytest.approx(max(l - s, 0))
        assert f.charge_kwh == 0 and f.discharge_kwh == 0


def test_run_eco_chains_soc(lim):
    _, _, _, solar, load = synthetic_day()
    flows = run_eco(lim.min_soc_kwh, solar, load, lim)
    for a, b in zip(flows, flows[1:]):
        assert b.soc_start_kwh == pytest.approx(a.soc_end_kwh)
    assert max(f.soc_end_kwh for f in flows) > lim.min_soc_kwh + 1
