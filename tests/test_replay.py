"""Rolling replay: the price horizon the live controller had, per tariff shape."""
from datetime import datetime, timedelta, timezone

import pytest

from octoopt2.backtest import replay as R
from octoopt2.backtest.data import LONDON, Segment, Slot
from octoopt2.backtest.simulate import SimLimits, simulate_slot


def L(d, h, m=0):
    return datetime(2026, 11, d, h, m, tzinfo=LONDON)


def E(d):
    return datetime(2026, 11, d, 23, 0, tzinfo=LONDON)


def test_known_horizon_end_agile_rule():
    assert R._known_horizon_end(L(10, 10)) == E(10)
    assert R._known_horizon_end(L(10, 16, 30)) == E(11)
    assert R._known_horizon_end(L(10, 23, 30)) == E(11)


def test_known_horizon_end_banded_always_sees_tomorrow():
    assert R._known_horizon_end(L(10, 10), banded_import=True) == E(11)
    assert R._known_horizon_end(L(10, 16, 30), banded_import=True) == E(11)
    assert R._known_horizon_end(L(10, 23, 30), banded_import=True) == E(11)


def test_is_banded_day():
    cosy = [0.140651] * 16 + [0.286808] * 26 + [0.430212] * 6
    assert R._is_banded_day(cosy)
    assert not R._is_banded_day([0.10 + 0.001 * i for i in range(48)])


def _slots(n, t0=datetime(2026, 11, 10, 12, tzinfo=timezone.utc)):
    return [Slot(slot_start=t0 + timedelta(minutes=30 * i), buy=0.14, sell=0.1, solar_kwh=0.0,
                 load_kwh=0.4, act_import_kwh=0, act_export_kwh=0, source="actuals") for i in range(n)]


def test_replay_sell_falls_back_after_publication_edge():
    slots = _slots(4)
    edge = slots[2].slot_start
    sell = [0.10, 0.12, 0.30, 0.30]
    assert R._replay_sell([0.14] * 4, sell, [0, 1, 2, 3], slots, edge) == [0.10, 0.12, 0.12, 0.12]
    assert R._replay_sell([0.14, 0.14, 0.05, 0.14], sell, [0, 1, 2, 3], slots, edge) == [0.10, 0.12, 0.05, 0.12]
    # nothing published before the edge → import price
    assert R._replay_sell([0.14, 0.14], sell[:2], [0, 1], slots[:2], slots[0].slot_start) == [0.14, 0.14]


def test_rolling_replay_runs_on_a_flat_band(battery, dhw, givenergy):
    """Wiring check: a banded day goes through the banded horizon and sell
    fallback without error and yields one flow per slot."""
    t0 = datetime(2026, 11, 10, tzinfo=timezone.utc)
    sl = _slots(48, t0)
    day = sl[0].london_day
    seg = Segment(days=[day], slots=sl, soc0_kwh=battery.min_soc_kwh, day_index={day: list(range(48))})
    buy = [0.14] * 8 + [0.287] * 24 + [0.43] * 6 + [0.287] * 6 + [0.14] * 4
    sell = [0.10] * 48
    lim = SimLimits.from_config(battery, givenergy)
    opts = R.RollingOptions(replan_every=48, time_limit_s=10)
    flows = R.run_rolling_replay(seg, buy, sell, battery, dhw, givenergy, lim, opts)
    assert len(flows) == 48
    # cheap morning band → the battery is filled before the 43p peak
    assert sum(f.charge_kwh for f in flows[:8]) > 3.0


def test_hold_mode_does_not_discharge(battery, givenergy):
    lim = SimLimits.from_config(battery, givenergy)
    f = simulate_slot("HOLD", 0.0, lim.cap_kwh, 0.0, 1.0, lim)
    assert f.discharge_kwh == 0.0
    assert f.import_kwh == pytest.approx(1.0)
    assert f.soc_end_kwh == pytest.approx(lim.cap_kwh)
    g = simulate_slot("HOLD", 0.0, 2.0, 2.0, 0.5, lim)      # surplus solar still charges
    assert g.charge_kwh == pytest.approx(1.5) and g.import_kwh == 0.0 and g.discharge_kwh == 0.0
