"""Regression tests for the inverter command builders.

The important one: ECO must disable the *charge* schedule. Before this was
fixed, the 00:00–23:59 charge window armed by a CHARGE slot stayed active
through every following ECO slot, and the inverter neither discharged to the
house nor stopped grid-charging until a forced-export slot cleared it.
"""
from datetime import datetime, timedelta, timezone

from octoopt2.config import BatteryConfig
from octoopt2.control import inverter as ci
from octoopt2.givenergy_modbus_async.client.commands import RegisterMap as HR
from octoopt2.optimizer.model import SlotDecision


def _writes(reqs) -> dict[int, object]:
    return {int(r.register): r.value for r in reqs}


def test_eco_disables_both_schedules_and_sets_reserve():
    w = _writes(ci._cmds_eco(soc_reserve_pct=10))
    assert w[int(HR.ENABLE_CHARGE)] is False
    assert w[int(HR.ENABLE_DISCHARGE)] is False
    assert w[int(HR.BATTERY_SOC_RESERVE)] == 10
    assert w[int(HR.BATTERY_DISCHARGE_MIN_POWER_RESERVE)] == 10
    assert w[int(HR.ECO_MODE)] == 1


def test_charge_arms_window_and_enables_charge():
    w = _writes(ci._cmds_charge(2.0, 29, last_mode="ECO"))
    assert w[int(HR.ENABLE_CHARGE)] is True
    assert w[int(HR.ENABLE_DISCHARGE)] is False
    assert w[int(HR.BATTERY_CHARGE_LIMIT)] == 29


def test_export_disables_charge():
    w = _writes(ci._cmds_discharge(2.0, 36, last_mode="CHARGE"))
    assert w[int(HR.ENABLE_CHARGE)] is False
    assert w[int(HR.ENABLE_DISCHARGE)] is True


def _decision(charge=0.0, discharge=0.0, export=0.0) -> SlotDecision:
    return SlotDecision(
        slot_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        battery_charge_kwh=charge, battery_discharge_kwh=discharge,
        grid_import_kwh=0.0, grid_export_kwh=export, dhw_on=False,
        soc_start_kwh=0.0, soc_end_kwh=0.0, slot_cost_gbp=0.0,
    )


def test_decision_to_mode_rules():
    b = BatteryConfig()
    assert ci.decision_to_mode(_decision(charge=1.0), b) == ("CHARGE", 2.0)
    assert ci.decision_to_mode(_decision(discharge=1.0, export=0.5), b) == ("DISCHARGE_EXPORT", 2.0)
    # discharge-to-meet-load runs as ECO, not forced discharge
    assert ci.decision_to_mode(_decision(discharge=1.0), b) == ("ECO", 0.0)
    assert ci.decision_to_mode(_decision(), b) == ("ECO", 0.0)
    # below threshold → ECO
    assert ci.decision_to_mode(_decision(charge=0.01), b) == ("ECO", 0.0)


def test_kw_to_register_scales_to_measured_max():
    b = BatteryConfig()
    assert ci._kw_to_register(b.max_charge_rate_kw, b.max_charge_rate_kw) == 50
    assert ci._kw_to_register(b.max_charge_rate_kw / 2, b.max_charge_rate_kw) == 25
    assert ci._kw_to_register(0.0, b.max_charge_rate_kw) == 1


# ── Register-truth check (drift re-assert) ────────────────────────────────────

import pytest

from octoopt2.config import GivEnergyConfig
from octoopt2.data.inverter import InverterReading
from octoopt2.db import get_conn, init_db


def _reading(at: datetime, **hr) -> InverterReading:
    base = dict(recorded_at=at, soc_pct=50.0, solar_w=0.0, grid_import_w=0.0, grid_export_w=0.0,
                battery_charge_w=0.0, battery_discharge_w=0.0, load_w=500.0)
    return InverterReading(**base, **hr)


ECO_OK = dict(hr_eco_mode=1, hr_enable_charge=0, hr_enable_discharge=0, hr_soc_reserve=10, hr_power_reserve=10)


def test_registers_consistent_charge():
    r = _reading(datetime.now(timezone.utc), hr_enable_charge=1, hr_enable_discharge=0, hr_charge_limit=29)
    assert ci.registers_consistent("CHARGE", 29, r, 10) is True
    assert ci.registers_consistent("CHARGE", 30, r, 10) is False


def test_registers_consistent_export_and_eco():
    now = datetime.now(timezone.utc)
    r = _reading(now, hr_enable_discharge=1, hr_enable_charge=0, hr_eco_mode=0, hr_discharge_limit=36)
    assert ci.registers_consistent("DISCHARGE_EXPORT", 36, r, 10) is True
    assert ci.registers_consistent("ECO", 0, _reading(now, **ECO_OK), 10) is True
    # reserve left at 4% by a fallback → ECO is not consistent with hw_reserve 10
    assert ci.registers_consistent("ECO", 0, _reading(now, **{**ECO_OK, "hr_soc_reserve": 4}), 10) is False


def test_registers_consistent_unknown():
    now = datetime.now(timezone.utc)
    assert ci.registers_consistent("ECO", 0, None, 10) is None
    assert ci.registers_consistent("ECO", 0, _reading(now), 10) is None                      # all None
    assert ci.registers_consistent("ECO", 0, _reading(now, **{**ECO_OK, "hr_enable_charge": 3}), 10) is None
    # eco_mode is the same Enable enum: an unknown read (3) must not count as drift
    assert ci.registers_consistent("ECO", 0, _reading(now, **{**ECO_OK, "hr_eco_mode": 3}), 10) is None
    r = _reading(now, hr_enable_discharge=1, hr_enable_charge=0, hr_eco_mode=3, hr_discharge_limit=36)
    assert ci.registers_consistent("DISCHARGE_EXPORT", 36, r, 10) is None


@pytest.fixture
def db(tmp_path):
    p = str(tmp_path / "t.db")
    init_db(p)
    return p


@pytest.fixture
def sent(monkeypatch):
    calls: list[list] = []
    monkeypatch.setattr(ci, "_run_with_retry", lambda cfg, reqs: calls.append(list(reqs)))
    return calls


def _triggers(db):
    with get_conn(db) as conn:
        return [r["trigger"] for r in conn.execute("SELECT trigger FROM inverter_commands ORDER BY applied_at")]


GE = GivEnergyConfig(host="")
BAT = BatteryConfig(hw_reserve_pct=10)


def test_apply_decision_skips_when_last_matches_and_registers_agree(db, sent):
    ci.apply_decision(_decision(), GE, BAT, db, trigger="slow")          # ECO written once
    assert len(sent) == 1
    later = datetime.now(timezone.utc) + timedelta(seconds=5)
    ci.apply_decision(_decision(), GE, BAT, db, trigger="fast", reading=_reading(later, **ECO_OK))
    assert len(sent) == 1
    assert _triggers(db) == ["slow"]


def test_apply_decision_rewrites_on_drift(db, sent):
    ci.set_eco_mode(GE, db, reserve_pct=4)                                # fallback leaves 4%
    later = datetime.now(timezone.utc) + timedelta(seconds=5)
    drifted = _reading(later, **{**ECO_OK, "hr_soc_reserve": 4, "hr_power_reserve": 4})
    ci.apply_decision(_decision(), GE, BAT, db, trigger="fast", reading=drifted)
    assert len(sent) == 2
    assert _writes(sent[-1])[int(HR.BATTERY_SOC_RESERVE)] == 10
    assert _triggers(db) == ["fallback", "fast+drift"]


def test_apply_decision_ignores_reading_older_than_last_command(db, sent):
    ci.set_eco_mode(GE, db, reserve_pct=4)
    earlier = datetime.now(timezone.utc) - timedelta(seconds=5)
    stale = _reading(earlier, **{**ECO_OK, "hr_soc_reserve": 4, "hr_power_reserve": 4})
    ci.apply_decision(_decision(), GE, BAT, db, trigger="fast", reading=stale)
    assert len(sent) == 1                                                 # no write loop


def test_apply_decision_skips_when_registers_unknown(db, sent):
    ci.set_eco_mode(GE, db, reserve_pct=4)
    later = datetime.now(timezone.utc) + timedelta(seconds=5)
    ci.apply_decision(_decision(), GE, BAT, db, trigger="fast", reading=_reading(later))
    ci.apply_decision(_decision(), GE, BAT, db, trigger="fast")           # no reading stored either
    assert len(sent) == 1


def test_set_eco_mode_trigger_default_and_away(db, sent):
    ci.set_eco_mode(GE, db, reserve_pct=4)
    ci.set_eco_mode(GE, db, reserve_pct=20, trigger="away")
    assert _triggers(db) == ["fallback", "away"]


def test_drift_is_reasserted_only_once_per_command(db, sent):
    """A register a third party insists on must not start a write tug-of-war."""
    ci.set_eco_mode(GE, db, reserve_pct=4)
    t0 = datetime.now(timezone.utc)
    drifted = lambda s: _reading(t0 + timedelta(seconds=s), **{**ECO_OK, "hr_soc_reserve": 4, "hr_power_reserve": 4})
    ci.apply_decision(_decision(), GE, BAT, db, trigger="fast", reading=drifted(5))
    assert len(sent) == 2 and _triggers(db)[-1] == "fast+drift"
    # still disagreeing on the next ticks (reading newer than the re-assert) → no further writes
    ci.apply_decision(_decision(), GE, BAT, db, trigger="fast", reading=drifted(600))
    ci.apply_decision(_decision(), GE, BAT, db, trigger="slow", reading=drifted(1200))
    assert len(sent) == 2
    # a different plan writes normally, and the drift guard is armed again afterwards
    ci.apply_decision(_decision(charge=1.0), GE, BAT, db, trigger="slow")
    assert len(sent) == 3 and _triggers(db)[-1] == "slow"
    bad = _reading(t0 + timedelta(seconds=1800), hr_enable_charge=0, hr_enable_discharge=0, hr_charge_limit=29)
    ci.apply_decision(_decision(charge=1.0), GE, BAT, db, trigger="fast", reading=bad)
    assert len(sent) == 4 and _triggers(db)[-1] == "fast+drift"


def test_drift_reassert_rewrites_slot_window(db, sent):
    """A third party may have narrowed discharge slot 1; the re-assert restores 00:00-23:59."""
    ci.apply_decision(_decision(discharge=1.0, export=1.0), GE, BAT, db, trigger="slow")
    first = _writes(sent[-1])
    assert int(HR.DISCHARGE_SLOT_1_START) in first                      # entering the mode arms the window
    later = datetime.now(timezone.utc) + timedelta(seconds=5)
    reg = ci._kw_to_register(2.0, BAT.max_discharge_rate_kw)
    drifted = _reading(later, hr_enable_discharge=1, hr_enable_charge=0, hr_eco_mode=0, hr_discharge_limit=reg + 1)
    ci.apply_decision(_decision(discharge=1.0, export=1.0), GE, BAT, db, trigger="fast", reading=drifted)
    assert len(sent) == 2
    assert int(HR.DISCHARGE_SLOT_1_START) in _writes(sent[-1])          # window rewritten, not just the limit
    assert _writes(sent[-1])[int(HR.BATTERY_DISCHARGE_LIMIT)] == reg
