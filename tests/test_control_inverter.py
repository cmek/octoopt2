"""Regression tests for the inverter command builders.

The important one: ECO must disable the *charge* schedule. Before this was
fixed, the 00:00–23:59 charge window armed by a CHARGE slot stayed active
through every following ECO slot, and the inverter neither discharged to the
house nor stopped grid-charging until a forced-export slot cleared it.
"""
from datetime import datetime, timezone

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
