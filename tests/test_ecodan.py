"""DHW control: the plan's off slots must actually hold the Ecodan off.

Forced-DHW mode is one-shot (the Ecodan clears it once the tank hits target),
so "auto" is the resting state, not an off state. Under off_mode="setpoint" an
off slot holds the tank target low; a planned slot forces at the normal target;
the comfort guard and every release path hand back the normal target.
"""
import asyncio

import pytest

from octoopt2.config import DhwConfig, MelCloudConfig
from octoopt2.control import ecodan
from octoopt2.control.ecodan import DhwCommand, dhw_command, guard_active, release_command

CFG = DhwConfig(off_mode="setpoint", target_c=50.0, off_target_c=40.0, min_tank_c=35.0)
LEGACY = DhwConfig(off_mode="auto")
NO_GUARD = DhwConfig(off_mode="setpoint", min_tank_c=0.0)


def test_planned_on_forces_at_normal_target():
    assert dhw_command(True, 45.0, CFG) == DhwCommand(force=True, target_c=50.0)
    assert dhw_command(True, None, CFG) == DhwCommand(force=True, target_c=50.0)


def test_off_slot_holds_target_low():
    assert dhw_command(False, 47.0, CFG) == DhwCommand(force=False, target_c=40.0)
    # Unknown tank temperature: hold low (the guard needs a reading to fire).
    assert dhw_command(False, None, CFG) == DhwCommand(force=False, target_c=40.0)


def test_comfort_guard_restores_normal_target():
    assert guard_active(34.9, CFG)
    assert not guard_active(35.0, CFG)
    assert dhw_command(False, 33.0, CFG) == DhwCommand(force=False, target_c=50.0)


def test_guard_disabled_by_zero_floor():
    assert not guard_active(20.0, NO_GUARD)
    assert dhw_command(False, 20.0, NO_GUARD) == DhwCommand(force=False, target_c=40.0)


def test_legacy_auto_mode_never_touches_target():
    assert dhw_command(False, 30.0, LEGACY) == DhwCommand(force=False, target_c=None)
    assert release_command(LEGACY) == DhwCommand(force=False, target_c=None)


def test_release_is_auto_at_normal_target():
    assert release_command(CFG) == DhwCommand(force=False, target_c=50.0)


def test_describe_and_mode():
    assert DhwCommand(True, 50.0).mode == "force_hot_water"
    assert DhwCommand(False, 40.0).describe() == "auto 40°C"
    assert DhwCommand(False, None).describe() == "auto"


def test_config_validation():
    with pytest.raises(ValueError):
        DhwConfig(off_mode="prohibit")
    with pytest.raises(ValueError):
        DhwConfig(off_target_c=55.0, target_c=50.0)
    with pytest.raises(ValueError):
        DhwConfig(min_tank_c=45.0, off_target_c=40.0)
    DhwConfig(min_tank_c=0.0, off_target_c=40.0)  # 0 disables the guard


# ── write path ─────────────────────────────────────────────────────────────

class _FakeDevice:
    def __init__(self, mode: str, target: float | None):
        self.operation_mode = mode
        self.target_tank_temperature = target
        self.writes: list[dict] = []

    async def update(self):
        pass

    async def set(self, props):
        self.writes.append(props)


class _FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


MC = MelCloudConfig(email="e", password="p", device_id=1)


def _apply(monkeypatch, device: _FakeDevice, cmd: DhwCommand) -> list[dict]:
    async def fake_get_device(config, session):
        return device
    monkeypatch.setattr(ecodan, "_get_device", fake_get_device)
    monkeypatch.setattr(ecodan.aiohttp, "ClientSession", lambda: _FakeSession())
    asyncio.run(ecodan.apply_dhw_async(MC, cmd))
    return device.writes


def test_write_sends_mode_and_target_together(monkeypatch):
    dev = _FakeDevice("auto", 40.0)
    writes = _apply(monkeypatch, dev, DhwCommand(force=True, target_c=50.0))
    assert writes == [{"operation_mode": "force_hot_water", "target_tank_temperature": 50.0}]


def test_write_skips_fields_already_matching(monkeypatch):
    dev = _FakeDevice("auto", 50.0)
    writes = _apply(monkeypatch, dev, DhwCommand(force=False, target_c=40.0))
    assert writes == [{"target_tank_temperature": 40.0}]


def test_write_is_noop_when_device_matches(monkeypatch):
    dev = _FakeDevice("auto", 40.0)
    assert _apply(monkeypatch, dev, DhwCommand(force=False, target_c=40.0)) == []


def test_write_with_none_target_only_touches_mode(monkeypatch):
    dev = _FakeDevice("force_hot_water", 50.0)
    writes = _apply(monkeypatch, dev, DhwCommand(force=False, target_c=None))
    assert writes == [{"operation_mode": "auto"}]
