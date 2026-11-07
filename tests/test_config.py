"""Env parsing of config values that gate behaviour."""
import pytest

from octoopt2.config import BatteryConfig


def test_idle_mode_default_and_env(monkeypatch):
    assert BatteryConfig().idle_mode == "eco"
    monkeypatch.delenv("BATTERY_IDLE_MODE", raising=False)
    assert BatteryConfig.from_env().idle_mode == "eco"
    monkeypatch.setenv("BATTERY_IDLE_MODE", "hold")
    assert BatteryConfig.from_env().idle_mode == "hold"
    monkeypatch.setenv("BATTERY_IDLE_MODE", " HOLD ")
    assert BatteryConfig.from_env().idle_mode == "hold"
    monkeypatch.setenv("BATTERY_IDLE_MODE", "pause")
    with pytest.raises(ValueError):
        BatteryConfig.from_env()
