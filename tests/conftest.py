"""Shared fixtures for octoopt2 tests."""
from datetime import datetime, timedelta, timezone

import pytest

from octoopt2.config import BatteryConfig, DhwConfig, GivEnergyConfig


@pytest.fixture
def battery() -> BatteryConfig:
    return BatteryConfig(cycle_cost_gbp_kwh=0.0)


@pytest.fixture
def dhw() -> DhwConfig:
    return DhwConfig()


@pytest.fixture
def givenergy() -> GivEnergyConfig:
    return GivEnergyConfig(host="", max_export_kw=3.6, max_import_kw=11.0)


def synthetic_day(day: datetime | None = None):
    """48 UTC half-hour slots with a cheap night (0.10) and dear day (0.35),
    a constant 0.15 export price, bell-shaped solar peaking at 1.9 kWh and a
    flat 0.4 kWh load. Returns (slot_starts, buy, sell, solar, load)."""
    day = day or datetime(2026, 6, 15, tzinfo=timezone.utc)
    slots = [day + timedelta(minutes=30 * i) for i in range(48)]
    buy = [0.10 if i < 12 else 0.35 for i in range(48)]
    sell = [0.15] * 48
    solar = [max(0.0, 1.9 * (1 - ((i - 26) / 10) ** 2)) for i in range(48)]
    load = [0.4] * 48
    return slots, buy, sell, solar, load
