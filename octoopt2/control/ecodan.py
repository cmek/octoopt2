"""Control Mitsubishi Ecodan DHW via MELCloud API (pymelcloud).

pymelcloud is async-only. All public functions here are synchronous wrappers
that run the async code via asyncio.run(), keeping the rest of the codebase
sync-friendly.

DHW is controlled by switching the heat pump's operation mode:
  "force_hot_water"  — actively heat the tank now
  "auto"             — return to the heat pump's own schedule
"""
import asyncio
import logging

import aiohttp
import pymelcloud
from pymelcloud import DEVICE_TYPE_ATW

from ..config import MelCloudConfig

logger = logging.getLogger(__name__)


def set_dhw(config: MelCloudConfig, enabled: bool) -> None:
    """Enable or disable forced DHW heating.

    enabled=True  → operation_mode = "force_hot_water"
    enabled=False → operation_mode = "auto"
    """
    asyncio.run(_set_dhw_async(config, enabled))


def get_dhw_state(config: MelCloudConfig) -> dict:
    """Return current DHW state from MELCloud.

    Returns a dict with keys: operation_mode, tank_temperature,
    target_tank_temperature, status.
    """
    return asyncio.run(_get_dhw_state_async(config))


# ── Async internals ────────────────────────────────────────────────────────

async def _get_device(
    config: MelCloudConfig,
    session: aiohttp.ClientSession,
):
    """Authenticate and return the ATW device matching config.device_id."""
    token = await pymelcloud.login(
        config.email,
        config.password,
        session=session,
    )
    devices = await pymelcloud.get_devices(token, session=session)
    atw_devices = devices.get(DEVICE_TYPE_ATW, [])

    if not atw_devices:
        raise RuntimeError("No ATW (air-to-water) devices found in MELCloud account")

    for device in atw_devices:
        if device.device_id == config.device_id:
            return device

    available = [d.device_id for d in atw_devices]
    raise RuntimeError(
        f"Device ID {config.device_id} not found in MELCloud. "
        f"Available ATW devices: {available}"
    )


async def _set_dhw_async(config: MelCloudConfig, enabled: bool) -> None:
    mode = "force_hot_water" if enabled else "auto"
    async with aiohttp.ClientSession() as session:
        device = await _get_device(config, session)
        await device.update()
        current_mode = device.operation_mode
        if current_mode == mode:
            logger.debug("DHW already in mode '%s', no change needed", mode)
            return
        logger.info("DHW → %s (was: %s)", mode, current_mode)
        await device.set({"operation_mode": mode})


async def _get_dhw_state_async(config: MelCloudConfig) -> dict:
    async with aiohttp.ClientSession() as session:
        device = await _get_device(config, session)
        await device.update()
        return {
            "operation_mode": device.operation_mode,
            "tank_temperature": device.tank_temperature,
            "target_tank_temperature": device.target_tank_temperature,
            "status": device.status,
        }
