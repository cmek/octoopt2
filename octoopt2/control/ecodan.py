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
from datetime import datetime, timezone

import aiohttp
import pymelcloud
from pymelcloud import DEVICE_TYPE_ATW

from ..config import MelCloudConfig
from ..db import get_conn, record_fetch

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


def read_and_store_dhw_state(config: MelCloudConfig, db_path: str) -> dict | None:
    """Read DHW state from MELCloud and persist it to dhw_readings. Best-effort.

    Returns the state dict, or None if the read failed (logged, never raised) —
    so a MELCloud hiccup can't break a scheduler tick.
    """
    try:
        state = get_dhw_state(config)
    except Exception as exc:
        logger.warning("Failed to read DHW state for logging: %s", exc)
        return None
    store_dhw_state(state, db_path)
    return state


def store_dhw_state(state: dict, db_path: str, recorded_at: datetime | None = None) -> None:
    """Persist one DHW state dict (as returned by get_dhw_state) to dhw_readings.

    Used by both the scheduler's slot-boundary read and the daemon's 5-min
    poll. Missing keys are stored as NULL; stamps the "dhw" feed.
    """
    ts = (recorded_at or datetime.now(timezone.utc)).isoformat()
    power = state.get("power")
    with get_conn(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO dhw_readings
                (recorded_at, operation_mode, tank_temperature_c,
                 target_tank_temperature_c, status,
                 outside_temperature_c, flow_temperature_c, return_temperature_c,
                 target_flow_temperature_c, room_temperature_c, room_target_c, power)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                state.get("operation_mode"),
                state.get("tank_temperature"),
                state.get("target_tank_temperature"),
                state.get("status"),
                state.get("outside_temperature"),
                state.get("flow_temperature"),
                state.get("return_temperature"),
                state.get("target_flow_temperature"),
                state.get("room_temperature"),
                state.get("room_target"),
                None if power is None else int(bool(power)),
            ),
        )
    record_fetch(db_path, "dhw")


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


def _opt(obj, name: str):
    """getattr(obj, name) or None — pymelcloud properties raise KeyError when
    MELCloud omits a field (e.g. zone flow temperature outside a full poll),
    and a missing optional must never fail the read."""
    try:
        return getattr(obj, name, None)
    except Exception:
        return None


async def _get_dhw_state_async(config: MelCloudConfig) -> dict:
    async with aiohttp.ClientSession() as session:
        device = await _get_device(config, session)
        await device.update()
        # Flow/return/target-flow and room temperatures live on the zone
        # objects (pymelcloud.atw_device.Zone); zones without a thermostat are
        # not listed, so zone 1 may be absent.
        zones = _opt(device, "zones") or []
        zone = zones[0] if zones else None
        return {
            "operation_mode": device.operation_mode,
            "tank_temperature": device.tank_temperature,
            "target_tank_temperature": device.target_tank_temperature,
            "status": device.status,
            # Optional extras (None when unavailable) — see store_dhw_state.
            "outside_temperature": _opt(device, "outside_temperature"),
            "flow_temperature": _opt(zone, "flow_temperature") if zone else None,
            "return_temperature": _opt(zone, "return_temperature") if zone else None,
            "target_flow_temperature": _opt(zone, "target_flow_temperature") if zone else None,
            "room_temperature": _opt(zone, "room_temperature") if zone else None,
            "room_target": _opt(zone, "target_temperature") if zone else None,
            "power": _opt(device, "power"),
        }
