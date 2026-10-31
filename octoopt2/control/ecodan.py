"""Control Mitsubishi Ecodan DHW via MELCloud API (pymelcloud).

pymelcloud is async-only. All public functions here are synchronous wrappers
that run the async code via asyncio.run(), keeping the rest of the codebase
sync-friendly.

Two MELCloud properties control DHW:

  operation_mode           "force_hot_water" heats the tank now. The Ecodan
                           clears it by itself once the tank reaches target, so
                           a forced slot is one-shot — "auto" is the resting
                           state, not an "off" state.
  target_tank_temperature  The Ecodan's own thermostat reheats whenever the
                           tank falls ~10 °C below this, whatever the price.
                           Holding it at DhwConfig.off_target_c outside planned
                           slots is what actually keeps DHW off (off_mode
                           "setpoint"); off_mode "auto" leaves it untouched.

dhw_command() turns a plan decision plus the live tank temperature into the
(force, target) pair to write; every writer (scheduler tick, daemon comfort
guard, safe fallback, away mode) goes through apply_dhw().
"""
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp
import pymelcloud
from pymelcloud import DEVICE_TYPE_ATW

from ..config import DhwConfig, MelCloudConfig
from ..db import get_conn, record_fetch

logger = logging.getLogger(__name__)

MODE_FORCE = "force_hot_water"
MODE_AUTO = "auto"


@dataclass(frozen=True)
class DhwCommand:
    """What to write to the Ecodan: forced-DHW flag and tank target (°C).

    target_c=None leaves the target as it is on the device.
    """
    force: bool
    target_c: float | None

    @property
    def mode(self) -> str:
        return MODE_FORCE if self.force else MODE_AUTO

    def describe(self) -> str:
        t = "" if self.target_c is None else f" {self.target_c:g}°C"
        return f"{self.mode}{t}"


def dhw_command(planned_on: bool, tank_c: float | None, dhw: DhwConfig) -> DhwCommand:
    """Decide the Ecodan command for a slot.

    planned ON                         → force, normal target
    off, off_mode "auto"               → clear force, leave target (legacy)
    off, tank below min_tank_c (guard) → clear force, normal target so the
                                         Ecodan reheats; the tank is too cold
                                         to wait for the next planned slot
    off, otherwise                     → clear force, hold off_target_c
    """
    if planned_on:
        return DhwCommand(force=True, target_c=dhw.target_c)
    if dhw.off_mode == "auto":
        return DhwCommand(force=False, target_c=None)
    if guard_active(tank_c, dhw):
        return DhwCommand(force=False, target_c=dhw.target_c)
    return DhwCommand(force=False, target_c=dhw.off_target_c)


def guard_active(tank_c: float | None, dhw: DhwConfig) -> bool:
    """True when the comfort guard should override an off slot."""
    return bool(dhw.min_tank_c) and tank_c is not None and tank_c < dhw.min_tank_c


def release_command(dhw: DhwConfig) -> DhwCommand:
    """Hand the Ecodan back to itself: auto mode at the normal target.

    Used whenever octoopt2 stops managing DHW (safe fallback, daemon shutdown,
    away mode, --no-dhw) so a lowered target can never outlive the controller.
    Under off_mode "auto" the target is left alone, as before.
    """
    return DhwCommand(force=False, target_c=None if dhw.off_mode == "auto" else dhw.target_c)


def apply_dhw(config: MelCloudConfig, cmd: DhwCommand) -> None:
    """Write a DhwCommand to MELCloud (no-op when the device already matches)."""
    asyncio.run(apply_dhw_async(config, cmd))


def release_dhw(config: MelCloudConfig, dhw: DhwConfig) -> None:
    """Sync shortcut for apply_dhw(release_command(dhw))."""
    apply_dhw(config, release_command(dhw))


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


async def apply_dhw_async(config: MelCloudConfig, cmd: DhwCommand) -> None:
    """Write cmd to the device in one MELCloud call, skipping unchanged fields.

    Reads the device first so a slot-boundary re-apply (same mode, same target)
    costs one GET and no POST.
    """
    async with aiohttp.ClientSession() as session:
        device = await _get_device(config, session)
        await device.update()
        current_mode = device.operation_mode
        current_target = device.target_tank_temperature
        props: dict = {}
        if current_mode != cmd.mode:
            props["operation_mode"] = cmd.mode
        if cmd.target_c is not None and not _same_temp(current_target, cmd.target_c):
            props["target_tank_temperature"] = cmd.target_c
        if not props:
            logger.debug("DHW already %s, no change needed", cmd.describe())
            return
        was = f"{current_mode} {current_target:g}°C" if current_target is not None else str(current_mode)
        logger.info("DHW → %s (was: %s)", cmd.describe(), was)
        await device.set(props)


def _same_temp(a: float | None, b: float) -> bool:
    return a is not None and abs(float(a) - b) < 0.25


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
