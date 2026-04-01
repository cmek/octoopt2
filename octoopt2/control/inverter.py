"""Send charge/discharge commands to GivEnergy inverter via local Modbus TCP.

Uses the vendored givenergy_modbus_async client (from GivTCP) which supports
firmware 912+ / battery firmware 3015+. Async calls are wrapped synchronously.

Three operating states per slot:
  CHARGE    — force grid charging at desired rate
  DISCHARGE — discharge to meet demand, or at max power when exporting
  ECO       — dynamic mode: solar self-consumption, no forced activity
"""
import asyncio
import logging
import time
from datetime import time as dt_time

from ..config import BatteryConfig, GivEnergyConfig
from ..givenergy_modbus_async.client.client import Client
from ..givenergy_modbus_async.client import commands
from ..optimizer.model import SlotDecision

logger = logging.getLogger(__name__)

_THRESHOLD_KWH = 0.05
_REGISTER_MAX = 50
_RETRY_ATTEMPTS = 3
_RETRY_DELAY_S = 2


def apply_decision(
    decision: SlotDecision,
    config: GivEnergyConfig,
    battery: BatteryConfig,
) -> None:
    """Translate a slot decision into inverter register writes."""
    wants_charge = decision.battery_charge_kwh > _THRESHOLD_KWH
    wants_discharge = decision.battery_discharge_kwh > _THRESHOLD_KWH
    wants_export = decision.grid_export_kwh > _THRESHOLD_KWH

    if wants_charge:
        charge_kw = decision.battery_charge_kwh / 0.5
        _run_with_retry(config, _cmds_charge(charge_kw, battery))
    elif wants_discharge:
        discharge_kw = decision.battery_discharge_kwh / 0.5
        _run_with_retry(config, _cmds_discharge(discharge_kw, battery, wants_export))
    else:
        _run_with_retry(config, _cmds_eco())


def _cmds_charge(charge_kw: float, battery: BatteryConfig) -> list:
    limit = _kw_to_register(charge_kw, battery.max_charge_rate_kw)
    logger.info("Inverter → CHARGE %.2f kW (register %d)", charge_kw, limit)
    return [
        *commands.set_charge_slot_1((dt_time(0, 0), dt_time(23, 59))),
        *commands.set_battery_charge_limit(limit),
        *commands.enable_charge(),
        *commands.disable_discharge(),
    ]


def _cmds_discharge(discharge_kw: float, battery: BatteryConfig, export: bool) -> list:
    limit = _kw_to_register(discharge_kw, battery.max_discharge_rate_kw)
    mode = "EXPORT" if export else "DEMAND"
    logger.info("Inverter → DISCHARGE/%s %.2f kW (register %d)", mode, discharge_kw, limit)
    reqs = [*commands.set_battery_discharge_limit(limit)]
    if export:
        reqs += [*commands.set_discharge_mode_max_power()]
    else:
        reqs += [*commands.set_discharge_mode_to_match_demand()]
    reqs += [
        *commands.enable_discharge(),
        *commands.disable_charge(),
    ]
    return reqs


def _cmds_eco() -> list:
    logger.info("Inverter → ECO (dynamic mode)")
    return [
        *commands.set_discharge_mode_to_match_demand(),
        *commands.set_shallow_charge(4),
        *commands.disable_discharge(),
    ]


async def _send_async(config: GivEnergyConfig, reqs: list) -> None:
    client = Client(host=config.host, port=config.port)
    await client.one_shot_command(reqs, timeout=3, retries=3)


def _run_with_retry(config: GivEnergyConfig, reqs: list) -> None:
    """Send commands with retry on failure."""
    last_exc: Exception | None = None
    for attempt in range(1, _RETRY_ATTEMPTS + 1):
        try:
            asyncio.run(_send_async(config, reqs))
            return
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Inverter command attempt %d/%d failed: %s",
                attempt, _RETRY_ATTEMPTS, exc,
            )
            if attempt < _RETRY_ATTEMPTS:
                time.sleep(_RETRY_DELAY_S)

    raise RuntimeError(
        f"Failed to apply inverter command after {_RETRY_ATTEMPTS} attempts"
    ) from last_exc


def _kw_to_register(kw: float, max_kw: float) -> int:
    """Map a desired power (kW) to the 1–50 register range."""
    fraction = min(kw / max_kw, 1.0) if max_kw > 0 else 1.0
    return max(1, min(_REGISTER_MAX, round(fraction * _REGISTER_MAX)))
