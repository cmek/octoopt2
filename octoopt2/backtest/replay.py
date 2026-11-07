"""Replays that separate planning error from execution error.

Level 1 — planned-executed: take the last plan the controller wrote for each
slot (the `schedule` row, optimised at the slot's own start with the live
SoC), map it through the controller's own mode rules, and execute it on the
simulator against *actual* solar and load. Any difference between this and
the measured actuals is pure execution error (hardware not doing what it was
told), and the difference to the hindsight optimum is planning error (short
horizon, forecast noise, model mismatch).

Level 2 — rolling-horizon replay (opt-in, slow): re-solve the MILP at every
slot with only the information the live controller had — prices known up to
23:00 UK (for a half-hourly import, tomorrow's only after 16:00; for a banded
import such as Cosy, whose rates are published far ahead, always 23:00 UK
tomorrow, with tomorrow's export price replaced by the controller's
``min(buy, last published sell)`` fallback until 16:00), the stored load
prediction and the Solcast P50 — and execute each first decision on the
simulator. This is "what the current algorithm would have done with perfect
execution". Whether a day is banded is read off the price structure of the
day being replayed (``_is_banded_day``), so the stored-DB tariff is handled
correctly across a tariff switch; ``RollingOptions.horizon`` overrides it.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from ..config import BatteryConfig, DhwConfig, GivEnergyConfig
from ..control.inverter import decision_to_mode
from ..optimizer.model import OptimizerInput, SlotDecision, optimize
from .data import LONDON, Segment, Slot
from .simulate import SimFlow, SimLimits, simulate_slot

logger = logging.getLogger(__name__)


def _decision_from_plan(s: Slot) -> SlotDecision | None:
    if s.plan_charge_kwh is None:
        return None
    return SlotDecision(
        slot_start=s.slot_start,
        battery_charge_kwh=s.plan_charge_kwh or 0.0,
        battery_discharge_kwh=s.plan_discharge_kwh or 0.0,
        grid_import_kwh=s.plan_import_kwh or 0.0,
        grid_export_kwh=s.plan_export_kwh or 0.0,
        dhw_on=bool(s.plan_dhw_on),
        soc_start_kwh=0.0, soc_end_kwh=0.0, slot_cost_gbp=0.0,
    )


def planned_modes(slots: list[Slot], battery: BatteryConfig) -> list[tuple[str, float]]:
    """The hardware mode the controller applied for each slot (ECO when no plan)."""
    modes = []
    for s in slots:
        d = _decision_from_plan(s)
        modes.append(decision_to_mode(d, battery) if d else ("ECO", 0.0))
    return modes


def run_planned_executed(
    segment: Segment, battery: BatteryConfig, lim: SimLimits, *, chain: bool = False,
) -> list[SimFlow]:
    """Level 1: the controller's own plan, executed perfectly on actual flows.

    Each stored plan was solved from the SoC measured at that slot's start, so
    by default every slot is executed from that *measured* SoC too: the
    difference to the measured flows is then purely "the hardware did not do
    what it was told", with identical starting state, solar and load.
    chain=True instead carries the simulated SoC forward (a counterfactual
    trajectory that drifts from the plans' assumptions).
    """
    flows: list[SimFlow] = []
    soc = segment.soc0_kwh
    for s, (mode, kw) in zip(segment.slots, planned_modes(segment.slots, battery)):
        if not chain and s.soc_start_pct is not None:
            soc = s.soc_start_pct / 100 * lim.cap_kwh
        f = simulate_slot(mode, kw, soc, s.solar_kwh or 0.0, s.load_kwh or 0.0, lim)
        flows.append(f)
        soc = f.soc_end_kwh
    return flows


# ── Level 2 ───────────────────────────────────────────────────────────────────

def _known_horizon_end(now: datetime, *, banded_import: bool = False) -> datetime:
    """Prices the live controller had at `now`. Half-hourly import (Agile):
    to 23:00 UK today, tomorrow's day once local time is past 16:00. Banded
    import: the feed keeps today and tomorrow, so always 23:00 UK tomorrow."""
    local = now.astimezone(LONDON)
    if banded_import or local.hour >= 16:
        end_day = local.date() + timedelta(days=1)
    else:
        end_day = local.date()
    end = datetime(end_day.year, end_day.month, end_day.day, 23, 0, tzinfo=LONDON)
    if end <= local:
        end += timedelta(days=1)
    return end


# A banded day (Cosy 3 bands, Go 2, Flux 3, flexible 1) has a handful of
# distinct prices; an Agile day has dozens.
BANDED_MAX_DISTINCT = 8


def _is_banded_day(prices: list[float]) -> bool:
    return len({round(p, 6) for p in prices}) <= BANDED_MAX_DISTINCT


def _replay_sell(
    buy: list[float], sell: list[float], idx: list[int], slots: list[Slot], sell_known_end: datetime,
) -> list[float]:
    """Sell prices as the controller saw them on a banded day: the real rate up
    to `sell_known_end` (Agile Outgoing's publication edge), then
    ``min(buy, last published)`` exactly as scheduler.run fills NULL rates."""
    out: list[float] = []
    last: float | None = None
    for k in idx:
        if slots[k].slot_start < sell_known_end:
            last = sell[k]
            out.append(sell[k])
        else:
            out.append(min(buy[k], last) if last is not None else buy[k])
    return out


@dataclass
class RollingOptions:
    replan_every: int = 1
    time_limit_s: int = 10
    solar_scale: float = 1.0
    solar_source: str = "p50"       # p50 | pred | actual
    load_source: str = "pred"       # pred | actual
    cycle_cost: float | None = None  # None → battery config value
    terminal: str = "value"
    horizon: str = "auto"           # auto (per-day from price structure) | agile | banded


def run_rolling_replay(
    segment: Segment,
    buy: list[float],
    sell: list[float],
    battery: BatteryConfig,
    dhw: DhwConfig,
    givenergy: GivEnergyConfig,
    lim: SimLimits,
    opts: RollingOptions,
) -> list[SimFlow]:
    bat = battery if opts.cycle_cost is None else replace(battery, cycle_cost_gbp_kwh=opts.cycle_cost)
    slots = segment.slots
    n = len(slots)
    flows: list[SimFlow] = []
    soc = segment.soc0_kwh
    pending: list[SlotDecision] = []
    if opts.horizon == "auto":
        banded_by_day = {d: _is_banded_day([buy[k] for k in ix]) for d, ix in segment.day_index.items()}
    else:
        banded_by_day = {}
    for t in range(n):
        if not pending or (t % opts.replan_every == 0):
            if opts.horizon == "auto":
                banded = banded_by_day.get(slots[t].london_day, False)
            else:
                banded = opts.horizon == "banded"
            end = _known_horizon_end(slots[t].slot_start, banded_import=banded)
            idx = [k for k in range(t, n) if slots[k].slot_start < end]
            if not idx:
                idx = [t]
            win = [slots[k] for k in idx]
            if banded:
                # Export is still day-ahead: beyond its publication edge the
                # controller planned with the NULL-sell fallback, not real rates.
                sell_win = _replay_sell(buy, sell, idx, slots, _known_horizon_end(slots[t].slot_start))
            else:
                sell_win = [sell[k] for k in idx]
            if opts.solar_source == "actual":
                solar = [s.solar_kwh or 0.0 for s in win]
            elif opts.solar_source == "pred":
                solar = [(s.pred_solar_kwh if s.pred_solar_kwh is not None else s.solar_kwh or 0.0) for s in win]
            else:
                solar = [(s.solcast_p50_kwh if s.solcast_p50_kwh is not None else s.solar_kwh or 0.0) for s in win]
            solar = [x * opts.solar_scale for x in solar]
            if opts.load_source == "actual":
                load = [s.load_kwh or 0.0 for s in win]
            else:
                load = [(s.pred_load_kwh if s.pred_load_kwh is not None else s.load_kwh or 0.0) for s in win]
            inputs = OptimizerInput(
                slot_starts=[s.slot_start for s in win], initial_soc_kwh=soc,
                buy_prices=[buy[k] for k in idx], sell_prices=sell_win,
                solar_forecast=solar, load_forecast=load,
            )
            kwargs: dict = {"time_limit_s": opts.time_limit_s}
            if opts.terminal == "none":
                kwargs["terminal_soc_value_gbp_kwh"] = 0.0
            try:
                res = optimize(inputs, bat, dhw, givenergy, manage_dhw=False, **kwargs)
                pending = list(res.decisions)
            except RuntimeError as exc:
                logger.warning("Replay %s: %s — ECO this slot", slots[t].slot_start, exc)
                pending = []
        d = pending.pop(0) if pending else None
        mode, kw = decision_to_mode(d, bat) if d else ("ECO", 0.0)
        f = simulate_slot(mode, kw, soc, slots[t].solar_kwh or 0.0, slots[t].load_kwh or 0.0, lim)
        flows.append(f)
        soc = f.soc_end_kwh
    return flows
