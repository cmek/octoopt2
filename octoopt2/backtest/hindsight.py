"""Perfect-foresight ("hindsight") optimisation over actual load and solar.

For every London day in a segment the MILP is solved over that day plus
`lookahead_days` following days (so the battery has a reason to hold energy at
midnight), only the first day's decisions are kept, and the end-of-day SoC is
carried into the next solve. With actual load and solar as inputs this is the
cheapest the current hardware could possibly have done under a given price
series — the ceiling the live controller is measured against.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import date

from ..config import BatteryConfig, DhwConfig, GivEnergyConfig
from ..optimizer.model import OptimizerInput, SlotDecision, optimize
from .data import Segment

logger = logging.getLogger(__name__)


@dataclass
class HindsightDay:
    day: date
    decisions: list[SlotDecision]
    solver_status: str
    solve_seconds: float

    @property
    def cost_gbp(self) -> float:
        return sum(d.slot_cost_gbp for d in self.decisions)


def run_hindsight(
    segment: Segment,
    buy: list[float],
    sell: list[float],
    battery: BatteryConfig,
    dhw: DhwConfig,
    givenergy: GivEnergyConfig,
    *,
    lookahead_days: int = 1,
    time_limit_s: int = 60,
    terminal: str = "value",       # value | end-as-start | none
    cycle_cost: float = 0.0,
    dhw_beta_kwh: float | None = None,
) -> list[HindsightDay]:
    """Solve day by day with perfect information. `buy`/`sell` are aligned
    with segment.slots (so alternative tariffs can be passed in).

    dhw_beta_kwh: when given, DHW energy is removed from the actual load on
      slots the live plan enabled DHW and the MILP re-places it (manage_dhw).
      Not energy-conserving, hence opt-in.
    """
    bat = replace(battery, cycle_cost_gbp_kwh=cycle_cost)
    out: list[HindsightDay] = []
    soc = segment.soc0_kwh
    days = segment.days
    for i, day in enumerate(days):
        idx = list(segment.day_index[day])
        n_keep = len(idx)
        for j in range(1, lookahead_days + 1):
            if i + j < len(days):
                idx += segment.day_index[days[i + j]]
        slots = [segment.slots[k] for k in idx]
        load = [s.load_kwh or 0.0 for s in slots]
        solar = [s.solar_kwh or 0.0 for s in slots]
        manage_dhw = False
        if dhw_beta_kwh:
            manage_dhw = True
            load = [max(0.2, l - dhw_beta_kwh) if s.plan_dhw_on else l for l, s in zip(load, slots)]
        inputs = OptimizerInput(
            slot_starts=[s.slot_start for s in slots],
            initial_soc_kwh=soc,
            buy_prices=[buy[k] for k in idx],
            sell_prices=[sell[k] for k in idx],
            solar_forecast=solar,
            load_forecast=load,
        )
        last_window = i + lookahead_days >= len(days)
        kwargs: dict = {"time_limit_s": time_limit_s, "allow_curtailment": True}
        if terminal == "none":
            kwargs["terminal_soc_value_gbp_kwh"] = 0.0
        elif terminal == "end-as-start":
            kwargs["terminal_soc_value_gbp_kwh"] = 0.0
            if last_window:
                kwargs["min_terminal_soc_kwh"] = min(soc, battery.max_soc_kwh)
        # "value": model default — mean positive buy × discharge efficiency
        res = optimize(inputs, bat, dhw, givenergy, manage_dhw=manage_dhw,
                       dhw_kwh_per_slot=dhw_beta_kwh, **kwargs)
        kept = res.decisions[:n_keep]
        out.append(HindsightDay(day=day, decisions=kept, solver_status=res.solver_status,
                                solve_seconds=res.solve_seconds))
        soc = kept[-1].soc_end_kwh
        if res.solver_status != "Optimal":
            logger.warning("Hindsight %s: solver status %s", day, res.solver_status)
    return out
