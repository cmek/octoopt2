"""MILP energy cost optimizer.

Formulation (per 30-min slot t):
─────────────────────────────────
Decision variables:
  grid_import[t]    kWh from grid          (continuous, ≥ 0)
  grid_export[t]    kWh to grid            (continuous, ≥ 0)
  bat_charge[t]     kWh into battery       (continuous, ≥ 0, AC-side)
  bat_discharge[t]  kWh out of battery     (continuous, ≥ 0, AC-side)
  soc[t]            battery SoC kWh        (continuous, at START of slot t)
  dhw_on[t]         DHW heating active     (binary)
  is_charging[t]    battery direction flag (binary, prevents simultaneous charge+discharge)
  is_importing[t]   grid direction flag    (binary, prevents simultaneous import+export)

AC power balance (per slot):
  solar[t] + grid_import[t] + bat_discharge[t]
      = load[t] + dhw_on[t]*dhw_kwh_per_slot + grid_export[t] + bat_charge[t]

SoC update:
  soc[t+1] = soc[t] + bat_charge[t]*charge_eff - bat_discharge[t]/discharge_eff

Objective:
  minimise Σ_t ( buy_price[t]*grid_import[t] - sell_price[t]*grid_export[t] )
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pulp

from ..config import BatteryConfig, DhwConfig, GivEnergyConfig

logger = logging.getLogger(__name__)

LONDON = ZoneInfo("Europe/London")
SLOT_HOURS = 0.5  # each slot is 30 minutes


@dataclass
class OptimizerInput:
    slot_starts: list[datetime]   # UTC, 30-min intervals
    initial_soc_kwh: float        # battery SoC at start of first slot
    buy_prices: list[float]       # £/kWh per slot
    sell_prices: list[float]      # £/kWh per slot
    solar_forecast: list[float]   # kWh per slot
    load_forecast: list[float]    # kWh per slot (predicted home consumption)

    def __post_init__(self) -> None:
        n = len(self.slot_starts)
        if not (len(self.buy_prices) == len(self.sell_prices)
                == len(self.solar_forecast) == len(self.load_forecast) == n):
            raise ValueError("All input lists must have the same length")


@dataclass
class SlotDecision:
    slot_start: datetime
    battery_charge_kwh: float
    battery_discharge_kwh: float
    grid_import_kwh: float
    grid_export_kwh: float
    dhw_on: bool
    soc_start_kwh: float   # SoC at start of this slot
    soc_end_kwh: float     # SoC at end of this slot
    slot_cost_gbp: float   # negative = net revenue


@dataclass
class OptimizerResult:
    decisions: list[SlotDecision]
    total_cost_gbp: float
    solver_status: str
    optimized_at: datetime


def optimize(
    inputs: OptimizerInput,
    battery: BatteryConfig,
    dhw: DhwConfig,
    givenergy: GivEnergyConfig,
    manage_dhw: bool = True,
) -> OptimizerResult:
    """Run the MILP optimizer and return a schedule.

    If the solver cannot find an optimal solution it falls back to a feasible
    solution, or raises RuntimeError if the problem is infeasible.
    """
    n = len(inputs.slot_starts)
    if n == 0:
        raise ValueError("No slots to optimize")

    dhw_kwh_per_slot = dhw.power_kw * SLOT_HOURS
    max_charge_kwh = battery.max_charge_rate_kw * SLOT_HOURS
    max_discharge_kwh = battery.max_discharge_rate_kw * SLOT_HOURS
    max_import_kwh = givenergy.max_import_kw * SLOT_HOURS
    max_export_kwh = givenergy.max_export_kw * SLOT_HOURS

    prob = pulp.LpProblem("energy_cost", pulp.LpMinimize)

    # ── Continuous variables ──────────────────────────────────────────────
    grid_import = [
        pulp.LpVariable(f"gi_{t}", lowBound=0, upBound=max_import_kwh)
        for t in range(n)
    ]
    grid_export = [
        pulp.LpVariable(f"ge_{t}", lowBound=0, upBound=max_export_kwh)
        for t in range(n)
    ]
    bat_charge = [
        pulp.LpVariable(f"bc_{t}", lowBound=0, upBound=max_charge_kwh)
        for t in range(n)
    ]
    bat_discharge = [
        pulp.LpVariable(f"bd_{t}", lowBound=0, upBound=max_discharge_kwh)
        for t in range(n)
    ]
    # soc[0] = initial, soc[1..n] = end-of-slot values
    soc = [
        pulp.LpVariable(
            f"soc_{t}",
            lowBound=battery.min_soc_kwh,
            upBound=battery.max_soc_kwh,
        )
        for t in range(n + 1)
    ]

    # ── Binary variables ──────────────────────────────────────────────────
    dhw_on = [pulp.LpVariable(f"dhw_{t}", cat="Binary") for t in range(n)]
    # is_charging[t]=1 → battery is charging (discharge forced to 0)
    is_charging = [pulp.LpVariable(f"ic_{t}", cat="Binary") for t in range(n)]
    # is_importing[t]=1 → grid is importing (export forced to 0)
    is_importing = [pulp.LpVariable(f"ii_{t}", cat="Binary") for t in range(n)]

    # ── Objective ─────────────────────────────────────────────────────────
    prob += pulp.lpSum(
        inputs.buy_prices[t] * grid_import[t]
        - inputs.sell_prices[t] * grid_export[t]
        for t in range(n)
    )

    # ── Initial SoC ───────────────────────────────────────────────────────
    prob += soc[0] == inputs.initial_soc_kwh

    for t in range(n):
        load_t = inputs.load_forecast[t]
        solar_t = inputs.solar_forecast[t]

        # ── AC power balance ──────────────────────────────────────────────
        prob += (
            solar_t + grid_import[t] + bat_discharge[t]
            == load_t + dhw_on[t] * dhw_kwh_per_slot + grid_export[t] + bat_charge[t]
        )

        # ── SoC update ────────────────────────────────────────────────────
        prob += (
            soc[t + 1]
            == soc[t]
            + bat_charge[t] * battery.charge_efficiency
            - bat_discharge[t] / battery.discharge_efficiency
        )

        # ── No simultaneous charge + discharge ────────────────────────────
        prob += bat_charge[t] <= max_charge_kwh * is_charging[t]
        prob += bat_discharge[t] <= max_discharge_kwh * (1 - is_charging[t])

        # ── No simultaneous import + export ───────────────────────────────
        prob += grid_import[t] <= max_import_kwh * is_importing[t]
        prob += grid_export[t] <= max_export_kwh * (1 - is_importing[t])

    # ── DHW daily minimums and maximums ───────────────────────────────────
    if manage_dhw:
        for day_slots in _group_slots_by_day(inputs.slot_starts):
            prob += pulp.lpSum(dhw_on[t] for t in day_slots) >= dhw.min_slots_per_day
            prob += pulp.lpSum(dhw_on[t] for t in day_slots) <= dhw.max_slots_per_day
    else:
        for t in range(n):
            prob += dhw_on[t] == 0

    # ── Solve ─────────────────────────────────────────────────────────────
    solver = pulp.getSolver("PULP_CBC_CMD", msg=False, timeLimit=30)
    prob.solve(solver)

    status = pulp.LpStatus[prob.status]
    if prob.status not in (pulp.constants.LpStatusOptimal, pulp.constants.LpStatusNotSolved):
        # LpStatusNotSolved can still yield a feasible solution within timeLimit
        if pulp.value(prob.objective) is None:
            raise RuntimeError(f"Optimizer returned no solution: {status}")

    logger.info(
        "Optimizer: status=%s cost=£%.4f slots=%d",
        status,
        pulp.value(prob.objective) or 0.0,
        n,
    )

    # ── Extract decisions ─────────────────────────────────────────────────
    decisions = []
    for t in range(n):
        bc = _val(bat_charge[t])
        bd = _val(bat_discharge[t])
        gi = _val(grid_import[t])
        ge = _val(grid_export[t])
        soc_start = _val(soc[t])
        soc_end = _val(soc[t + 1])

        decisions.append(SlotDecision(
            slot_start=inputs.slot_starts[t],
            battery_charge_kwh=bc,
            battery_discharge_kwh=bd,
            grid_import_kwh=gi,
            grid_export_kwh=ge,
            dhw_on=bool(round(_val(dhw_on[t]))),
            soc_start_kwh=soc_start,
            soc_end_kwh=soc_end,
            slot_cost_gbp=inputs.buy_prices[t] * gi - inputs.sell_prices[t] * ge,
        ))

    return OptimizerResult(
        decisions=decisions,
        total_cost_gbp=pulp.value(prob.objective) or 0.0,
        solver_status=status,
        optimized_at=datetime.now(timezone.utc),
    )


def _val(var: pulp.LpVariable) -> float:
    """Extract solved variable value, defaulting to 0 if not set."""
    v = pulp.value(var)
    return 0.0 if v is None else float(v)


def _group_slots_by_day(slot_starts: list[datetime]) -> list[list[int]]:
    """Group slot indices by UK calendar day.

    Returns a list of lists of indices. Only days with at least min_slots_per_day
    slots in the horizon are included (partial days at the edges get their own
    group and the constraint applies to however many slots are present).
    """
    from collections import defaultdict
    day_map: dict = defaultdict(list)
    for t, slot in enumerate(slot_starts):
        local_date = slot.astimezone(LONDON).date()
        day_map[local_date].append(t)
    return list(day_map.values())
