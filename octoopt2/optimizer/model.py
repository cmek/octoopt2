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

  curtail[t]        kWh of solar not used   (continuous, ≥ 0; what the inverter
                                            clips when the battery is full and
                                            export is capped)

AC power balance (per slot):
  solar[t] - curtail[t] + grid_import[t] + bat_discharge[t]
      = load[t] + dhw_on[t]*dhw_kwh_per_slot + grid_export[t] + bat_charge[t]

SoC update:
  soc[t+1] = soc[t] + bat_charge[t]*charge_eff - bat_discharge[t]/discharge_eff

DHW per UK calendar day d (manage_dhw):
  min_slots − done[d] ≤ Σ_{t∈d} dhw_on[t] ≤ max_slots − done[d]
  where done[d] is the number of ON slots already executed that day before the
  horizon started (dhw_done_today), so a re-run mid-day cannot re-plan a full
  quota into the remaining slots.

Objective:
  minimise Σ_t ( buy_eff[t]*grid_import[t] - sell_eff[t]*grid_export[t]
                 + cycle_cost*(bat_charge[t] + bat_discharge[t]) )
           - terminal_value * soc[n]
  where buy_eff/sell_eff are the tariff prices plus any Axle event uplift
  (inputs.axle_uplift: + on sell for export events, − on buy for import
  events). slot_cost_gbp uses the same effective prices so it reflects what the
  plan is worth; the tariff prices stay in inputs for persistence.
"""
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
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
    load_forecast: list[float]    # kWh per slot (predicted NON-DHW gross load;
                                  # DHW added separately via dhw_kwh_per_slot)
    axle_uplift: list[float] | None = None  # £/kWh: >0 added to sell (export
                                  # event), <0 added to buy (import event)

    def __post_init__(self) -> None:
        n = len(self.slot_starts)
        if not (len(self.buy_prices) == len(self.sell_prices)
                == len(self.solar_forecast) == len(self.load_forecast) == n):
            raise ValueError("All input lists must have the same length")
        if self.axle_uplift is not None and len(self.axle_uplift) != n:
            raise ValueError("axle_uplift must have the same length as slot_starts")

    def effective_prices(self) -> tuple[list[float], list[float]]:
        """(buy, sell) with any Axle uplift applied."""
        if not self.axle_uplift:
            return list(self.buy_prices), list(self.sell_prices)
        buy = [b + min(u, 0.0) for b, u in zip(self.buy_prices, self.axle_uplift)]
        sell = [s + max(u, 0.0) for s, u in zip(self.sell_prices, self.axle_uplift)]
        return buy, sell


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
    slot_cost_gbp: float   # negative = net revenue (grid cost only, no virtual terms)
    curtailed_kwh: float = 0.0  # solar the plan expects to clip this slot


@dataclass
class OptimizerResult:
    decisions: list[SlotDecision]
    total_cost_gbp: float      # objective value: grid cost + virtual cycle cost - terminal value
    solver_status: str
    optimized_at: datetime
    solve_seconds: float = 0.0
    grid_cost_gbp: float = 0.0  # Σ slot_cost_gbp — the money part only


def optimize(
    inputs: OptimizerInput,
    battery: BatteryConfig,
    dhw: DhwConfig,
    givenergy: GivEnergyConfig,
    manage_dhw: bool = True,
    dhw_kwh_per_slot: float | None = None,
    *,
    dhw_done_today: dict[date, int] | None = None,
    time_limit_s: int = 30,
    allow_curtailment: bool = True,
    terminal_soc_value_gbp_kwh: float | None = None,
    min_terminal_soc_kwh: float | None = None,
) -> OptimizerResult:
    """Run the MILP optimizer and return a schedule.

    If the solver cannot find an optimal solution it falls back to a feasible
    solution, or raises RuntimeError if the problem is infeasible.

    allow_curtailment: let the plan clip solar when the battery is full and the
        export cap binds (what the inverter physically does). Without it the
        balance is a strict equality and a sunny day with a full battery is
        infeasible.
    terminal_soc_value_gbp_kwh: credit per kWh left in the battery at the end of
        the horizon. None → battery.terminal_value_gbp_kwh, which itself defaults
        to the horizon's mean buy price × discharge efficiency. Pass 0 for the
        legacy "drain to the floor at the horizon end" behaviour.
    min_terminal_soc_kwh: hard lower bound on the final SoC (backtest use).

    dhw_kwh_per_slot: energy added to the load balance for each slot DHW is
        enabled. Pass the load model's fitted β (the empirically-observed
        average draw per enabled slot) here; it is materially lower than the
        nameplate `dhw.power_kw * SLOT_HOURS` because the tank is often already
        at target when DHW is commanded. Falls back to the nameplate value when
        None (e.g. before a model has been fitted).
    dhw_done_today: ON slots already executed per UK date before the horizon
        (see the scheduler). Subtracted from that day's min/max so the daily
        quota is honoured across re-runs. None/missing day → 0.
    """
    n = len(inputs.slot_starts)
    if n == 0:
        raise ValueError("No slots to optimize")

    if dhw_kwh_per_slot is None:
        dhw_kwh_per_slot = dhw.power_kw * SLOT_HOURS
    max_charge_kwh = battery.max_charge_rate_kw * SLOT_HOURS
    max_discharge_kwh = battery.max_discharge_rate_kw * SLOT_HOURS
    max_import_kwh = givenergy.max_import_kw * SLOT_HOURS
    max_export_kwh = givenergy.max_export_kw * SLOT_HOURS

    if terminal_soc_value_gbp_kwh is None:
        terminal_soc_value_gbp_kwh = battery.terminal_value_gbp_kwh
    if terminal_soc_value_gbp_kwh is None:
        positive = [p for p in inputs.buy_prices if p > 0]
        mean_buy = sum(positive) / len(positive) if positive else 0.0
        terminal_soc_value_gbp_kwh = mean_buy * battery.discharge_efficiency

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
    curtail = [
        pulp.LpVariable(
            f"cu_{t}", lowBound=0,
            upBound=max(0.0, inputs.solar_forecast[t]) if allow_curtailment else 0,
        )
        for t in range(n)
    ]
    # soc[0] = initial (measured reality — no floor constraint, it may already be below min)
    # soc[1..n] = planned future states — must respect the configured floor
    if inputs.initial_soc_kwh < battery.min_soc_kwh:
        logger.warning(
            "Battery SoC %.2f kWh (%.0f%%) is below configured minimum %.2f kWh (%.0f%%) — "
            "optimizer will prioritize charging to recover",
            inputs.initial_soc_kwh,
            inputs.initial_soc_kwh / battery.capacity_kwh * 100,
            battery.min_soc_kwh,
            battery.min_soc_pct,
        )
    soc = (
        [pulp.LpVariable("soc_0", lowBound=0, upBound=battery.max_soc_kwh)]
        + [
            pulp.LpVariable(f"soc_{t}", lowBound=battery.min_soc_kwh, upBound=battery.max_soc_kwh)
            for t in range(1, n + 1)
        ]
    )

    # ── Binary variables ──────────────────────────────────────────────────
    dhw_on = [pulp.LpVariable(f"dhw_{t}", cat="Binary") for t in range(n)]
    # is_charging[t]=1 → battery is charging (discharge forced to 0)
    is_charging = [pulp.LpVariable(f"ic_{t}", cat="Binary") for t in range(n)]
    # is_importing[t]=1 → grid is importing (export forced to 0)
    is_importing = [pulp.LpVariable(f"ii_{t}", cat="Binary") for t in range(n)]

    # ── Objective ─────────────────────────────────────────────────────────
    # Grid cost/revenue + a small per-kWh penalty on battery throughput.
    # The cycle cost makes the optimizer demand a minimum economic justification
    # before cycling the battery, filtering out thin-margin cycles driven by
    # forecast noise. It is not a real cost — it is never paid — but it shifts
    # the break-even spread by roughly 2× cycle_cost (≈3p with the default 1.5p).
    # Energy left in the battery at the horizon end is credited at
    # terminal_soc_value so the plan does not drain to the floor merely because
    # the horizon (which can be as short as a few hours before tomorrow's
    # prices are published) ends.
    buy_eff, sell_eff = inputs.effective_prices()
    uplift = inputs.axle_uplift or [0.0] * n
    prob += (
        pulp.lpSum(
            buy_eff[t] * grid_import[t]
            - sell_eff[t] * grid_export[t]
            + battery.cycle_cost_gbp_kwh * (bat_charge[t] + bat_discharge[t])
            for t in range(n)
        )
        - terminal_soc_value_gbp_kwh * soc[n]
    )

    # ── Initial SoC ───────────────────────────────────────────────────────
    prob += soc[0] == inputs.initial_soc_kwh
    if min_terminal_soc_kwh is not None:
        prob += soc[n] >= min_terminal_soc_kwh

    for t in range(n):
        load_t = inputs.load_forecast[t]
        solar_t = inputs.solar_forecast[t]

        # ── AC power balance ──────────────────────────────────────────────
        prob += (
            solar_t - curtail[t] + grid_import[t] + bat_discharge[t]
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

        # ── No grid round-tripping during negative-import slots ────────────
        # When buy_price < 0 the optimizer is *paid* to import. If export were a
        # free energy sink it would churn the battery — import+charge one slot,
        # discharge+export the next — purely to harvest import payments
        # (grid → battery → grid). Cap export to genuine solar surplus in these
        # slots so it can never re-export battery- or grid-sourced energy. The
        # cap is always satisfiable (excess solar = solar − load − bat_charge ≤
        # solar), so it cannot make the problem infeasible. Positive-price slots
        # are unconstrained here and keep full battery-export arbitrage. An Axle
        # export event (uplift > 0) pays ~£1/kWh for export, which is a real
        # payment rather than a churn artefact, so it lifts the cap.
        if buy_eff[t] < 0 and uplift[t] <= 0:
            prob += grid_export[t] <= solar_t

    # ── DHW daily minimums and maximums ───────────────────────────────────
    if manage_dhw:
        done_by_day = dhw_done_today or {}
        for day, day_slots in _group_slots_by_day(inputs.slot_starts).items():
            done = done_by_day.get(day, 0)
            need = max(0, dhw.min_slots_per_day - done)
            cap = max(0, dhw.max_slots_per_day - done)
            # The horizon starts mid-day and ends mid-day, so the first and last
            # UK days are partial. A partial day holding fewer slots than the
            # outstanding minimum cannot satisfy it — forcing it would make the
            # whole problem infeasible — so only require the minimum on days the
            # horizon covers enough of. The cap still applies to every (partial)
            # day to avoid over-heating at the edges.
            if need and len(day_slots) >= need:
                prob += pulp.lpSum(dhw_on[t] for t in day_slots) >= need
            prob += pulp.lpSum(dhw_on[t] for t in day_slots) <= cap
    else:
        for t in range(n):
            prob += dhw_on[t] == 0

    # ── Solve ─────────────────────────────────────────────────────────────
    solver = pulp.getSolver("PULP_CBC_CMD", msg=False, timeLimit=time_limit_s)
    t0 = time.perf_counter()
    prob.solve(solver)
    solve_seconds = time.perf_counter() - t0

    status = pulp.LpStatus[prob.status]
    # Accept a proven optimum, or a feasible incumbent found before the time
    # limit was hit (NotSolved). Anything else — notably Infeasible/Unbounded —
    # must NOT be turned into a schedule: pulp.value() still returns numbers
    # from stale/zero variables for an infeasible solve, so without this guard
    # the caller would silently apply a garbage plan. Raising lets the scheduler
    # fall back to a safe state instead.
    usable = prob.status == pulp.constants.LpStatusOptimal or (
        prob.status == pulp.constants.LpStatusNotSolved
        and pulp.value(prob.objective) is not None
    )
    if not usable:
        raise RuntimeError(f"Optimizer found no usable solution: {status}")

    logger.info(
        "Optimizer: status=%s objective=£%.4f slots=%d solve=%.2fs",
        status,
        pulp.value(prob.objective) or 0.0,
        n,
        solve_seconds,
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
            slot_cost_gbp=buy_eff[t] * gi - sell_eff[t] * ge,
            curtailed_kwh=_val(curtail[t]),
        ))

    return OptimizerResult(
        decisions=decisions,
        total_cost_gbp=pulp.value(prob.objective) or 0.0,
        solver_status=status,
        optimized_at=datetime.now(timezone.utc),
        solve_seconds=solve_seconds,
        grid_cost_gbp=sum(d.slot_cost_gbp for d in decisions),
    )


def _val(var: pulp.LpVariable) -> float:
    """Extract solved variable value, defaulting to 0 if not set."""
    v = pulp.value(var)
    return 0.0 if v is None else float(v)


def _group_slots_by_day(slot_starts: list[datetime]) -> dict[date, list[int]]:
    """Group slot indices by UK calendar day.

    Returns {uk_date: [slot indices]} in horizon order, including partial days
    at the edges. The caller decides how to treat under-covered partial days
    (see the DHW constraints in optimize()).
    """
    day_map: dict[date, list[int]] = {}
    for t, slot in enumerate(slot_starts):
        day_map.setdefault(slot.astimezone(LONDON).date(), []).append(t)
    return day_map
