"""Assemble backtest results per London day / month / tariff and render them."""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from ..config import BatteryConfig, DhwConfig, GivEnergyConfig
from .data import LONDON, Segment, Slot
from .hindsight import run_hindsight
from .replay import RollingOptions, planned_modes, run_planned_executed, run_rolling_replay
from .simulate import SimFlow, SimLimits, run_eco, run_no_battery

logger = logging.getLogger(__name__)


@dataclass
class DayResult:
    day: str
    slots: int
    filled: int
    actual: float                 # cost of measured flows under this tariff
    planned: float | None         # controller's own plan (schedule), agile prices only
    planned_exec: float           # plan executed perfectly on the simulator (SoC chained)
    hindsight: float
    eco: float
    no_battery: float
    replay: float | None
    standing: float
    act_import: float
    act_export: float
    hind_import: float
    hind_export: float
    hind_curtail: float
    load: float
    solar: float
    solver_status: str
    soc0_source: str


@dataclass
class TariffResult:
    name: str
    notes: list[str]
    days: list[DayResult]
    months: dict[str, dict[str, float]] = field(default_factory=dict)
    totals: dict[str, float] = field(default_factory=dict)


def _sum(flows: list[SimFlow], buy: list[float], sell: list[float], idx: list[int]) -> float:
    return sum(flows[k].cost(buy[k], sell[k]) for k in idx)


def evaluate_tariff(
    name: str,
    segments: list[Segment],
    price_for: "callable",
    battery: BatteryConfig,
    dhw: DhwConfig,
    givenergy: GivEnergyConfig,
    *,
    lim: SimLimits,
    eco_lim: SimLimits,
    lookahead_days: int,
    time_limit_s: int,
    terminal: str,
    cycle_cost: float,
    dhw_beta: float | None,
    rolling: RollingOptions | None,
    is_db_tariff: bool,
) -> TariffResult:
    """price_for(segment) -> (buy, sell, standing_by_day, notes)."""
    days_out: list[DayResult] = []
    notes: list[str] = []
    for seg in segments:
        buy, sell, standing, n = price_for(seg)
        notes.extend(x for x in n if x not in notes)
        solar = [s.solar_kwh or 0.0 for s in seg.slots]
        load = [s.load_kwh or 0.0 for s in seg.slots]
        eco = run_eco(seg.soc0_kwh, solar, load, eco_lim)
        nob = run_no_battery(solar, load, lim)
        pex = run_planned_executed(seg, battery, lim, chain=True)
        hind = run_hindsight(seg, buy, sell, battery, dhw, givenergy,
                             lookahead_days=lookahead_days, time_limit_s=time_limit_s,
                             terminal=terminal, cycle_cost=cycle_cost, dhw_beta_kwh=dhw_beta)
        rep = run_rolling_replay(seg, buy, sell, battery, dhw, givenergy, lim, rolling) if rolling else None
        for hd in hind:
            idx = seg.day_index[hd.day]
            sl = [seg.slots[k] for k in idx]
            actual = sum(buy[k] * (seg.slots[k].act_import_kwh or 0) - sell[k] * (seg.slots[k].act_export_kwh or 0) for k in idx)
            planned = None
            if is_db_tariff and all(s.plan_import_kwh is not None for s in sl):
                planned = sum((s.plan_buy or s.buy) * s.plan_import_kwh - (s.plan_sell or s.sell) * s.plan_export_kwh for s in sl)
            days_out.append(DayResult(
                day=hd.day.isoformat(), slots=len(sl), filled=sum(1 for s in sl if s.source == "filled"),
                actual=actual, planned=planned,
                planned_exec=_sum(pex, buy, sell, idx), hindsight=hd.cost_gbp,
                eco=_sum(eco, buy, sell, idx), no_battery=_sum(nob, buy, sell, idx),
                replay=_sum(rep, buy, sell, idx) if rep else None,
                standing=standing.get(hd.day, 0.0),
                act_import=sum(s.act_import_kwh or 0 for s in sl), act_export=sum(s.act_export_kwh or 0 for s in sl),
                hind_import=sum(d.grid_import_kwh for d in hd.decisions), hind_export=sum(d.grid_export_kwh for d in hd.decisions),
                hind_curtail=sum(d.curtailed_kwh for d in hd.decisions),
                load=sum(load[k] for k in idx), solar=sum(solar[k] for k in idx),
                solver_status=hd.solver_status, soc0_source=seg.soc0_source,
            ))
    tr = TariffResult(name=name, notes=notes, days=days_out)
    tr.months = _aggregate(days_out, key=lambda d: d.day[:7])
    tr.totals = _aggregate(days_out, key=lambda d: "total")["total"] if days_out else {}
    return tr


_SUM_FIELDS = ("actual", "planned", "planned_exec", "hindsight", "eco", "no_battery", "replay", "standing",
               "act_import", "act_export", "hind_import", "hind_export", "hind_curtail", "load", "solar")


def _aggregate(days: list[DayResult], key) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for d in days:
        k = key(d)
        o = out.setdefault(k, {"days": 0, "non_optimal": 0})
        o["days"] += 1
        o["non_optimal"] += 0 if d.solver_status == "Optimal" else 1
        for f in _SUM_FIELDS:
            v = getattr(d, f)
            if v is None:
                o.setdefault(f, None)
                continue
            o[f] = (o.get(f) or 0.0) + v
    return out


# ── Execution diagnostics (agile / DB prices only) ────────────────────────────

def execution_diagnostics(segments: list[Segment], battery: BatteryConfig, lim: SimLimits) -> list[dict]:
    """Group slots by the plan's action and the most recent *forced* mode
    (CHARGE or DISCHARGE_EXPORT) before them. Compares planned import/export
    with what was measured and with the plan executed on the simulator."""
    groups: dict[tuple[str, str], dict] = defaultdict(lambda: defaultdict(float))
    for seg in segments:
        modes = planned_modes(seg.slots, battery)
        # per-slot execution from the *measured* SoC: same state, solar and
        # load as reality, so sim-vs-actual isolates the hardware's behaviour
        pex = run_planned_executed(seg, battery, lim, chain=False)
        prev_forced = "none"
        for s, (mode, _), f in zip(seg.slots, modes, pex):
            if s.plan_charge_kwh is None:
                continue
            act = mode
            if mode == "ECO" and (s.plan_discharge_kwh or 0) > 0.05:
                act = "ECO (discharge-to-load)"
            g = groups[(act, prev_forced)]
            g["slots"] += 1
            g["plan_import"] += s.plan_import_kwh or 0
            g["plan_export"] += s.plan_export_kwh or 0
            g["act_import"] += s.act_import_kwh or 0
            g["act_export"] += s.act_export_kwh or 0
            g["sim_import"] += f.import_kwh
            g["sim_export"] += f.export_kwh
            g["act_discharge"] += s.bat_discharge_kwh or 0
            g["act_charge"] += s.bat_charge_kwh or 0
            g["net_load"] += max(0.0, s.net_load_kwh)
            g["plan_cost"] += s.buy * (s.plan_import_kwh or 0) - s.sell * (s.plan_export_kwh or 0)
            g["act_cost"] += s.act_cost_gbp or 0
            g["sim_cost"] += f.cost(s.buy, s.sell)
            if mode in ("CHARGE", "DISCHARGE_EXPORT"):
                prev_forced = mode
    rows = []
    for (act, prev), g in sorted(groups.items()):
        rows.append({"planned": act, "after": prev, **{k: round(v, 3) for k, v in g.items()}})
    return rows


# ── Forecast accuracy ─────────────────────────────────────────────────────────

def forecast_accuracy(slots: list[Slot]) -> dict:
    sol = [(s.solcast_p50_kwh, s.solar_kwh) for s in slots if s.solcast_p50_kwh is not None and s.solar_kwh is not None]
    ld = [(s.pred_load_kwh, s.load_kwh) for s in slots if s.pred_load_kwh is not None and s.load_kwh is not None]
    out: dict = {}
    if sol:
        fp = sum(p for p, _ in sol); fa = sum(a for _, a in sol)
        out["solar"] = {
            "slots": len(sol), "p50_kwh": round(fp, 1), "actual_kwh": round(fa, 1),
            "scale_actual_over_p50": round(fa / fp, 3) if fp else None,
            "mae_kwh_slot": round(sum(abs(p - a) for p, a in sol) / len(sol), 4),
            "bias_kwh_slot": round(sum(p - a for p, a in sol) / len(sol), 4),
        }
    if ld:
        out["load"] = {
            "slots": len(ld), "pred_kwh": round(sum(p for p, _ in ld), 1), "actual_kwh": round(sum(a for _, a in ld), 1),
            "mae_kwh_slot": round(sum(abs(p - a) for p, a in ld) / len(ld), 4),
            "bias_kwh_slot": round(sum(p - a for p, a in ld) / len(ld), 4),
            "mean_actual_kwh_slot": round(sum(a for _, a in ld) / len(ld), 4),
        }
        byh: dict[int, list] = defaultdict(list)
        for s in slots:
            if s.pred_load_kwh is not None and s.load_kwh is not None:
                byh[s.slot_start.astimezone(timezone.utc).hour].append((s.pred_load_kwh, s.load_kwh))
        out["load_by_hour_utc"] = {h: round(sum(abs(p - a) for p, a in v) / len(v), 4) for h, v in sorted(byh.items())}
        byl: dict[int, list] = defaultdict(list)
        for s in slots:
            if s.pred_load_kwh is not None and s.load_kwh is not None:
                byl[s.slot_start.astimezone(LONDON).hour].append((s.pred_load_kwh, s.load_kwh))
        out["load_by_hour_local"] = {
            h: {"mae": round(sum(abs(p - a) for p, a in v) / len(v), 4),
                "bias": round(sum(p - a for p, a in v) / len(v), 4), "n": len(v)}
            for h, v in sorted(byl.items())
        }
    return out


# ── Rendering ─────────────────────────────────────────────────────────────────

def _f(v: float | None) -> str:
    return "   —  " if v is None else f"{v:7.2f}"


def render_markdown(results: list[TariffResult], diag: list[dict], acc: dict, meta: dict) -> str:
    L: list[str] = []
    L.append(f"# octoopt2 backtest {meta['from']} → {meta['to']}")
    L.append("")
    L.append(f"Battery {meta['battery']['capacity_kwh']} kWh, floor {meta['battery']['min_soc_pct']}%, "
             f"charge/discharge {meta['battery']['max_charge_rate_kw']}/{meta['battery']['max_discharge_rate_kw']} kW, "
             f"export cap {meta['max_export_kw']} kW, hindsight cycle cost {meta['cycle_cost']} £/kWh, "
             f"terminal '{meta['terminal']}', lookahead {meta['lookahead_days']} d.")
    if meta.get("excluded"):
        L.append("")
        L.append("Excluded days: " + ", ".join(f"{d} ({r})" for d, r in meta["excluded"]))
    for tr in results:
        L.append("")
        L.append(f"## Tariff: {tr.name}")
        for n in tr.notes:
            L.append(f"_{n}_")
        L.append("")
        L.append("| month | days | actual | planned | plan-exec | hindsight | ECO-only | no-batt | replay | standing | load kWh | solar kWh | imp act/hind | exp act/hind |")
        L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        rows = list(tr.months.items()) + ([("TOTAL", tr.totals)] if tr.totals else [])
        for m, o in rows:
            L.append(f"| {m} | {o['days']} | {_f(o.get('actual'))} | {_f(o.get('planned'))} | {_f(o.get('planned_exec'))} | "
                     f"{_f(o.get('hindsight'))}{'*' if o.get('non_optimal') else ''} | {_f(o.get('eco'))} | {_f(o.get('no_battery'))} | "
                     f"{_f(o.get('replay'))} | {_f(o.get('standing'))} | {o.get('load', 0):.0f} | {o.get('solar', 0):.0f} | "
                     f"{o.get('act_import', 0):.0f}/{o.get('hind_import', 0):.0f} | {o.get('act_export', 0):.0f}/{o.get('hind_export', 0):.0f} |")
        if tr.totals:
            t = tr.totals
            L.append("")
            L.append(f"Gap actual − hindsight: £{t['actual'] - t['hindsight']:.2f} "
                     f"(execution {t['actual'] - t['planned_exec']:+.2f}, planning {t['planned_exec'] - t['hindsight']:+.2f}); "
                     f"battery value vs no-battery: hindsight £{t['no_battery'] - t['hindsight']:.2f}, actual £{t['no_battery'] - t['actual']:.2f}.")
    L.append("")
    L.append("Costs in £ excluding standing charge (shown separately); negative = net income. '*' = a day's solve hit the time limit.")
    if diag:
        L.append("")
        L.append("## Execution diagnostics (DB prices): planned action × last forced mode")
        L.append("")
        L.append("| planned | after | slots | plan imp | act imp | sim imp | plan exp | act exp | sim exp | batt dis | batt chg | net load | plan £ | act £ | sim £ |")
        L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        for r in diag:
            L.append(f"| {r['planned']} | {r['after']} | {int(r['slots'])} | {r['plan_import']:.1f} | {r['act_import']:.1f} | {r['sim_import']:.1f} | "
                     f"{r['plan_export']:.1f} | {r['act_export']:.1f} | {r['sim_export']:.1f} | {r['act_discharge']:.1f} | {r['act_charge']:.1f} | "
                     f"{r['net_load']:.1f} | {r['plan_cost']:.2f} | {r['act_cost']:.2f} | {r['sim_cost']:.2f} |")
    if acc:
        L.append("")
        L.append("## Forecast accuracy (near-slot forecasts stored in `schedule`/`solar_forecast`)")
        L.append("")
        L.append("```")
        L.append(json.dumps({k: v for k, v in acc.items() if k != "load_by_hour_utc"}, indent=2))
        L.append("```")
    return "\n".join(L)


def to_json(results: list[TariffResult], diag: list[dict], acc: dict, meta: dict) -> dict:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "meta": meta,
        "tariffs": {tr.name: {"notes": tr.notes, "days": [asdict(d) for d in tr.days],
                              "months": tr.months, "totals": tr.totals} for tr in results},
        "execution": diag,
        "forecast_accuracy": acc,
    }
