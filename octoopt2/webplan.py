"""Build the schedule page's JSON document (web/index.html ← /schedule.json).

The plan the page shows must be the plan the daemon is acting on, so the
canonical source is the database: the newest ``optimized_at`` in
``schedule_history`` (which keeps the SoC trajectory the live ``schedule``
table drops) plus the matching ``optimizer_runs`` row for solver status and
objective. The daemon serves this live at ``/schedule.json``; nothing on disk
can go stale.

The same renderer also takes a fresh ``OptimizerResult`` so the stateless
``octoopt2 --output`` CLI path (and ``--dry-run --output``, which saves
nothing) can still write ``web/schedule.json`` for a static web server.
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .config import AppConfig
from .control.inverter import _kw_to_register
from .db import get_conn
from .optimizer.model import OptimizerInput, OptimizerResult, apply_uplift
from .status import _slot_mode

logger = logging.getLogger(__name__)

LONDON = ZoneInfo("Europe/London")

# record_optimizer_run stamps its own run_at a moment after the solver's
# optimized_at (the solve itself is bounded by a 30 s time limit). A run row
# further away than this from the plan's optimized_at is some other tick's.
_RUN_MATCH_WINDOW = timedelta(minutes=5)


@dataclass(frozen=True)
class PlanSlot:
    """One planned half-hour, source-agnostic (DB row or solver decision)."""
    slot_start: datetime          # UTC
    buy_gbp_kwh: float            # tariff price (no Axle uplift)
    sell_gbp_kwh: float
    solar_kwh: float
    load_kwh: float
    battery_charge_kwh: float
    battery_discharge_kwh: float
    grid_import_kwh: float
    grid_export_kwh: float
    dhw_on: bool
    soc_end_kwh: float | None     # None for plans saved before SoC was persisted
    slot_cost_gbp: float          # at effective prices (uplift folded in), like the solver


@dataclass(frozen=True)
class Plan:
    optimized_at: datetime
    solver_status: str | None
    total_cost_gbp: float | None  # solver objective (incl. virtual terms); None if unknown
    slots: list[PlanSlot]


def slot_cost_gbp(
    buy: float, sell: float, grid_import_kwh: float, grid_export_kwh: float,
    uplift: float | None = None,
) -> float:
    """Money value of a slot at the prices the optimizer saw (Axle uplift folded in)."""
    (buy_eff,), (sell_eff,) = apply_uplift([buy], [sell], [uplift] if uplift else None)
    return buy_eff * grid_import_kwh - sell_eff * grid_export_kwh


# ── Sources ──────────────────────────────────────────────────────────────────

def plan_from_result(result: OptimizerResult, inputs: OptimizerInput) -> Plan:
    """Plan straight from a solver result (the ``--output`` CLI path)."""
    slots = [
        PlanSlot(
            slot_start=d.slot_start.astimezone(timezone.utc),
            buy_gbp_kwh=inputs.buy_prices[t],
            sell_gbp_kwh=inputs.sell_prices[t],
            solar_kwh=inputs.solar_forecast[t],
            load_kwh=inputs.load_forecast[t],
            battery_charge_kwh=d.battery_charge_kwh,
            battery_discharge_kwh=d.battery_discharge_kwh,
            grid_import_kwh=d.grid_import_kwh,
            grid_export_kwh=d.grid_export_kwh,
            dhw_on=bool(d.dhw_on),
            soc_end_kwh=d.soc_end_kwh,
            slot_cost_gbp=d.slot_cost_gbp,
        )
        for t, d in enumerate(result.decisions)
    ]
    return Plan(
        optimized_at=result.optimized_at.astimezone(timezone.utc),
        solver_status=result.solver_status,
        total_cost_gbp=result.total_cost_gbp,
        slots=slots,
    )


def plan_from_db(db_path: str) -> Plan | None:
    """The newest saved plan, or None when nothing has been saved yet.

    Reads ``schedule_history`` at its latest ``optimized_at`` because that
    table keeps ``soc_end_kwh``; falls back to the ``schedule`` table (no SoC)
    for databases whose history is empty. Solver status and objective come
    from the first non-failed ``optimizer_runs`` row recorded within
    _RUN_MATCH_WINDOW after ``optimized_at``: that is the run that produced the
    plan (a failed run never replaces the saved plan). If that row is missing
    the status is reported as unknown rather than borrowed from a later run.
    """
    with get_conn(db_path) as conn:
        table, soc_col = "schedule_history", "soc_end_kwh"
        ts_row = conn.execute(f"SELECT MAX(optimized_at) AS ts FROM {table}").fetchone()  # noqa: S608
        optimized_at = ts_row["ts"] if ts_row else None
        if not optimized_at:
            table, soc_col = "schedule", "NULL"
            ts_row = conn.execute(f"SELECT MAX(optimized_at) AS ts FROM {table}").fetchone()  # noqa: S608
            optimized_at = ts_row["ts"] if ts_row else None
            if not optimized_at:
                return None
        rows = conn.execute(
            f"""
            SELECT slot_start, buy_gbp_kwh, sell_gbp_kwh, axle_uplift_gbp_kwh,
                   predicted_solar_kwh, predicted_load_kwh,
                   battery_charge_kwh, battery_discharge_kwh,
                   grid_import_kwh, grid_export_kwh, dhw_on, {soc_col} AS soc_end_kwh
            FROM {table} WHERE optimized_at = ? ORDER BY slot_start
            """,  # noqa: S608 - table/column names are fixed literals above
            (optimized_at,),
        ).fetchall()

        opt_dt = datetime.fromisoformat(optimized_at).astimezone(timezone.utc)
        run = conn.execute(
            """
            SELECT status, objective_gbp FROM optimizer_runs
            WHERE run_at >= ? AND run_at < ? AND status NOT IN ('error', 'fallback')
            ORDER BY run_at LIMIT 1
            """,
            (opt_dt.isoformat(), (opt_dt + _RUN_MATCH_WINDOW).isoformat()),
        ).fetchone()

    slots = [
        PlanSlot(
            slot_start=datetime.fromisoformat(r["slot_start"]).astimezone(timezone.utc),
            buy_gbp_kwh=r["buy_gbp_kwh"],
            sell_gbp_kwh=r["sell_gbp_kwh"],
            solar_kwh=r["predicted_solar_kwh"],
            load_kwh=r["predicted_load_kwh"],
            battery_charge_kwh=r["battery_charge_kwh"],
            battery_discharge_kwh=r["battery_discharge_kwh"],
            grid_import_kwh=r["grid_import_kwh"],
            grid_export_kwh=r["grid_export_kwh"],
            dhw_on=bool(r["dhw_on"]),
            soc_end_kwh=r["soc_end_kwh"],
            slot_cost_gbp=slot_cost_gbp(
                r["buy_gbp_kwh"], r["sell_gbp_kwh"],
                r["grid_import_kwh"], r["grid_export_kwh"], r["axle_uplift_gbp_kwh"],
            ),
        )
        for r in rows
    ]
    return Plan(
        optimized_at=opt_dt,
        solver_status=run["status"] if run else None,
        total_cost_gbp=run["objective_gbp"] if run else None,
        slots=slots,
    )


# ── Rendering ────────────────────────────────────────────────────────────────

def _mode_label(s: PlanSlot, config: AppConfig) -> str:
    """status._slot_mode's label plus the inverter power register it maps to."""
    mode = _slot_mode(s.battery_charge_kwh, s.battery_discharge_kwh, s.grid_export_kwh)
    if mode == "ECO":
        return mode
    if mode == "CHARGE":
        reg = _kw_to_register(s.battery_charge_kwh / 0.5, config.battery.max_charge_rate_kw)
    else:
        reg = _kw_to_register(s.battery_discharge_kwh / 0.5, config.battery.max_discharge_rate_kw)
    return f"{mode} r={reg}/50"


def _yesterday_summary(db_path: str, now: datetime) -> dict | None:
    now_london = now.astimezone(LONDON)
    today_london = now_london.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_london = today_london - timedelta(days=1)
    today_utc = today_london.astimezone(timezone.utc)
    yesterday_utc = yesterday_london.astimezone(timezone.utc)

    with get_conn(db_path) as conn:
        row = conn.execute(
            """
            SELECT SUM(solar_kwh) AS solar, SUM(load_kwh) AS load,
                   SUM(grid_import_kwh) AS imp, SUM(grid_export_kwh) AS exp,
                   SUM(cost_gbp) AS cost, COUNT(*) AS slots
            FROM actuals WHERE slot_start >= ? AND slot_start < ?
            """,
            (yesterday_utc.isoformat(), today_utc.isoformat()),
        ).fetchone()
    if not row or row["slots"] == 0:
        return None
    return {
        "date":         yesterday_london.strftime("%Y-%m-%d"),
        "label":        yesterday_london.strftime("%-d %B, %A"),
        "solar_kwh":    round(row["solar"] or 0, 2),
        "load_kwh":     round(row["load"]  or 0, 2),
        "import_kwh":   round(row["imp"]   or 0, 2),
        "export_kwh":   round(row["exp"]   or 0, 2),
        "net_cost_gbp": round(row["cost"]  or 0, 2),
        "slots":        row["slots"],
    }


def build_schedule_payload(
    plan: Plan, now: datetime, config: AppConfig, manage_dhw: bool,
) -> dict:
    """The JSON document web/index.html renders.

    ``generated_at`` is when this document was built (now, for the live
    endpoint); ``optimized_at`` is when the plan was computed and
    ``plan_age_s`` its age by the server clock, which is what the page should
    judge freshness by (browser clocks drift).
    """
    now_utc = now.astimezone(timezone.utc)
    current_slot = now_utc.replace(minute=(now_utc.minute // 30) * 30, second=0, microsecond=0)
    cap = config.battery.capacity_kwh

    slots = []
    for s in plan.slots:
        slots.append({
            "slot_start":        s.slot_start.isoformat(),
            "time_uk":           s.slot_start.astimezone(LONDON).strftime("%a %H:%M"),
            "buy_p":             round(s.buy_gbp_kwh * 100, 3),
            "sell_p":            round(s.sell_gbp_kwh * 100, 3),
            "solar_kwh":         round(s.solar_kwh, 3),
            "load_kwh":          round(s.load_kwh, 3),
            "bat_charge_kwh":    round(s.battery_charge_kwh, 3),
            "bat_discharge_kwh": round(s.battery_discharge_kwh, 3),
            "grid_export_kwh":   round(s.grid_export_kwh, 3),
            "grid_import_kwh":   round(s.grid_import_kwh, 3),
            "dhw_on":            s.dhw_on,
            "soc_pct":           None if s.soc_end_kwh is None else round(s.soc_end_kwh / cap * 100, 1),
            "soc_kwh":           None if s.soc_end_kwh is None else round(s.soc_end_kwh, 3),
            "slot_cost_gbp":     round(s.slot_cost_gbp, 4),
            "mode":              _mode_label(s, config),
            "is_current":        s.slot_start == current_slot,
        })

    try:
        yesterday = _yesterday_summary(config.db_path, now_utc)
    except Exception as exc:
        logger.warning("Could not fetch yesterday's actuals for web output: %s", exc)
        yesterday = None

    total_cost = plan.total_cost_gbp
    if total_cost is None:
        total_cost = sum(s.slot_cost_gbp for s in plan.slots)

    return {
        "generated_at":   now_utc.isoformat(),
        "optimized_at":   plan.optimized_at.isoformat(),
        "plan_age_s":     round((now_utc - plan.optimized_at).total_seconds(), 1),
        "solver_status":  plan.solver_status or "Unknown",
        "total_cost_gbp": round(total_cost, 4),
        "manage_dhw":     manage_dhw,
        "dhw_off_mode":   config.dhw.off_mode,
        "dhw_target_c":   config.dhw.target_c,
        "dhw_off_target_c": config.dhw.off_target_c,
        "battery_capacity_kwh": cap,
        "schedule":       slots,
        "yesterday":      yesterday,
    }


def build_schedule_from_db(
    config: AppConfig, manage_dhw: bool, now: datetime | None = None,
) -> dict | None:
    """Live payload for the daemon's /schedule.json; None if no plan is saved."""
    now = now or datetime.now(timezone.utc)
    plan = plan_from_db(config.db_path)
    if plan is None:
        return None
    return build_schedule_payload(plan, now, config, manage_dhw)
