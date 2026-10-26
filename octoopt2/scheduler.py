"""Main optimization loop. Called every 5 minutes from cron.

Each tick runs two paths:

Fast path — runs every tick (every 5 min):
  1. Read inverter state (SoC, power flows) and store reading
  2. Record actuals for the slot that just completed
  3. Apply the current slot's saved inverter decision
     (apply_decision skips the hardware write when mode/power are unchanged,
      so this is a no-op on all but the first tick of a new slot)

Slow path — runs at slot boundaries only (:00 and :30 ticks):
  4. Refresh data feeds (TTL-aware — most calls are no-ops)
  5. Fit load model from consumption history
  6. Build optimizer inputs for remaining slots in the pricing window
  7. Run MILP optimizer → schedule
  8. Persist schedule to DB
  9. Apply fresh slot decision to inverter and Ecodan (inverter + DHW)

The slow path also runs on first startup if no schedule is saved, and whenever
--dry-run or --output is passed.
"""
import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .config import AppConfig
from .control.ecodan import apply_dhw, dhw_command, read_and_store_dhw_state, release_command, release_dhw
from .control.inverter import apply_decision
from .data.consumption import fetch_and_store_consumption
from .data.inverter import get_latest_reading, read_and_store
from .data.octopus import (
    fetch_and_store_prices,
    fetch_and_store_standing_charges,
    get_prices_from,
    missing_price_dates,
)
from .data.solcast import fetch_and_store_actuals, fetch_and_store_forecast, get_forecast
from .data.weather import backfill_weather_history, fetch_and_store_weather, get_temperature_forecast
from .db import get_conn, record_fetch
from .optimizer.forecast import fit_load_model, get_temperature_per_slot
from .optimizer.model import OptimizerInput, optimize
from .optimizer.schedule import get_current_decision, record_optimizer_run, save_schedule

logger = logging.getLogger(__name__)

LONDON = ZoneInfo("Europe/London")

# Fallback load per slot (kWh) used when the load model has insufficient data.
_FALLBACK_LOAD_KWH = 0.5

# How far back each consumption refresh reaches. Must exceed the worst-case
# Octopus publication lag — see _refresh_feeds.
_CONSUMPTION_WINDOW_DAYS = 4


def run(config: AppConfig, dry_run: bool = False, manage_dhw: bool = True, output: bool = False) -> None:
    """Execute one scheduler tick (see module docstring for fast/slow path details)."""
    now = datetime.now(timezone.utc)
    logger.info("── Scheduler tick %s ──", now.strftime("%Y-%m-%dT%H:%M:%SZ"))

    # ════════════════════════════════════════════════════════════════════════
    # FAST PATH — runs every tick
    # ════════════════════════════════════════════════════════════════════════

    # ── 1. Read inverter state ─────────────────────────────────────────────
    reading = _read_inverter(config, now)
    initial_soc_kwh = reading.soc_pct / 100 * config.battery.capacity_kwh if reading else None

    if initial_soc_kwh is None:
        logger.error("No inverter state available — cannot proceed")
        return

    logger.info(
        "Inverter: SoC=%.1f%% (%.2f kWh) solar=%.0fW load=%.0fW",
        reading.soc_pct, initial_soc_kwh, reading.solar_w, reading.load_w,
    )

    # ── 2. Record actuals for the completed slot ───────────────────────────
    _record_actuals(config, now)

    # ── 3. Apply current saved decision to inverter ────────────────────────
    # Skips the hardware write when mode and power are unchanged (see apply_decision).
    # On most ticks this is a no-op; it writes only when the slot first starts.
    saved_decision = None
    if not dry_run:
        saved_decision = get_current_decision(config.db_path, now=now)
        if saved_decision is not None:
            try:
                apply_decision(saved_decision, config.givenergy, config.battery, config.db_path,
                               trigger="fast", reading=reading)
            except Exception as exc:
                logger.error("Failed to apply inverter decision: %s", exc)

    # ════════════════════════════════════════════════════════════════════════
    # SLOW PATH — slot boundaries, first startup, or forced by flags
    # ════════════════════════════════════════════════════════════════════════

    at_slot_boundary = (now.minute % 30) < 5
    no_saved_schedule = not dry_run and saved_decision is None

    if not at_slot_boundary and not no_saved_schedule and not dry_run and not output:
        logger.debug("Mid-slot tick — skipping optimization")
        return

    if at_slot_boundary:
        logger.info("Slot boundary — running full optimization")
    elif no_saved_schedule:
        logger.info("No saved schedule — running initial optimization")

    # ── 4. Refresh data feeds ──────────────────────────────────────────────
    _refresh_feeds(config, now)

    # ── 4b. Log ground-truth DHW tank state (best-effort) ──────────────────
    # One MELCloud read per slot boundary; failures are logged, not fatal. The
    # tank temperature also feeds the comfort guard in step 9.
    dhw_state = None
    if not dry_run:
        dhw_state = read_and_store_dhw_state(config.melcloud, config.db_path)

    # ── 5. Fit load model ─────────────────────────────────────────────────
    load_model = fit_load_model(config.db_path)
    if load_model is None:
        logger.warning(
            "Load model unavailable — using fallback %.2f kWh/slot", _FALLBACK_LOAD_KWH
        )

    # Every outcome of this slow path (solved / error / fallback) lands in
    # optimizer_runs. Best-effort and skipped under --dry-run so a manual
    # inspection run does not pollute the history.
    model_fields = {
        "alpha": load_model.alpha if load_model else None,
        "beta": load_model.dhw_kwh_per_slot if load_model else None,
        "ref_temp": load_model.ref_temp if load_model else None,
        "n_train_slots": load_model.n_slots if load_model else None,
        "manage_dhw": manage_dhw,
        "soc_start_kwh": initial_soc_kwh,
    }

    def _record_run(**fields) -> None:
        if dry_run:
            return
        try:
            record_optimizer_run(config.db_path, **model_fields, **fields)
        except Exception as exc:
            logger.warning("Could not record optimizer run: %s", exc)

    # ── 6. Build optimizer inputs ──────────────────────────────────────────
    slot_starts = _remaining_slots(config.db_path, now)
    if not slot_starts:
        logger.warning("No price data for upcoming slots — skipping optimization")
        _record_run(status="fallback", error="no price data for upcoming slots")
        _apply_safe_fallback(config, manage_dhw=manage_dhw, dry_run=dry_run)
        return

    logger.info("Optimizing %d slots from %s", len(slot_starts), slot_starts[0].strftime("%H:%M"))

    prices = get_prices_from(config.db_path, slot_starts[0], slot_starts[-1] + timedelta(minutes=30))
    price_map = {p["slot_start"].astimezone(timezone.utc): p for p in prices}

    solar = get_forecast(config.db_path, slot_starts[0], slot_starts[-1] + timedelta(minutes=30))
    solar_map = {s["slot_start"].astimezone(timezone.utc): s["pv_estimate_kwh"] for s in solar}

    temperatures = get_temperature_per_slot(config.db_path, slot_starts)

    buy_prices, sell_prices, solar_forecast, load_forecast = [], [], [], []
    last_sell = _last_published_sell(config.db_path, slot_starts[0])
    for slot in slot_starts:
        slot_utc = slot.astimezone(timezone.utc)
        p = price_map.get(slot_utc, {})
        buy = p.get("buy_gbp_kwh", 0.30)
        # Outgoing rate unknown (slot absent, or sell is NULL = not yet
        # published)? Never 0: a free export sink makes the optimizer churn the
        # battery during negative-import slots. Mirroring the import price (the
        # old default) is only pessimistic when prices are negative — in normal
        # positive slots it over-values export and schedules battery exports
        # that the real rate never pays for. Use the last published outgoing
        # rate, capped at the import price, and fall back to the import price
        # only when no outgoing rate has ever been seen.
        sell = p.get("sell_gbp_kwh")
        if sell is None:
            sell = min(buy, last_sell) if last_sell is not None else buy
        else:
            last_sell = sell
        buy_prices.append(buy)
        sell_prices.append(sell)
        solar_forecast.append(solar_map.get(slot_utc, 0.0))

        if load_model:
            predicted = load_model.predict([slot], temperatures)
            load_forecast.append(predicted[0])
        else:
            load_forecast.append(_FALLBACK_LOAD_KWH)

    inputs = OptimizerInput(
        slot_starts=slot_starts,
        initial_soc_kwh=initial_soc_kwh,
        buy_prices=buy_prices,
        sell_prices=sell_prices,
        solar_forecast=solar_forecast,
        load_forecast=load_forecast,
    )

    # ── 7. Optimize ────────────────────────────────────────────────────────
    # DHW slots already executed today count against today's min/max, or every
    # re-run would plan a fresh full quota into the remaining slots.
    dhw_done = _dhw_slots_done_today(config.db_path, now) if manage_dhw else None
    try:
        result = optimize(
            inputs, config.battery, config.dhw, config.givenergy,
            manage_dhw=manage_dhw,
            dhw_kwh_per_slot=load_model.dhw_kwh_per_slot if load_model else None,
            dhw_done_today=dhw_done,
        )
        logger.info(
            "Optimization complete: status=%s total_cost=£%.4f",
            result.solver_status,
            result.total_cost_gbp,
        )
    except Exception as exc:
        logger.error("Optimizer failed: %s — applying safe fallback", exc)
        _record_run(
            status="error",
            error=str(exc),
            horizon_slots=len(slot_starts),
            horizon_start=slot_starts[0].isoformat(),
        )
        _apply_safe_fallback(config, manage_dhw=manage_dhw, dry_run=dry_run)
        return

    _record_run(
        status=result.solver_status,
        objective_gbp=result.total_cost_gbp,
        grid_cost_gbp=getattr(result, "grid_cost_gbp", None),
        solve_seconds=getattr(result, "solve_seconds", None),
        horizon_slots=len(slot_starts),
        horizon_start=slot_starts[0].isoformat(),
    )

    # ── 8. Save schedule / write output ───────────────────────────────────
    if output:
        try:
            _write_web_output(result, inputs, now, config, manage_dhw)
        except Exception as exc:
            logger.warning("Failed to write web output: %s", exc)

    if dry_run:
        _print_dry_run(result, inputs, now, config, manage_dhw=manage_dhw)
        return

    try:
        save_schedule(result, inputs, config.db_path)
    except Exception as exc:
        logger.error("Failed to save schedule: %s", exc)

    # ── 9. Apply fresh decision (inverter + DHW) ───────────────────────────
    decision = get_current_decision(config.db_path, now=now)
    if decision is None:
        logger.warning("No decision found for current slot — applying safe fallback")
        _record_run(status="fallback", error="no decision saved for current slot")
        _apply_safe_fallback(config, manage_dhw=manage_dhw, dry_run=dry_run)
        return

    logger.info(
        "Applying: charge=%.2f kWh discharge=%.2f kWh export=%.2f kWh dhw=%s",
        decision.battery_charge_kwh,
        decision.battery_discharge_kwh,
        decision.grid_export_kwh,
        decision.dhw_on,
    )

    try:
        apply_decision(decision, config.givenergy, config.battery, config.db_path,
                       trigger="slow", reading=reading)
    except Exception as exc:
        logger.error("Failed to apply inverter decision: %s", exc)

    try:
        if manage_dhw:
            tank_c = dhw_state.get("tank_temperature") if dhw_state else None
            apply_dhw(config.melcloud, dhw_command(decision.dhw_on, tank_c, config.dhw))
        else:
            apply_dhw(config.melcloud, release_command(config.dhw))
    except Exception as exc:
        logger.error("Failed to apply DHW decision: %s", exc)


# ── Dry-run output ────────────────────────────────────────────────────────

def _print_dry_run(result, inputs, now: datetime, config: AppConfig, manage_dhw: bool = True) -> None:
    """Print the full planned schedule and current-slot command to stdout."""
    from .control.inverter import _kw_to_register

    # Determine current slot
    current_slot = now.replace(
        minute=(now.minute // 30) * 30, second=0, microsecond=0
    )

    print()
    print("=" * 100)
    print("  DRY RUN — optimizer result (no commands sent)")
    print(f"  Optimized at : {result.optimized_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Solver status: {result.solver_status}")
    print(f"  Total cost   : £{result.total_cost_gbp:.4f}")
    print(f"  Slots        : {len(result.decisions)}")
    print(f"  DHW control  : {_dhw_control_label(config, manage_dhw)}")
    print("=" * 100)

    # Column header
    hdr = (
        f"  {'Time (UK)':<10} {'Buy':>7} {'Sell':>7} {'Solar':>7} {'Load':>7}"
        f" {'BatC':>6} {'BatD':>6} {'Export':>7} {'Import':>7}"
        f" {'DHW':>4} {'SoC%':>6} {'SoC kWh':>8} {'SlotCost':>10}  {'Mode':<18}"
    )
    print(hdr)
    print("  " + "─" * 98)

    total_buy = total_sell = 0.0
    for t, d in enumerate(result.decisions):
        local_time = d.slot_start.astimezone(LONDON)
        is_current = d.slot_start.replace(tzinfo=timezone.utc) == current_slot.replace(tzinfo=timezone.utc)
        marker = "◄ NOW" if is_current else ""

        # Determine inverter mode label
        if d.battery_charge_kwh > 0.05:
            charge_kw = d.battery_charge_kwh / 0.5
            reg = _kw_to_register(charge_kw, config.battery.max_charge_rate_kw)
            mode = f"CHARGE r={reg}/50"
        elif d.battery_discharge_kwh > 0.05:
            discharge_kw = d.battery_discharge_kwh / 0.5
            reg = _kw_to_register(discharge_kw, config.battery.max_discharge_rate_kw)
            if d.grid_export_kwh > 0.05:
                mode = f"DISCHARGE/EXPORT r={reg}/50"
            else:
                mode = f"DISCHARGE/DEMAND r={reg}/50"
        else:
            mode = "ECO"

        soc_pct = d.soc_end_kwh / config.battery.capacity_kwh * 100
        slot_cost = d.slot_cost_gbp
        total_buy += inputs.buy_prices[t] * d.grid_import_kwh
        total_sell += inputs.sell_prices[t] * d.grid_export_kwh

        print(
            f"  {local_time.strftime('%a %H:%M'):<10}"
            f" {inputs.buy_prices[t]:>7.4f}"
            f" {inputs.sell_prices[t]:>7.4f}"
            f" {inputs.solar_forecast[t]:>7.3f}"
            f" {inputs.load_forecast[t]:>7.3f}"
            f" {d.battery_charge_kwh:>6.3f}"
            f" {d.battery_discharge_kwh:>6.3f}"
            f" {d.grid_export_kwh:>7.3f}"
            f" {d.grid_import_kwh:>7.3f}"
            f" {'ON' if d.dhw_on else 'off':>4}"
            f" {soc_pct:>6.1f}"
            f" {d.soc_end_kwh:>8.3f}"
            f" {slot_cost:>+10.4f}"
            f"  {mode:<18} {marker}"
        )

    print("  " + "─" * 98)
    print(f"  {'TOTAL':>10}  gross buy=£{total_buy:.4f}  gross sell=£{total_sell:.4f}  net=£{result.total_cost_gbp:.4f}")

    # Current slot command detail
    current_decisions = [d for d in result.decisions
                         if d.slot_start.replace(tzinfo=timezone.utc) == current_slot.replace(tzinfo=timezone.utc)]
    if current_decisions:
        d = current_decisions[0]
        print()
        print("  Current slot command (what WOULD be sent now):")
        if d.battery_charge_kwh > 0.05:
            charge_kw = d.battery_charge_kwh / 0.5
            reg = _kw_to_register(charge_kw, config.battery.max_charge_rate_kw)
            print(f"    Inverter : CHARGE at {charge_kw:.2f} kW")
            print(f"               set_charge_slot_1(00:00–23:59)")
            print(f"               set_battery_charge_limit({reg})  [{reg}/50 = {charge_kw:.2f} kW]")
            print(f"               enable_charge() + disable_discharge()")
        elif d.battery_discharge_kwh > 0.05:
            discharge_kw = d.battery_discharge_kwh / 0.5
            reg = _kw_to_register(discharge_kw, config.battery.max_discharge_rate_kw)
            if d.grid_export_kwh > 0.05:
                print(f"    Inverter : DISCHARGE/EXPORT at {discharge_kw:.2f} kW")
                print(f"               set_discharge_slot_1(00:00–23:59)")
                print(f"               set_battery_discharge_limit({reg})  [{reg}/50 = {discharge_kw:.2f} kW]")
                print(f"               set_discharge_mode_max_power()")
            else:
                print(f"    Inverter : DISCHARGE/DEMAND at {discharge_kw:.2f} kW")
                print(f"               set_discharge_slot_1(00:00–23:59)")
                print(f"               set_battery_discharge_limit({reg})  [{reg}/50 = {discharge_kw:.2f} kW]")
                print(f"               set_discharge_mode_to_match_demand()")
            print(f"               enable_discharge() + disable_charge()")
        else:
            print(f"    Inverter : ECO — set_mode_dynamic()")
        if manage_dhw:
            cmd = dhw_command(d.dhw_on, None, config.dhw)
            print(f"    Ecodan   : DHW {'ON' if d.dhw_on else 'off'} → {cmd.describe()}")
        else:
            print(f"    Ecodan   : DHW unmanaged → {release_command(config.dhw).describe()}")
    print()


def _dhw_control_label(config: AppConfig, manage_dhw: bool) -> str:
    if not manage_dhw:
        return "disabled (auto)"
    if config.dhw.off_mode == "auto":
        return "managed by optimizer (off slots: auto — Ecodan may still reheat)"
    return (
        f"managed by optimizer (off slots: target held at {config.dhw.off_target_c:g}°C, "
        f"ON: forced to {config.dhw.target_c:g}°C)"
    )


# ── Web output ───────────────────────────────────────────────────────────────

def _write_web_output(result, inputs, now: datetime, config: AppConfig, manage_dhw: bool) -> None:
    """Write optimizer result to web/schedule.json for the dashboard."""
    import json
    from pathlib import Path
    from .control.inverter import _kw_to_register

    current_slot = now.replace(minute=(now.minute // 30) * 30, second=0, microsecond=0)

    slots = []
    for t, d in enumerate(result.decisions):
        local_time = d.slot_start.astimezone(LONDON)
        is_current = (
            d.slot_start.replace(tzinfo=timezone.utc)
            == current_slot.replace(tzinfo=timezone.utc)
        )
        if d.battery_charge_kwh > 0.05:
            charge_kw = d.battery_charge_kwh / 0.5
            reg = _kw_to_register(charge_kw, config.battery.max_charge_rate_kw)
            mode = f"CHARGE r={reg}/50"
        elif d.battery_discharge_kwh > 0.05:
            discharge_kw = d.battery_discharge_kwh / 0.5
            reg = _kw_to_register(discharge_kw, config.battery.max_discharge_rate_kw)
            mode = (
                f"DISCHARGE/EXPORT r={reg}/50"
                if d.grid_export_kwh > 0.05
                else f"DISCHARGE/DEMAND r={reg}/50"
            )
        else:
            mode = "ECO"

        slots.append({
            "slot_start":        d.slot_start.astimezone(timezone.utc).isoformat(),
            "time_uk":           local_time.strftime("%a %H:%M"),
            "buy_p":             round(inputs.buy_prices[t] * 100, 3),
            "sell_p":            round(inputs.sell_prices[t] * 100, 3),
            "solar_kwh":         round(inputs.solar_forecast[t], 3),
            "load_kwh":          round(inputs.load_forecast[t], 3),
            "bat_charge_kwh":    round(d.battery_charge_kwh, 3),
            "bat_discharge_kwh": round(d.battery_discharge_kwh, 3),
            "grid_export_kwh":   round(d.grid_export_kwh, 3),
            "grid_import_kwh":   round(d.grid_import_kwh, 3),
            "dhw_on":            d.dhw_on,
            "soc_pct":           round(d.soc_end_kwh / config.battery.capacity_kwh * 100, 1),
            "soc_kwh":           round(d.soc_end_kwh, 3),
            "slot_cost_gbp":     round(d.slot_cost_gbp, 4),
            "mode":              mode,
            "is_current":        is_current,
        })

    # Yesterday's actuals summary
    yesterday_summary = None
    try:
        now_london = now.astimezone(LONDON)
        today_london = now_london.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_london = today_london - timedelta(days=1)
        today_utc = today_london.astimezone(timezone.utc)
        yesterday_utc = yesterday_london.astimezone(timezone.utc)

        with get_conn(config.db_path) as conn:
            row = conn.execute(
                """
                SELECT SUM(solar_kwh) AS solar, SUM(load_kwh) AS load,
                       SUM(grid_import_kwh) AS imp, SUM(grid_export_kwh) AS exp,
                       SUM(cost_gbp) AS cost, COUNT(*) AS slots
                FROM actuals WHERE slot_start >= ? AND slot_start < ?
                """,
                (yesterday_utc.isoformat(), today_utc.isoformat()),
            ).fetchone()
        if row["slots"] > 0:
            yesterday_summary = {
                "date":         yesterday_london.strftime("%Y-%m-%d"),
                "label":        yesterday_london.strftime("%-d %B, %A"),
                "solar_kwh":    round(row["solar"] or 0, 2),
                "load_kwh":     round(row["load"]  or 0, 2),
                "import_kwh":   round(row["imp"]   or 0, 2),
                "export_kwh":   round(row["exp"]   or 0, 2),
                "net_cost_gbp": round(row["cost"]  or 0, 2),
                "slots":        row["slots"],
            }
    except Exception as exc:
        logger.warning("Could not fetch yesterday's actuals for web output: %s", exc)

    payload = {
        "generated_at":   now.isoformat(),
        "optimized_at":   result.optimized_at.isoformat(),
        "solver_status":  result.solver_status,
        "total_cost_gbp": round(result.total_cost_gbp, 4),
        "manage_dhw":     manage_dhw,
        "dhw_off_mode":   config.dhw.off_mode,
        "dhw_target_c":   config.dhw.target_c,
        "dhw_off_target_c": config.dhw.off_target_c,
        "battery_capacity_kwh": config.battery.capacity_kwh,
        "schedule":       slots,
        "yesterday":      yesterday_summary,
    }

    out_path = Path("web") / "schedule.json"
    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(json.dumps(payload))
    logger.info("Web output written to %s", out_path)


# ── Helpers ────────────────────────────────────────────────────────────────

def _read_inverter(config: AppConfig, now: datetime):
    """Read inverter state, falling back to last stored reading on failure."""
    try:
        return read_and_store(config.givenergy, config.db_path)
    except Exception as exc:
        logger.warning("Live inverter read failed (%s) — using last stored reading", exc)
        reading = get_latest_reading(config.db_path)
        if reading is None:
            return None
        age = (now - reading.recorded_at).total_seconds() / 60
        logger.warning("Using reading from %.1f minutes ago", age)
        return reading


def _refresh_feeds(config: AppConfig, now: datetime) -> None:
    """Refresh all data feeds. Failures are logged but do not abort the tick."""
    # Octopus prices — fetch for any date missing from DB
    try:
        for d in missing_price_dates(config.db_path, look_ahead_days=2):
            n = fetch_and_store_prices(config.octopus, config.db_path, for_date=d)
            if n == 0 and d > now.date():
                logger.info("Tomorrow's prices not published yet")
        # Stamped for the whole cycle rather than inside fetch_and_store_prices:
        # prices are fetched only for dates missing from the DB, so on most ticks
        # there is no call at all. "Refresh completed, nothing outstanding" is the
        # health signal we want; a raised exception skips the stamp.
        record_fetch(config.db_path, "prices")
    except Exception as exc:
        logger.warning("Octopus price fetch failed: %s", exc)

    # Octopus standing charge for the import tariff (24h TTL — skips if fresh)
    try:
        fetch_and_store_standing_charges(config.octopus, config.db_path)
    except Exception as exc:
        logger.warning("Octopus standing charge fetch failed: %s", exc)

    # Octopus consumption — refetch a rolling window to catch the API's data lag
    # (typically 1-2 days). The window has to outrun the worst lag, not the
    # typical one: a slot that falls out of it before Octopus publishes it is
    # never requested again, leaving a permanent hole. 4 days leaves headroom
    # for a couple of days of meter comms trouble; upserts make the overlap free.
    try:
        window_start = (now - timedelta(days=_CONSUMPTION_WINDOW_DAYS)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        fetch_and_store_consumption(config.octopus, config.db_path, window_start, now)
    except Exception as exc:
        logger.warning("Octopus consumption fetch failed: %s", exc)

    # Solcast forecast (2h TTL — skips if fresh)
    try:
        fetch_and_store_forecast(config.solcast, config.db_path)
    except Exception as exc:
        logger.warning("Solcast forecast fetch failed: %s", exc)

    # Solcast actuals (24h TTL — skips if fresh)
    try:
        fetch_and_store_actuals(config.solcast, config.db_path)
    except Exception as exc:
        logger.warning("Solcast actuals fetch failed: %s", exc)

    # Open-Meteo weather (1h TTL — skips if fresh)
    try:
        fetch_and_store_weather(config.location, config.db_path)
    except Exception as exc:
        logger.warning("Weather fetch failed: %s", exc)

    # Open-Meteo archive — backfill historical weather for load model training
    # (no-op once sufficient history is present)
    try:
        backfill_weather_history(config.location, config.db_path)
    except Exception as exc:
        logger.warning("Weather history backfill failed: %s", exc)


def _last_published_sell(db_path: str, before: datetime) -> float | None:
    """Most recent non-NULL outgoing rate strictly before `before`, if any."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            """
            SELECT sell_gbp_kwh FROM prices
            WHERE slot_start < ? AND sell_gbp_kwh IS NOT NULL
            ORDER BY slot_start DESC LIMIT 1
            """,
            (before.astimezone(timezone.utc).isoformat(),),
        ).fetchone()
    return float(row["sell_gbp_kwh"]) if row else None


def _remaining_slots(db_path: str, now: datetime) -> list[datetime]:
    """Return upcoming 30-min slots that have price data.

    Starts from the slot containing now, ends at the last slot with buy price
    data in the DB, capped at 48 h: typically 23:00 UK time tomorrow on Agile,
    the full 48 h on a banded tariff such as Cosy (rates are published months
    ahead). Slots whose Outgoing rate is not yet published get the pessimistic
    ``min(buy, last published sell)`` fallback in ``run``.
    """
    # Round down to current slot boundary
    current_slot = now.replace(
        minute=(now.minute // 30) * 30,
        second=0,
        microsecond=0,
    )
    # Look up to 48 hours ahead
    horizon = current_slot + timedelta(hours=48)

    prices = get_prices_from(db_path, current_slot, horizon)
    return [p["slot_start"].astimezone(timezone.utc) for p in prices]


def _dhw_slots_done_today(db_path: str, now: datetime) -> dict[date, int]:
    """Planned-ON DHW slots already executed today, keyed by UK date.

    `schedule` keeps the last plan for each slot, and the last plan covering a
    past slot is the one solved at that slot's boundary — i.e. the decision that
    was actually sent to the Ecodan. Counting those from UK midnight up to (but
    excluding) the current slot gives the quota already used, which optimize()
    subtracts from today's min/max.
    """
    current_slot = now.replace(minute=(now.minute // 30) * 30, second=0, microsecond=0)
    local_midnight = current_slot.astimezone(LONDON).replace(hour=0, minute=0, second=0, microsecond=0)
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM schedule WHERE dhw_on = 1 AND slot_start >= ? AND slot_start < ?",
            (local_midnight.astimezone(timezone.utc).isoformat(),
             current_slot.astimezone(timezone.utc).isoformat()),
        ).fetchone()
    done = int(row["n"]) if row else 0
    return {local_midnight.date(): done} if done else {}


def _apply_safe_fallback(config: AppConfig, *, manage_dhw: bool = True, dry_run: bool = False) -> None:
    """Apply a safe state when the optimizer cannot run: eco mode, DHW released.

    "Released" = auto mode at the normal tank target, so a target lowered for an
    off slot never outlives the controller. Best-effort — never raises. Also
    used as the daemon's shutdown fallback.
    """
    if dry_run:
        logger.info("Dry-run — skipping safe fallback hardware writes")
        return
    logger.info("Applying safe fallback: eco mode, DHW %s", release_command(config.dhw).describe())
    try:
        from .control.inverter import set_eco_mode
        set_eco_mode(config.givenergy, config.db_path, skip_if_current=True)
    except Exception as exc:
        logger.error("Safe fallback inverter command failed: %s", exc)

    if manage_dhw:
        try:
            release_dhw(config.melcloud, config.dhw)
        except Exception as exc:
            logger.error("Safe fallback DHW command failed: %s", exc)


def _record_actuals(config: AppConfig, now: datetime) -> None:
    """Aggregate inverter readings into per-slot actuals.

    Covers every 30-min slot in the last 24 h that has at least one reading and
    either no actuals row yet or one written before the battery/SoC columns
    existed (n_samples IS NULL) — so a missed tick or an upgrade is healed on
    the next run. Only the just-completed slot (the one before the current
    slot) is logged, to keep the log at one line per tick.
    """
    slot_end = now.replace(
        minute=(now.minute // 30) * 30,
        second=0,
        microsecond=0,
    )
    just_finished = slot_end - timedelta(minutes=30)
    window_start = slot_end - timedelta(hours=24)

    with get_conn(config.db_path) as conn:
        # One query for the candidate slots. recorded_at is always UTC ISO
        # "YYYY-MM-DDTHH:MM:SS.ffffff+00:00", so the first 14 chars are the
        # hour prefix and chars 15-16 the minute — bucket in SQL rather than
        # pulling every reading into Python.
        candidates = [
            r["slot_start"]
            for r in conn.execute(
                """
                WITH buckets AS (
                    SELECT DISTINCT
                        substr(recorded_at, 1, 14)
                        || CASE WHEN CAST(substr(recorded_at, 15, 2) AS INTEGER) < 30
                                THEN '00' ELSE '30' END
                        || ':00+00:00' AS slot_start
                    FROM inverter_readings
                    WHERE recorded_at >= ? AND recorded_at < ?
                )
                SELECT b.slot_start
                FROM buckets b
                LEFT JOIN actuals a ON a.slot_start = b.slot_start
                WHERE a.slot_start IS NULL OR a.n_samples IS NULL
                ORDER BY b.slot_start
                """,
                (window_start.isoformat(), slot_end.isoformat()),
            )
        ]

        for slot_iso in candidates:
            slot_start = datetime.fromisoformat(slot_iso)
            summary = _aggregate_slot(conn, slot_start)
            if summary is None:
                continue
            if slot_start == just_finished:
                logger.info(
                    "Actuals for %s: import=%.3f kWh export=%.3f kWh solar=%.3f kWh "
                    "load=%.3f kWh batt +%.3f/-%.3f kWh soc %s→%s%% (n=%d) cost=%s",
                    slot_start.strftime("%H:%M"),
                    summary["grid_import_kwh"], summary["grid_export_kwh"],
                    summary["solar_kwh"], summary["load_kwh"],
                    summary["battery_charge_kwh"], summary["battery_discharge_kwh"],
                    summary["soc_start_pct"], summary["soc_end_pct"], summary["n_samples"],
                    f"£{summary['cost_gbp']:.4f}" if summary["cost_gbp"] is not None else "n/a",
                )


def _aggregate_slot(conn, slot_start: datetime) -> dict | None:
    """Aggregate inverter_readings for one 30-min slot and upsert into actuals.

    Returns the stored values, or None when the slot has no readings.
    Energy = mean power (W) × 0.5 h / 1000; SoC endpoints are the first and
    last samples in the slot. curtailed_kwh is left NULL: clipped solar cannot
    be told from the inverter's own readings (they only see what was produced).
    """
    slot_end = slot_start + timedelta(minutes=30)
    window = (slot_start.isoformat(), slot_end.isoformat())

    rows = conn.execute(
        """
        SELECT AVG(solar_w)             AS solar_w,
               AVG(grid_import_w)       AS grid_import_w,
               AVG(grid_export_w)       AS grid_export_w,
               AVG(load_w)              AS load_w,
               AVG(battery_charge_w)    AS battery_charge_w,
               AVG(battery_discharge_w) AS battery_discharge_w,
               COUNT(*)                 AS n
        FROM inverter_readings
        WHERE recorded_at >= ? AND recorded_at < ?
        """,
        window,
    ).fetchone()
    if rows["solar_w"] is None:
        return None  # no readings in that window

    first = conn.execute(
        "SELECT soc_pct FROM inverter_readings WHERE recorded_at >= ? AND recorded_at < ? "
        "ORDER BY recorded_at ASC LIMIT 1",
        window,
    ).fetchone()
    last = conn.execute(
        "SELECT soc_pct FROM inverter_readings WHERE recorded_at >= ? AND recorded_at < ? "
        "ORDER BY recorded_at DESC LIMIT 1",
        window,
    ).fetchone()

    # Convert average W over 30 min → kWh
    factor = 0.5 / 1000
    out = {
        "solar_kwh": rows["solar_w"] * factor,
        "grid_import_kwh": rows["grid_import_w"] * factor,
        "grid_export_kwh": rows["grid_export_w"] * factor,
        "load_kwh": rows["load_w"] * factor,
        "battery_charge_kwh": rows["battery_charge_w"] * factor,
        "battery_discharge_kwh": rows["battery_discharge_w"] * factor,
        "soc_start_pct": first["soc_pct"] if first else None,
        "soc_end_pct": last["soc_pct"] if last else None,
        "n_samples": rows["n"],
        "cost_gbp": None,
    }

    # Fetch applicable prices
    price_row = conn.execute(
        "SELECT buy_gbp_kwh, sell_gbp_kwh FROM prices WHERE slot_start = ?",
        (slot_start.isoformat(),),
    ).fetchone()
    if price_row:
        # sell may be NULL if the outgoing rate was never published for this
        # slot; mirror the import price (the same pessimistic default the
        # optimizer used) so the recorded cost stays consistent with the plan.
        sell = price_row["sell_gbp_kwh"]
        if sell is None:
            sell = price_row["buy_gbp_kwh"]
        out["cost_gbp"] = (
            price_row["buy_gbp_kwh"] * out["grid_import_kwh"] - sell * out["grid_export_kwh"]
        )

    conn.execute(
        """
        INSERT INTO actuals (slot_start, grid_import_kwh, grid_export_kwh,
                             solar_kwh, load_kwh, cost_gbp,
                             battery_charge_kwh, battery_discharge_kwh,
                             soc_start_pct, soc_end_pct, n_samples)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(slot_start) DO UPDATE SET
            grid_import_kwh       = excluded.grid_import_kwh,
            grid_export_kwh       = excluded.grid_export_kwh,
            solar_kwh             = excluded.solar_kwh,
            load_kwh              = excluded.load_kwh,
            cost_gbp              = excluded.cost_gbp,
            battery_charge_kwh    = excluded.battery_charge_kwh,
            battery_discharge_kwh = excluded.battery_discharge_kwh,
            soc_start_pct         = excluded.soc_start_pct,
            soc_end_pct           = excluded.soc_end_pct,
            n_samples             = excluded.n_samples
        """,
        (
            slot_start.isoformat(),
            out["grid_import_kwh"], out["grid_export_kwh"],
            out["solar_kwh"], out["load_kwh"], out["cost_gbp"],
            out["battery_charge_kwh"], out["battery_discharge_kwh"],
            out["soc_start_pct"], out["soc_end_pct"], out["n_samples"],
        ),
    )
    return out
