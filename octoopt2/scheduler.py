"""Main optimization loop. Called every 5 minutes from cron.

Run order per tick:
  1. Read inverter state (SoC, power flows)
  2. Refresh data feeds (TTL-aware — most calls are no-ops)
  3. Fit load model from consumption history
  4. Build optimizer inputs for remaining slots in the pricing window
  5. Run MILP optimizer → schedule
  6. Persist schedule to DB
  7. Apply current slot's decision to inverter and Ecodan
  8. Record actuals for the slot that just completed
"""
import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .config import AppConfig
from .control.ecodan import set_dhw
from .control.inverter import apply_decision
from .data.consumption import fetch_and_store_consumption
from .data.inverter import get_latest_reading, read_and_store
from .data.octopus import fetch_and_store_prices, get_prices_from, missing_price_dates
from .data.solcast import fetch_and_store_actuals, fetch_and_store_forecast, get_forecast
from .data.weather import fetch_and_store_weather, get_temperature_forecast
from .db import get_conn
from .optimizer.forecast import fit_load_model, get_temperature_per_slot
from .optimizer.model import OptimizerInput, optimize
from .optimizer.schedule import get_current_decision, save_schedule

logger = logging.getLogger(__name__)

LONDON = ZoneInfo("Europe/London")

# Fallback load per slot (kWh) used when the load model has insufficient data.
_FALLBACK_LOAD_KWH = 0.5


def run(config: AppConfig, dry_run: bool = False, manage_dhw: bool = True) -> None:
    """Execute one optimization tick.

    dry_run=True:    runs every step except writing to hardware. Prints the
                     full planned schedule and the command that would be sent
                     for the current slot, then exits without touching hardware.
    manage_dhw=False: skip all DHW control; Ecodan stays in its own auto mode
                     and DHW is excluded from the optimization.
    """
    now = datetime.now(timezone.utc)
    logger.info("── Scheduler tick %s ──", now.strftime("%Y-%m-%dT%H:%M:%SZ"))

    # ── 1. Read inverter state ─────────────────────────────────────────────
    reading = _read_inverter(config, now)
    initial_soc_kwh = reading.soc_pct / 100 * config.battery.capacity_kwh if reading else None

    if initial_soc_kwh is None:
        logger.error("No inverter state available — cannot optimize")
        return

    logger.info(
        "Inverter: SoC=%.1f%% (%.2f kWh) solar=%.0fW load=%.0fW",
        reading.soc_pct, initial_soc_kwh, reading.solar_w, reading.load_w,
    )

    # ── 2. Refresh data feeds ──────────────────────────────────────────────
    _refresh_feeds(config, now)

    # ── 3. Fit load model ─────────────────────────────────────────────────
    load_model = fit_load_model(config.db_path)
    if load_model is None:
        logger.warning(
            "Load model unavailable — using fallback %.2f kWh/slot", _FALLBACK_LOAD_KWH
        )

    # ── 4. Build optimizer inputs ──────────────────────────────────────────
    slot_starts = _remaining_slots(config.db_path, now)
    if not slot_starts:
        logger.warning("No price data for upcoming slots — skipping optimization")
        _apply_safe_fallback(config)
        return

    logger.info("Optimizing %d slots from %s", len(slot_starts), slot_starts[0].strftime("%H:%M"))

    prices = get_prices_from(config.db_path, slot_starts[0], slot_starts[-1] + timedelta(minutes=30))
    price_map = {p["slot_start"].astimezone(timezone.utc): p for p in prices}

    solar = get_forecast(config.db_path, slot_starts[0], slot_starts[-1] + timedelta(minutes=30))
    solar_map = {s["slot_start"].astimezone(timezone.utc): s["pv_estimate_kwh"] for s in solar}

    temperatures = get_temperature_per_slot(config.db_path, slot_starts)

    buy_prices, sell_prices, solar_forecast, load_forecast = [], [], [], []
    for slot in slot_starts:
        slot_utc = slot.astimezone(timezone.utc)
        p = price_map.get(slot_utc, {})
        buy_prices.append(p.get("buy_gbp_kwh", 0.30))
        sell_prices.append(p.get("sell_gbp_kwh", 0.0))
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

    # ── 5. Optimize ────────────────────────────────────────────────────────
    try:
        result = optimize(inputs, config.battery, config.dhw, config.givenergy, manage_dhw=manage_dhw)
        logger.info(
            "Optimization complete: status=%s total_cost=£%.4f",
            result.solver_status,
            result.total_cost_gbp,
        )
    except Exception as exc:
        logger.error("Optimizer failed: %s — applying safe fallback", exc)
        _apply_safe_fallback(config)
        return

    # ── 6. Save schedule (skipped in dry-run) ────────────────────────────
    if dry_run:
        _print_dry_run(result, inputs, now, config, manage_dhw=manage_dhw)
        return

    try:
        save_schedule(result, inputs, config.db_path)
    except Exception as exc:
        logger.error("Failed to save schedule: %s", exc)

    # ── 7. Apply current slot decision ────────────────────────────────────
    decision = get_current_decision(config.db_path, now=now)
    if decision is None:
        logger.warning("No decision found for current slot — applying safe fallback")
        _apply_safe_fallback(config)
        return

    logger.info(
        "Applying: charge=%.2f kWh discharge=%.2f kWh export=%.2f kWh dhw=%s",
        decision.battery_charge_kwh,
        decision.battery_discharge_kwh,
        decision.grid_export_kwh,
        decision.dhw_on,
    )

    try:
        apply_decision(decision, config.givenergy, config.battery)
    except Exception as exc:
        logger.error("Failed to apply inverter decision: %s", exc)

    try:
        set_dhw(config.melcloud, decision.dhw_on if manage_dhw else False)
    except Exception as exc:
        logger.error("Failed to apply DHW decision: %s", exc)

    # ── 8. Record actuals for completed slot ───────────────────────────────
    _record_actuals(config, now)


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
    print(f"  DHW control  : {'managed by optimizer' if manage_dhw else 'disabled (auto)'}")
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
                print(f"               set_battery_discharge_limit({reg})  [{reg}/50 = {discharge_kw:.2f} kW]")
                print(f"               set_battery_discharge_mode_max_power()")
            else:
                print(f"    Inverter : DISCHARGE/DEMAND at {discharge_kw:.2f} kW")
                print(f"               set_battery_discharge_limit({reg})  [{reg}/50 = {discharge_kw:.2f} kW]")
                print(f"               set_battery_discharge_mode_demand()")
            print(f"               set_discharge_enable(True) + disable_charge()")
        else:
            print(f"    Inverter : ECO — set_mode_dynamic()")
        if manage_dhw:
            print(f"    Ecodan   : DHW {'force_hot_water' if d.dhw_on else 'auto (off)'}")
        else:
            print(f"    Ecodan   : DHW unmanaged (auto)")
    print()


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
    except Exception as exc:
        logger.warning("Octopus price fetch failed: %s", exc)

    # Octopus consumption — fetch last 2 days to catch API data lag (typically 1-2 days)
    try:
        two_days_ago = (now - timedelta(days=2)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        fetch_and_store_consumption(config.octopus, config.db_path, two_days_ago, now)
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


def _remaining_slots(db_path: str, now: datetime) -> list[datetime]:
    """Return upcoming 30-min slots that have price data.

    Starts from the slot containing now, ends at the last slot with
    price data in the DB (typically 23:00 UK time tomorrow).
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


def _apply_safe_fallback(config: AppConfig) -> None:
    """Apply a safe state when the optimizer cannot run: eco mode, DHW off."""
    logger.info("Applying safe fallback: eco mode, DHW off")
    try:
        from givenergy_modbus.client import GivEnergyClient
        client = GivEnergyClient(host=config.givenergy.host, port=config.givenergy.port)
        client.set_mode_dynamic()
    except Exception as exc:
        logger.error("Safe fallback inverter command failed: %s", exc)

    try:
        set_dhw(config.melcloud, enabled=False)
    except Exception as exc:
        logger.error("Safe fallback DHW command failed: %s", exc)


def _record_actuals(config: AppConfig, now: datetime) -> None:
    """Aggregate inverter readings for the slot that just completed and save to actuals.

    The just-completed slot is the one before the current slot.
    """
    slot_end = now.replace(
        minute=(now.minute // 30) * 30,
        second=0,
        microsecond=0,
    )
    slot_start = slot_end - timedelta(minutes=30)

    with get_conn(config.db_path) as conn:
        rows = conn.execute(
            """
            SELECT AVG(solar_w)       AS solar_w,
                   AVG(grid_import_w) AS grid_import_w,
                   AVG(grid_export_w) AS grid_export_w,
                   AVG(load_w)        AS load_w
            FROM inverter_readings
            WHERE recorded_at >= ? AND recorded_at < ?
            """,
            (slot_start.isoformat(), slot_end.isoformat()),
        ).fetchone()

        if rows["solar_w"] is None:
            return  # no readings in that window

        # Convert average W over 30 min → kWh
        factor = 0.5 / 1000
        solar_kwh = rows["solar_w"] * factor
        import_kwh = rows["grid_import_w"] * factor
        export_kwh = rows["grid_export_w"] * factor
        load_kwh = rows["load_w"] * factor

        # Fetch applicable prices
        price_row = conn.execute(
            "SELECT buy_gbp_kwh, sell_gbp_kwh FROM prices WHERE slot_start = ?",
            (slot_start.isoformat(),),
        ).fetchone()

        cost_gbp = None
        if price_row:
            cost_gbp = (
                price_row["buy_gbp_kwh"] * import_kwh
                - price_row["sell_gbp_kwh"] * export_kwh
            )

        conn.execute(
            """
            INSERT INTO actuals (slot_start, grid_import_kwh, grid_export_kwh,
                                 solar_kwh, load_kwh, cost_gbp)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(slot_start) DO UPDATE SET
                grid_import_kwh = excluded.grid_import_kwh,
                grid_export_kwh = excluded.grid_export_kwh,
                solar_kwh       = excluded.solar_kwh,
                load_kwh        = excluded.load_kwh,
                cost_gbp        = excluded.cost_gbp
            """,
            (slot_start.isoformat(), import_kwh, export_kwh, solar_kwh, load_kwh, cost_gbp),
        )

    logger.info(
        "Actuals for %s: import=%.3f kWh export=%.3f kWh solar=%.3f kWh load=%.3f kWh cost=%s",
        slot_start.strftime("%H:%M"),
        import_kwh, export_kwh, solar_kwh, load_kwh,
        f"£{cost_gbp:.4f}" if cost_gbp is not None else "n/a",
    )
