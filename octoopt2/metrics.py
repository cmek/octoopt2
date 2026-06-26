"""Prometheus metrics for the octoopt2 daemon.

A single custom collector exposes three groups of gauges on /metrics:

  Inverter  — live power flows and SoC, sampled by the daemon's inverter poller
              (DaemonState.latest_reading, refreshed every INVERTER_POLL_SECONDS).
  Ecodan    — DHW tank temperatures and operation mode, from the MELCloud poller
              (DaemonState.latest_dhw).
  Optimizer — the current slot's planned decision, last-optimization age, and
              per-feed data freshness, read straight from SQLite at scrape time.

The collector pulls live values from the in-memory DaemonState (no DB round-trip
for the fast-moving inverter/DHW numbers) and reads the slower optimizer/feed
state from the database when scraped. Nothing here is persisted — Prometheus is
the time-series store.
"""
import logging
from datetime import datetime, timezone

from prometheus_client.core import GaugeMetricFamily

from .db import get_conn
from .optimizer.schedule import get_current_decision

logger = logging.getLogger(__name__)

# DHW operation modes exposed as a labelled enum-style gauge (1 = active mode).
_DHW_MODES = ("force_hot_water", "auto", "unknown")


def _age_seconds(iso_ts: str | None, now: datetime) -> float | None:
    """Seconds between an ISO8601 timestamp and now. None if unparseable/missing."""
    if not iso_ts:
        return None
    try:
        ts = datetime.fromisoformat(iso_ts)
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (now - ts).total_seconds()


class OctooptCollector:
    """prometheus_client custom collector backed by the daemon's shared state."""

    def __init__(self, state, db_path: str) -> None:
        self._state = state
        self._db_path = db_path

    def collect(self):
        now = datetime.now(timezone.utc)
        yield from self._inverter_metrics(now)
        yield from self._ecodan_metrics(now)
        # Optimizer/feed metrics touch the DB; never let a DB hiccup break a scrape.
        try:
            yield from self._optimizer_metrics(now)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to collect optimizer metrics: %s", exc)

    # ── Inverter (live) ─────────────────────────────────────────────────────

    def _inverter_metrics(self, now: datetime):
        reading = self._state.latest_reading
        gauges = {
            "octoopt2_battery_soc_percent": ("Battery state of charge (%)", None),
            "octoopt2_solar_power_watts": ("Solar generation (W)", None),
            "octoopt2_grid_import_watts": ("Power imported from grid (W)", None),
            "octoopt2_grid_export_watts": ("Power exported to grid (W)", None),
            "octoopt2_battery_charge_watts": ("Power into battery (W)", None),
            "octoopt2_battery_discharge_watts": ("Power out of battery (W)", None),
            "octoopt2_load_watts": ("Home consumption (W)", None),
        }
        values = {}
        if reading is not None:
            values = {
                "octoopt2_battery_soc_percent": reading.soc_pct,
                "octoopt2_solar_power_watts": reading.solar_w,
                "octoopt2_grid_import_watts": reading.grid_import_w,
                "octoopt2_grid_export_watts": reading.grid_export_w,
                "octoopt2_battery_charge_watts": reading.battery_charge_w,
                "octoopt2_battery_discharge_watts": reading.battery_discharge_w,
                "octoopt2_load_watts": reading.load_w,
            }
        for name, (doc, _) in gauges.items():
            g = GaugeMetricFamily(name, doc)
            if name in values:
                g.add_metric([], values[name])
            yield g

        age = GaugeMetricFamily(
            "octoopt2_reading_age_seconds",
            "Seconds since the last successful inverter reading",
        )
        if reading is not None:
            age.add_metric([], (now - reading.recorded_at).total_seconds())
        yield age

    # ── Ecodan / DHW ────────────────────────────────────────────────────────

    def _ecodan_metrics(self, now: datetime):
        dhw = self._state.latest_dhw

        tank = GaugeMetricFamily(
            "octoopt2_dhw_tank_temp_celsius", "DHW tank temperature (°C)"
        )
        target = GaugeMetricFamily(
            "octoopt2_dhw_target_temp_celsius", "DHW target tank temperature (°C)"
        )
        if dhw is not None:
            if dhw.get("tank_temperature") is not None:
                tank.add_metric([], float(dhw["tank_temperature"]))
            if dhw.get("target_tank_temperature") is not None:
                target.add_metric([], float(dhw["target_tank_temperature"]))
        yield tank
        yield target

        mode = GaugeMetricFamily(
            "octoopt2_dhw_mode",
            "DHW operation mode (1 = active mode)",
            labels=["mode"],
        )
        if dhw is not None:
            current = dhw.get("operation_mode") or "unknown"
            for candidate in _DHW_MODES:
                mode.add_metric([candidate], 1.0 if candidate == current else 0.0)
        yield mode

        age = GaugeMetricFamily(
            "octoopt2_dhw_reading_age_seconds",
            "Seconds since the last successful DHW (MELCloud) reading",
        )
        if self._state.latest_dhw_at is not None:
            age.add_metric([], (now - self._state.latest_dhw_at).total_seconds())
        yield age

    # ── Optimizer / schedule / feed freshness (from DB) ─────────────────────

    def _optimizer_metrics(self, now: datetime):
        decision = get_current_decision(self._db_path, now=now)

        charge = GaugeMetricFamily(
            "octoopt2_planned_battery_charge_kwh",
            "Planned battery charge for the current slot (kWh)",
        )
        discharge = GaugeMetricFamily(
            "octoopt2_planned_battery_discharge_kwh",
            "Planned battery discharge for the current slot (kWh)",
        )
        grid_import = GaugeMetricFamily(
            "octoopt2_planned_grid_import_kwh",
            "Planned grid import for the current slot (kWh)",
        )
        grid_export = GaugeMetricFamily(
            "octoopt2_planned_grid_export_kwh",
            "Planned grid export for the current slot (kWh)",
        )
        dhw_on = GaugeMetricFamily(
            "octoopt2_planned_dhw_on",
            "Whether DHW heating is planned for the current slot (1 = on)",
        )
        slot_cost = GaugeMetricFamily(
            "octoopt2_planned_slot_cost_gbp",
            "Planned net cost for the current slot (GBP; negative = earning)",
        )
        if decision is not None:
            charge.add_metric([], decision.battery_charge_kwh)
            discharge.add_metric([], decision.battery_discharge_kwh)
            grid_import.add_metric([], decision.grid_import_kwh)
            grid_export.add_metric([], decision.grid_export_kwh)
            dhw_on.add_metric([], 1.0 if decision.dhw_on else 0.0)
            slot_cost.add_metric([], decision.slot_cost_gbp)
        yield from (charge, discharge, grid_import, grid_export, dhw_on, slot_cost)

        # Freshness of the last optimization and of each data feed.
        opt_age = GaugeMetricFamily(
            "octoopt2_last_optimization_age_seconds",
            "Seconds since the most recent saved schedule was optimized",
        )
        feed_age = GaugeMetricFamily(
            "octoopt2_feed_age_seconds",
            "Seconds since a data feed was last refreshed (lower = fresher)",
            labels=["feed"],
        )
        with get_conn(self._db_path) as conn:
            opt_row = conn.execute("SELECT MAX(optimized_at) AS ts FROM schedule").fetchone()
            opt_seconds = _age_seconds(opt_row["ts"] if opt_row else None, now)
            if opt_seconds is not None:
                opt_age.add_metric([], opt_seconds)

            # Feeds with an explicit fetched_at column: time since last fetch.
            for feed, query in (
                ("solar", "SELECT MAX(fetched_at) AS ts FROM solar_forecast"),
                ("weather", "SELECT MAX(fetched_at) AS ts FROM weather_forecast"),
                # Consumption/prices have no fetched_at; use the latest slot they cover.
                ("consumption", "SELECT MAX(slot_start) AS ts FROM consumption"),
                ("prices", "SELECT MAX(slot_start) AS ts FROM prices"),
            ):
                row = conn.execute(query).fetchone()
                secs = _age_seconds(row["ts"] if row else None, now)
                if secs is not None:
                    feed_age.add_metric([feed], secs)
        yield opt_age
        yield feed_age
