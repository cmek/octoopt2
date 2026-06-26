"""Build a live JSON status snapshot for the dashboard (web/status.html).

This is the same data the Prometheus collector exposes (live inverter flows,
DHW state, the current slot's planned decision, feed freshness), assembled into
a single JSON document the browser can render directly — plus a short rolling
history of inverter readings (held in memory by the daemon) for sparklines.

Served same-origin from the daemon's HTTP server at /status.json, so the
dashboard needs no Prometheus query API and no CORS gymnastics.
"""
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .db import get_conn

logger = logging.getLogger(__name__)

LONDON = ZoneInfo("Europe/London")

# A live inverter reading older than this is treated as "stale" / offline.
_ONLINE_MAX_AGE_S = 150.0


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


def _slot_mode(charge_kwh: float, discharge_kwh: float, export_kwh: float) -> str:
    """Human label for the planned battery action in a slot."""
    if charge_kwh > 0.05:
        return "CHARGE"
    if discharge_kwh > 0.05:
        return "DISCHARGE/EXPORT" if export_kwh > 0.05 else "DISCHARGE/DEMAND"
    return "ECO"


def _live(state, now: datetime) -> dict:
    """Live inverter power flows from the daemon's latest poll."""
    r = state.latest_reading
    if r is None:
        return {"online": False, "reading_age_s": None}
    age = (now - r.recorded_at).total_seconds()
    return {
        "online": age <= _ONLINE_MAX_AGE_S,
        "reading_age_s": round(age, 1),
        "soc_pct": round(r.soc_pct, 1),
        "solar_w": round(r.solar_w),
        "load_w": round(r.load_w),
        "grid_import_w": round(r.grid_import_w),
        "grid_export_w": round(r.grid_export_w),
        "battery_charge_w": round(r.battery_charge_w),
        "battery_discharge_w": round(r.battery_discharge_w),
        # Signed conveniences: + = importing / discharging.
        "grid_net_w": round(r.grid_import_w - r.grid_export_w),
        "battery_net_w": round(r.battery_discharge_w - r.battery_charge_w),
    }


def _dhw(state, now: datetime) -> dict:
    """DHW tank state from the latest MELCloud poll."""
    dhw = state.latest_dhw
    age = None
    if state.latest_dhw_at is not None:
        age = round((now - state.latest_dhw_at).total_seconds(), 1)
    if dhw is None:
        return {"available": False, "age_s": age}
    mode = dhw.get("operation_mode") or "unknown"
    return {
        "available": True,
        "tank_temp_c": dhw.get("tank_temperature"),
        "target_temp_c": dhw.get("target_tank_temperature"),
        "mode": mode,
        "heating": mode == "force_hot_water",
        "age_s": age,
    }


def _planner_and_cost(db_path: str, now: datetime) -> tuple[dict, dict, list]:
    """Current slot decision, cost summary, and an upcoming price/plan strip."""
    slot_start = now.replace(minute=(now.minute // 30) * 30, second=0, microsecond=0)

    planner: dict = {"has_decision": False}
    upcoming: list = []
    cost: dict = {}

    with get_conn(db_path) as conn:
        row = conn.execute(
            """
            SELECT battery_charge_kwh, battery_discharge_kwh,
                   grid_import_kwh, grid_export_kwh, dhw_on,
                   predicted_load_kwh, predicted_solar_kwh,
                   buy_gbp_kwh, sell_gbp_kwh
            FROM schedule WHERE slot_start = ?
            """,
            (slot_start.isoformat(),),
        ).fetchone()

        if row is not None:
            slot_cost = (
                row["buy_gbp_kwh"] * row["grid_import_kwh"]
                - row["sell_gbp_kwh"] * row["grid_export_kwh"]
            )
            planner = {
                "has_decision": True,
                "mode": _slot_mode(
                    row["battery_charge_kwh"],
                    row["battery_discharge_kwh"],
                    row["grid_export_kwh"],
                ),
                "dhw_on": bool(row["dhw_on"]),
                "planned": {
                    "charge_kwh": round(row["battery_charge_kwh"], 3),
                    "discharge_kwh": round(row["battery_discharge_kwh"], 3),
                    "import_kwh": round(row["grid_import_kwh"], 3),
                    "export_kwh": round(row["grid_export_kwh"], 3),
                },
                "predicted_load_kwh": round(row["predicted_load_kwh"], 3),
                "predicted_solar_kwh": round(row["predicted_solar_kwh"], 3),
                "buy_p": round(row["buy_gbp_kwh"] * 100, 2),
                "sell_p": round(row["sell_gbp_kwh"] * 100, 2),
                "slot_cost_gbp": round(slot_cost, 4),
                "slot_start": slot_start.isoformat(),
                "slot_end": (slot_start + timedelta(minutes=30)).isoformat(),
            }

        # Freshness of the most recent optimization.
        opt_row = conn.execute("SELECT MAX(optimized_at) AS ts FROM schedule").fetchone()
        planner["last_optimization_age_s"] = _age_seconds(
            opt_row["ts"] if opt_row else None, now
        )

        # Upcoming slots (this slot onward) for a compact price/plan strip.
        up_rows = conn.execute(
            """
            SELECT slot_start, battery_charge_kwh, battery_discharge_kwh,
                   grid_export_kwh, dhw_on, buy_gbp_kwh, sell_gbp_kwh
            FROM schedule WHERE slot_start >= ? ORDER BY slot_start LIMIT 24
            """,
            (slot_start.isoformat(),),
        ).fetchall()
        for r in up_rows:
            ss = datetime.fromisoformat(r["slot_start"])
            upcoming.append({
                "slot_start": r["slot_start"],
                "time_uk": ss.astimezone(LONDON).strftime("%H:%M"),
                "buy_p": round(r["buy_gbp_kwh"] * 100, 2),
                "sell_p": round(r["sell_gbp_kwh"] * 100, 2),
                "mode": _slot_mode(
                    r["battery_charge_kwh"], r["battery_discharge_kwh"], r["grid_export_kwh"]
                ),
                "dhw_on": bool(r["dhw_on"]),
                "is_current": r["slot_start"] == slot_start.isoformat(),
            })

        # Cost so far today and yesterday's net, from recorded actuals.
        now_london = now.astimezone(LONDON)
        today_local = now_london.replace(hour=0, minute=0, second=0, microsecond=0)
        today_utc = today_local.astimezone(timezone.utc)
        yest_utc = (today_local - timedelta(days=1)).astimezone(timezone.utc)
        try:
            today = conn.execute(
                "SELECT SUM(cost_gbp) AS cost, COUNT(*) AS n FROM actuals WHERE slot_start >= ?",
                (today_utc.isoformat(),),
            ).fetchone()
            yest = conn.execute(
                "SELECT SUM(cost_gbp) AS cost FROM actuals WHERE slot_start >= ? AND slot_start < ?",
                (yest_utc.isoformat(), today_utc.isoformat()),
            ).fetchone()
            cost = {
                "today_gbp": round(today["cost"] or 0.0, 3) if today else None,
                "today_slots": today["n"] if today else 0,
                "yesterday_gbp": round(yest["cost"] or 0.0, 3) if yest and yest["cost"] is not None else None,
            }
        except Exception as exc:  # actuals table may be empty/missing columns
            logger.debug("actuals cost summary unavailable: %s", exc)
            cost = {}

    if planner.get("has_decision"):
        cost["current_slot_gbp"] = planner["slot_cost_gbp"]
    return planner, cost, upcoming


def _feeds(db_path: str, now: datetime) -> dict:
    """Seconds since each data feed was last refreshed (lower = fresher)."""
    feeds: dict = {}
    queries = (
        ("solar", "SELECT MAX(fetched_at) AS ts FROM solar_forecast"),
        ("weather", "SELECT MAX(fetched_at) AS ts FROM weather_forecast"),
        ("consumption", "SELECT MAX(slot_start) AS ts FROM consumption"),
        ("prices", "SELECT MAX(slot_start) AS ts FROM prices"),
    )
    with get_conn(db_path) as conn:
        for feed, q in queries:
            try:
                row = conn.execute(q).fetchone()
                feeds[feed] = _age_seconds(row["ts"] if row else None, now)
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("feed %s age unavailable: %s", feed, exc)
                feeds[feed] = None
    return feeds


def _history(state) -> list:
    """Recent inverter readings for sparklines (oldest → newest)."""
    out = []
    for r in list(state.history):
        out.append({
            "t": r.recorded_at.isoformat(),
            "soc_pct": round(r.soc_pct, 1),
            "solar_w": round(r.solar_w),
            "load_w": round(r.load_w),
            "grid_net_w": round(r.grid_import_w - r.grid_export_w),
            "battery_net_w": round(r.battery_discharge_w - r.battery_charge_w),
        })
    return out


def build_status(state, db_path: str, capacity_kwh: float, now: datetime | None = None) -> dict:
    """Assemble the full live status snapshot for the dashboard."""
    if now is None:
        now = datetime.now(timezone.utc)
    planner, cost, upcoming = _planner_and_cost(db_path, now)
    return {
        "now": now.isoformat(),
        "battery_capacity_kwh": capacity_kwh,
        "live": _live(state, now),
        "dhw": _dhw(state, now),
        "planner": planner,
        "cost": cost,
        "upcoming": upcoming,
        "feeds": _feeds(db_path, now),
        "history": _history(state),
    }
