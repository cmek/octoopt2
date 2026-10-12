"""Load per-slot history for the backtest from the octoopt2 SQLite database.

Everything is keyed by the UTC half-hour `slot_start` string the rest of the
code uses (`YYYY-MM-DDTHH:MM:SS+00:00`). Rows are driven from `prices`, which
is complete, so missing actuals show up as holes rather than silently
shortening a day.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

LONDON = ZoneInfo("Europe/London")
SLOT = timedelta(minutes=30)
KWH_PER_W = 0.5 / 1000  # average W over 30 min → kWh


@dataclass
class Slot:
    slot_start: datetime            # UTC aware
    buy: float
    sell: float
    # actual energy flows (kWh per slot); None when the slot has no data
    solar_kwh: float | None = None
    load_kwh: float | None = None
    act_import_kwh: float | None = None
    act_export_kwh: float | None = None
    act_cost_gbp: float | None = None
    # from inverter_readings aggregation
    bat_charge_kwh: float | None = None
    bat_discharge_kwh: float | None = None
    soc_start_pct: float | None = None
    soc_end_pct: float | None = None
    n_samples: int = 0
    source: str = "none"            # actuals | readings | filled | none
    # from schedule (the last plan written for the slot)
    pred_load_kwh: float | None = None
    pred_solar_kwh: float | None = None
    plan_charge_kwh: float | None = None
    plan_discharge_kwh: float | None = None
    plan_import_kwh: float | None = None
    plan_export_kwh: float | None = None
    plan_dhw_on: int | None = None
    plan_buy: float | None = None
    plan_sell: float | None = None
    # Solcast
    solcast_p50_kwh: float | None = None
    solcast_p10_kwh: float | None = None

    @property
    def london_day(self) -> date:
        return self.slot_start.astimezone(LONDON).date()

    @property
    def net_load_kwh(self) -> float:
        return (self.load_kwh or 0.0) - (self.solar_kwh or 0.0)


@dataclass
class Segment:
    """A run of consecutive complete London days; SoC is chained across them."""
    days: list[date]
    slots: list[Slot]
    soc0_kwh: float
    soc0_source: str = "reading"    # reading | floor
    day_index: dict[date, list[int]] = field(default_factory=dict)


def _utc(s: str) -> datetime:
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def _key(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def london_day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=LONDON)
    return start.astimezone(timezone.utc), (start + timedelta(days=1)).astimezone(timezone.utc)


def _bucket(recorded_at: str) -> str:
    """Map a reading timestamp to its half-hour slot key."""
    dt = _utc(recorded_at)
    dt = dt.replace(minute=0 if dt.minute < 30 else 30, second=0, microsecond=0)
    return dt.isoformat()


def aggregate_readings(conn: sqlite3.Connection, from_utc: datetime, to_utc: datetime) -> dict[str, dict]:
    """Bucket inverter_readings into half-hour slots exactly like
    scheduler._record_actuals (mean W × 0.5/1000), plus battery flows and SoC."""
    rows = conn.execute(
        """
        SELECT recorded_at, soc_pct, solar_w, load_w, grid_import_w, grid_export_w,
               battery_charge_w, battery_discharge_w
        FROM inverter_readings
        WHERE recorded_at >= ? AND recorded_at < ?
        ORDER BY recorded_at
        """,
        (_key(from_utc), _key(to_utc)),
    ).fetchall()
    out: dict[str, dict] = {}
    for r in rows:
        k = _bucket(r["recorded_at"])
        b = out.setdefault(k, {"n": 0, "solar": 0.0, "load": 0.0, "imp": 0.0, "exp": 0.0,
                               "chg": 0.0, "dis": 0.0, "soc_first": r["soc_pct"], "soc_last": r["soc_pct"]})
        b["n"] += 1
        b["solar"] += r["solar_w"]
        b["load"] += r["load_w"]
        b["imp"] += r["grid_import_w"]
        b["exp"] += r["grid_export_w"]
        b["chg"] += r["battery_charge_w"]
        b["dis"] += r["battery_discharge_w"]
        b["soc_last"] = r["soc_pct"]
    for b in out.values():
        n = b["n"]
        for f in ("solar", "load", "imp", "exp", "chg", "dis"):
            b[f] = b[f] / n * KWH_PER_W
    return out


def load_slots(db_path: str, from_utc: datetime, to_utc: datetime) -> list[Slot]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT p.slot_start, p.buy_gbp_kwh, p.sell_gbp_kwh,
                   a.solar_kwh, a.load_kwh, a.grid_import_kwh, a.grid_export_kwh, a.cost_gbp,
                   s.predicted_load_kwh, s.predicted_solar_kwh,
                   s.battery_charge_kwh, s.battery_discharge_kwh,
                   s.grid_import_kwh AS plan_import, s.grid_export_kwh AS plan_export,
                   s.dhw_on, s.buy_gbp_kwh AS plan_buy, s.sell_gbp_kwh AS plan_sell,
                   f.pv_estimate_kwh, f.pv_estimate_p10
            FROM prices p
            LEFT JOIN actuals a        ON a.slot_start = p.slot_start
            LEFT JOIN schedule s       ON s.slot_start = p.slot_start
            LEFT JOIN solar_forecast f ON f.slot_start = p.slot_start
            WHERE p.slot_start >= ? AND p.slot_start < ?
            ORDER BY p.slot_start
            """,
            (_key(from_utc), _key(to_utc)),
        ).fetchall()
        readings = aggregate_readings(conn, from_utc, to_utc)
    finally:
        conn.close()

    slots: list[Slot] = []
    for r in rows:
        buy = float(r["buy_gbp_kwh"])
        sell = r["sell_gbp_kwh"]
        sell = buy if sell is None else float(sell)
        sl = Slot(
            slot_start=_utc(r["slot_start"]), buy=buy, sell=sell,
            pred_load_kwh=r["predicted_load_kwh"], pred_solar_kwh=r["predicted_solar_kwh"],
            plan_charge_kwh=r["battery_charge_kwh"], plan_discharge_kwh=r["battery_discharge_kwh"],
            plan_import_kwh=r["plan_import"], plan_export_kwh=r["plan_export"],
            plan_dhw_on=r["dhw_on"], plan_buy=r["plan_buy"], plan_sell=r["plan_sell"],
            solcast_p50_kwh=r["pv_estimate_kwh"], solcast_p10_kwh=r["pv_estimate_p10"],
        )
        b = readings.get(r["slot_start"])
        if b:
            sl.bat_charge_kwh, sl.bat_discharge_kwh = b["chg"], b["dis"]
            sl.soc_start_pct, sl.soc_end_pct, sl.n_samples = b["soc_first"], b["soc_last"], b["n"]
        if r["solar_kwh"] is not None and r["load_kwh"] is not None:
            sl.solar_kwh, sl.load_kwh = float(r["solar_kwh"]), float(r["load_kwh"])
            sl.act_import_kwh, sl.act_export_kwh = float(r["grid_import_kwh"] or 0), float(r["grid_export_kwh"] or 0)
            sl.act_cost_gbp = r["cost_gbp"]
            sl.source = "actuals"
        elif b:
            sl.solar_kwh, sl.load_kwh = b["solar"], b["load"]
            sl.act_import_kwh, sl.act_export_kwh = b["imp"], b["exp"]
            sl.act_cost_gbp = buy * b["imp"] - sell * b["exp"]
            sl.source = "readings"
        if sl.act_cost_gbp is None and sl.act_import_kwh is not None:
            sl.act_cost_gbp = buy * sl.act_import_kwh - sell * sl.act_export_kwh
        slots.append(sl)
    return slots


def _fill_holes(day_slots: list[Slot], max_holes: int) -> bool:
    """Linearly interpolate solar/load/import/export for ≤ max_holes missing
    slots inside a day. Returns False if the day is still incomplete."""
    missing = [i for i, s in enumerate(day_slots) if s.solar_kwh is None]
    if not missing:
        return True
    if len(missing) > max_holes:
        return False
    for i in missing:
        prev = next((day_slots[j] for j in range(i - 1, -1, -1) if day_slots[j].solar_kwh is not None), None)
        nxt = next((day_slots[j] for j in range(i + 1, len(day_slots)) if day_slots[j].solar_kwh is not None), None)
        ref = [x for x in (prev, nxt) if x is not None]
        if not ref:
            return False
        s = day_slots[i]
        for f in ("solar_kwh", "load_kwh", "act_import_kwh", "act_export_kwh"):
            setattr(s, f, sum(getattr(x, f) for x in ref) / len(ref))
        s.act_cost_gbp = s.buy * s.act_import_kwh - s.sell * s.act_export_kwh
        s.source = "filled"
    return True


def segment_days(
    slots: list[Slot],
    *,
    min_slots_per_day: int = 46,
    max_holes: int = 2,
    floor_kwh: float,
    capacity_kwh: float,
) -> tuple[list[Segment], list[tuple[date, str]]]:
    """Group slots into complete London days and chain consecutive days into
    segments. Returns (segments, excluded_days_with_reason)."""
    by_day: dict[date, list[Slot]] = {}
    for s in slots:
        by_day.setdefault(s.london_day, []).append(s)

    complete: dict[date, list[Slot]] = {}
    excluded: list[tuple[date, str]] = []
    for day, ds in sorted(by_day.items()):
        expected = int((london_day_bounds(day)[1] - london_day_bounds(day)[0]) / SLOT)
        if len(ds) < expected:
            excluded.append((day, f"only {len(ds)}/{expected} price slots"))
            continue
        have = sum(1 for s in ds if s.solar_kwh is not None)
        if have < min_slots_per_day or not _fill_holes(ds, max_holes):
            excluded.append((day, f"{have}/{expected} slots with actuals"))
            continue
        complete[day] = ds

    segments: list[Segment] = []
    cur: list[date] = []
    for day in sorted(complete):
        if cur and day != cur[-1] + timedelta(days=1):
            segments.append(_make_segment(cur, complete, floor_kwh, capacity_kwh))
            cur = []
        cur.append(day)
    if cur:
        segments.append(_make_segment(cur, complete, floor_kwh, capacity_kwh))
    return segments, excluded


def _make_segment(days: list[date], complete: dict[date, list[Slot]], floor_kwh: float, capacity_kwh: float) -> Segment:
    slots: list[Slot] = []
    index: dict[date, list[int]] = {}
    for d in days:
        index[d] = list(range(len(slots), len(slots) + len(complete[d])))
        slots.extend(complete[d])
    first = slots[0]
    if first.soc_start_pct is not None:
        soc0, src = first.soc_start_pct / 100 * capacity_kwh, "reading"
    else:
        soc0, src = floor_kwh, "floor"
    return Segment(days=days, slots=slots, soc0_kwh=soc0, soc0_source=src, day_index=index)
