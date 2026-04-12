"""Forecast accuracy and cost outcomes report.

Compares optimizer predictions against actual measured outcomes over a rolling
window of days, showing:
  - Solar forecast accuracy  (Solcast p50 vs inverter-measured actual)
  - Load forecast accuracy   (optimizer prediction vs inverter-measured actual)
  - Cost outcomes            (planned schedule cost vs actual cost)

Usage:
    uv run octoopt2-accuracy-report
    uv run octoopt2-accuracy-report --days 30
    uv run octoopt2-accuracy-report --output          # save web/reports/YYYY-MM-DD.json
    uv run python scripts/accuracy_report.py --days 14
"""
import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, ".")

LONDON = ZoneInfo("Europe/London")
W = 76


def _section(title: str) -> None:
    print(f"\n{'─' * W}")
    print(f"  {title}")
    print(f"{'─' * W}")


def _london_day(dt: datetime) -> str:
    """Return YYYY-MM-DD string in London local time."""
    return dt.astimezone(LONDON).strftime("%Y-%m-%d")


def _day_label(day_str: str) -> str:
    """'2026-04-01' → 'Wed  1 Apr'"""
    d = datetime.strptime(day_str, "%Y-%m-%d")
    return d.strftime("%a %e %b").replace("  ", " ")


# ── Data loading ──────────────────────────────────────────────────────────────

def _load_data(db_path: str, from_utc: datetime, to_utc: datetime) -> dict:
    """Load all relevant tables for the window into per-London-day dicts."""
    from octoopt2.db import get_conn

    from_str = from_utc.isoformat()
    to_str   = to_utc.isoformat()

    with get_conn(db_path) as conn:
        actuals = conn.execute(
            """
            SELECT slot_start, solar_kwh, load_kwh,
                   grid_import_kwh, grid_export_kwh, cost_gbp
            FROM actuals
            WHERE slot_start >= ? AND slot_start < ?
            ORDER BY slot_start
            """,
            (from_str, to_str),
        ).fetchall()

        forecasts = conn.execute(
            """
            SELECT slot_start, pv_estimate_kwh
            FROM solar_forecast
            WHERE slot_start >= ? AND slot_start < ?
            ORDER BY slot_start
            """,
            (from_str, to_str),
        ).fetchall()

        schedule = conn.execute(
            """
            SELECT slot_start, predicted_load_kwh, predicted_solar_kwh,
                   buy_gbp_kwh, sell_gbp_kwh,
                   grid_import_kwh, grid_export_kwh
            FROM schedule
            WHERE slot_start >= ? AND slot_start < ?
            ORDER BY slot_start
            """,
            (from_str, to_str),
        ).fetchall()

    def _group(rows, *cols):
        """Group rows by London calendar day, summing the named columns."""
        by_day = defaultdict(lambda: defaultdict(float))
        counts = defaultdict(int)
        for r in rows:
            day = _london_day(datetime.fromisoformat(r["slot_start"]))
            for col in cols:
                v = r[col]
                if v is not None:
                    by_day[day][col] += v
            counts[day] += 1
        return dict(by_day), dict(counts)

    actual_by_day,   actual_counts   = _group(actuals,   "solar_kwh", "load_kwh",
                                               "grid_import_kwh", "grid_export_kwh", "cost_gbp")
    forecast_by_day, forecast_counts = _group(forecasts,  "pv_estimate_kwh")
    schedule_by_day, schedule_counts = _group(schedule,   "predicted_load_kwh", "predicted_solar_kwh",
                                               "grid_import_kwh", "grid_export_kwh")

    # Planned cost per day: Σ (buy * import - sell * export) from schedule
    planned_cost_by_day: dict[str, float] = defaultdict(float)
    for r in schedule:
        day = _london_day(datetime.fromisoformat(r["slot_start"]))
        planned_cost_by_day[day] += (
            r["buy_gbp_kwh"] * r["grid_import_kwh"]
            - r["sell_gbp_kwh"] * r["grid_export_kwh"]
        )

    return {
        "actual":       actual_by_day,
        "actual_cnt":   actual_counts,
        "forecast":     forecast_by_day,
        "schedule":     schedule_by_day,
        "sched_cnt":    schedule_counts,
        "planned_cost": dict(planned_cost_by_day),
    }


# ── Solar accuracy ────────────────────────────────────────────────────────────

def report_solar(data: dict, days: list[str]) -> None:
    _section("Solar Forecast Accuracy  (Solcast p50 vs inverter actual)")

    print(f"  {'Date':<12} {'Forecast':>10} {'Actual':>10} {'Error':>10} {'Error%':>8}  {'Slots':>5}")
    print("  " + "─" * 62)

    errors, biases = [], []
    n_days = 0

    for day in days:
        fc  = data["forecast"].get(day, {}).get("pv_estimate_kwh")
        act = data["actual"].get(day, {}).get("solar_kwh")
        cnt = data["actual_cnt"].get(day, 0)

        if fc is None or act is None or cnt < 10:
            tag = " (partial)" if cnt > 0 else " (no data)"
            print(f"  {_day_label(day):<12} {'—':>10} {'—':>10} {'—':>10} {'—':>8}  {cnt:>5}{tag}")
            continue

        err  = fc - act           # positive = Solcast over-predicted
        pct  = err / act * 100 if act > 0 else 0.0
        errors.append(abs(err))
        biases.append(err)
        n_days += 1

        print(
            f"  {_day_label(day):<12}"
            f" {fc:>9.2f}k"
            f" {act:>9.2f}k"
            f" {err:>+9.2f}k"
            f" {pct:>+7.1f}%"
            f"  {cnt:>5}"
        )

    print("  " + "─" * 62)
    if n_days >= 2:
        mae  = sum(errors) / len(errors)
        bias = sum(biases) / len(biases)
        bias_dir = "over-predicts" if bias > 0 else "under-predicts"
        print(f"  {n_days}-day MAE : {mae:.2f} kWh/day")
        print(f"  {n_days}-day Bias: {bias:+.2f} kWh/day  (Solcast {bias_dir} on average)")
    else:
        print("  Insufficient data for summary statistics.")


# ── Load accuracy ─────────────────────────────────────────────────────────────

def report_load(data: dict, days: list[str]) -> None:
    _section("Load Forecast Accuracy  (optimizer prediction vs inverter actual)")

    print(f"  {'Date':<12} {'Forecast':>10} {'Actual':>10} {'Error':>10} {'Error%':>8}  {'Slots':>5}")
    print("  " + "─" * 62)

    errors, biases = [], []
    n_days = 0

    for day in days:
        fc  = data["schedule"].get(day, {}).get("predicted_load_kwh")
        act = data["actual"].get(day, {}).get("load_kwh")
        cnt = data["actual_cnt"].get(day, 0)

        if fc is None or act is None or cnt < 10:
            tag = " (partial)" if cnt > 0 else " (no data)"
            print(f"  {_day_label(day):<12} {'—':>10} {'—':>10} {'—':>10} {'—':>8}  {cnt:>5}{tag}")
            continue

        err = fc - act
        pct = err / act * 100 if act > 0 else 0.0
        errors.append(abs(err))
        biases.append(err)
        n_days += 1

        print(
            f"  {_day_label(day):<12}"
            f" {fc:>9.2f}k"
            f" {act:>9.2f}k"
            f" {err:>+9.2f}k"
            f" {pct:>+7.1f}%"
            f"  {cnt:>5}"
        )

    print("  " + "─" * 62)
    if n_days >= 2:
        mae  = sum(errors) / len(errors)
        bias = sum(biases) / len(biases)
        bias_dir = "over-predicts" if bias > 0 else "under-predicts"
        print(f"  {n_days}-day MAE : {mae:.2f} kWh/day")
        print(f"  {n_days}-day Bias: {bias:+.2f} kWh/day  (model {bias_dir} on average)")
    else:
        print("  Insufficient data for summary statistics.")


# ── Cost outcomes ─────────────────────────────────────────────────────────────

def report_cost(data: dict, days: list[str]) -> None:
    _section("Cost Outcomes  (optimizer planned vs actual)")

    print(f"  {'Date':<12} {'Planned':>9} {'Actual':>9} {'Diff':>9}  {'Import':>8} {'Export':>8}  {'Slots':>5}")
    print("  " + "─" * 70)

    total_planned = total_actual = total_import = total_export = 0.0
    n_days = 0

    for day in days:
        planned = data["planned_cost"].get(day)
        actual  = data["actual"].get(day, {}).get("cost_gbp")
        imp     = data["actual"].get(day, {}).get("grid_import_kwh", 0.0)
        exp     = data["actual"].get(day, {}).get("grid_export_kwh", 0.0)
        cnt     = data["actual_cnt"].get(day, 0)

        if planned is None or actual is None or cnt < 10:
            tag = " (partial)" if cnt > 0 else " (no data)"
            print(f"  {_day_label(day):<12} {'—':>9} {'—':>9} {'—':>9}  {'—':>8} {'—':>8}  {cnt:>5}{tag}")
            continue

        diff = actual - planned
        total_planned += planned
        total_actual  += actual
        total_import  += imp
        total_export  += exp
        n_days += 1

        print(
            f"  {_day_label(day):<12}"
            f" {planned*100:>+8.1f}p"
            f" {actual*100:>+8.1f}p"
            f" {diff*100:>+8.1f}p"
            f"  {imp:>7.2f}k"
            f" {exp:>7.2f}k"
            f"  {cnt:>5}"
        )

    print("  " + "─" * 70)
    if n_days >= 2:
        total_diff = total_actual - total_planned
        diff_dir   = "cheaper" if total_diff < 0 else "more expensive"
        print(
            f"  {n_days}-day totals:"
            f"  planned=£{total_planned:.2f}"
            f"  actual=£{total_actual:.2f}"
            f"  diff={total_diff*100:+.1f}p"
        )
        print(f"  Actual was {abs(total_diff)*100:.1f}p {diff_dir} than planned over {n_days} days.")
        if n_days > 0:
            print(
                f"  Import: {total_import:.1f} kWh total"
                f"  Export: {total_export:.1f} kWh total"
            )
    else:
        print("  Insufficient data for summary statistics.")


# ── JSON builders (for --output / web) ───────────────────────────────────────

def _build_solar_json(data: dict, days: list[str]) -> dict:
    rows = []
    errors, biases = [], []
    for day in days:
        fc  = data["forecast"].get(day, {}).get("pv_estimate_kwh")
        act = data["actual"].get(day, {}).get("solar_kwh")
        cnt = data["actual_cnt"].get(day, 0)
        if fc is None or act is None or cnt < 10:
            rows.append({"day": day, "label": _day_label(day), "slots": cnt, "partial": True})
            continue
        err = fc - act
        pct = err / act * 100 if act > 0 else 0.0
        errors.append(abs(err))
        biases.append(err)
        rows.append({
            "day": day, "label": _day_label(day),
            "forecast_kwh": round(fc, 3), "actual_kwh": round(act, 3),
            "error_kwh": round(err, 3), "error_pct": round(pct, 1),
            "slots": cnt, "partial": False,
        })
    result: dict = {"rows": rows}
    if len(errors) >= 2:
        result["summary"] = {
            "n_days": len(errors),
            "mae": round(sum(errors) / len(errors), 3),
            "bias": round(sum(biases) / len(biases), 3),
        }
    return result


def _build_load_json(data: dict, days: list[str]) -> dict:
    rows = []
    errors, biases = [], []
    for day in days:
        fc  = data["schedule"].get(day, {}).get("predicted_load_kwh")
        act = data["actual"].get(day, {}).get("load_kwh")
        cnt = data["actual_cnt"].get(day, 0)
        if fc is None or act is None or cnt < 10:
            rows.append({"day": day, "label": _day_label(day), "slots": cnt, "partial": True})
            continue
        err = fc - act
        pct = err / act * 100 if act > 0 else 0.0
        errors.append(abs(err))
        biases.append(err)
        rows.append({
            "day": day, "label": _day_label(day),
            "forecast_kwh": round(fc, 3), "actual_kwh": round(act, 3),
            "error_kwh": round(err, 3), "error_pct": round(pct, 1),
            "slots": cnt, "partial": False,
        })
    result: dict = {"rows": rows}
    if len(errors) >= 2:
        result["summary"] = {
            "n_days": len(errors),
            "mae": round(sum(errors) / len(errors), 3),
            "bias": round(sum(biases) / len(biases), 3),
        }
    return result


def _build_cost_json(data: dict, days: list[str]) -> dict:
    rows = []
    totals: dict[str, float] = {"planned": 0.0, "actual": 0.0, "imp": 0.0, "exp": 0.0}
    n = 0
    for day in days:
        planned = data["planned_cost"].get(day)
        actual  = data["actual"].get(day, {}).get("cost_gbp")
        imp     = data["actual"].get(day, {}).get("grid_import_kwh", 0.0)
        exp     = data["actual"].get(day, {}).get("grid_export_kwh", 0.0)
        cnt     = data["actual_cnt"].get(day, 0)
        if planned is None or actual is None or cnt < 10:
            rows.append({"day": day, "label": _day_label(day), "slots": cnt, "partial": True})
            continue
        diff = actual - planned
        totals["planned"] += planned
        totals["actual"]  += actual
        totals["imp"]     += imp
        totals["exp"]     += exp
        n += 1
        rows.append({
            "day": day, "label": _day_label(day),
            "planned_gbp": round(planned, 4), "actual_gbp": round(actual, 4),
            "diff_gbp": round(diff, 4),
            "import_kwh": round(imp, 3), "export_kwh": round(exp, 3),
            "slots": cnt, "partial": False,
        })
    result: dict = {"rows": rows}
    if n >= 2:
        result["summary"] = {
            "n_days": n,
            "total_planned_gbp": round(totals["planned"], 4),
            "total_actual_gbp":  round(totals["actual"], 4),
            "total_diff_gbp":    round(totals["actual"] - totals["planned"], 4),
            "total_import_kwh":  round(totals["imp"], 3),
            "total_export_kwh":  round(totals["exp"], 3),
        }
    return result


def _save_web_output(config, today_london: datetime, n_days: int, data: dict, days: list[str]) -> None:
    """Save timestamped JSON report and update the index."""
    report_date = (today_london - timedelta(days=1)).strftime("%Y-%m-%d")

    payload = {
        "report_date": report_date,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "days": n_days,
        "solar": _build_solar_json(data, days),
        "load":  _build_load_json(data, days),
        "cost":  _build_cost_json(data, days),
    }

    web_dir = os.path.join("web", "reports")
    os.makedirs(web_dir, exist_ok=True)

    out_path = os.path.join(web_dir, f"{report_date}.json")
    with open(out_path, "w") as f:
        json.dump(payload, f)
    print(f"\nSaved report to {out_path}")

    # Update index
    index_path = os.path.join(web_dir, "index.json")
    if os.path.exists(index_path):
        with open(index_path) as f:
            index = json.load(f)
    else:
        index = {"reports": []}

    if report_date not in index["reports"]:
        index["reports"].append(report_date)
        index["reports"].sort(reverse=True)

    with open(index_path, "w") as f:
        json.dump(index, f)
    print(f"Updated index: {len(index['reports'])} report(s) available")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="octoopt2 accuracy and cost report")
    parser.add_argument(
        "--days", type=int, default=14,
        help="Number of past days to include (default: 14)",
    )
    parser.add_argument(
        "--output", action="store_true",
        help="Save report as web/reports/YYYY-MM-DD.json and update web/reports/index.json",
    )
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()

    from octoopt2.config import AppConfig
    config = AppConfig.from_env()

    now_london = datetime.now(LONDON)
    # End of report window = start of today (so we only show completed days)
    today_london = now_london.replace(hour=0, minute=0, second=0, microsecond=0)
    start_london = today_london - timedelta(days=args.days)

    to_utc   = today_london.astimezone(timezone.utc)
    from_utc = start_london.astimezone(timezone.utc)

    # Build list of completed calendar days (London) in ascending order
    days = []
    d = start_london
    while d < today_london:
        days.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    print("=" * W)
    print(f"  octoopt2 — Accuracy Report   {args.days} days to {now_london.strftime('%-d %B %Y')}")
    print("=" * W)

    data = _load_data(config.db_path, from_utc, to_utc)

    report_solar(data, days)
    report_load(data, days)
    report_cost(data, days)
    print()

    if args.output:
        _save_web_output(config, today_london, args.days, data, days)


if __name__ == "__main__":
    main()
