"""Daily energy report.

Shows yesterday's actual performance and today's optimizer forecast.

Usage:
    uv run octoopt2-report
    uv run python scripts/report.py
"""
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, ".")

LONDON = ZoneInfo("Europe/London")
W = 72  # report width


def _section(title: str) -> None:
    print(f"\n{'─' * W}")
    print(f"  {title}")
    print(f"{'─' * W}")


def _fmt_cost(gbp: float, earned: bool = False) -> str:
    """Format a cost in pence, with sign label."""
    p = gbp * 100
    if earned:
        return f"{p:+.1f}p"
    return f"{p:.1f}p"


def _pence(gbp: float) -> str:
    return f"{gbp * 100:.2f}p/kWh"


# ── Yesterday actuals ─────────────────────────────────────────────────────────

def report_yesterday(db_path: str, yesterday: datetime, today: datetime) -> None:
    from octoopt2.db import get_conn

    _section(f"Yesterday ({yesterday.strftime('%-d %B %Y, %A')}) — Actuals")

    with get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT a.slot_start,
                   a.grid_import_kwh, a.grid_export_kwh,
                   a.solar_kwh, a.load_kwh, a.cost_gbp,
                   p.buy_gbp_kwh, p.sell_gbp_kwh
            FROM actuals a
            LEFT JOIN prices p ON p.slot_start = a.slot_start
            WHERE a.slot_start >= ? AND a.slot_start < ?
            ORDER BY a.slot_start
            """,
            (yesterday.isoformat(), today.isoformat()),
        ).fetchall()

    if not rows:
        print("  No actuals recorded for yesterday.")
        return

    total_import = sum(r["grid_import_kwh"] or 0 for r in rows)
    total_export = sum(r["grid_export_kwh"] or 0 for r in rows)
    total_solar  = sum(r["solar_kwh"]       or 0 for r in rows)
    total_load   = sum(r["load_kwh"]        or 0 for r in rows)
    total_cost   = sum(r["cost_gbp"]        or 0 for r in rows)
    total_earned = sum(
        (r["sell_gbp_kwh"] or 0) * (r["grid_export_kwh"] or 0) for r in rows
    )
    total_gross  = sum(
        (r["buy_gbp_kwh"] or 0) * (r["grid_import_kwh"] or 0) for r in rows
    )

    print(f"  Solar generated  : {total_solar:6.2f} kWh")
    print(f"  Home load        : {total_load:6.2f} kWh")
    print(f"  Grid imported    : {total_import:6.2f} kWh  (gross cost  £{total_gross:.2f})")
    print(f"  Grid exported    : {total_export:6.2f} kWh  (earned      £{total_earned:.2f})")
    net_label = "earned" if total_cost < 0 else "cost"
    print(f"  Net energy {net_label:6s}: £{abs(total_cost):.2f}")
    print(f"  Slots recorded   : {len(rows)} of 48")

    # Slot-by-slot table
    print()
    hdr = f"  {'Time':<8} {'Import':>7} {'Export':>7} {'Solar':>7} {'Load':>7}  {'Buy p/kWh':>10}  {'Slot cost':>10}"
    print(hdr)
    print("  " + "─" * (len(hdr) - 2))
    for r in rows:
        local = datetime.fromisoformat(r["slot_start"]).astimezone(LONDON)
        buy_str = f"{(r['buy_gbp_kwh'] or 0)*100:.2f}p" if r["buy_gbp_kwh"] is not None else "    —"
        cost_str = f"{(r['cost_gbp'] or 0)*100:+.1f}p" if r["cost_gbp"] is not None else "    —"
        print(
            f"  {local.strftime('%H:%M'):<8}"
            f" {(r['grid_import_kwh'] or 0):>7.3f}"
            f" {(r['grid_export_kwh'] or 0):>7.3f}"
            f" {(r['solar_kwh'] or 0):>7.3f}"
            f" {(r['load_kwh'] or 0):>7.3f}"
            f"  {buy_str:>10}"
            f"  {cost_str:>10}"
        )
    print("  " + "─" * (len(hdr) - 2))
    print(
        f"  {'TOTAL':<8}"
        f" {total_import:>7.3f}"
        f" {total_export:>7.3f}"
        f" {total_solar:>7.3f}"
        f" {total_load:>7.3f}"
        f"  {'':>10}"
        f"  {total_cost*100:>+10.1f}p"
    )


# ── Today forecast ────────────────────────────────────────────────────────────

def report_today(db_path: str, today: datetime, tomorrow: datetime) -> None:
    from octoopt2.db import get_conn

    _section(f"Today ({today.strftime('%-d %B %Y, %A')}) — Optimizer Forecast")

    with get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT slot_start, battery_charge_kwh, battery_discharge_kwh,
                   grid_import_kwh, grid_export_kwh, dhw_on,
                   predicted_load_kwh, predicted_solar_kwh,
                   buy_gbp_kwh, sell_gbp_kwh, optimized_at
            FROM schedule
            WHERE slot_start >= ? AND slot_start < ?
            ORDER BY slot_start
            """,
            (today.isoformat(), tomorrow.isoformat()),
        ).fetchall()

    if not rows:
        print("  No schedule found for today — optimizer may not have run yet.")
        return

    optimized_at = datetime.fromisoformat(rows[-1]["optimized_at"]).astimezone(LONDON)
    print(f"  Schedule last computed: {optimized_at.strftime('%H:%M')}")
    print()

    total_import = sum(r["grid_import_kwh"] for r in rows)
    total_export = sum(r["grid_export_kwh"] for r in rows)
    total_solar  = sum(r["predicted_solar_kwh"] for r in rows)
    total_load   = sum(r["predicted_load_kwh"] for r in rows)
    total_cost   = sum(
        r["buy_gbp_kwh"] * r["grid_import_kwh"] - r["sell_gbp_kwh"] * r["grid_export_kwh"]
        for r in rows
    )
    total_earned = sum(r["sell_gbp_kwh"] * r["grid_export_kwh"] for r in rows)
    total_gross  = sum(r["buy_gbp_kwh"] * r["grid_import_kwh"] for r in rows)

    print(f"  Forecast solar         : {total_solar:6.2f} kWh")
    print(f"  Forecast load          : {total_load:6.2f} kWh")
    print(f"  Planned grid import    : {total_import:6.2f} kWh  (gross cost  £{total_gross:.2f})")
    print(f"  Planned grid export    : {total_export:6.2f} kWh  (earned      £{total_earned:.2f})")
    net_label = "earned" if total_cost < 0 else "cost"
    print(f"  Estimated net {net_label:6s}: £{abs(total_cost):.2f}")

    # ── DHW schedule ─────────────────────────────────────────────────────────
    dhw_slots = [r for r in rows if r["dhw_on"]]
    print()
    if dhw_slots:
        # Merge consecutive slots into windows
        windows = _merge_consecutive(
            [datetime.fromisoformat(r["slot_start"]).astimezone(LONDON) for r in dhw_slots]
        )
        window_strs = [
            f"{s.strftime('%H:%M')}–{(e + timedelta(minutes=30)).strftime('%H:%M')}"
            for s, e in windows
        ]
        print(f"  DHW heating ({len(dhw_slots)} slots, {len(dhw_slots)*0.5:.1f}h):")
        for w in window_strs:
            print(f"    {w}")
    else:
        print("  DHW heating: none scheduled today")

    # ── Best appliance windows ────────────────────────────────────────────────
    print()
    print("  Best slots to run appliances (cheapest grid import):")
    now_local = datetime.now(LONDON)

    future_rows = [
        r for r in rows
        if datetime.fromisoformat(r["slot_start"]).astimezone(LONDON) > now_local
    ]
    if not future_rows:
        future_rows = rows  # all past — show anyway

    sorted_by_price = sorted(future_rows, key=lambda r: r["buy_gbp_kwh"])
    top = sorted_by_price[:10]

    print(f"  {'Time':<8}  {'Buy p/kWh':>10}  {'Battery':>12}  {'Notes'}")
    print("  " + "─" * 56)
    for r in top:
        local = datetime.fromisoformat(r["slot_start"]).astimezone(LONDON)
        marker = "◄ now" if local <= now_local < local + timedelta(minutes=30) else ""
        if r["battery_charge_kwh"] > 0.05:
            bat_note = f"bat charging  {marker}"
        elif r["battery_discharge_kwh"] > 0.05:
            bat_note = f"bat export    {marker}"
        else:
            bat_note = f"              {marker}"
        print(
            f"  {local.strftime('%H:%M'):<8}"
            f"  {r['buy_gbp_kwh']*100:>9.2f}p"
            f"  {bat_note}"
        )

    # Highlight cheapest contiguous 1h window (2 slots)
    best_window = _best_window(future_rows, n_slots=2)
    if best_window:
        t0 = datetime.fromisoformat(best_window[0]["slot_start"]).astimezone(LONDON)
        t1 = t0 + timedelta(hours=1)
        avg_p = sum(r["buy_gbp_kwh"] for r in best_window) / len(best_window) * 100
        print()
        print(f"  Best 1-hour window  : {t0.strftime('%H:%M')}–{t1.strftime('%H:%M')}  (avg {avg_p:.2f}p/kWh)")

    best_window_90 = _best_window(future_rows, n_slots=3)
    if best_window_90:
        t0 = datetime.fromisoformat(best_window_90[0]["slot_start"]).astimezone(LONDON)
        t1 = t0 + timedelta(minutes=90)
        avg_p = sum(r["buy_gbp_kwh"] for r in best_window_90) / len(best_window_90) * 100
        print(f"  Best 1.5-hour window: {t0.strftime('%H:%M')}–{t1.strftime('%H:%M')}  (avg {avg_p:.2f}p/kWh)")


def _merge_consecutive(
    slots: list[datetime],
) -> list[tuple[datetime, datetime]]:
    """Merge a sorted list of 30-min slot starts into contiguous windows (start, last_slot)."""
    if not slots:
        return []
    windows = []
    start = end = slots[0]
    for s in slots[1:]:
        if s == end + timedelta(minutes=30):
            end = s
        else:
            windows.append((start, end))
            start = end = s
    windows.append((start, end))
    return windows


def _best_window(rows: list, n_slots: int) -> list | None:
    """Find the contiguous window of n_slots with the lowest total buy price."""
    if len(rows) < n_slots:
        return None
    # Rows must be consecutive by slot_start (they are, from ORDER BY)
    best_cost = float("inf")
    best_start = 0
    for i in range(len(rows) - n_slots + 1):
        window = rows[i : i + n_slots]
        # Check contiguity
        starts = [datetime.fromisoformat(r["slot_start"]) for r in window]
        contiguous = all(
            starts[j + 1] == starts[j] + timedelta(minutes=30) for j in range(len(starts) - 1)
        )
        if not contiguous:
            continue
        cost = sum(r["buy_gbp_kwh"] for r in window)
        if cost < best_cost:
            best_cost = cost
            best_start = i
    return rows[best_start : best_start + n_slots]


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    from dotenv import load_dotenv
    load_dotenv()

    from octoopt2.config import AppConfig
    config = AppConfig.from_env()

    now_london = datetime.now(LONDON)
    today_london    = now_london.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_london = today_london - timedelta(days=1)
    tomorrow_london  = today_london + timedelta(days=1)

    # Convert boundaries to UTC for DB queries
    today_utc     = today_london.astimezone(timezone.utc)
    yesterday_utc = yesterday_london.astimezone(timezone.utc)
    tomorrow_utc  = tomorrow_london.astimezone(timezone.utc)

    print("=" * W)
    print(f"  octoopt2 — Daily Report   {now_london.strftime('%-d %B %Y, %H:%M')}")
    print("=" * W)

    report_yesterday(config.db_path, yesterday_utc, today_utc)
    report_today(config.db_path, today_utc, tomorrow_utc)
    print()


if __name__ == "__main__":
    main()
