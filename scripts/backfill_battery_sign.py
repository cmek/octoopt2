"""One-off backfill: correct the swapped battery charge/discharge columns.

Before the p_battery sign fix in octoopt2/data/inverter.py, every inverter_readings
row stored battery_charge_w and battery_discharge_w swapped (p_battery is positive
when discharging on this inverter, but the code treated positive as charging).

This script swaps the two columns back for rows written by the buggy code.

IMPORTANT — run this AFTER deploying the code fix, and pass --before set to the
moment the fixed code started running, so that correct rows written by the new
code are left untouched. Rows with recorded_at >= --before are not modified.

Usage:
    # dry run (default): report how many rows would change, touch nothing
    python -m scripts.backfill_battery_sign --before 2026-06-25T18:15:00+00:00

    # apply
    python -m scripts.backfill_battery_sign --before 2026-06-25T18:15:00+00:00 --apply

DB path defaults to $DB_PATH (falling back to octoopt2.db), matching the app.
"""
import argparse
import os
import sqlite3
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--before",
        required=True,
        help="ISO-8601 cutover timestamp; only rows with recorded_at < this are "
        "corrected. Use the moment the fixed code went live.",
    )
    parser.add_argument(
        "--db",
        default=os.getenv("DB_PATH", "octoopt2.db"),
        help="Path to the sqlite database (default: $DB_PATH or octoopt2.db).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually perform the swap. Without this flag the script is a dry run.",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        affected = conn.execute(
            "SELECT COUNT(*) AS n FROM inverter_readings WHERE recorded_at < ?",
            (args.before,),
        ).fetchone()["n"]
        total = conn.execute(
            "SELECT COUNT(*) AS n FROM inverter_readings"
        ).fetchone()["n"]

        print(f"DB: {args.db}")
        print(f"Rows total: {total}")
        print(f"Rows with recorded_at < {args.before}: {affected} (these will be swapped)")
        print(f"Rows left untouched (>= cutover): {total - affected}")

        if not args.apply:
            print("\nDRY RUN — no changes written. Re-run with --apply to commit.")
            return 0

        # In SQLite, all SET right-hand sides see the row's pre-update values,
        # so this swaps the two columns in a single pass.
        cur = conn.execute(
            """
            UPDATE inverter_readings
               SET battery_charge_w    = battery_discharge_w,
                   battery_discharge_w = battery_charge_w
             WHERE recorded_at < ?
            """,
            (args.before,),
        )
        conn.commit()
        print(f"\nSwapped charge/discharge on {cur.rowcount} rows.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
