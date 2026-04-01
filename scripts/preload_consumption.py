"""One-time script to seed the database with historical consumption data.

Run this once before starting the optimizer to populate the load forecasting model.

Usage:
    uv run python scripts/preload_consumption.py           # last 30 days (default)
    uv run python scripts/preload_consumption.py --days 60 # last 60 days

Requires .env to be configured with:
    OCTOPUS_API_KEY, OCTOPUS_MPAN, OCTOPUS_SERIAL

Data is fetched from the Octopus smart meter API in weekly chunks and stored
in the local SQLite database (DB_PATH in .env, defaults to octoopt2.db).
Previously stored slots are overwritten if re-fetched, so this is safe to
re-run if interrupted.
"""
import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)

# Allow running from repo root without installing the package
sys.path.insert(0, ".")

from octoopt2.config import AppConfig
from octoopt2.data.consumption import consumption_coverage, preload_consumption
from octoopt2.db import init_db


def main() -> None:
    parser = argparse.ArgumentParser(description="Preload Octopus consumption history")
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days of history to fetch (default: 30)",
    )
    args = parser.parse_args()

    config = AppConfig.from_env()
    init_db(config.db_path)

    print(f"Fetching {args.days} days of consumption into {config.db_path} ...")
    total = preload_consumption(config.octopus, config.db_path, days=args.days)

    coverage = consumption_coverage(config.db_path, days=args.days)
    print(
        f"\nDone. {total} slots stored."
        f" Coverage: {coverage['stored']}/{coverage['expected']}"
        f" ({coverage['coverage_pct']}%)"
    )
    if coverage["coverage_pct"] < 90:
        print(
            "Warning: coverage below 90% — some slots may be missing from the smart"
            " meter (common if meter readings are delayed by a day or two)."
        )


if __name__ == "__main__":
    main()
