"""Entry point. Run directly or via cron every 5 minutes.

    # Normal run (cron):
    uv run octoopt2

    # Dry run — shows planned schedule and current-slot command, no hardware writes:
    uv run octoopt2 --dry-run

    # Cron entry:
    */5 * * * * cd /path/to/octoopt2 && uv run octoopt2 >> /var/log/octoopt2.log 2>&1
"""
import argparse
import logging
import sys

from .config import AppConfig
from .db import init_db
from .scheduler import run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="octoopt2 energy optimizer")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run optimizer and print planned schedule without sending any commands to hardware",
    )
    args = parser.parse_args()

    config = AppConfig.from_env()
    init_db(config.db_path)
    try:
        run(config, dry_run=args.dry_run)
    except Exception as exc:
        logger.exception("Unhandled error in scheduler tick: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
