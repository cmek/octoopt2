"""Set the inverter's hardware battery reserve (the hard discharge floor).

The optimizer's ``min_soc_pct`` (config) only constrains the *plan*. The actual
floor that physically stops the battery discharging is the GivEnergy inverter's
reserve register, which ships at 4% — that's why the battery drains to ~5%.
This script writes that hardware floor so the inverter refuses to discharge
below it, regardless of operating mode.

It's a one-off: the inverter holds the value in flash until changed again.

Usage:
    uv run octoopt2-set-reserve 10        # set reserve to 10%
    uv run octoopt2-set-reserve           # show current reserve, no change
    uv run python scripts/set_reserve.py 10

Run with no argument to read the current value without writing anything.
"""
import asyncio
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, ".")


async def _read_reserve(config) -> tuple[int | None, int | None, int | None]:
    """Return (current SoC %, soc_reserve %, discharge_min_power_reserve %)."""
    from octoopt2.givenergy_modbus_async.client.client import Client

    client = Client(host=config.host, port=config.port)
    await client.connect()
    try:
        await client.refresh_plant(full_refresh=True, number_batteries=config.number_batteries)
        inv = client.plant.inverter
        return (
            int(inv.battery_percent),
            int(inv.battery_soc_reserve),
            int(inv.battery_discharge_min_power_reserve),
        )
    finally:
        await client.close()


def main() -> int:
    from octoopt2.config import GivEnergyConfig
    from octoopt2.control.inverter import set_battery_reserve

    config = GivEnergyConfig.from_env()

    # Show current state first.
    try:
        soc, soc_reserve, power_reserve = asyncio.run(_read_reserve(config))
    except Exception as exc:
        logger.error("Failed to read inverter: %s", exc)
        return 1

    print(f"Current SoC:                 {soc}%")
    print(f"Battery reserve (SoC):       {soc_reserve}%")
    print(f"Discharge min power reserve: {power_reserve}%")

    if len(sys.argv) < 2:
        print("\nNo target given — nothing changed. Pass a percentage to set it, e.g. 10")
        return 0

    try:
        target = int(sys.argv[1])
    except ValueError:
        logger.error("Target must be an integer percentage, got %r", sys.argv[1])
        return 2

    if not 4 <= target <= 100:
        logger.error("Target must be in [4, 100]%%, got %d", target)
        return 2

    if target == soc_reserve == power_reserve:
        print(f"\nReserve already {target}% — nothing to do.")
        return 0

    print(f"\nSetting battery reserve floor to {target}% …")
    try:
        set_battery_reserve(config, target)
    except Exception as exc:
        logger.error("Failed to set reserve: %s", exc)
        return 1

    # Verify the write landed.
    try:
        _, soc_reserve, power_reserve = asyncio.run(_read_reserve(config))
        print(f"Verified — battery reserve: {soc_reserve}%, discharge reserve: {power_reserve}%")
    except Exception as exc:
        logger.warning("Wrote reserve but could not verify: %s", exc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
