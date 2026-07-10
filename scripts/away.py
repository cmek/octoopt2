"""Put the system into a safe unattended ("away") state.

The inverter follows its last-written flash registers autonomously, so once
this state is applied it needs no server or internet: ECO mode self-consumes
solar and never force-imports or force-exports. One command does three things:

  1. Inverter → ECO (dynamic self-consumption)
  2. Battery reserve floor → --reserve % (default 20), written in the SAME
     register batch as ECO so the ECO command's own reserve write can never
     transiently clobber it to 4%
  3. Ecodan DHW → "auto" (heat pump follows its own onboard schedule)

Stop the daemon FIRST — its next optimizer tick would overwrite all of this:

    sudo systemctl disable --now octoopt2-daemon   # disable too, or a reboot resurrects it
    uv run octoopt2-away --reserve 20

The daemon check below probes the local metrics port only; a daemon running on
another host or a non-default port will not be detected.

The optimizer's ``min_soc_pct`` is irrelevant while away — it only shapes the
plan, and the planner isn't running.

Usage:
    uv run octoopt2-away                 # ECO + reserve 20% + DHW auto
    uv run octoopt2-away --reserve 30    # custom reserve floor (4-100)
    uv run octoopt2-away --no-dhw        # skip the MELCloud call
    uv run octoopt2-away --dry-run       # print intended actions, write nothing
    uv run octoopt2-away --force         # proceed even if a daemon is detected

Exit codes: 0 ok, 1 hardware/cloud failure, 2 bad args, 3 daemon detected.
"""
import argparse
import asyncio
import logging
import sys
import urllib.request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, ".")


async def _read_away_state(config) -> dict:
    """Return the inverter registers the away state depends on."""
    from octoopt2.givenergy_modbus_async.client.client import Client

    client = Client(host=config.host, port=config.port)
    await client.connect()
    try:
        await client.refresh_plant(full_refresh=True, number_batteries=config.number_batteries)
        inv = client.plant.inverter
        return {
            "soc": int(inv.battery_percent),
            "eco_mode": int(inv.eco_mode),
            "enable_discharge": bool(inv.enable_discharge),
            "soc_reserve": int(inv.battery_soc_reserve),
            "power_reserve": int(inv.battery_discharge_min_power_reserve),
        }
    finally:
        await client.close()


def _daemon_running(metrics_port: int) -> bool:
    """Best-effort probe for a locally running octoopt2-daemon."""
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{metrics_port}/metrics", timeout=2
        ) as resp:
            return resp.status == 200
    except Exception:
        return False


def main() -> int:
    from octoopt2.config import AppConfig
    from octoopt2.control.ecodan import get_dhw_state, set_dhw
    from octoopt2.control.inverter import set_eco_mode

    parser = argparse.ArgumentParser(
        description="Put inverter in ECO with a reserve floor and DHW in auto for unattended operation"
    )
    parser.add_argument(
        "--reserve", type=int, default=20,
        help="Battery reserve floor %% while away (4-100, default 20)",
    )
    parser.add_argument("--no-dhw", action="store_true", help="Skip the MELCloud DHW call")
    parser.add_argument("--dry-run", action="store_true", help="Print intended actions, write nothing")
    parser.add_argument("--force", action="store_true", help="Proceed even if a daemon is detected")
    args = parser.parse_args()

    if not 4 <= args.reserve <= 100:
        parser.error(f"--reserve must be in [4, 100], got {args.reserve}")

    config = AppConfig.from_env()

    # ── 1. Refuse to fight a running daemon ────────────────────────────────
    if _daemon_running(config.daemon.metrics_port):
        print(
            f"WARNING: octoopt2-daemon appears to be running (port {config.daemon.metrics_port}).\n"
            "Its next 5-minute tick will re-apply the optimizer schedule and OVERWRITE\n"
            "the away state (ECO mode and reserve). Stop it first:\n"
            "    sudo systemctl disable --now octoopt2-daemon\n"
            "(disable, so a reboot doesn't resurrect it)"
        )
        if not args.force:
            print("\nAborting — pass --force to proceed anyway.")
            return 3
        print("\n--force given — proceeding anyway.")

    # ── 2. Before-state ─────────────────────────────────────────────────────
    try:
        before = asyncio.run(_read_away_state(config.givenergy))
    except Exception as exc:
        logger.error("Failed to read inverter: %s", exc)
        return 1

    print(f"Current SoC:                 {before['soc']}%")
    print(f"Eco mode:                    {before['eco_mode']}")
    print(f"Forced discharge enabled:    {before['enable_discharge']}")
    print(f"Battery reserve (SoC):       {before['soc_reserve']}%")
    print(f"Discharge min power reserve: {before['power_reserve']}%")

    if args.dry_run:
        print("\nDry-run — would do:")
        print(f"  1. Inverter → ECO mode with reserve floor {args.reserve}% (both reserve registers)")
        print("  2. DHW → auto" if not args.no_dhw else "  2. DHW: skipped (--no-dhw)")
        return 0

    # ── 3. Inverter → ECO + reserve in one batch ───────────────────────────
    print(f"\nSetting inverter to ECO with reserve floor {args.reserve}% …")
    try:
        set_eco_mode(config.givenergy, config.db_path, reserve_pct=args.reserve)
    except Exception as exc:
        logger.error("Failed to set ECO mode: %s", exc)
        return 1

    # ── 4. Verify ───────────────────────────────────────────────────────────
    try:
        after = asyncio.run(_read_away_state(config.givenergy))
    except Exception as exc:
        logger.error("Commands sent but verification read failed: %s", exc)
        return 1

    checks = [
        ("Eco mode on", after["eco_mode"] == 1),
        ("Forced discharge off", not after["enable_discharge"]),
        (f"SoC reserve {args.reserve}%", after["soc_reserve"] == args.reserve),
        (f"Power reserve {args.reserve}%", after["power_reserve"] == args.reserve),
    ]
    all_ok = True
    for label, ok in checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}")
        all_ok = all_ok and ok
    if not all_ok:
        logger.error("Inverter writes were sent but did not verify — check the GivEnergy app")
        return 1

    # ── 5. DHW → auto ───────────────────────────────────────────────────────
    if not args.no_dhw:
        print("\nSetting DHW to auto …")
        try:
            set_dhw(config.melcloud, enabled=False)
            state = get_dhw_state(config.melcloud)
        except Exception as exc:
            logger.error(
                "Inverter away state IS applied, but the DHW call failed: %s — "
                "set the Ecodan to auto (or holiday mode) manually in MELCloud", exc
            )
            return 1
        mode = state.get("operation_mode")
        if mode != "auto":
            logger.error("DHW mode is %r, expected 'auto' — check MELCloud", mode)
            return 1
        print(f"  PASS  DHW auto (tank {state.get('tank_temperature')}°C)")

    # ── 6. Summary ───────────────────────────────────────────────────────────
    print(
        "\nAway state applied. The inverter now self-consumes solar autonomously —\n"
        "no server or internet needed.\n"
        "Do NOT start octoopt2-daemon while away: its first optimization tick will\n"
        "overwrite this state, and even its shutdown fallback restores ECO with the\n"
        f"default 4% reserve, not your {args.reserve}% floor."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
