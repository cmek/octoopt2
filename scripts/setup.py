"""First-run setup script.

Validates configuration, tests API connectivity, initialises the database,
and seeds all data feeds so the optimizer is ready to run.

Usage:
    uv run python scripts/setup.py

Run this once before starting the cron job. Safe to re-run — all data
fetches are upserts so nothing will be duplicated or overwritten incorrectly.
"""
import os
import sys
import logging
from datetime import date, datetime, timedelta, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)

sys.path.insert(0, ".")

# ── Helpers ────────────────────────────────────────────────────────────────

PASS = "✓"
FAIL = "✗"
WARN = "!"


def _ok(msg: str) -> None:
    print(f"  {PASS}  {msg}")


def _fail(msg: str) -> None:
    print(f"  {FAIL}  {msg}")


def _warn(msg: str) -> None:
    print(f"  {WARN}  {msg}")


def _section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ── Step 1: validate .env ─────────────────────────────────────────────────

REQUIRED_VARS = [
    ("GIVENERGY_HOST",           "192.168.1.x",  "GivEnergy inverter local IP"),
    ("OCTOPUS_API_KEY",          None,           "Octopus API key"),
    ("OCTOPUS_ACCOUNT_NUMBER",   None,           "Octopus account number"),
    ("OCTOPUS_MPAN",             None,           "Electricity meter MPAN"),
    ("OCTOPUS_SERIAL",           None,           "Electricity meter serial"),
    ("OCTOPUS_AGILE_TARIFF_CODE",None,           "Agile buy tariff code"),
    ("OCTOPUS_OUTGOING_TARIFF_CODE", None,       "Agile sell tariff code"),
    ("OCTOPUS_DNO_REGION",       None,           "DNO region letter"),
    ("SOLCAST_API_KEY",          "xxxx",         "Solcast API key"),
    ("SOLCAST_RESOURCE_ID",      "xxxx",         "Solcast rooftop resource ID"),
    ("MELCLOUD_EMAIL",           None,           "MELCloud email"),
    ("MELCLOUD_PASSWORD",        None,           "MELCloud password"),
    ("MELCLOUD_DEVICE_ID",       None,           "MELCloud device ID"),
    ("LATITUDE",                 None,           "Home latitude"),
    ("LONGITUDE",                None,           "Home longitude"),
]


def validate_env() -> bool:
    _section("1. Validating .env configuration")
    errors = []
    warnings = []

    if not os.path.exists(".env"):
        _fail(".env file not found — copy .env.example to .env and fill it in")
        return False

    from dotenv import load_dotenv
    load_dotenv()

    for var, placeholder, description in REQUIRED_VARS:
        val = os.getenv(var)
        if not val:
            errors.append(f"{var} — {description}")
        elif placeholder and val == placeholder:
            errors.append(f"{var} — still set to placeholder '{placeholder}'")
        else:
            _ok(f"{var}")

    for msg in errors:
        _fail(f"Missing / unset: {msg}")

    if errors:
        print(f"\n  {len(errors)} item(s) need to be configured in .env before continuing.")
        return False

    _ok("All required variables are set")
    return True


# ── Step 2: test API connectivity ─────────────────────────────────────────

def test_octopus(config) -> bool:
    _section("2a. Testing Octopus Energy API")
    import requests
    try:
        resp = requests.get(
            "https://api.octopus.energy/v1/products/",
            auth=(config.octopus.api_key, ""),
            timeout=10,
        )
        resp.raise_for_status()
        _ok(f"Octopus API reachable (HTTP {resp.status_code})")

        # Check the tariff codes resolve
        for label, tariff_code in [
            ("buy", config.octopus.agile_tariff_code),
            ("sell", config.octopus.outgoing_tariff_code),
        ]:
            product_code = tariff_code.split("-")[2:-1]
            product_code = "-".join(tariff_code.split("-")[2:-1])
            url = (
                f"https://api.octopus.energy/v1/products/{product_code}"
                f"/electricity-tariffs/{tariff_code}/standard-unit-rates/"
                f"?page_size=1"
            )
            r = requests.get(url, auth=(config.octopus.api_key, ""), timeout=10)
            if r.status_code == 200 and r.json().get("count", 0) > 0:
                _ok(f"Tariff {label} ({tariff_code}) resolves OK")
            else:
                _warn(f"Tariff {label} ({tariff_code}) returned no results — check code is current")
        return True
    except Exception as exc:
        _fail(f"Octopus API error: {exc}")
        return False


def test_solcast(config) -> bool:
    _section("2b. Testing Solcast API")
    import requests
    try:
        url = f"https://api.solcast.com.au/rooftop_sites/{config.solcast.resource_id}/forecasts"
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {config.solcast.api_key}"},
            params={"hours": 1, "format": "json"},
            timeout=15,
        )
        remaining = resp.headers.get("x-rate-limit-remaining", "?")
        if resp.status_code == 200:
            count = len(resp.json().get("forecasts", []))
            _ok(f"Solcast reachable — {count} forecast periods returned")
            _ok(f"Rate limit remaining today: {remaining}")
            return True
        elif resp.status_code == 429:
            _warn(f"Solcast rate limit hit (daily quota exhausted). Try again tomorrow.")
            _warn("Setup will continue but solar forecast will be empty until tomorrow.")
            return False
        else:
            _fail(f"Solcast returned HTTP {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as exc:
        _fail(f"Solcast API error: {exc}")
        return False


def test_open_meteo(config) -> bool:
    _section("2c. Testing Open-Meteo API")
    import requests
    try:
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": config.location.latitude,
                "longitude": config.location.longitude,
                "minutely_15": "temperature_2m",
                "forecast_days": 1,
                "timezone": "UTC",
            },
            timeout=10,
        )
        resp.raise_for_status()
        count = len(resp.json().get("minutely_15", {}).get("time", []))
        _ok(f"Open-Meteo reachable — {count} 15-min slots for today")
        return True
    except Exception as exc:
        _fail(f"Open-Meteo error: {exc}")
        return False


def test_melcloud(config) -> bool:
    _section("2d. Testing MELCloud (Ecodan)")
    import asyncio, aiohttp, pymelcloud
    from pymelcloud import DEVICE_TYPE_ATW

    async def _test():
        async with aiohttp.ClientSession() as session:
            token = await pymelcloud.login(config.melcloud.email, config.melcloud.password, session=session)
            devices = await pymelcloud.get_devices(token, session=session)
            atw = devices.get(DEVICE_TYPE_ATW, [])
            return token, atw

    try:
        token, atw_devices = asyncio.run(_test())
        if not token:
            _fail("MELCloud login returned no token")
            return False
        _ok("MELCloud login successful")
        if not atw_devices:
            _warn("No ATW (air-to-water) devices found in account")
        else:
            for d in atw_devices:
                marker = " ← configured" if d.device_id == config.melcloud.device_id else ""
                _ok(f"  ATW device: id={d.device_id} name={d.name}{marker}")
            ids = [d.device_id for d in atw_devices]
            if config.melcloud.device_id not in ids:
                _fail(f"MELCLOUD_DEVICE_ID={config.melcloud.device_id} not found. Available: {ids}")
                return False
        return True
    except Exception as exc:
        _fail(f"MELCloud error: {exc}")
        return False


def test_inverter(config) -> bool:
    _section("2e. Testing GivEnergy inverter (Modbus TCP)")
    try:
        from octoopt2.data.inverter import read_inverter
        reading = read_inverter(config.givenergy)
        _ok(
            f"Inverter reachable: SoC={reading.soc_pct:.1f}% "
            f"solar={reading.solar_w:.0f}W load={reading.load_w:.0f}W"
        )
        return True
    except Exception as exc:
        _warn(f"Inverter not reachable: {exc}")
        _warn("This is OK if the inverter is not on the network yet.")
        _warn("The optimizer will not run until the inverter is reachable.")
        return False


# ── Step 3: initialise database ───────────────────────────────────────────

def init_database(config) -> None:
    _section("3. Initialising database")
    from octoopt2.db import init_db
    init_db(config.db_path)
    _ok(f"Database ready at {config.db_path}")


# ── Step 4: seed data ─────────────────────────────────────────────────────

def seed_consumption(config, days: int = 30) -> None:
    _section(f"4a. Seeding {days} days of consumption history")
    from octoopt2.data.consumption import preload_consumption, consumption_coverage
    total = preload_consumption(config.octopus, config.db_path, days=days)
    coverage = consumption_coverage(config.db_path, days=days)
    _ok(f"Stored {total} slots — coverage {coverage['coverage_pct']}% ({coverage['stored']}/{coverage['expected']})")
    if coverage["coverage_pct"] < 90:
        _warn("Coverage below 90% — smart meter data may be delayed by a day or two. This is normal.")


def seed_prices(config) -> None:
    _section("4b. Seeding Agile prices (today + tomorrow)")
    from octoopt2.data.octopus import fetch_and_store_prices
    today = datetime.now(timezone.utc).date()
    tomorrow = today + timedelta(days=1)

    for d in [today, tomorrow]:
        try:
            n = fetch_and_store_prices(config.octopus, config.db_path, for_date=d)
            if n > 0:
                _ok(f"{d}: {n} price slots fetched")
            else:
                _warn(f"{d}: no prices returned (tomorrow's prices publish after ~4PM UK time)")
        except Exception as exc:
            _fail(f"{d}: {exc}")


def seed_solar(config) -> None:
    _section("4c. Seeding Solcast forecast + estimated actuals")
    from octoopt2.data.solcast import fetch_and_store_forecast, fetch_and_store_actuals
    try:
        n = fetch_and_store_forecast(config.solcast, config.db_path, hours=48, force=True)
        _ok(f"Solar forecast: {n} slots stored")
    except Exception as exc:
        _fail(f"Solar forecast: {exc}")
    try:
        n = fetch_and_store_actuals(config.solcast, config.db_path, force=True)
        _ok(f"Solar actuals:  {n} slots stored")
    except Exception as exc:
        _fail(f"Solar actuals: {exc}")


def seed_weather(config) -> None:
    _section("4d. Seeding weather forecast + history")
    from octoopt2.data.weather import backfill_weather_history, fetch_and_store_weather
    try:
        n = fetch_and_store_weather(config.location, config.db_path, forecast_days=3, force=True)
        _ok(f"Weather forecast: {n} 15-min slots stored")
    except Exception as exc:
        _fail(f"Weather forecast: {exc}")
    try:
        n = backfill_weather_history(config.location, config.db_path, days=35)
        _ok(f"Weather history:  {n} hourly slots stored (for load model)")
    except Exception as exc:
        _fail(f"Weather history: {exc}")


# ── Step 5: summary ───────────────────────────────────────────────────────

def print_summary(config) -> None:
    _section("5. Data summary")
    from octoopt2.db import get_conn
    from octoopt2.optimizer.forecast import fit_load_model

    with get_conn(config.db_path) as conn:
        for table, label in [
            ("prices",          "Price slots     "),
            ("consumption",     "Consumption slots"),
            ("solar_forecast",  "Solar forecast  "),
            ("solar_actuals",   "Solar actuals   "),
            ("weather_forecast","Weather slots   "),
        ]:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            _ok(f"{label}: {count} rows")

    model = fit_load_model(config.db_path)
    if model:
        _ok(f"Load model fitted: α={model.alpha:.4f} kWh/°C, ref={model.ref_temp:.1f}°C, n={model.n_slots} slots")
    else:
        _warn("Load model not fitted — insufficient consumption history (need ≥7 days with weather overlap)")

    _section("Next steps")
    print("  1. Set up cron job:")
    print("       crontab -e")
    print(f"       */5 * * * * cd {os.getcwd()} && uv run octoopt2 >> /var/log/octoopt2.log 2>&1")
    print()
    print("  2. First manual run to verify:")
    print("       uv run octoopt2")
    print()
    print("  3. Monitor logs:")
    print("       tail -f /var/log/octoopt2.log")


# ── Main ──────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("  octoopt2 — first-run setup")
    print("=" * 60)

    if not validate_env():
        sys.exit(1)

    from octoopt2.config import AppConfig
    config = AppConfig.from_env()

    test_octopus(config)
    test_solcast(config)
    test_open_meteo(config)
    test_melcloud(config)
    test_inverter(config)

    init_database(config)
    seed_consumption(config, days=30)
    seed_prices(config)
    seed_solar(config)
    seed_weather(config)

    print_summary(config)
    print("\n  Setup complete.\n")


if __name__ == "__main__":
    main()
