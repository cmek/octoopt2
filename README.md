# octoopt2

Home energy cost optimizer for a GivEnergy solar + battery system on Octopus Agile, with optional Mitsubishi Ecodan DHW scheduling.

Runs every 5 minutes via cron. Each tick it re-optimizes the remaining half-hourly slots for the day using a Mixed Integer Linear Program (MILP), then applies the current slot's decision to the inverter and heat pump.

---

## What it does

**Battery scheduling** — decides for each 30-minute slot whether to charge from the grid, discharge to meet demand or export, or sit in ECO mode. Decisions are driven by Octopus Agile buy/sell prices, a solar generation forecast, and a predicted household load.

**DHW scheduling** — schedules the Ecodan heat pump's domestic hot water heating into the cheapest available slots each day, subject to a configurable minimum and maximum number of slots. The heat pump's own thermostat still governs when it actually heats; the optimizer just picks the cheapest windows to allow it.

**Self-correcting** — the optimizer re-runs every 5 minutes with the live battery state of charge, so it continuously adapts to what actually happened (cloud cover, unexpected load, etc.).

---

## Hardware and services

| Component | Details |
|-----------|---------|
| Inverter | GivEnergy (local Modbus TCP, firmware 912+ / battery firmware 3015+) |
| Battery | 9.5 kWh, 93% round-trip efficiency |
| Solar | Roof PV — forecast via Solcast |
| Tariff | Octopus Agile (half-hourly buy and sell prices) |
| Heat pump | Mitsubishi Ecodan (DHW only, via MELCloud API) |
| Weather | Open-Meteo (free, no API key) |

---

## How it works

### Data feeds

Every tick refreshes data feeds with TTL-aware caching to avoid unnecessary API calls:

- **Octopus prices** — half-hourly buy/sell rates fetched for today and tomorrow (publishes ~4 PM UK time)
- **Octopus consumption** — smart meter half-hourly consumption, used to train the load model (1–2 day API lag is normal)
- **Solcast** — solar generation forecast (p10/p50/p90), 2-hour TTL, 10 calls/day limit respected
- **Open-Meteo** — 15-minute temperature forecast, 1-hour TTL; historical hourly data backfilled for load model training
- **GivEnergy inverter** — live SoC, solar, grid import/export, battery charge/discharge, home load polled each tick

### Load forecasting

A temperature-adjusted baseline model is fitted from historical consumption:

```
predicted_load[t] = baseline[day_type][slot_index] + α × (temp[t] − ref_temp)
```

- `day_type` is one of `weekday`, `weekend`, `holiday` (UK England public holidays)
- `slot_index` is 0–47, the half-hour position within the UK local day
- `α` is an OLS-fitted temperature coefficient (kWh per °C)
- Falls back to a baseline-only model (α = 0) when historical weather coverage is insufficient
- A minimum floor of 0.2 kWh/slot is applied to account for always-on base load

### Optimization

The MILP is solved with PuLP/CBC for all remaining slots in the current pricing window (up to ~48 slots). Decision variables per slot:

- `grid_import`, `grid_export` — energy from/to grid (kWh)
- `bat_charge`, `bat_discharge` — battery charge/discharge (kWh, AC-side)
- `soc` — battery state of charge at each slot boundary
- `dhw_on` — binary: DHW heating active this slot

Constraints enforce AC power balance, SoC bounds, no simultaneous charge+discharge, no simultaneous import+export, and daily DHW minimum/maximum slot counts.

The objective is to minimise `Σ (buy_price × import − sell_price × export)` across all slots.

### Control

After each optimization run the current slot's decision is translated into inverter register writes via local Modbus TCP and an MELCloud API call:

| Inverter state | Registers written |
|---------------|------------------|
| CHARGE | charge slot 1 set to 00:00–23:59, charge limit set, charge enabled |
| DISCHARGE/DEMAND | discharge limit set, match-demand mode, discharge enabled |
| DISCHARGE/EXPORT | discharge limit set, max-power mode, discharge enabled |
| ECO | dynamic mode, discharge disabled |

Charge/discharge power is mapped to the inverter's 1–50 register scale proportional to the battery's configured max rate.

### Persistence

All data is stored in a local SQLite database (WAL mode). Tables:

| Table | Contents |
|-------|----------|
| `prices` | Half-hourly Agile buy/sell rates |
| `consumption` | Octopus smart meter half-hourly consumption |
| `solar_forecast` | Solcast p10/p50/p90 per slot |
| `solar_actuals` | Solcast tuned actuals (retrospective) |
| `weather_forecast` | Open-Meteo 15-min temperature, cloud, wind, humidity, precipitation |
| `inverter_readings` | Point-in-time inverter snapshots (every 5 min) |
| `schedule` | Latest optimizer plan per slot |
| `actuals` | Measured outcomes per completed slot (aggregated from inverter readings) |

---

## Getting started

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) for dependency management
- GivEnergy inverter reachable on local network (Modbus TCP port 8899)
- Octopus Energy account on Agile tariff with API access
- Solcast account with a rooftop site configured
- MELCloud account (only required if using DHW scheduling)

### Install

```bash
git clone <repo>
cd octoopt2
uv sync
```

### Configure

```bash
cp .env.example .env
# Edit .env and fill in all values
```

Required environment variables:

| Variable | Description |
|----------|-------------|
| `GIVENERGY_HOST` | Inverter local IP address |
| `OCTOPUS_API_KEY` | Octopus Energy API key |
| `OCTOPUS_ACCOUNT_NUMBER` | Octopus account number |
| `OCTOPUS_MPAN` | Electricity meter MPAN |
| `OCTOPUS_SERIAL` | Electricity meter serial number |
| `OCTOPUS_AGILE_TARIFF_CODE` | Full Agile buy tariff code (e.g. `E-1R-AGILE-24-10-01-C`) |
| `OCTOPUS_OUTGOING_TARIFF_CODE` | Full Agile export tariff code |
| `OCTOPUS_DNO_REGION` | DNO region letter (e.g. `C` for London) |
| `SOLCAST_API_KEY` | Solcast API key |
| `SOLCAST_RESOURCE_ID` | Solcast rooftop site resource ID |
| `MELCLOUD_EMAIL` | MELCloud account email |
| `MELCLOUD_PASSWORD` | MELCloud account password |
| `MELCLOUD_DEVICE_ID` | MELCloud device ID (found during setup) |
| `LATITUDE` | Home latitude |
| `LONGITUDE` | Home longitude |

### First run

```bash
# Validates config, tests all APIs, seeds database with 30 days of history
uv run python scripts/setup.py

# Preview what the optimizer would do without touching any hardware
uv run octoopt2 --dry-run

# Live run
uv run octoopt2
```

### Cron job

```bash
crontab -e
```

Add:

```
*/5 * * * * cd /path/to/octoopt2 && uv run octoopt2 >> /var/log/octoopt2.log 2>&1
```

### DHW-only mode

To run without active DHW scheduling (Ecodan stays in its own auto mode):

```bash
uv run octoopt2 --no-dhw
```

This still controls the battery normally. On each tick it sends an `auto` command to MELCloud so the heat pump falls back to its internal schedule.

---

## Reports

### Daily report

Shows yesterday's actual energy flows and cost, today's optimizer plan, cheapest appliance windows, and DHW schedule:

```bash
uv run octoopt2-report
```

### Accuracy report

Compares forecast accuracy and planned vs actual costs over a rolling window:

```bash
uv run octoopt2-accuracy-report           # last 14 days
uv run octoopt2-accuracy-report --days 30
```

Sections:

- **Solar forecast accuracy** — Solcast p50 vs inverter-measured actual per day, with MAE and bias (positive bias = Solcast over-predicts)
- **Load forecast accuracy** — optimizer load prediction vs inverter-measured actual per day
- **Cost outcomes** — planned cost from optimizer schedule vs actual cost from inverter readings, with per-day and cumulative totals

### Preload historical consumption

If you want to seed more than 30 days of consumption history for the load model:

```bash
uv run octoopt2-preload-consumption --days 90
```

---

## Project structure

```
octoopt2/
  config.py                  — all configuration as frozen dataclasses
  db.py                      — SQLite schema and connection helpers
  scheduler.py               — main optimization loop (called every 5 min)
  main.py                    — CLI entry point
  data/
    octopus.py               — Agile prices and consumption from Octopus API
    solcast.py               — solar forecast and actuals from Solcast
    weather.py               — weather forecast + historical backfill from Open-Meteo
    inverter.py              — live inverter readings via Modbus TCP
    consumption.py           — smart meter consumption fetch and storage
  optimizer/
    model.py                 — MILP formulation (PuLP/CBC)
    forecast.py              — temperature-adjusted load model
    schedule.py              — schedule persistence and retrieval
  control/
    inverter.py              — translate slot decisions into inverter register writes
    ecodan.py                — MELCloud DHW control (force_hot_water / auto)
  givenergy_modbus_async/    — vendored async Modbus client from GivTCP
scripts/
  setup.py                   — first-run setup and data seeding
  report.py                  — daily performance report
  accuracy_report.py         — forecast accuracy and cost outcomes report
  preload_consumption.py     — bulk-load historical consumption
```
