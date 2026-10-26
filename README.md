# octoopt2

Home energy cost optimizer for a GivEnergy solar + battery system on an Octopus smart tariff (Agile, or a banded tariff such as Cosy) with Agile Outgoing export, plus optional Mitsubishi Ecodan DHW scheduling.

Runs every 5 minutes via cron. Each tick it re-optimizes the remaining half-hourly slots for the day using a Mixed Integer Linear Program (MILP), then applies the current slot's decision to the inverter and heat pump.

---

## What it does

**Battery scheduling** — decides for each 30-minute slot whether to charge from the grid, discharge to meet demand or export, or sit in ECO mode. Decisions are driven by Octopus buy/sell prices (half-hourly Agile or banded Cosy, expanded to half-hours), a solar generation forecast, and a predicted household load.

**DHW scheduling** — schedules the Ecodan heat pump's domestic hot water heating into the cheapest available slots each day, subject to a configurable minimum and maximum number of slots. In a planned slot the Ecodan is put into forced-DHW mode at the normal tank target (`DHW_TARGET_C`, 50 °C). Outside planned slots the tank target is held at `DHW_OFF_TARGET_C` (40 °C, MELCloud's minimum) so the heat pump's own thermostat does not reheat after a shower at whatever price the slot happens to have. Forced mode is one-shot on an Ecodan (it clears itself once the tank reaches target), so the held-low target is what actually keeps DHW off. A comfort guard restores the normal target if the tank falls below `DHW_MIN_TANK_C` (35 °C) in an off slot. `DHW_OFF_MODE=auto` reverts to the legacy behaviour where off slots only clear forced mode and the Ecodan reheats on its own schedule. Slots already executed today count against the daily minimum/maximum, so a mid-day re-plan cannot schedule a fresh full quota.

**Self-correcting** — the optimizer re-runs every 5 minutes with the live battery state of charge, so it continuously adapts to what actually happened (cloud cover, unexpected load, etc.).

---

## Hardware and services

| Component | Details |
|-----------|---------|
| Inverter | GivEnergy (local Modbus TCP, firmware 912+ / battery firmware 3015+) |
| Battery | 9.5 kWh, 93% round-trip efficiency; measured sustained ≈3.4 kW charge / ≈2.8 kW discharge (`BATTERY_MAX_CHARGE_KW` / `BATTERY_MAX_DISCHARGE_KW`) |
| Solar | Roof PV — forecast via Solcast |
| Tariff | Octopus Agile or Cosy import (banded rates are expanded to half-hours) + Agile Outgoing export |
| Heat pump | Mitsubishi Ecodan (DHW only, via MELCloud API) |
| Weather | Open-Meteo (free, no API key) |

---

## How it works

### Data feeds

Every tick refreshes data feeds with TTL-aware caching to avoid unnecessary API calls:

- **Octopus prices** — half-hourly buy/sell rates fetched for today and tomorrow (publishes ~4 PM UK time)
- **Octopus consumption** — smart meter half-hourly net grid import, kept as meter-side ground truth for reconciliation (1–2 day API lag is normal). Not used by the load model, which fits gross household load from `inverter_readings` — see "Load forecasting" below
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

#### Battery reserve floor (minimum SoC)

There are **two** floors, and only one is a hard limit:

- **`min_soc_pct`** (in `BatteryConfig`, default 10%) is a *planning* constraint inside the optimizer's LP. It shapes what the optimizer intends but is never written to the inverter, so it cannot physically stop a discharge.
- The **inverter's hardware reserve** (`BATTERY_SOC_RESERVE` + `BATTERY_DISCHARGE_MIN_POWER_RESERVE`) is the real floor. The inverter refuses to discharge below it in any mode. GivEnergy ships this at **4%**, which is why an unconfigured battery drains to ~5%.

To enforce a higher hard floor, set the hardware reserve with the one-off command:

```bash
uv run octoopt2-set-reserve         # show current SoC + reserve, change nothing
uv run octoopt2-set-reserve 10      # set the hardware reserve floor to 10%
```

The value persists in the inverter's flash until changed again. Keep `min_soc_pct` equal to (or just above) the hardware reserve so the plan and the hardware agree.

> Every time the daemon switches the inverter to ECO mode it writes **both**
> reserve registers to `BATTERY_HW_RESERVE_PCT` (default 10, the same as
> `min_soc_pct`), so plan and hardware agree and a floor left behind by
> `octoopt2-away --reserve 20` is cleared on the first ECO slot. ECO also
> disables the charge schedule: before this, the 00:00–23:59 charge window armed
> by a CHARGE slot stayed active through every following ECO slot, during which
> the inverter neither discharged to the house nor stopped grid-charging
> (measured Sep–Oct 2026: the battery served under 1% of load in those slots).

#### Away mode (leaving the system unattended)

The inverter follows its last-written registers autonomously, so it needs no
server or internet once left in a safe state. Before going away for an extended
period:

```bash
sudo systemctl disable --now octoopt2-daemon   # stop AND disable, so a reboot doesn't resurrect it
uv run octoopt2-away --reserve 20              # inverter → ECO + 20% reserve floor, DHW → auto at normal target
```

The command verifies each register after writing and refuses to run (exit 3) if
it detects a daemon on the local metrics port — a running daemon would overwrite
the away state on its next tick. Flags: `--reserve N` (default 20), `--no-dhw`,
`--dry-run`, `--force`.

To confirm everything is (still) set correctly — before leaving, or later over
SSH — run the read-only check; it writes nothing and prints PASS/FAIL for each
condition (daemon stopped, ECO on, forced discharge off, both reserve registers,
DHW auto at `DHW_TARGET_C`), exiting 0 only when all pass:

```bash
uv run octoopt2-away --check                # expects the default 20% reserve
uv run octoopt2-away --check --reserve 30   # if you applied a different floor
```

In ECO mode the battery self-consumes solar and never force-imports or
force-exports. The Ecodan in `auto` at its normal target follows its own onboard
schedule (any target held low for an off slot is restored); for a long absence
consider also setting MELCloud's holiday mode with your return date.

As a backstop, the daemon applies the same safe state (ECO + DHW released) on every
shutdown — a `systemctl stop`, host reboot, or crash-exit lands the hardware in
ECO rather than a stuck force-charge/force-export window. This fallback restores
the default 4% reserve, not a custom floor — hence the explicit `octoopt2-away`
step above.

### Persistence

All data is stored in a local SQLite database (WAL mode). Tables:

| Table | Contents |
|-------|----------|
| `prices` | Half-hourly buy/sell rates (banded import tariffs expanded) |
| `consumption` | Octopus smart meter half-hourly net grid import |
| `solar_forecast` | Solcast p10/p50/p90 per slot |
| `solar_actuals` | Solcast tuned actuals (retrospective) |
| `weather_forecast` | Open-Meteo 15-min temperature, cloud, wind, humidity, precipitation |
| `inverter_readings` | Point-in-time inverter snapshots (every 5 min) |
| `feed_fetches` | Last successful refresh per data feed (backs `octoopt2_feed_age_seconds`) |
| `schedule` | Latest optimizer plan per slot |
| `actuals` | Measured outcomes per completed slot (aggregated from inverter readings) |

---

## Getting started

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) for dependency management
- GivEnergy inverter reachable on local network (Modbus TCP port 8899)
- Octopus Energy account on Agile or Cosy import (any product with published unit rates) with API access
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
| `OCTOPUS_IMPORT_TARIFF_CODE` | Full import (buy) tariff code, e.g. `E-1R-AGILE-24-10-01-C` or `E-1R-COSY-22-12-08-G`; the product code is derived from it (`OCTOPUS_AGILE_TARIFF_CODE` still works, with a deprecation warning) |
| `OCTOPUS_OUTGOING_TARIFF_CODE` | Full export (sell) tariff code, normally Agile Outgoing |
| `OCTOPUS_DNO_REGION` | DNO region letter (e.g. `C` for London) |
| `SOLCAST_API_KEY` | Solcast API key |
| `SOLCAST_RESOURCE_ID` | Solcast rooftop site resource ID |
| `MELCLOUD_EMAIL` | MELCloud account email |
| `MELCLOUD_PASSWORD` | MELCloud account password |
| `MELCLOUD_DEVICE_ID` | MELCloud device ID (found during setup) |
| `DHW_MIN_SLOTS_PER_DAY` / `DHW_MAX_SLOTS_PER_DAY` | Daily DHW slot quota (default 4 / 8) |
| `DHW_OFF_MODE` | `setpoint` (default: hold tank target low outside planned slots) or `auto` (legacy: Ecodan reheats on its own) |
| `DHW_TARGET_C` | Normal tank target, °C (default 50) — written in planned slots and on release |
| `DHW_OFF_TARGET_C` | Target held in off slots, °C (default 40, MELCloud's minimum) |
| `DHW_MIN_TANK_C` | Comfort guard floor, °C (default 35; 0 disables) — below this in an off slot the normal target is restored |
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

### Daemon (recommended)

Run octoopt2 as a long-lived background process instead of cron. It performs the
same fast (5-min) and slow (30-min) optimizer tasks itself, and additionally polls
the inverter every 30 s to expose near-live statistics as Prometheus metrics:

```bash
uv run octoopt2-daemon
```

The daemon does **not** hold a persistent connection to the inverter. A GivEnergy
inverter accepts only one Modbus TCP connection at a time, so the daemon opens a
short-lived connection only when it polls or applies a command — exactly like the
cron job did — leaving the socket free for other tools (GivTCP, Home Assistant, a
manual `uv run octoopt2 --dry-run`) between polls. A single internal lock keeps the
30 s poller and the 5-min optimizer tick from connecting at the same instant.

> The daemon **replaces** cron. Remove any `*/5 * * * * … uv run octoopt2` crontab
> line before starting it — the two cannot run simultaneously (single connection).

Flags: `--no-dhw` (see below), `--output` (write `web/schedule.json` each tick),
`--dry-run` (optimize and poll, but send no commands), `--metrics-port N`.

Supervise it however you like (systemd, tmux, `nohup`, a container). It logs to
stdout and shuts down cleanly on SIGINT/SIGTERM. On shutdown it applies a safe
fallback — inverter → ECO, DHW → auto at `DHW_TARGET_C` (skipped under `--dry-run`, DHW part
skipped under `--no-dhw`) — so a stop or reboot never strands the inverter in a
force-charge/force-export window. Worst case this takes ~60 s against an
unreachable inverter, so if you run it under systemd keep `TimeoutStopSec` at
75 s or more (the systemd default of 90 s is fine).

#### Prometheus metrics

The daemon serves Prometheus metrics at `http://<host>:9876/metrics` (port set via
`METRICS_PORT`). Exposed gauges include live inverter power flows and SoC
(`octoopt2_load_watts`, `octoopt2_solar_power_watts`, `octoopt2_battery_soc_percent`,
grid import/export, battery charge/discharge), Ecodan tank temperatures and mode
(`octoopt2_dhw_tank_temp_celsius`, `octoopt2_dhw_mode`), the current slot's planned
action and cost (`octoopt2_planned_*`, `octoopt2_planned_slot_cost_gbp`), and data-feed
freshness (`octoopt2_feed_age_seconds`, `octoopt2_feed_coverage_lag_seconds`,
`octoopt2_last_optimization_age_seconds`).

Feed freshness is two separate numbers, because they fail independently:

- **`octoopt2_feed_age_seconds{feed=...}`** — seconds since the feed was last
  successfully refreshed, for every persisted source: `solar`, `weather`,
  `prices`, `consumption`, `inverter`, `dhw`, `standing` (the import tariff's
  standing charge). A fetch skipped because the data is
  still inside its TTL counts as a refresh, so every feed is stamped on each
  ~30-minute slot boundary and one alerting threshold fits all of them. This is
  the number that says a feed has gone quiet — alert above ~2 h.
- **`octoopt2_feed_coverage_lag_seconds{feed=...}`** — how far behind the newest
  data point is. Negative means coverage runs into the future, which is normal
  for forecasts and published prices. `consumption` sits at 1–2 days in healthy
  operation because that is how late Octopus publishes smart-meter data; only
  alert past ~3 days. Not emitted for `inverter`/`dhw`, where the two numbers
  would be identical.

Keeping these apart matters: before the split, a stalled Octopus fetch and normal
upstream publication lag produced an identical rising number.

`octoopt2_reading_age_seconds` and `octoopt2_dhw_reading_age_seconds` are a
different thing again — the age of the daemon's own 30 s / 5-min polls.
`octoopt2_feed_age_seconds{feed="inverter"}` is stamped only by the scheduler's
own live read, so it still catches a scheduler that has been falling back to
stale stored readings while the poller keeps running.

Example scrape config:

```yaml
scrape_configs:
  - job_name: octoopt2
    scrape_interval: 30s
    static_configs:
      - targets: ["192.168.1.x:9876"]
```

The 30 s inverter polls and 5-min DHW polls are also written to SQLite
(`inverter_readings`, `dhw_readings`): they feed the load model, the per-slot
`actuals` (battery flows, SoC endpoints, sample counts) and the backtest.
Prometheus remains the place to chart them; the database keeps them so the
optimizer's history does not depend on a scrape. Every optimizer run is logged
to `optimizer_runs`, every plan revision to `schedule_history`, and every
Solcast / Open-Meteo forecast revision to `solar_forecast_history` /
`weather_forecast_history`, with archive observations in `weather_observed`.

#### Live dashboard

The same HTTP server also hosts a single-page live dashboard at
`http://<host>:9876/` (`web/status.html`). It renders, from the metric data and a
short in-memory history of inverter readings:

- an **energy-flow diagram** (solar / home / battery / grid) with animated spokes
  whose direction follows the real power flow;
- **battery**, **current-cost** (this slot's net cost, live buy/sell price, today's
  and yesterday's totals), **hot-water** (tank temp, target, mode), and **planner**
  (current mode, last-optimization age, planned action) cards;
- **sparklines** for SoC, solar, home load and net grid flow over the last ~2 h;
- the **upcoming buy-price** strip with DHW-planned markers; and
- **data-feed freshness** indicators.

It auto-refreshes every 5 s from `/status.json` (served same-origin, so no
Prometheus query API or CORS setup is needed). The server also serves the existing
`/index.html` (schedule) and `/accuracy.html` pages, so a single port covers
metrics and all three dashboards.

### Cron job (legacy)

If you prefer the stateless cron model instead of the daemon, the `octoopt2` entry
point still works tick-by-tick:

```
*/5 * * * * cd /path/to/octoopt2 && uv run octoopt2 >> /var/log/octoopt2.log 2>&1
```

Do not run the cron job and the daemon at the same time.

### DHW-only mode

To run without active DHW scheduling (Ecodan stays in its own auto mode):

```bash
uv run octoopt2 --no-dhw
```

This still controls the battery normally. On each slot boundary it releases the Ecodan — `auto` mode at `DHW_TARGET_C` — so the heat pump falls back to its internal schedule at its normal target.

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

### Backtest (how good was the plan, and what was possible?)

Replays the measured half-hourly load and solar through several strategies and
tariffs so the controller can be judged against what was physically possible:

```bash
cp octoopt2.db /tmp/bt.db      # always run against a copy
uv run octoopt2-backtest --db /tmp/bt.db --from 2026-09-01 --to 2026-10-12
uv run octoopt2-backtest --db /tmp/bt.db --from 2026-04-02 --to 2026-10-12 \
    --tariffs agile,cosy+agileout,flux,cosy+fixed,flexible+fixed --cycle-cost 0.015 --json out.json
```

Per London day and tariff it reports:

- **actual** — the measured flows priced under the tariff (standing charge shown separately)
- **planned** — the controller's own final plan per slot (DB prices only)
- **plan-exec** — that plan executed perfectly on a dispatch simulator (SoC chained)
- **hindsight** — a perfect-foresight MILP over actual load/solar: the ceiling for this hardware
- **ECO-only** — plain self-consumption with no scheduling; **no-batt** — no battery at all
- **replay** (`--replay`) — the rolling-horizon MILP re-run with the stored forecasts and perfect execution

`actual − hindsight` is split into an execution gap (`actual − plan-exec`) and a
planning gap (`plan-exec − hindsight`). The execution-diagnostics table groups
slots by planned action and the last forced mode, which is how the ECO
charge-window bug (see below) was found. Alternative tariffs are fetched from the
public Octopus products API and cached in the DB (`tariff_rates`); banded tariffs
are expanded in UTC exactly as Octopus publishes them. `--sell-fixed`,
`--cycle-cost`, `--min-soc`, `--max-discharge-kw` and `--terminal` are the
sensitivity knobs. `--output` writes `web/backtests/<from>_<to>.json`.

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
    octopus.py               — import/export prices and consumption from Octopus API
    rates.py                 — rate record parsing and band→half-hour expansion
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
  set_reserve.py             — set the inverter's hardware battery reserve floor
```
