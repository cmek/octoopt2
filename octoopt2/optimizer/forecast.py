"""Load forecasting model.

Fits a temperature-adjusted, DHW-adjusted baseline from historical *gross*
household load:

    predicted_load[t] = baseline[day_type][slot_index] + α * (temp[t] - ref_temp)

The baseline represents NON-DHW gross load. The fit is a single least-squares
regression of measured gross load on:
  - one intercept per (day_type, slot_index) group  → baseline[day_type][slot]
  - α: temperature coefficient (kWh per °C)
  - β: DHW coefficient (kWh per slot the optimizer enabled DHW) → dhw_kwh_per_slot

Where:
  - slot_index: 0–47, half-hour slot number within the day in UK local time
  - day_type: 'weekday', 'weekend', or 'holiday'
  - ref_temp: mean historical temperature (so baseline represents typical conditions)

Gross load is taken from inverter_readings.load_w (p_load_demand) — the real
household consumption measured behind the grid CT, which includes the Ecodan
DHW load. We deliberately do NOT use the Octopus consumption (net grid import):
it nets out solar and battery, so it is a poor — nearly inverted — proxy for
gross load (inflated at night by battery grid-charging, ~0 midday under solar).

The β·dhw_on regressor pulls the DHW load out of the baseline, so the baseline
is the non-DHW load the optimizer needs, and β is the empirical average DHW
energy actually drawn per enabled slot (typically << the nameplate 1.5 kW
because the tank is often already at target when DHW is commanded).

The model is cheap to fit (~tens of ms) so it is re-fitted on every optimizer run.
"""
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import holidays
import numpy as np

from ..db import get_conn

# Each 30-min slot is 0.5 h, so kWh = mean_power_W / 1000 * 0.5.
_SLOT_HOURS = 0.5

logger = logging.getLogger(__name__)

LONDON = ZoneInfo("Europe/London")
UK_HOLIDAYS = holidays.country_holidays("GB", subdiv="ENG")

# Minimum historical slots needed before the model is considered reliable.
# 48 slots/day × 7 days = one week minimum.
MIN_SLOTS = 48 * 7

# Floor applied to every slot prediction — represents the always-on base load
# (router, standby devices, fridge, etc.).  300 W × 0.5 h = 0.150 kWh.
MIN_LOAD_KWH = 0.200


@dataclass
class LoadModel:
    """Fitted load model. Call predict() to get consumption estimates."""

    # baseline[day_type][slot_index] → mean kWh for that slot under ref_temp
    baseline: dict[str, np.ndarray]
    # OLS temperature coefficient (kWh per °C)
    alpha: float
    # Reference temperature used during fitting (°C)
    ref_temp: float
    # How many historical slots were used to fit the model
    n_slots: int
    # Empirical average DHW energy drawn per enabled slot (kWh). This is the
    # fitted β; the optimizer should use it as dhw_kwh_per_slot instead of the
    # nameplate config value (see optimizer.model.optimize).
    dhw_kwh_per_slot: float = 0.0
    fitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def predict(
        self,
        slot_starts: list[datetime],
        temperatures: dict[datetime, float],
    ) -> list[float]:
        """Predict consumption (kWh) for each slot.

        slot_starts: list of UTC datetimes for each 30-min slot.
        temperatures: mapping of UTC datetime → temperature (°C) at 30-min
                      resolution (or finer — nearest slot is used).
        Returns a list of predicted kWh values, same length as slot_starts.
        """
        predictions = []
        for slot_start in slot_starts:
            day_type = _day_type(slot_start)
            slot_idx = _slot_index(slot_start)
            baseline_kwh = self.baseline[day_type][slot_idx]

            temp = _nearest_temperature(slot_start, temperatures)
            temp_adjustment = self.alpha * (temp - self.ref_temp) if temp is not None else 0.0

            predictions.append(max(MIN_LOAD_KWH, baseline_kwh + temp_adjustment))
        return predictions


def fit_load_model(db_path: str) -> LoadModel | None:
    """Fit the load model from historical gross load, weather, and DHW schedule.

    Jointly fits, by least squares:
      gross_load[t] = baseline[day_type, slot_index]
                      + α·(temp[t] - ref_temp)
                      + β·dhw_on[t]
    so `baseline` is the non-DHW load and β is the empirical DHW draw per
    enabled slot. Returns None if there is insufficient data (< MIN_SLOTS).
    """
    gross = _load_gross_load(db_path)
    if len(gross) < MIN_SLOTS:
        logger.warning(
            "Only %d gross-load slots available (need %d) — load model not fitted",
            len(gross),
            MIN_SLOTS,
        )
        return None

    weather = _load_weather(db_path)
    dhw_sched = _load_dhw_schedule(db_path)
    logger.info(
        "Fitting load model from %d gross-load slots, %d weather slots",
        len(gross),
        len(weather),
    )

    slot_starts = list(gross.keys())
    loads = np.array([gross[s] for s in slot_starts])
    temps = [_nearest_temperature(s, weather) for s in slot_starts]
    dhw_on = np.array([float(dhw_sched.get(s, 0)) for s in slot_starts])

    # Use temperature only if enough slots have a matching reading; otherwise
    # fall back to a baseline+DHW model with α=0.
    have_temp = np.array([t is not None for t in temps])
    use_temp = int(have_temp.sum()) >= MIN_SLOTS
    fit_mask = have_temp if use_temp else np.ones(len(slot_starts), dtype=bool)
    if not use_temp:
        logger.warning(
            "Only %d slots have matching temperature data — fitting baseline+DHW model (α=0)",
            int(have_temp.sum()),
        )

    fit_idx = np.flatnonzero(fit_mask)
    temp_vals = np.array([temps[i] if temps[i] is not None else 0.0 for i in fit_idx])
    ref_temp = float(temp_vals.mean()) if use_temp else 0.0

    # One design column per (day_type, slot_index) group present in the data,
    # plus (optionally) a temperature column and a DHW column.
    group_of = [(_day_type(slot_starts[i]), _slot_index(slot_starts[i])) for i in fit_idx]
    groups = {g: j for j, g in enumerate(sorted(set(group_of)))}
    p = len(groups)
    n_extra = (1 if use_temp else 0) + 1  # temp? + dhw
    X = np.zeros((len(fit_idx), p + n_extra))
    for row, (i, g) in enumerate(zip(fit_idx, group_of)):
        X[row, groups[g]] = 1.0
        col = p
        if use_temp:
            X[row, col] = temp_vals[row] - ref_temp
            col += 1
        X[row, col] = dhw_on[i]

    coef, *_ = np.linalg.lstsq(X, loads[fit_idx], rcond=None)
    alpha = float(coef[p]) if use_temp else 0.0
    beta = float(coef[p + (1 if use_temp else 0)])
    # β is an energy and feeds the optimizer — clamp away negative noise.
    beta = max(0.0, beta)

    baseline: dict[str, np.ndarray] = {
        "weekday": np.zeros(48),
        "weekend": np.zeros(48),
        "holiday": np.zeros(48),
    }
    for (day_type, slot_idx), j in groups.items():
        baseline[day_type][slot_idx] = coef[j]

    logger.info(
        "Load model fitted: α=%.4f kWh/°C, β(DHW)=%.3f kWh/slot, ref_temp=%.1f°C, n=%d slots",
        alpha,
        beta,
        ref_temp,
        len(fit_idx),
    )
    return LoadModel(
        baseline=baseline,
        alpha=alpha,
        ref_temp=ref_temp,
        n_slots=int(len(fit_idx)),
        dhw_kwh_per_slot=beta,
    )


def get_temperature_per_slot(
    db_path: str,
    slot_starts: list[datetime],
) -> dict[datetime, float]:
    """Return a temperature mapping for the given 30-min slots.

    Fetches weather at 15-min resolution and averages the two sub-slots
    that fall within each 30-min window.
    """
    if not slot_starts:
        return {}

    from_dt = min(slot_starts)
    to_dt = max(slot_starts) + timedelta(minutes=30)

    from_str = from_dt.astimezone(timezone.utc).isoformat()
    to_str = to_dt.astimezone(timezone.utc).isoformat()

    with get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT slot_start, temperature_c
            FROM weather_forecast
            WHERE slot_start >= ? AND slot_start < ?
              AND temperature_c IS NOT NULL
            ORDER BY slot_start
            """,
            (from_str, to_str),
        ).fetchall()

    # Average 15-min temperatures into 30-min buckets
    buckets: dict[datetime, list[float]] = {}
    for row in rows:
        t = datetime.fromisoformat(row["slot_start"])
        # Round down to the nearest 30-min boundary
        bucket = t.replace(minute=(t.minute // 30) * 30, second=0, microsecond=0)
        buckets.setdefault(bucket, []).append(row["temperature_c"])

    return {bucket: float(np.mean(temps)) for bucket, temps in buckets.items()}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _day_type(slot_start: datetime) -> str:
    """Return 'holiday', 'weekend', or 'weekday' for a UTC datetime."""
    local = slot_start.astimezone(LONDON)
    d = local.date()
    if d in UK_HOLIDAYS:
        return "holiday"
    if local.weekday() >= 5:  # Saturday=5, Sunday=6
        return "weekend"
    return "weekday"


def _slot_index(slot_start: datetime) -> int:
    """Return 0–47 slot index based on UK local time (00:00=0, 23:30=47)."""
    local = slot_start.astimezone(LONDON)
    return (local.hour * 60 + local.minute) // 30


def _nearest_temperature(
    slot_start: datetime,
    weather: dict[datetime, float],
) -> float | None:
    """Find the nearest temperature reading within ±30 minutes of slot_start."""
    if not weather:
        return None
    slot_utc = slot_start.astimezone(timezone.utc)
    best = min(weather.keys(), key=lambda t: abs((t - slot_utc).total_seconds()))
    if abs((best - slot_utc).total_seconds()) <= 1800:
        return weather[best]
    return None


def _load_gross_load(db_path: str) -> dict[datetime, float]:
    """Aggregate inverter_readings.load_w into gross load (kWh) per 30-min slot.

    load_w is p_load_demand — gross household consumption behind the grid CT,
    including the Ecodan DHW load. Readings arrive every ~5 min (irregular), so
    each slot's mean power is converted to energy: mean_W / 1000 * 0.5 h.
    """
    buckets: dict[datetime, list[float]] = defaultdict(list)
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT recorded_at, load_w FROM inverter_readings"
        ).fetchall()
    for r in rows:
        dt = datetime.fromisoformat(r["recorded_at"]).astimezone(timezone.utc)
        slot = dt.replace(minute=(dt.minute // 30) * 30, second=0, microsecond=0)
        buckets[slot].append(r["load_w"])
    return {
        slot: float(np.mean(ws)) / 1000.0 * _SLOT_HOURS
        for slot, ws in buckets.items()
        if ws
    }


def _load_dhw_schedule(db_path: str) -> dict[datetime, int]:
    """Load historical DHW enable flags as slot_start → 0/1 from the schedule.

    This is the *planned* dhw_on (what the optimizer enabled), used as the DHW
    regressor. It is not confirmed actuation — see dhw_readings for ground-truth
    tank state logged each slot, which a future refit can switch to.
    """
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT slot_start, dhw_on FROM schedule"
        ).fetchall()
    return {
        datetime.fromisoformat(r["slot_start"]).astimezone(timezone.utc): int(r["dhw_on"])
        for r in rows
    }


def _load_weather(db_path: str) -> dict[datetime, float]:
    """Load all weather temperature records as slot_start → temperature_c."""
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT slot_start, temperature_c FROM weather_forecast
            WHERE temperature_c IS NOT NULL
            ORDER BY slot_start
            """
        ).fetchall()
    return {
        datetime.fromisoformat(r["slot_start"]): r["temperature_c"]
        for r in rows
    }
