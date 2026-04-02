"""Load forecasting model.

Fits a temperature-adjusted baseline from historical Octopus consumption data:

    predicted_load[t] = baseline[day_type][slot_index] + α * (temp[t] - ref_temp)

Where:
  - slot_index: 0–47, half-hour slot number within the day in UK local time
  - day_type: 'weekday', 'weekend', or 'holiday'
  - α: single temperature coefficient fitted by OLS across all historical slots
  - ref_temp: mean historical temperature (so baseline represents typical conditions)

The model is cheap to fit (~milliseconds on 30 days of data) so it is re-fitted
on every optimizer run, always incorporating the latest consumption data.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import holidays
import numpy as np

from ..db import get_conn

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
    """Fit the load model from historical consumption and weather data.

    Returns None if there is insufficient data (less than MIN_SLOTS).
    """
    consumption = _load_consumption(db_path)
    if len(consumption) < MIN_SLOTS:
        logger.warning(
            "Only %d consumption slots available (need %d) — load model not fitted",
            len(consumption),
            MIN_SLOTS,
        )
        return None

    weather = _load_weather(db_path)
    logger.info(
        "Fitting load model from %d consumption slots, %d weather slots",
        len(consumption),
        len(weather),
    )

    # Build arrays for regression
    slot_starts = []
    consumptions = []
    temperatures = []

    for slot_start, kwh in consumption.items():
        temp = _nearest_temperature(slot_start, weather)
        if temp is None:
            continue
        slot_starts.append(slot_start)
        consumptions.append(kwh)
        temperatures.append(temp)

    if len(slot_starts) < MIN_SLOTS:
        logger.warning(
            "Only %d slots have matching temperature data — fitting baseline-only model (α=0)",
            len(slot_starts),
        )
        # Fall back to fitting the per-(day_type, slot_index) baseline from all
        # consumption data, without a temperature coefficient.
        slot_starts = list(consumption.keys())
        consumptions = np.array(list(consumption.values()))
        temperatures = np.zeros(len(consumptions))  # unused when alpha=0

    consumptions = np.array(consumptions)
    temperatures = np.array(temperatures)
    ref_temp = float(np.mean(temperatures))

    # Compute per-(day_type, slot_index) baselines using the mean at ref_temp.
    # We fit α using OLS on the residuals after removing group means.
    baseline: dict[str, np.ndarray] = {
        "weekday": np.zeros(48),
        "weekend": np.zeros(48),
        "holiday": np.zeros(48),
    }
    counts: dict[str, np.ndarray] = {k: np.zeros(48) for k in baseline}

    for i, slot_start in enumerate(slot_starts):
        day_type = _day_type(slot_start)
        slot_idx = _slot_index(slot_start)
        baseline[day_type][slot_idx] += consumptions[i]
        counts[day_type][slot_idx] += 1

    # Convert sums to means; leave zero for slots with no data
    for day_type in baseline:
        mask = counts[day_type] > 0
        baseline[day_type][mask] /= counts[day_type][mask]

    # Compute group-mean residuals and fit α via OLS
    residuals = np.zeros(len(slot_starts))
    for i, slot_start in enumerate(slot_starts):
        day_type = _day_type(slot_start)
        slot_idx = _slot_index(slot_start)
        residuals[i] = consumptions[i] - baseline[day_type][slot_idx]

    temp_deviations = temperatures - ref_temp
    denom = np.sum(temp_deviations ** 2)
    alpha = float(np.sum(residuals * temp_deviations) / denom) if denom > 0 else 0.0

    logger.info(
        "Load model fitted: α=%.4f kWh/°C, ref_temp=%.1f°C, n=%d slots",
        alpha,
        ref_temp,
        len(slot_starts),
    )
    return LoadModel(
        baseline=baseline,
        alpha=alpha,
        ref_temp=ref_temp,
        n_slots=len(slot_starts),
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


def _load_consumption(db_path: str) -> dict[datetime, float]:
    """Load all consumption records from DB as slot_start → kWh."""
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT slot_start, consumption_kwh FROM consumption ORDER BY slot_start"
        ).fetchall()
    return {
        datetime.fromisoformat(r["slot_start"]): r["consumption_kwh"]
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
