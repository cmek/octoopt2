"""Walk-forward evaluation of the load model.

`schedule.predicted_load_kwh` holds the output of whatever model the live
controller ran at the time, so a new fit cannot be judged from stored
predictions. This module re-fits the model at the start of every London day
using only data before that moment (fit_load_model's `now` cutoff) and
predicts that day's 48 slots, giving a prediction series the backtest can
drop in place of the stored one (`--replay-load model`).

Temperatures come from `weather_observed` by default (an upper bound: the live
controller only had the forecast); `temp_source="forecast"` uses the latest
stored `weather_forecast` rows instead.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

import numpy as np

from ..config import LoadModelConfig
from ..db import get_conn
from ..optimizer.forecast import LONDON, fit_load_model

logger = logging.getLogger(__name__)


def day_slots(day: date) -> list[datetime]:
    """UTC half-hour slot starts of a London day (46/48/50 on DST days)."""
    start = datetime(day.year, day.month, day.day, tzinfo=LONDON)
    end = start + timedelta(days=1)
    t = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    out = []
    while t < end_utc:
        out.append(t)
        t += timedelta(minutes=30)
    return out


def _temperatures(db_path: str, from_utc: datetime, to_utc: datetime, source: str) -> dict[datetime, float]:
    """slot_start → temperature over [from, to), averaged into 30-min buckets."""
    tables = ["weather_observed", "weather_forecast"] if source == "observed" else ["weather_forecast"]
    buckets: dict[datetime, list[float]] = {}
    with get_conn(db_path) as conn:
        for table in tables:
            rows = conn.execute(
                f"""
                SELECT slot_start, temperature_c FROM {table}
                WHERE slot_start >= ? AND slot_start < ? AND temperature_c IS NOT NULL
                """,  # noqa: S608 - table names are a fixed list
                (from_utc.isoformat(), to_utc.isoformat()),
            ).fetchall()
            if rows:
                for r in rows:
                    t = datetime.fromisoformat(r["slot_start"]).astimezone(timezone.utc)
                    b = t.replace(minute=(t.minute // 30) * 30, second=0, microsecond=0)
                    buckets.setdefault(b, []).append(float(r["temperature_c"]))
                break
    return {b: float(np.mean(v)) for b, v in buckets.items()}


def walk_forward_predictions(
    db_path: str,
    days: list[date],
    cfg: LoadModelConfig,
    temp_source: str = "observed",
) -> dict[datetime, float]:
    """Re-fit at each London midnight and predict that day. Returns
    slot_start (UTC) → predicted kWh; days whose fit fails are omitted."""
    out: dict[datetime, float] = {}
    for day in sorted(days):
        slots = day_slots(day)
        now = slots[0]
        model = fit_load_model(db_path, cfg, now=now)
        if model is None:
            logger.warning("Walk-forward %s: load model not fitted (insufficient history)", day)
            continue
        temps = _temperatures(db_path, slots[0], slots[-1] + timedelta(minutes=30), temp_source)
        for s, v in zip(slots, model.predict(slots, temps)):
            out[s] = v
    return out
