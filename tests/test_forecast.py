"""Load-model tests: recency weighting, heating-degree term, `now` cutoff,
day-type fallback, and the walk-forward evaluation helper."""
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from octoopt2.backtest.loadmodel import day_slots, walk_forward_predictions
from octoopt2.config import LoadModelConfig
from octoopt2.db import get_conn, init_db
from octoopt2.optimizer.forecast import MIN_LOAD_KWH, fit_load_model

LONDON = ZoneInfo("Europe/London")
# 56 days ending here contain no English bank holiday (1 Sep – 26 Oct 2026).
NOW = datetime(2026, 10, 26, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def db_path(tmp_path) -> str:
    path = str(tmp_path / "test.db")
    init_db(path)
    return path


def _seed(db_path: str, days: int, load_fn, temp_fn=None, end: datetime = NOW) -> None:
    """One inverter reading per half-hour slot for `days` days ending at `end`,
    load_w derived from load_fn(slot_start, temp); optional hourly weather."""
    start = end - timedelta(days=days)
    rows, weather = [], []
    t = start
    while t < end:
        temp = temp_fn(t) if temp_fn else None
        kwh = load_fn(t, temp)
        rows.append((t.isoformat(), 50.0, 0.0, 0.0, 0.0, 0.0, 0.0, kwh / 0.5 * 1000.0))
        if temp_fn and t.minute == 0:
            weather.append((t.isoformat(), temp, end.isoformat()))
        t += timedelta(minutes=30)
    with get_conn(db_path) as conn:
        conn.executemany(
            "INSERT INTO inverter_readings (recorded_at, soc_pct, solar_w, grid_import_w, grid_export_w, "
            "battery_charge_w, battery_discharge_w, load_w) VALUES (?,?,?,?,?,?,?,?)",
            rows,
        )
        if weather:
            conn.executemany(
                "INSERT INTO weather_observed (slot_start, temperature_c, fetched_at) VALUES (?,?,?)",
                weather,
            )


def _predict(model, when: datetime, temp: float | None = None) -> float:
    temps = {when: temp} if temp is not None else {}
    return model.predict([when], temps)[0]


def test_recency_weights_track_level_shift(db_path):
    # 46 days at 0.4 kWh/slot then 10 days at 0.8 (no weather → α=0 path).
    shift = NOW - timedelta(days=10)
    _seed(db_path, 56, lambda t, _: 0.8 if t >= shift else 0.4)
    when = NOW + timedelta(hours=12)
    weighted = fit_load_model(db_path, LoadModelConfig(half_life_days=10.0, hdd_base_c=None), now=NOW)
    flat = fit_load_model(db_path, LoadModelConfig(half_life_days=None, hdd_base_c=None), now=NOW)
    assert flat.half_life_days is None and flat.n_eff == pytest.approx(flat.n_slots)
    assert _predict(flat, when) == pytest.approx(0.47, abs=0.03)
    # Half-life 10 d: the last 10 days carry ~45 % of the weight → ~0.58.
    assert _predict(weighted, when) > _predict(flat, when) + 0.08
    assert weighted.n_eff < weighted.n_slots
    sharper = fit_load_model(db_path, LoadModelConfig(half_life_days=5.0, hdd_base_c=None), now=NOW)
    assert _predict(sharper, when) > _predict(weighted, when)


def _daily_temp(t: datetime) -> float:
    # 5 °C … 25 °C sawtooth over a 10-day cycle, so both sides of the base appear.
    return 5.0 + 20.0 * ((t - NOW).total_seconds() / 86400.0 % 10) / 10.0


def test_hdd_adds_load_below_base_only(db_path):
    base = 15.5
    _seed(db_path, 56, lambda t, temp: 0.4 + 0.05 * max(0.0, base - temp), _daily_temp)
    model = fit_load_model(db_path, LoadModelConfig(half_life_days=None, hdd_base_c=base), now=NOW)
    assert model.hdd_base_c == base
    assert model.alpha == pytest.approx(0.05, abs=0.003)
    when = NOW + timedelta(hours=12)
    assert _predict(model, when, 20.0) == pytest.approx(0.4, abs=0.02)
    assert _predict(model, when, 10.0) == pytest.approx(0.4 + 0.05 * 5.5, abs=0.02)


def test_hdd_none_matches_legacy_linear(db_path):
    _seed(db_path, 56, lambda t, temp: 0.6 - 0.02 * (temp - 15.0), _daily_temp)
    model = fit_load_model(db_path, LoadModelConfig(half_life_days=None, hdd_base_c=None), now=NOW)
    assert model.hdd_base_c is None
    assert model.alpha == pytest.approx(-0.02, abs=0.002)
    when = NOW + timedelta(hours=12)
    assert _predict(model, when, 5.0) == pytest.approx(0.8, abs=0.02)
    assert _predict(model, when, 25.0) == pytest.approx(0.4, abs=0.02)


def test_fit_respects_now_cutoff(db_path):
    cutoff = NOW - timedelta(days=5)
    # Rows after the cutoff carry an absurd load that must not leak into a fit at `cutoff`.
    _seed(db_path, 56, lambda t, _: 5.0 if t >= cutoff else 0.4)
    model = fit_load_model(db_path, LoadModelConfig(half_life_days=None, hdd_base_c=None), now=cutoff)
    assert model.n_slots == 51 * 48
    assert _predict(model, cutoff + timedelta(hours=12)) == pytest.approx(0.4, abs=0.01)


def test_holiday_group_falls_back_to_weekend(db_path):
    def load(t, _):
        return 0.9 if t.astimezone(LONDON).weekday() >= 5 else 0.3
    _seed(db_path, 56, load)
    model = fit_load_model(db_path, LoadModelConfig(hdd_base_c=None), now=NOW)
    christmas = datetime(2026, 12, 25, 12, 0, tzinfo=LONDON)
    assert _predict(model, christmas) == pytest.approx(0.9, abs=0.02)
    assert _predict(model, datetime(2026, 12, 23, 12, 0, tzinfo=LONDON)) == pytest.approx(0.3, abs=0.02)
    assert _predict(model, christmas) > MIN_LOAD_KWH


def test_default_config_is_weighted_hdd():
    cfg = LoadModelConfig()
    assert cfg.half_life_days and cfg.hdd_base_c is not None and cfg.train_days == 56


def test_env_parsing(monkeypatch):
    monkeypatch.setenv("LOAD_MODEL_DAYS", "21")
    monkeypatch.setenv("LOAD_MODEL_HALF_LIFE_DAYS", "0")
    monkeypatch.setenv("LOAD_MODEL_HDD_BASE_C", "")
    cfg = LoadModelConfig.from_env()
    assert cfg == LoadModelConfig(train_days=21, half_life_days=None, hdd_base_c=None)


def test_day_slots_dst():
    assert len(day_slots(date(2026, 10, 25))) == 50   # clocks go back
    assert len(day_slots(date(2026, 3, 29))) == 46    # clocks go forward
    assert len(day_slots(date(2026, 10, 20))) == 48


def test_walk_forward_predictions_cover_days(db_path):
    _seed(db_path, 20, lambda t, temp: 0.4 + 0.05 * max(0.0, 15.5 - temp), _daily_temp)
    days = [date(2026, 10, 23), date(2026, 10, 24), date(2026, 10, 25)]
    preds = walk_forward_predictions(db_path, days, LoadModelConfig(half_life_days=None))
    expected = [s for d in days for s in day_slots(d)]
    assert set(preds) == set(expected)
    assert len(preds) == 48 + 48 + 50
    assert all(v >= MIN_LOAD_KWH for v in preds.values())
    # Too little history → day omitted, not an error.
    early = walk_forward_predictions(db_path, [date(2026, 10, 7)], LoadModelConfig())
    assert early == {}
