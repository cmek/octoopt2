"""Persist and retrieve the optimizer schedule from SQLite."""
import logging
from datetime import datetime, timezone

from ..db import get_conn
from .model import OptimizerInput, OptimizerResult, SlotDecision

logger = logging.getLogger(__name__)


def save_schedule(
    result: OptimizerResult,
    inputs: OptimizerInput,
    db_path: str,
) -> None:
    """Upsert all slot decisions into the schedule table."""
    optimized_at = result.optimized_at.isoformat()
    rows = []
    for t, decision in enumerate(result.decisions):
        rows.append((
            decision.slot_start.astimezone(timezone.utc).isoformat(),
            decision.battery_charge_kwh,
            decision.battery_discharge_kwh,
            decision.grid_import_kwh,
            decision.grid_export_kwh,
            int(decision.dhw_on),
            inputs.load_forecast[t],
            inputs.solar_forecast[t],
            inputs.buy_prices[t],
            inputs.sell_prices[t],
            optimized_at,
        ))

    with get_conn(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO schedule (
                slot_start, battery_charge_kwh, battery_discharge_kwh,
                grid_import_kwh, grid_export_kwh, dhw_on,
                predicted_load_kwh, predicted_solar_kwh,
                buy_gbp_kwh, sell_gbp_kwh, optimized_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(slot_start) DO UPDATE SET
                battery_charge_kwh    = excluded.battery_charge_kwh,
                battery_discharge_kwh = excluded.battery_discharge_kwh,
                grid_import_kwh       = excluded.grid_import_kwh,
                grid_export_kwh       = excluded.grid_export_kwh,
                dhw_on                = excluded.dhw_on,
                predicted_load_kwh    = excluded.predicted_load_kwh,
                predicted_solar_kwh   = excluded.predicted_solar_kwh,
                buy_gbp_kwh           = excluded.buy_gbp_kwh,
                sell_gbp_kwh          = excluded.sell_gbp_kwh,
                optimized_at          = excluded.optimized_at
            """,
            rows,
        )
    logger.info(
        "Saved %d slot decisions (cost=£%.4f, status=%s)",
        len(rows),
        result.total_cost_gbp,
        result.solver_status,
    )


def get_current_decision(db_path: str, now: datetime | None = None) -> SlotDecision | None:
    """Return the scheduled decision for the current 30-min slot, or None."""
    if now is None:
        now = datetime.now(timezone.utc)

    # Round down to the current 30-min slot boundary
    slot_start = now.replace(
        minute=(now.minute // 30) * 30,
        second=0,
        microsecond=0,
    )

    with get_conn(db_path) as conn:
        row = conn.execute(
            """
            SELECT slot_start, battery_charge_kwh, battery_discharge_kwh,
                   grid_import_kwh, grid_export_kwh, dhw_on,
                   predicted_load_kwh, predicted_solar_kwh,
                   buy_gbp_kwh, sell_gbp_kwh
            FROM schedule
            WHERE slot_start = ?
            """,
            (slot_start.isoformat(),),
        ).fetchone()

    if row is None:
        return None

    cost = row["buy_gbp_kwh"] * row["grid_import_kwh"] - row["sell_gbp_kwh"] * row["grid_export_kwh"]
    return SlotDecision(
        slot_start=datetime.fromisoformat(row["slot_start"]),
        battery_charge_kwh=row["battery_charge_kwh"],
        battery_discharge_kwh=row["battery_discharge_kwh"],
        grid_import_kwh=row["grid_import_kwh"],
        grid_export_kwh=row["grid_export_kwh"],
        dhw_on=bool(row["dhw_on"]),
        soc_start_kwh=0.0,   # not stored — use inverter reading for actual SoC
        soc_end_kwh=0.0,
        slot_cost_gbp=cost,
    )


def get_upcoming_schedule(
    db_path: str,
    from_dt: datetime | None = None,
    limit: int = 48,
) -> list[SlotDecision]:
    """Return upcoming scheduled decisions from from_dt, up to limit slots."""
    if from_dt is None:
        from_dt = datetime.now(timezone.utc)
    from_str = from_dt.astimezone(timezone.utc).isoformat()

    with get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT slot_start, battery_charge_kwh, battery_discharge_kwh,
                   grid_import_kwh, grid_export_kwh, dhw_on,
                   buy_gbp_kwh, sell_gbp_kwh
            FROM schedule
            WHERE slot_start >= ?
            ORDER BY slot_start
            LIMIT ?
            """,
            (from_str, limit),
        ).fetchall()

    return [
        SlotDecision(
            slot_start=datetime.fromisoformat(r["slot_start"]),
            battery_charge_kwh=r["battery_charge_kwh"],
            battery_discharge_kwh=r["battery_discharge_kwh"],
            grid_import_kwh=r["grid_import_kwh"],
            grid_export_kwh=r["grid_export_kwh"],
            dhw_on=bool(r["dhw_on"]),
            soc_start_kwh=0.0,
            soc_end_kwh=0.0,
            slot_cost_gbp=r["buy_gbp_kwh"] * r["grid_import_kwh"]
                - r["sell_gbp_kwh"] * r["grid_export_kwh"],
        )
        for r in rows
    ]
