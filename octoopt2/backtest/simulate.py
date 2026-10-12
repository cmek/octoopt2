"""Pure per-slot dispatch simulation of the inverter in each hardware mode.

Everything is AC-side kWh per 30-minute slot, with the same efficiencies and
power caps the MILP uses, so simulated and optimised flows are comparable.

Modes (see control/inverter.py for what the hardware does):
  ECO               — solar → load → battery → export → curtail; a deficit is
                      met from the battery first, then the grid.
  CHARGE            — force-charge at `power_kw` (from solar first, grid for the
                      rest); the battery never discharges.
  DISCHARGE_EXPORT  — force-discharge at `power_kw`, exporting what the house
                      does not use (capped at the export limit).
  NO_BATTERY        — reference: solar offsets load, surplus exports.
"""
from __future__ import annotations

from dataclasses import dataclass

from ..config import BatteryConfig, GivEnergyConfig

SLOT_HOURS = 0.5
_EPS = 1e-9


@dataclass(frozen=True)
class SimLimits:
    cap_kwh: float
    min_soc_kwh: float
    max_soc_kwh: float
    eff_c: float
    eff_d: float
    max_charge_kwh: float      # per slot, AC side
    max_discharge_kwh: float   # per slot, AC side
    max_export_kwh: float      # per slot
    max_import_kwh: float      # per slot

    @classmethod
    def from_config(
        cls,
        battery: BatteryConfig,
        givenergy: GivEnergyConfig,
        *,
        min_soc_pct: float | None = None,
    ) -> "SimLimits":
        floor_pct = battery.min_soc_pct if min_soc_pct is None else min_soc_pct
        return cls(
            cap_kwh=battery.capacity_kwh,
            min_soc_kwh=battery.capacity_kwh * floor_pct / 100,
            max_soc_kwh=battery.max_soc_kwh,
            eff_c=battery.charge_efficiency,
            eff_d=battery.discharge_efficiency,
            max_charge_kwh=battery.max_charge_rate_kw * SLOT_HOURS,
            max_discharge_kwh=battery.max_discharge_rate_kw * SLOT_HOURS,
            max_export_kwh=givenergy.max_export_kw * SLOT_HOURS,
            max_import_kwh=givenergy.max_import_kw * SLOT_HOURS,
        )

    def charge_headroom_kwh(self, soc_kwh: float) -> float:
        """AC energy that can still go in before the battery is full."""
        return max(0.0, (self.max_soc_kwh - soc_kwh) / self.eff_c)

    def discharge_available_kwh(self, soc_kwh: float) -> float:
        """AC energy that can still come out before hitting the floor."""
        return max(0.0, (soc_kwh - self.min_soc_kwh) * self.eff_d)


@dataclass
class SimFlow:
    import_kwh: float
    export_kwh: float
    charge_kwh: float
    discharge_kwh: float
    curtail_kwh: float
    soc_start_kwh: float
    soc_end_kwh: float

    def cost(self, buy: float, sell: float) -> float:
        return buy * self.import_kwh - sell * self.export_kwh


def _finish(lim: SimLimits, soc: float, imp: float, exp: float, chg: float, dis: float, cur: float) -> SimFlow:
    soc_end = soc + chg * lim.eff_c - dis / lim.eff_d
    soc_end = min(max(soc_end, 0.0), lim.max_soc_kwh)
    return SimFlow(
        import_kwh=max(0.0, imp), export_kwh=max(0.0, exp),
        charge_kwh=max(0.0, chg), discharge_kwh=max(0.0, dis),
        curtail_kwh=max(0.0, cur), soc_start_kwh=soc, soc_end_kwh=soc_end,
    )


def simulate_slot(
    mode: str,
    power_kw: float,
    soc_kwh: float,
    solar_kwh: float,
    load_kwh: float,
    lim: SimLimits,
) -> SimFlow:
    """Dispatch one slot in `mode` from SoC `soc_kwh` and return the flows."""
    solar = max(0.0, solar_kwh)
    load = max(0.0, load_kwh)

    if mode == "NO_BATTERY":
        net = solar - load
        if net >= 0:
            exp = min(net, lim.max_export_kwh)
            return _finish(lim, soc_kwh, 0.0, exp, 0.0, 0.0, net - exp)
        return _finish(lim, soc_kwh, -net, 0.0, 0.0, 0.0, 0.0)

    if mode == "CHARGE":
        want = max(0.0, power_kw) * SLOT_HOURS
        chg = min(want, lim.max_charge_kwh, lim.charge_headroom_kwh(soc_kwh))
        net = solar - load - chg
        if net >= 0:
            exp = min(net, lim.max_export_kwh)
            return _finish(lim, soc_kwh, 0.0, exp, chg, 0.0, net - exp)
        return _finish(lim, soc_kwh, min(-net, lim.max_import_kwh), 0.0, chg, 0.0, 0.0)

    if mode == "DISCHARGE_EXPORT":
        want = max(0.0, power_kw) * SLOT_HOURS
        dis = min(want, lim.max_discharge_kwh, lim.discharge_available_kwh(soc_kwh))
        # The inverter will not push battery energy past the export cap, so the
        # usable discharge is bounded by what load + export can absorb.
        dis = min(dis, max(0.0, load + lim.max_export_kwh - solar))
        net = solar + dis - load
        if net >= 0:
            exp = min(net, lim.max_export_kwh)
            return _finish(lim, soc_kwh, 0.0, exp, 0.0, dis, net - exp)
        return _finish(lim, soc_kwh, -net, 0.0, 0.0, dis, 0.0)

    if mode == "ECO":
        surplus = solar - load
        if surplus >= 0:
            chg = min(surplus, lim.max_charge_kwh, lim.charge_headroom_kwh(soc_kwh))
            rest = surplus - chg
            exp = min(rest, lim.max_export_kwh)
            return _finish(lim, soc_kwh, 0.0, exp, chg, 0.0, rest - exp)
        deficit = -surplus
        dis = min(deficit, lim.max_discharge_kwh, lim.discharge_available_kwh(soc_kwh))
        return _finish(lim, soc_kwh, deficit - dis, 0.0, 0.0, dis, 0.0)

    raise ValueError(f"Unknown mode {mode!r}")


def run_modes(
    modes: list[tuple[str, float]],
    soc0_kwh: float,
    solar: list[float],
    load: list[float],
    lim: SimLimits,
) -> list[SimFlow]:
    """Simulate a sequence of (mode, power_kw) decisions, chaining SoC."""
    flows: list[SimFlow] = []
    soc = soc0_kwh
    for (mode, kw), s, l in zip(modes, solar, load):
        f = simulate_slot(mode, kw, soc, s, l, lim)
        flows.append(f)
        soc = f.soc_end_kwh
    return flows


def run_eco(soc0_kwh: float, solar: list[float], load: list[float], lim: SimLimits) -> list[SimFlow]:
    return run_modes([("ECO", 0.0)] * len(solar), soc0_kwh, solar, load, lim)


def run_no_battery(solar: list[float], load: list[float], lim: SimLimits) -> list[SimFlow]:
    return run_modes([("NO_BATTERY", 0.0)] * len(solar), 0.0, solar, load, lim)

