import logging
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class BatteryConfig:
    capacity_kwh: float = 9.5
    # Planning floor. Keep equal to hw_reserve_pct so plan and hardware agree.
    min_soc_pct: float = 10.0
    max_soc_pct: float = 100.0
    # Round-trip efficiency 93% → split symmetrically: √0.93
    charge_efficiency: float = 0.9644
    discharge_efficiency: float = 0.9644
    # Sustained AC-side battery power the inverter actually delivers at the
    # full-scale (50) charge/discharge limit register. Measured on the live
    # system (Sep–Oct 2026): ≈3.4 kW charging, ≈2.8 kW peak / 2.3 kW average
    # discharging — well below the 3.6 kW inverter rating previously assumed,
    # which made every planned export slot ~30% larger than deliverable.
    max_charge_rate_kw: float = 3.4
    max_discharge_rate_kw: float = 2.8
    # Virtual cost added to each kWh of battery throughput to avoid thin-margin cycles.
    # Effectively sets a minimum economic spread (~2× this value) before the optimizer
    # will bother cycling the battery. Default 1.5p/kWh.
    cycle_cost_gbp_kwh: float = 0.015
    # Hardware reserve written to the inverter (both BATTERY_SOC_RESERVE and the
    # discharge-side power reserve) whenever ECO mode is applied, so the
    # physical floor always matches the planning floor. GivEnergy ships 4%.
    hw_reserve_pct: int = 10
    # Value (£/kWh) credited to energy left in the battery at the end of the
    # optimisation horizon, so the plan does not drain the battery to the floor
    # just because the horizon ends. None → use the mean buy price of the
    # horizon scaled by discharge efficiency; 0 → legacy behaviour (no value).
    terminal_value_gbp_kwh: float | None = None

    @property
    def min_soc_kwh(self) -> float:
        return self.capacity_kwh * self.min_soc_pct / 100

    @property
    def max_soc_kwh(self) -> float:
        return self.capacity_kwh * self.max_soc_pct / 100

    @property
    def usable_kwh(self) -> float:
        return self.max_soc_kwh - self.min_soc_kwh

    @classmethod
    def from_env(cls) -> "BatteryConfig":
        d = cls()
        tv = os.getenv("BATTERY_TERMINAL_VALUE_GBP_KWH")
        return cls(
            capacity_kwh=float(os.getenv("BATTERY_CAPACITY_KWH", d.capacity_kwh)),
            min_soc_pct=float(os.getenv("BATTERY_MIN_SOC_PCT", d.min_soc_pct)),
            max_soc_pct=float(os.getenv("BATTERY_MAX_SOC_PCT", d.max_soc_pct)),
            charge_efficiency=float(os.getenv("BATTERY_CHARGE_EFFICIENCY", d.charge_efficiency)),
            discharge_efficiency=float(os.getenv("BATTERY_DISCHARGE_EFFICIENCY", d.discharge_efficiency)),
            max_charge_rate_kw=float(os.getenv("BATTERY_MAX_CHARGE_KW", d.max_charge_rate_kw)),
            max_discharge_rate_kw=float(os.getenv("BATTERY_MAX_DISCHARGE_KW", d.max_discharge_rate_kw)),
            cycle_cost_gbp_kwh=float(os.getenv("BATTERY_CYCLE_COST_GBP_KWH", d.cycle_cost_gbp_kwh)),
            hw_reserve_pct=int(os.getenv("BATTERY_HW_RESERVE_PCT", d.hw_reserve_pct)),
            terminal_value_gbp_kwh=None if tv in (None, "", "auto") else float(tv),
        )


@dataclass(frozen=True)
class GivEnergyConfig:
    host: str
    port: int = 8899
    number_batteries: int = 1
    # Max AC export to grid — limited by inverter rating (W → kW)
    max_export_kw: float = 3.6
    # Max AC import from grid — limited by consumer unit (typical single-phase UK)
    max_import_kw: float = 11.0

    @classmethod
    def from_env(cls) -> "GivEnergyConfig":
        return cls(
            host=os.environ["GIVENERGY_HOST"],
            port=int(os.getenv("GIVENERGY_PORT", "8899")),
            number_batteries=int(os.getenv("GIVENERGY_NUMBER_BATTERIES", "1")),
            max_export_kw=float(os.getenv("MAX_EXPORT_KW", "3.6")),
            max_import_kw=float(os.getenv("MAX_IMPORT_KW", "11.0")),
        )


# How the controller holds the Ecodan off outside planned DHW slots.
#   "setpoint" — lower the tank target to off_target_c so the heat pump's own
#                thermostat does not reheat after a shower until a planned slot
#                (or the comfort guard) restores target_c. Default.
#   "auto"     — legacy: only clear forced-DHW mode and leave the target alone.
#                The Ecodan then reheats on its own schedule whenever the tank
#                drops, regardless of price.
DHW_OFF_MODES = ("setpoint", "auto")


@dataclass(frozen=True)
class DhwConfig:
    # Power draw during DHW heating mode (kW) — Mitsubishi Ecodan typical
    power_kw: float = 1.5
    # Minimum 30-min slots DHW must run each calendar day (ensures hot water)
    min_slots_per_day: int = 4
    # Maximum slots per day (prevents over-heating the tank)
    max_slots_per_day: int = 8
    # See DHW_OFF_MODES.
    off_mode: str = "setpoint"
    # Normal tank target (°C): written in planned-ON slots and whenever control
    # is released (safe fallback, shutdown, away mode, comfort guard).
    target_c: float = 50.0
    # Tank target held in planned-off slots under off_mode="setpoint". 40 °C is
    # the lowest MELCloud accepts for an Ecodan tank.
    off_target_c: float = 40.0
    # Comfort guard: in an off slot, a tank below this (°C) gets target_c back
    # so a heavy-use day never ends cold. 0 disables the guard.
    min_tank_c: float = 35.0

    def __post_init__(self) -> None:
        if self.off_mode not in DHW_OFF_MODES:
            raise ValueError(f"DHW_OFF_MODE must be one of {DHW_OFF_MODES}, got {self.off_mode!r}")
        if not (self.off_target_c <= self.target_c):
            raise ValueError("DHW_OFF_TARGET_C must not exceed DHW_TARGET_C")
        if self.min_tank_c and not (self.min_tank_c < self.off_target_c):
            raise ValueError("DHW_MIN_TANK_C must be below DHW_OFF_TARGET_C (or 0 to disable)")

    @classmethod
    def from_env(cls) -> "DhwConfig":
        return cls(
            power_kw=float(os.getenv("DHW_POWER_KW", "1.5")),
            min_slots_per_day=int(os.getenv("DHW_MIN_SLOTS_PER_DAY", "4")),
            max_slots_per_day=int(os.getenv("DHW_MAX_SLOTS_PER_DAY", "8")),
            off_mode=os.getenv("DHW_OFF_MODE", "setpoint").strip().lower(),
            target_c=float(os.getenv("DHW_TARGET_C", "50")),
            off_target_c=float(os.getenv("DHW_OFF_TARGET_C", "40")),
            min_tank_c=float(os.getenv("DHW_MIN_TANK_C", "35")),
        )


def _import_tariff_code_from_env() -> str:
    """OCTOPUS_IMPORT_TARIFF_CODE, falling back to the deprecated
    OCTOPUS_AGILE_TARIFF_CODE (with a warning) so existing .env files keep working."""
    code = os.getenv("OCTOPUS_IMPORT_TARIFF_CODE")
    if code:
        return code
    legacy = os.environ["OCTOPUS_AGILE_TARIFF_CODE"]
    logging.getLogger(__name__).warning(
        "OCTOPUS_AGILE_TARIFF_CODE is deprecated; rename it to OCTOPUS_IMPORT_TARIFF_CODE"
    )
    return legacy


def _opt_float(name: str, default: float | None) -> float | None:
    """Float env var where "" / "none" / "0" mean "disabled" (None)."""
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip().lower()
    if raw in ("", "none", "off"):
        return None
    v = float(raw)
    return None if v <= 0 else v


@dataclass(frozen=True)
class LoadModelConfig:
    """Load-forecast model settings (see optimizer.forecast.fit_load_model)."""
    # How many days of gross-load history the fit looks back over.
    train_days: int = 56
    # Exponential recency weighting half-life (days); None → unweighted OLS.
    half_life_days: float | None = 10.0
    # Heating-degree base (°C): the temperature term is max(0, base − T), so
    # cold slots add load and warm ones add nothing. None → legacy linear
    # α·(T − ref_temp).
    hdd_base_c: float | None = 13.0

    @classmethod
    def from_env(cls) -> "LoadModelConfig":
        return cls(
            train_days=int(os.getenv("LOAD_MODEL_DAYS", "56")),
            half_life_days=_opt_float("LOAD_MODEL_HALF_LIFE_DAYS", 10.0),
            hdd_base_c=_opt_float("LOAD_MODEL_HDD_BASE_C", 13.0),
        )


@dataclass(frozen=True)
class OctopusConfig:
    api_key: str
    account_number: str
    mpan: str
    serial: str
    import_tariff_code: str   # buy tariff: Agile or a banded tariff such as Cosy
    outgoing_tariff_code: str
    dno_region: str

    @classmethod
    def from_env(cls) -> "OctopusConfig":
        return cls(
            api_key=os.environ["OCTOPUS_API_KEY"],
            account_number=os.environ["OCTOPUS_ACCOUNT_NUMBER"],
            mpan=os.environ["OCTOPUS_MPAN"],
            serial=os.environ["OCTOPUS_SERIAL"],
            import_tariff_code=_import_tariff_code_from_env(),
            outgoing_tariff_code=os.environ["OCTOPUS_OUTGOING_TARIFF_CODE"],
            dno_region=os.environ["OCTOPUS_DNO_REGION"],
        )


@dataclass(frozen=True)
class SolcastConfig:
    api_key: str
    resource_id: str

    @classmethod
    def from_env(cls) -> "SolcastConfig":
        return cls(
            api_key=os.environ["SOLCAST_API_KEY"],
            resource_id=os.environ["SOLCAST_RESOURCE_ID"],
        )


@dataclass(frozen=True)
class MelCloudConfig:
    email: str
    password: str
    device_id: int

    @classmethod
    def from_env(cls) -> "MelCloudConfig":
        return cls(
            email=os.environ["MELCLOUD_EMAIL"],
            password=os.environ["MELCLOUD_PASSWORD"],
            device_id=int(os.environ["MELCLOUD_DEVICE_ID"]),
        )


@dataclass(frozen=True)
class DaemonConfig:
    """Tunables for the long-running daemon (octoopt2-daemon)."""
    # Port the Prometheus /metrics HTTP server listens on
    metrics_port: int = 9876
    # How often to poll the inverter for live metrics (seconds)
    inverter_poll_seconds: int = 30
    # How often to poll MELCloud for DHW state (seconds) — keep modest, cloud API
    ecodan_poll_seconds: int = 300

    @classmethod
    def from_env(cls) -> "DaemonConfig":
        return cls(
            metrics_port=int(os.getenv("METRICS_PORT", "9876")),
            inverter_poll_seconds=int(os.getenv("INVERTER_POLL_SECONDS", "30")),
            ecodan_poll_seconds=int(os.getenv("ECODAN_POLL_SECONDS", "300")),
        )


@dataclass(frozen=True)
class LocationConfig:
    latitude: float
    longitude: float

    @classmethod
    def from_env(cls) -> "LocationConfig":
        return cls(
            latitude=float(os.environ["LATITUDE"]),
            longitude=float(os.environ["LONGITUDE"]),
        )


@dataclass(frozen=True)
class AxleConfig:
    """Axle Energy VPP, "Events Only" mode. Off unless AXLE_TOKEN is set.

    Axle pays per kWh of metered export during grid events (and may call import
    events). The event feed is polled with the token; the optimizer sees the
    payment as a per-slot price uplift and the controller holds max export
    (or max charge) through the event so Axle's own cloud command and ours agree.
    """
    token: str | None = None
    export_gbp_kwh: float = 1.0      # paid per kWh exported during an export event
    import_gbp_kwh: float = 1.0      # credited per kWh imported during an import event
    hold_margin_minutes: int = 0     # keep the hold this long either side of the event

    @property
    def enabled(self) -> bool:
        return bool(self.token)

    @classmethod
    def from_env(cls) -> "AxleConfig":
        return cls(
            token=os.getenv("AXLE_TOKEN") or None,
            export_gbp_kwh=float(os.getenv("AXLE_EXPORT_GBP_KWH", "1.0")),
            import_gbp_kwh=float(os.getenv("AXLE_IMPORT_GBP_KWH", "1.0")),
            hold_margin_minutes=int(os.getenv("AXLE_HOLD_MARGIN_MINUTES", "0")),
        )


@dataclass(frozen=True)
class AppConfig:
    givenergy: GivEnergyConfig
    octopus: OctopusConfig
    solcast: SolcastConfig
    melcloud: MelCloudConfig
    battery: BatteryConfig
    dhw: DhwConfig
    location: LocationConfig
    daemon: DaemonConfig
    db_path: str
    # Slot duration in minutes (Octopus rates are planned per half-hour; banded
    # tariffs are expanded to half-hours by the price feed)
    slot_minutes: int = 30
    axle: AxleConfig = field(default_factory=AxleConfig)

    load_model: LoadModelConfig = LoadModelConfig()

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            givenergy=GivEnergyConfig.from_env(),
            octopus=OctopusConfig.from_env(),
            solcast=SolcastConfig.from_env(),
            melcloud=MelCloudConfig.from_env(),
            battery=BatteryConfig.from_env(),
            dhw=DhwConfig.from_env(),
            location=LocationConfig.from_env(),
            daemon=DaemonConfig.from_env(),
            db_path=os.getenv("DB_PATH", "octoopt2.db"),
            axle=AxleConfig.from_env(),

            load_model=LoadModelConfig.from_env(),
        )
