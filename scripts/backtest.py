"""octoopt2-backtest — how good was the plan, and what was possible?

Replays actual load and solar through several strategies and tariffs:
  actual        what the inverter measured, priced under the tariff
  planned       the controller's own plan (schedule table; DB prices only)
  plan-exec     that plan executed perfectly on the simulator
  hindsight     perfect-foresight MILP (the ceiling for this hardware)
  ECO-only      plain self-consumption, no scheduling
  no-batt       no battery at all
  replay        (--replay) rolling-horizon MILP with the stored forecasts

Run against a COPY of the database:
  uv run octoopt2-backtest --db /path/to/copy.db --from 2026-09-01 --to 2026-10-12
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, ".")

from octoopt2.backtest.data import LONDON, load_slots, london_day_bounds, segment_days  # noqa: E402
from octoopt2.backtest.replay import RollingOptions  # noqa: E402
from octoopt2.backtest.report import (  # noqa: E402
    evaluate_tariff, execution_diagnostics, forecast_accuracy, render_markdown, to_json,
)
from octoopt2.backtest.simulate import SimLimits  # noqa: E402
from octoopt2.config import BatteryConfig, DhwConfig, GivEnergyConfig  # noqa: E402


def _parse_date(s: str) -> date:
    return date.fromisoformat(s)


def _standing_for(db_path, product, region, seg, api_key, refresh):
    from octoopt2.backtest import tariffs as T
    days = sorted({s.london_day for s in seg.slots})
    start = london_day_bounds(days[0])[0]
    end = london_day_bounds(days[-1])[1]
    T.ensure_cached(db_path, product, region, "standing", start, end, api_key=api_key, refresh=refresh)
    return T.expand_standing(T.load_records(db_path, T.tariff_code(product, region), "standing"), days)


def main() -> None:
    from dotenv import load_dotenv
    load_dotenv()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    today = datetime.now(LONDON).date()
    ap.add_argument("--from", dest="from_", type=_parse_date, default=today - timedelta(days=14), help="first London day (default: 14 days ago)")
    ap.add_argument("--to", dest="to", type=_parse_date, default=today, help="exclusive end London day (default: today)")
    ap.add_argument("--db", default=os.getenv("DB_PATH", "octoopt2.db"))
    ap.add_argument("--tariffs", default="agile", help="comma list of pair names/specs (see backtest.tariffs.DEFAULT_PAIRS)")
    ap.add_argument("--region", default=os.getenv("OCTOPUS_DNO_REGION", "G"))
    ap.add_argument("--standing-product", default=None, help="product whose standing charge applies to DB-priced pairs (default: the configured import tariff's product, else AGILE-24-10-01)")
    ap.add_argument("--sell-fixed", type=float, default=None, help="override every sell price with a constant £/kWh")
    ap.add_argument("--cycle-cost", type=float, default=0.0, help="virtual cycle cost for hindsight (£/kWh)")
    ap.add_argument("--min-soc", type=float, default=None, help="planning/sim floor %% (default: config)")
    ap.add_argument("--eco-reserve-pct", type=float, default=None, help="floor %% for the ECO-only baseline (default: same as --min-soc)")
    ap.add_argument("--max-charge-kw", type=float, default=None)
    ap.add_argument("--max-discharge-kw", type=float, default=None)
    ap.add_argument("--max-export-kw", type=float, default=float(os.getenv("MAX_EXPORT_KW", "3.6")))
    ap.add_argument("--terminal", choices=["value", "end-as-start", "none"], default="value")
    ap.add_argument("--lookahead-days", type=int, default=1)
    ap.add_argument("--time-limit", type=int, default=60)
    ap.add_argument("--dhw-replay", type=float, default=None, metavar="BETA_KWH", help="let the MILP re-place DHW (subtract BETA on planned DHW slots)")
    ap.add_argument("--replay", action="store_true", help="also run the rolling-horizon replay (slow)")
    ap.add_argument("--replan-every", type=int, default=1)
    ap.add_argument("--replay-solar", choices=["p50", "pred", "actual"], default="p50")
    ap.add_argument("--replay-load", choices=["pred", "actual", "model"], default="pred",
                    help="pred = stored prediction; actual = perfect load; model = re-fit the load model "
                         "walk-forward each day (see --load-*)")
    ap.add_argument("--load-half-life", type=float, default=None, help="load model recency half-life in days (0 = unweighted; default: config)")
    ap.add_argument("--load-hdd-base", default=None, help="heating-degree base °C ('' = legacy linear α; default: config)")
    ap.add_argument("--load-train-days", type=int, default=None)
    ap.add_argument("--load-temp", choices=["observed", "forecast"], default="observed", help="temperature source for --replay-load model")
    ap.add_argument("--solar-scale", type=float, default=1.0, help="multiply the replay's solar forecast")
    ap.add_argument("--axle-events", default=None, metavar="PATH",
                    help="JSON list of Axle events [{start_time,end_time,import_export}]; their payment is added to every strategy's prices")
    ap.add_argument("--refresh-tariffs", action="store_true")
    ap.add_argument("--json", dest="json_path", default=None)
    ap.add_argument("--output", action="store_true", help="write web/backtests/<from>_<to>.json + index.json")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("octoopt2.optimizer.model").setLevel(logging.ERROR if not args.verbose else logging.INFO)

    battery = BatteryConfig.from_env()
    overrides = {}
    if args.min_soc is not None:
        overrides["min_soc_pct"] = args.min_soc
    if args.max_charge_kw is not None:
        overrides["max_charge_rate_kw"] = args.max_charge_kw
    if args.max_discharge_kw is not None:
        overrides["max_discharge_rate_kw"] = args.max_discharge_kw
    if overrides:
        from dataclasses import replace
        battery = replace(battery, **overrides)
    dhw = DhwConfig.from_env()
    givenergy = GivEnergyConfig(host="", max_export_kw=args.max_export_kw,
                                max_import_kw=float(os.getenv("MAX_IMPORT_KW", "11.0")))
    lim = SimLimits.from_config(battery, givenergy)
    eco_lim = SimLimits.from_config(battery, givenergy, min_soc_pct=args.eco_reserve_pct)

    from_utc = london_day_bounds(args.from_)[0]
    to_utc = london_day_bounds(args.to)[0]
    slots = load_slots(args.db, from_utc, to_utc)
    if not slots:
        sys.exit(f"No price rows between {args.from_} and {args.to} in {args.db}")
    load_cfg = None
    if args.replay_load == "model":
        from dataclasses import replace as _replace
        from octoopt2.backtest.loadmodel import walk_forward_predictions
        from octoopt2.config import LoadModelConfig
        load_cfg = LoadModelConfig.from_env()
        if args.load_half_life is not None:
            load_cfg = _replace(load_cfg, half_life_days=args.load_half_life or None)
        if args.load_hdd_base is not None:
            base = args.load_hdd_base.strip().lower()
            load_cfg = _replace(load_cfg, hdd_base_c=None if base in ("", "none") else float(base))
        if args.load_train_days is not None:
            load_cfg = _replace(load_cfg, train_days=args.load_train_days)
        days = sorted({s.london_day for s in slots})
        print(f"Re-fitting load model walk-forward over {len(days)} days: {load_cfg}", file=sys.stderr)
        preds = walk_forward_predictions(args.db, days, load_cfg, temp_source=args.load_temp)
        for s in slots:
            if s.slot_start in preds:
                s.pred_load_kwh = preds[s.slot_start]
        args.replay_load = "pred"
    segments, excluded = segment_days(slots, floor_kwh=battery.min_soc_kwh, capacity_kwh=battery.capacity_kwh)
    n_days = sum(len(s.days) for s in segments)
    print(f"Loaded {len(slots)} slots; {n_days} complete days in {len(segments)} segment(s); {len(excluded)} excluded", file=sys.stderr)

    from octoopt2.backtest import tariffs as T
    pairs = [T.parse_pair_spec(x.strip()) for x in args.tariffs.split(",") if x.strip()]
    api_key = os.getenv("OCTOPUS_API_KEY")
    if args.standing_product is None:
        # DB prices come from the live import tariff; default its product's standing charge.
        live = os.getenv("OCTOPUS_IMPORT_TARIFF_CODE") or os.getenv("OCTOPUS_AGILE_TARIFF_CODE")
        args.standing_product = "-".join(live.split("-")[2:-1]) if live else "AGILE-24-10-01"

    axle_events = []
    if args.axle_events:
        from octoopt2.data.axle import parse_event
        with open(args.axle_events) as fh:
            axle_events = [e for e in (parse_event(x) for x in json.load(fh)) if e is not None]
        print(f"Axle: {len(axle_events)} event(s) loaded from {args.axle_events}", file=sys.stderr)

    def price_for_factory(pair):
        def price_for(seg):
            ps = T.load_price_series(
                args.db, pair, [s.slot_start for s in seg.slots], args.region,
                db_buy=[s.buy for s in seg.slots], db_sell=[s.sell for s in seg.slots],
                api_key=api_key, refresh=args.refresh_tariffs,
            )
            sell = [args.sell_fixed] * len(ps.sell) if args.sell_fixed is not None else ps.sell
            buy = ps.buy
            if axle_events:
                from octoopt2.config import AxleConfig
                from octoopt2.data.axle import slot_uplift
                up = slot_uplift(axle_events, [s.slot_start for s in seg.slots], AxleConfig())
                buy = [b + min(u, 0.0) for b, u in zip(buy, up)]
                sell = [x + max(u, 0.0) for x, u in zip(sell, up)]
            standing = ps.standing_by_day
            if pair.import_product is None:
                # DB prices are the live import tariff: price the standing charge from its product
                standing = _standing_for(args.db, args.standing_product, args.region, seg, api_key, args.refresh_tariffs)
            return buy, sell, standing, ps.notes
        return price_for

    rolling = RollingOptions(replan_every=args.replan_every, time_limit_s=min(args.time_limit, 20),
                             solar_scale=args.solar_scale, solar_source=args.replay_solar,
                             load_source=args.replay_load, terminal=args.terminal) if args.replay else None

    results = []
    for pair in pairs:
        print(f"Evaluating tariff {pair.name} …", file=sys.stderr)
        results.append(evaluate_tariff(
            pair.name, segments, price_for_factory(pair), battery, dhw, givenergy,
            lim=lim, eco_lim=eco_lim, lookahead_days=args.lookahead_days, time_limit_s=args.time_limit,
            terminal=args.terminal, cycle_cost=args.cycle_cost, dhw_beta=args.dhw_replay,
            rolling=rolling, is_db_tariff=(pair.import_product is None and pair.export_product is None and pair.sell_fixed is None),
        ))

    diag = execution_diagnostics(segments, battery, lim)
    acc = forecast_accuracy([s for seg in segments for s in seg.slots])
    meta = {
        "from": args.from_.isoformat(), "to": args.to.isoformat(), "db": args.db,
        "battery": {k: getattr(battery, k) for k in ("capacity_kwh", "min_soc_pct", "max_charge_rate_kw", "max_discharge_rate_kw", "charge_efficiency")},
        "max_export_kw": args.max_export_kw, "cycle_cost": args.cycle_cost, "terminal": args.terminal,
        "lookahead_days": args.lookahead_days, "excluded": [(d.isoformat(), r) for d, r in excluded],
        "days": n_days, "replay": vars(rolling) if rolling else None,
        "load_model": vars(load_cfg) if load_cfg else "stored",
    }
    print(render_markdown(results, diag, acc, meta))
    payload = to_json(results, diag, acc, meta)
    if args.json_path:
        with open(args.json_path, "w") as f:
            json.dump(payload, f, indent=1, default=str)
    if args.output:
        out_dir = os.path.join("web", "backtests")
        os.makedirs(out_dir, exist_ok=True)
        name = f"{args.from_.isoformat()}_{args.to.isoformat()}.json"
        with open(os.path.join(out_dir, name), "w") as f:
            json.dump(payload, f, default=str)
        idx_path = os.path.join(out_dir, "index.json")
        index = {"reports": []}
        if os.path.exists(idx_path):
            with open(idx_path) as f:
                index = json.load(f)
        if name not in index["reports"]:
            index["reports"].insert(0, name)
        with open(idx_path, "w") as f:
            json.dump(index, f)
        print(f"\nSaved {out_dir}/{name}", file=sys.stderr)


if __name__ == "__main__":
    main()
