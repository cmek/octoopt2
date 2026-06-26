"""Long-running daemon: replaces the cron job and exposes Prometheus metrics.

The cron model fired `uv run octoopt2` every 5 minutes — a fresh process that
opened a short-lived Modbus connection, ran the fast/slow optimizer path, and
exited. This daemon does the same two tasks itself on a timer, and adds a high-
frequency inverter poller plus a /metrics endpoint for near-live dashboards.

A GivEnergy inverter accepts only ONE Modbus TCP connection at a time, so the
daemon deliberately keeps NO persistent connection. Every inverter access opens
and closes its own short-lived connection (exactly as the cron job did), leaving
the socket free for other systems (GivTCP, Home Assistant, a manual --dry-run)
between polls. A single asyncio.Lock serialises the 30 s poller and the 5 min
optimizer tick so the two never open the socket at the same instant.

Three concurrent loops:

  inverter poller — every INVERTER_POLL_SECONDS, lock-guarded read_inverter()
                    into shared in-memory state (for /metrics only; not stored).
  ecodan poller   — every ECODAN_POLL_SECONDS, MELCloud DHW read into state.
  optimizer tick  — every 5 min (wall-clock aligned), lock-guarded scheduler.run()
                    in a worker thread — the existing fast/slow path, unchanged.

Run with:  uv run octoopt2-daemon
"""
import argparse
import asyncio
import json
import logging
import mimetypes
import signal
import sys
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest

from .config import AppConfig
from .control.ecodan import _get_dhw_state_async
from .data.inverter import InverterReading, read_inverter
from .db import init_db
from .metrics import OctooptCollector
from .status import build_status
from . import scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Optimizer tick interval (seconds). Aligned to the wall clock so ticks land on
# :00/:05/:10/… — scheduler.run() gates its slow path on now.minute, so the
# :00 and :30 ticks run the full optimization just as the old cron schedule did.
_TICK_SECONDS = 300


@dataclass
class DaemonState:
    """Shared in-memory snapshot read by the Prometheus collector.

    Updated by the poller loops; read (reference-only) by the metrics HTTP
    server thread. Plain attribute reads/writes are atomic under the GIL, so no
    locking is needed for metrics access. The lock guards inverter *socket*
    access between the poller and the optimizer tick, not this state.
    """
    inverter_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    latest_reading: InverterReading | None = None
    latest_dhw: dict | None = None
    latest_dhw_at: datetime | None = None
    # Rolling history of recent inverter readings for the dashboard's sparklines.
    # 240 samples × 30 s ≈ 2 h. deque.append/iterate are atomic under the GIL, so
    # the metrics HTTP thread can read it without locking.
    history: deque = field(default_factory=lambda: deque(maxlen=240))


# ── HTTP server (metrics + live dashboard) ──────────────────────────────────

# Directory holding the dashboard's static assets (status.html etc.), resolved
# relative to the working directory like the optimizer's web/schedule.json output.
_WEB_DIR = Path("web")


def _make_handler(state: "DaemonState", config: AppConfig):
    """Build a request handler serving /metrics, /status.json, and web/ statics."""
    web_root = _WEB_DIR.resolve()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args) -> None:  # silence per-request logging
            pass

        def _send(self, code: int, content_type: str, body: bytes, cache: bool = True) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            if not cache:
                self.send_header("Cache-Control", "no-store")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _serve_static(self, rel: str) -> None:
            try:
                target = (web_root / rel).resolve()
                target.relative_to(web_root)  # reject path traversal
                body = target.read_bytes()
            except (FileNotFoundError, IsADirectoryError, ValueError):
                self._send(404, "text/plain", b"Not found", cache=False)
                return
            ctype = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
            self._send(200, ctype, body)

        def do_GET(self) -> None:
            path = self.path.split("?", 1)[0]
            if path == "/metrics":
                self._send(200, CONTENT_TYPE_LATEST, generate_latest(REGISTRY), cache=False)
                return
            if path == "/status.json":
                try:
                    payload = build_status(
                        state, config.db_path, config.battery.capacity_kwh
                    )
                    body = json.dumps(payload).encode()
                    self._send(200, "application/json", body, cache=False)
                except Exception as exc:
                    logger.warning("status.json build failed: %s", exc)
                    self._send(
                        500, "application/json",
                        json.dumps({"error": str(exc)}).encode(), cache=False,
                    )
                return
            rel = path.lstrip("/") or "status.html"
            self._serve_static(rel)

        do_HEAD = do_GET

    return Handler


def _start_http_server(state: "DaemonState", config: AppConfig) -> None:
    """Start the combined metrics + dashboard HTTP server in a background thread."""
    handler = _make_handler(state, config)
    httpd = ThreadingHTTPServer(("", config.daemon.metrics_port), handler)
    thread = threading.Thread(target=httpd.serve_forever, name="http", daemon=True)
    thread.start()


def _seconds_until_next(period_seconds: int) -> float:
    """Seconds until the next wall-clock boundary that is a multiple of period."""
    now = datetime.now(timezone.utc)
    epoch_seconds = now.timestamp()
    return period_seconds - (epoch_seconds % period_seconds)


# ── Loops ──────────────────────────────────────────────────────────────────

async def _poll_inverter_loop(config: AppConfig, state: DaemonState) -> None:
    """Poll the inverter for live metrics every INVERTER_POLL_SECONDS."""
    period = config.daemon.inverter_poll_seconds
    while True:
        try:
            async with state.inverter_lock:
                reading = await asyncio.to_thread(read_inverter, config.givenergy)
            state.latest_reading = reading
            state.history.append(reading)
            logger.debug(
                "Poll: SoC=%.1f%% solar=%.0fW load=%.0fW",
                reading.soc_pct, reading.solar_w, reading.load_w,
            )
        except Exception as exc:
            logger.warning("Inverter poll failed: %s", exc)
        await asyncio.sleep(period)


async def _poll_ecodan_loop(config: AppConfig, state: DaemonState) -> None:
    """Poll MELCloud for DHW tank state every ECODAN_POLL_SECONDS."""
    period = config.daemon.ecodan_poll_seconds
    while True:
        try:
            dhw = await _get_dhw_state_async(config.melcloud)
            state.latest_dhw = dhw
            state.latest_dhw_at = datetime.now(timezone.utc)
            logger.debug(
                "DHW poll: mode=%s tank=%.1f°C target=%.1f°C",
                dhw.get("operation_mode"),
                dhw.get("tank_temperature") or float("nan"),
                dhw.get("target_tank_temperature") or float("nan"),
            )
        except Exception as exc:
            logger.warning("DHW poll failed: %s", exc)
        await asyncio.sleep(period)


async def _optimizer_loop(
    config: AppConfig,
    state: DaemonState,
    manage_dhw: bool,
    output: bool,
    dry_run: bool,
) -> None:
    """Run the optimizer tick every _TICK_SECONDS, aligned to the wall clock.

    Holds the inverter lock for the whole tick so the poller can't open a
    competing connection while scheduler.run() is talking to the inverter.
    """
    # First tick runs promptly on startup (no saved schedule → full optimization),
    # then we align to 5-minute boundaries.
    first = True
    while True:
        if first:
            first = False
        else:
            await asyncio.sleep(_seconds_until_next(_TICK_SECONDS))
        try:
            async with state.inverter_lock:
                await asyncio.to_thread(
                    scheduler.run,
                    config,
                    dry_run,
                    manage_dhw,
                    output,
                )
        except Exception as exc:
            logger.exception("Optimizer tick failed: %s", exc)


async def _run(
    config: AppConfig,
    manage_dhw: bool,
    output: bool,
    dry_run: bool,
) -> None:
    state = DaemonState()

    REGISTRY.register(OctooptCollector(state, config.db_path))
    _start_http_server(state, config)
    logger.info(
        "HTTP server listening on :%d — /metrics, /status.json, dashboard at /",
        config.daemon.metrics_port,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - non-unix
            pass

    tasks = [
        asyncio.create_task(_poll_inverter_loop(config, state), name="inverter_poll"),
        asyncio.create_task(_poll_ecodan_loop(config, state), name="ecodan_poll"),
        asyncio.create_task(
            _optimizer_loop(config, state, manage_dhw, output, dry_run),
            name="optimizer",
        ),
    ]
    logger.info(
        "Daemon started — inverter poll %ds, DHW poll %ds, optimizer tick %ds",
        config.daemon.inverter_poll_seconds,
        config.daemon.ecodan_poll_seconds,
        _TICK_SECONDS,
    )

    await stop.wait()
    logger.info("Shutdown signal received — stopping loops")
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="octoopt2 long-running daemon (optimizer + Prometheus metrics)"
    )
    parser.add_argument(
        "--no-dhw",
        action="store_true",
        help="Disable DHW management — Ecodan stays in auto mode, DHW excluded from optimization",
    )
    parser.add_argument(
        "--output",
        action="store_true",
        help="Write optimizer result to web/schedule.json on each tick",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the optimizer each tick but send no commands to hardware (still polls for metrics)",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=None,
        help="Override the Prometheus metrics port (default from METRICS_PORT env / 9876)",
    )
    args = parser.parse_args()

    config = AppConfig.from_env()
    if args.metrics_port is not None:
        config = _with_metrics_port(config, args.metrics_port)

    init_db(config.db_path)
    try:
        asyncio.run(
            _run(
                config,
                manage_dhw=not args.no_dhw,
                output=args.output,
                dry_run=args.dry_run,
            )
        )
    except KeyboardInterrupt:  # pragma: no cover
        pass
    logger.info("Daemon stopped")


def _with_metrics_port(config: AppConfig, port: int) -> AppConfig:
    """Return a copy of config with the metrics port overridden."""
    from dataclasses import replace
    return replace(config, daemon=replace(config.daemon, metrics_port=port))


if __name__ == "__main__":
    main()
