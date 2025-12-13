"""Long-running commands: ``loomwork worker`` and ``loomwork server``."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
from collections.abc import Iterator
from typing import Any

from loomwork.cli.commands.common import add_app_argument, open_session
from loomwork.cli.errors import ExitCode, UsageError
from loomwork.cli.loader import load_app
from loomwork.cli.output import Console
from loomwork.cli.parsing import parse_duration
from loomwork.worker.limits import QueueLimit
from loomwork.worker.worker import Worker, WorkerOptions

logger = logging.getLogger("loomwork.cli")


@contextlib.contextmanager
def shutdown_signals(stop: asyncio.Event) -> Iterator[None]:
    """Set ``stop`` on SIGINT or SIGTERM for the duration of the block.

    A second signal while shutting down is left to the default handler, so an operator
    can still force an exit when graceful shutdown hangs.
    """
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []

    def request_stop(sig: signal.Signals) -> None:
        logger.info("received %s, shutting down", sig.name)
        stop.set()
        for installed_sig in installed:
            loop.remove_signal_handler(installed_sig)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_stop, sig)
        except (NotImplementedError, RuntimeError):  # pragma: no cover - Windows, threads
            continue
        installed.append(sig)
    try:
        yield
    finally:
        for sig in installed:
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(sig)


def parse_queue_limit(text: str) -> tuple[str, QueueLimit]:
    """``QUEUE:rate=5,burst=10,concurrency=2`` (any subset of the three settings)."""
    queue, sep, settings = text.partition(":")
    if not sep or not queue or not settings:
        raise UsageError(f"--queue-limit must look like QUEUE:rate=5,concurrency=2, got {text!r}")
    values: dict[str, float] = {}
    for item in settings.split(","):
        key, eq, raw = item.partition("=")
        key = key.strip()
        if not eq or key not in {"rate", "burst", "concurrency"}:
            raise UsageError(f"unknown queue limit setting {item!r} (use rate, burst, concurrency)")
        try:
            values[key] = float(raw)
        except ValueError:
            raise UsageError(f"queue limit {key} must be a number, got {raw!r}") from None
    try:
        limit = QueueLimit(
            rate=values.get("rate"),
            burst=values.get("burst"),
            concurrency=int(values["concurrency"]) if "concurrency" in values else None,
        )
    except ValueError as exc:
        raise UsageError(f"invalid queue limit for {queue!r}: {exc}") from None
    return queue, limit


def worker_options(args: argparse.Namespace) -> WorkerOptions:
    limits = dict(parse_queue_limit(t) for t in args.queue_limit or [])
    queues = tuple(args.queue) if args.queue else None
    if limits and queues is None:
        queues = tuple(sorted({"default", *limits}))
    elif limits and queues is not None:
        unknown = sorted(set(limits) - set(queues))
        if unknown:
            raise UsageError(f"--queue-limit for queues not in --queue: {', '.join(unknown)}")
    try:
        return WorkerOptions(
            workflow_concurrency=args.workflow_concurrency,
            step_concurrency=args.step_concurrency,
            run_timers=not args.no_timers,
            run_outbox=not args.no_outbox,
            run_schedules=not args.no_schedules,
            shutdown_grace=parse_duration(args.shutdown_grace).total_seconds(),
            task_queues=queues,
            queue_limits=limits,
            build_id=args.build_id,
        )
    except ValueError as exc:
        raise UsageError(str(exc)) from None


async def run_worker(args: argparse.Namespace, console: Console) -> int:
    app = load_app(args.app)
    options = worker_options(args)
    telemetry: Any = None
    metrics_server = None
    if args.metrics_port is not None:
        from loomwork.observability import MetricsServer, MetricsTelemetry

        metrics = MetricsTelemetry()
        telemetry = metrics
        metrics_server = MetricsServer(metrics.registry, args.metrics_host, args.metrics_port)
    stop = asyncio.Event()
    async with open_session(args, app) as active:
        worker = Worker(
            active.store,
            app.registry,
            clock=getattr(active.store, "clock", None),
            converter=app.converter,
            options=options,
            worker_id=args.worker_id,
            telemetry=telemetry,
        )
        if metrics_server is not None:
            metrics_server.ready = lambda: worker.running
            await metrics_server.start()
        try:
            with shutdown_signals(stop):
                await worker.start()
                console.note(
                    f"worker {worker.worker_id} running {len(app.workflow_names)} workflow(s) "
                    f"and {len(app.step_names)} step(s) against {active.options.target}"
                )
                await stop.wait()
                console.note(
                    "stopping; in-flight work gets " f"{options.shutdown_grace:g}s to finish"
                )
                await worker.stop()
        finally:
            if worker.running:
                await worker.stop(grace=0)
            if metrics_server is not None:
                await metrics_server.stop()
    return ExitCode.OK


async def run_server(args: argparse.Namespace, console: Console) -> int:
    from loomwork.server.app import serve
    from loomwork.server.config import load_config

    try:
        config = load_config(args.config)
    except FileNotFoundError:
        raise UsageError(f"config file not found: {args.config}") from None
    except ValueError as exc:
        raise UsageError(f"invalid server config {args.config}: {exc}") from None
    updates: dict[str, Any] = {}
    if args.host is not None:
        updates["host"] = args.host
    if args.port is not None:
        updates["port"] = args.port
    if updates:
        config = config.model_copy(update=updates)
    if not config.namespaces:
        raise UsageError("the server config defines no namespaces")
    console.note(
        f"serving {len(config.namespaces)} namespace(s) on http://{config.host}:{config.port}"
    )
    await serve(config)
    return ExitCode.OK


def register(subparsers: Any) -> None:
    from loomwork.cli.connection import add_connection_arguments

    p = subparsers.add_parser(
        "worker",
        help="run a worker for the workflows and steps in --app",
        description="Execute workflow ticks, step attempts, timers, outbox relaying and "
        "schedules until SIGINT or SIGTERM, then shut down gracefully.",
    )
    add_app_argument(p, required=True)
    p.add_argument("--worker-id", help="identifies this worker in leases and logs")
    p.add_argument(
        "--build-id",
        metavar="ID",
        help="this worker's build: pinned runs are only processed by matching or declared "
        "compatible builds (default: unversioned; see docs/guides/versioning.md)",
    )
    p.add_argument("--workflow-concurrency", type=int, default=4, metavar="N")
    p.add_argument("--step-concurrency", type=int, default=16, metavar="N")
    p.add_argument(
        "--queue",
        action="append",
        metavar="NAME",
        help="task queue to serve (repeatable; default: all)",
    )
    p.add_argument(
        "--queue-limit",
        action="append",
        metavar="QUEUE:SETTINGS",
        help="per-queue limit, e.g. payments:rate=5,burst=10,concurrency=2 (repeatable)",
    )
    p.add_argument("--no-timers", action="store_true", help="do not fire timers")
    p.add_argument("--no-outbox", action="store_true", help="do not relay cross-run messages")
    p.add_argument("--no-schedules", action="store_true", help="do not evaluate schedules")
    p.add_argument("--shutdown-grace", default="10s", metavar="DURATION")
    p.add_argument(
        "--metrics-port", type=int, metavar="PORT", help="serve Prometheus metrics on this port"
    )
    p.add_argument("--metrics-host", default="127.0.0.1", metavar="HOST")
    add_connection_arguments(p)
    p.set_defaults(handler=run_worker)

    p = subparsers.add_parser("server", help="run the Loomwork HTTP server")
    p.add_argument("--config", "-c", required=True, help="server TOML config file")
    p.add_argument("--host", help="override the configured listen address")
    p.add_argument("--port", type=int, help="override the configured port")
    p.set_defaults(handler=run_server)
