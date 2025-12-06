"""``loomwork doctor``: a quick health check of a store or server.

Every check comes from :mod:`loomwork.diagnostics`, shared with the server's
``GET /control/diagnostics`` endpoint (used when ``--server`` names an operator/reader
token) and the console's diagnostics page, so the three report the same findings in the
same words. ``--store`` talks to the store directly; ``--server`` goes over the control
API and adds two checks a local connection cannot make on its own: clock skew against the
server, and the token's own roles and namespaces.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from typing import Any

from loomwork.cli.commands.common import open_session
from loomwork.cli.connection import ConnectionOptions, StoreUnavailableError
from loomwork.cli.errors import ExitCode
from loomwork.cli.output import Console
from loomwork.cli.parsing import parse_duration
from loomwork.diagnostics import (
    DEFAULT_HISTORY_WARNING_FRACTION,
    DEFAULT_LIMIT,
    DEFAULT_OUTBOX_BACKLOG_THRESHOLD,
    DEFAULT_TICK_FAILURE_THRESHOLD,
    DiagnosticsSnapshot,
    Finding,
    Status,
    build_findings,
    collect_diagnostics,
)


def _render(console: Console, findings: list[Finding]) -> None:
    ok = not any(f.status is Status.FAIL for f in findings)
    if console.json_output:
        console.json({"ok": ok, "findings": findings})
        return
    width = max(len(f.check) for f in findings)
    for finding in findings:
        console.print(
            f"[{finding.status.value.upper():<4}] {finding.check.ljust(width)}  "
            f"{finding.message}"
        )
        if finding.hint and finding.status is not Status.PASS:
            console.print(" " * (width + 9) + f"-> {finding.hint}")
    console.note("all checks passed" if ok else "one or more checks failed")


def _thresholds(args: argparse.Namespace) -> dict[str, float | int]:
    return {
        "outbox_backlog_threshold": args.outbox_backlog_threshold,
        "timer_overdue_seconds": parse_duration(args.timer_overdue).total_seconds(),
        "step_stall_seconds": parse_duration(args.step_stall).total_seconds(),
        "schedule_overdue_seconds": parse_duration(args.schedule_overdue).total_seconds(),
    }


async def _local_snapshot(args: argparse.Namespace) -> DiagnosticsSnapshot:
    async with open_session(args) as active:
        now = active.engine.clock.now()
        return await collect_diagnostics(
            active.store,
            active.engine,
            now,
            tick_failure_threshold=args.tick_failure_threshold,
            history_warning_fraction=args.history_warning_fraction,
            queues=args.queue or (),
            limit=args.limit,
        )


async def _server_diagnosis(
    args: argparse.Namespace, options: ConnectionOptions
) -> tuple[DiagnosticsSnapshot, dict[str, Any]]:
    """``(snapshot, extra)`` where ``extra`` is the ``build_findings`` kwargs only a
    server connection can supply (roles, namespaces, clock skew)."""
    from loomwork.cli.backend import _test_transports
    from loomwork.remote.control import ControlClient

    assert options.server_url is not None
    control = ControlClient(
        options.server_url,
        options.token,
        options.namespace,
        timeout=options.timeout,
        transport=_test_transports.get(options.server_url),
    )
    try:
        await control.open()
        who = await control.whoami()
        snapshot = await control.diagnostics(
            tick_failure_threshold=args.tick_failure_threshold,
            history_warning_fraction=args.history_warning_fraction,
            queues=list(args.queue or ()),
            limit=args.limit,
        )
    except OSError as exc:
        raise StoreUnavailableError(f"cannot reach server {options.server_url}: {exc}") from None
    finally:
        await control.close()
    extra: dict[str, Any] = {"roles": who.roles, "namespaces": who.namespaces}
    if who.server_time is not None:
        extra["clock_skew_seconds"] = (datetime.now(UTC) - who.server_time).total_seconds()
    return snapshot, extra


async def doctor(args: argparse.Namespace, console: Console) -> int:
    options = ConnectionOptions.from_args(args)
    thresholds = _thresholds(args)
    if options.server_url is None:
        snapshot = await _local_snapshot(args)
        findings = build_findings(snapshot, **thresholds)  # type: ignore[arg-type]
    else:
        snapshot, extra = await _server_diagnosis(args, options)
        findings = build_findings(snapshot, **thresholds, **extra)  # type: ignore[arg-type]
    _render(console, findings)
    return ExitCode.FAILURE if any(f.status is Status.FAIL for f in findings) else ExitCode.OK


def register(subparsers: Any) -> None:
    from loomwork.cli.connection import add_connection_arguments

    p = subparsers.add_parser(
        "doctor",
        help="check a deployment's health and print a pass/warn/fail report",
        description="Checks store reachability and schema, outbox backlog, tick "
        "failures, step and timer backlogs, overdue schedules, history size and (in "
        "--server mode) clock skew and token roles. Exits 1 if any check fails.",
    )
    add_connection_arguments(p)
    p.add_argument(
        "--tick-failure-threshold",
        type=int,
        default=DEFAULT_TICK_FAILURE_THRESHOLD,
        metavar="N",
        help=f"consecutive tick failures that count as unhealthy (default: "
        f"{DEFAULT_TICK_FAILURE_THRESHOLD})",
    )
    p.add_argument(
        "--timer-overdue",
        default="5m",
        metavar="DURATION",
        help="how overdue the next timer must be to fail (default: 5m)",
    )
    p.add_argument(
        "--step-stall",
        default="5m",
        metavar="DURATION",
        help="how old the oldest ready step job must be to fail (default: 5m)",
    )
    p.add_argument(
        "--schedule-overdue",
        default="5m",
        metavar="DURATION",
        help="how overdue a schedule's next fire time must be to fail (default: 5m)",
    )
    p.add_argument(
        "--outbox-backlog-threshold",
        type=int,
        default=DEFAULT_OUTBOX_BACKLOG_THRESHOLD,
        metavar="N",
        help=f"outbox messages waiting that count as unhealthy (default: "
        f"{DEFAULT_OUTBOX_BACKLOG_THRESHOLD})",
    )
    p.add_argument(
        "--history-warning-fraction",
        type=float,
        default=DEFAULT_HISTORY_WARNING_FRACTION,
        metavar="F",
        help=f"fraction of the history limit that counts as near it (default: "
        f"{DEFAULT_HISTORY_WARNING_FRACTION})",
    )
    p.add_argument(
        "--queue",
        action="append",
        metavar="NAME",
        help="also check this task queue's backlog specifically (repeatable; the "
        "store-wide backlog is always checked)",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        metavar="N",
        help=f"cap every query this command makes (default: {DEFAULT_LIMIT})",
    )
    p.set_defaults(handler=doctor)
