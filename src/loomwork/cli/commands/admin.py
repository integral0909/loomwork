"""Operator commands: ``store``, ``compat``, ``lint`` and ``cron``."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loomwork.cli.commands.common import (
    EXECUTION_HEADERS,
    add_app_argument,
    connected,
    execution_row,
    open_session,
)
from loomwork.cli.errors import ExitCode, UsageError
from loomwork.cli.loader import load_app
from loomwork.cli.output import Console, render_table
from loomwork.cli.parsing import parse_duration, parse_timestamp
from loomwork.schedules.cron import CronError, parse_cron
from loomwork.store.records import ExecutionStatus
from loomwork.versioning.compat import RunCheck, check_compatibility, check_history_file

# --------------------------------------------------------------------------- store

#: Runs counted per status by ``store info`` before it reports "N+".
_COUNT_LIMIT = 10_000


async def store_migrate(args: argparse.Namespace, console: Console) -> int:
    """Opening a store applies its pending migrations; report where that left it."""
    async with open_session(args) as active:
        version = getattr(active.store, "schema_version", None)
        if version is None:
            console.note(f"{active.options.target} has no schema to migrate")
            return ExitCode.OK
        current = await version()
    if console.json_output:
        console.json({"schema_version": current})
    else:
        console.print(f"schema is at version {current}")
    return ExitCode.OK


async def store_info(args: argparse.Namespace, console: Console) -> int:
    async with open_session(args) as active:
        store = active.store
        fields: list[tuple[str, Any]] = [
            ("store", active.options.target),
            ("type", type(store).__name__),
        ]
        version = getattr(store, "schema_version", None)
        if version is not None:
            fields.append(("schema version", await version()))
        for status in (ExecutionStatus.RUNNING, ExecutionStatus.FAILED):
            runs = await store.list_executions(status=status, limit=_COUNT_LIMIT)
            count = f"{len(runs)}+" if len(runs) >= _COUNT_LIMIT else len(runs)
            fields.append((f"{status.value} runs", count))
        fields.append(("outbox backlog", await store.outbox_size()))
        fields.append(("next timer", await store.next_timer_at()))
        schedules = await store.list_schedules(1000)
        fields.append(("schedules", len(schedules)))
    console.fields(fields)
    return ExitCode.OK


async def store_prune(args: argparse.Namespace, console: Console) -> int:
    age = parse_duration(args.older_than)
    if age.total_seconds() <= 0:
        raise UsageError("--older-than must be positive")
    cutoff = datetime.now(UTC) - age
    async with open_session(args) as active:
        if args.dry_run:
            query = f"closed_at < timestamp '{cutoff.isoformat()}' ORDER BY closed_at ASC"
            records = await active.engine.query_executions(query, args.batch)
            if console.json_output:
                console.json({"cutoff": cutoff, "would_delete": len(records), "runs": records})
            else:
                console.table(
                    EXECUTION_HEADERS,
                    [execution_row(r) for r in records],
                    empty="nothing to delete",
                )
                suffix = "+" if len(records) >= args.batch else ""
                console.note(
                    f"{len(records)}{suffix} run(s) closed before {cutoff:%Y-%m-%d %H:%M}Z"
                )
            return ExitCode.OK
        total = 0
        while True:
            deleted = await active.store.delete_closed_before(cutoff, limit=args.batch)
            total += deleted
            if deleted < args.batch or args.once:
                break
    if console.json_output:
        console.json({"cutoff": cutoff, "deleted": total})
    else:
        console.print(f"deleted {total} run(s) closed before {cutoff.isoformat()}")
    return ExitCode.OK


# --------------------------------------------------------------------------- compat


def _check_rows(checks: list[RunCheck]) -> list[list[Any]]:
    return [
        [
            c.run_id,
            c.workflow_name,
            c.status,
            "ok" if c.ok else "BROKEN",
            ",".join(c.code_versions) or None,
            c.error,
        ]
        for c in checks
    ]


async def compat(args: argparse.Namespace, console: Console) -> int:
    app = load_app(args.app)
    if args.history:
        checks = [
            check_history_file(path, app.registry, converter=app.converter) for path in args.history
        ]
        unknown: dict[str, int] = {}
    else:
        async with open_session(args, app) as active:
            report = await check_compatibility(
                active.store,
                app.registry,
                converter=app.converter,
                workflow_names=args.workflow or None,
                running_limit=args.running_limit,
                closed_limit=args.closed_limit,
            )
        checks = list(report.checks)
        unknown = dict(report.unknown_workflows)
    broken = [c for c in checks if not c.ok]
    shown = checks if args.all else broken
    if console.json_output:
        console.json(
            {
                "checked": len(checks),
                "broken": len(broken),
                "unknown_workflows": unknown,
                "runs": shown,
            }
        )
    else:
        if shown:
            console.print(
                render_table(
                    ["run id", "workflow", "status", "result", "code versions", "error"],
                    _check_rows(shown),
                )
            )
        console.note(
            f"checked {len(checks)} run(s): {len(checks) - len(broken)} ok, {len(broken)} broken"
        )
        for name, count in sorted(unknown.items()):
            console.note(f"skipped {count} run(s) of {name!r}: not in {app.reference}")
    return ExitCode.FAILURE if broken else ExitCode.OK


# --------------------------------------------------------------------------- lint


async def lint(args: argparse.Namespace, _console: Console) -> int:
    from loomwork.lint.cli import main as lint_main

    return lint_main(args.lint_args, prog="loomwork lint")


# --------------------------------------------------------------------------- cron


async def cron(args: argparse.Namespace, console: Console) -> int:
    try:
        expression = parse_cron(args.expression, args.timezone)
    except CronError as exc:
        raise UsageError(f"invalid cron expression: {exc}") from None
    start = parse_timestamp(args.after) if args.after else datetime.now(UTC)
    times = list(expression.iter_after(start, limit=args.count))
    if console.json_output:
        console.json({"expression": str(expression), "timezone": args.timezone, "next": times})
        return ExitCode.OK
    for moment in times:
        console.print(moment.astimezone(expression.zone).isoformat())
    return ExitCode.OK


# --------------------------------------------------------------------------- parser


def register(subparsers: Any) -> None:
    parser = subparsers.add_parser("store", help="migrate, inspect and prune a store")
    commands = parser.add_subparsers(dest="store_command", metavar="COMMAND", required=True)

    p = commands.add_parser("migrate", help="apply pending schema migrations")
    connected(p, store_migrate)

    p = commands.add_parser("info", help="schema version, backlog and run counts")
    connected(p, store_info)

    p = commands.add_parser("prune", help="delete closed runs older than a cutoff")
    p.add_argument("--older-than", required=True, metavar="DURATION", help="e.g. 30d")
    p.add_argument("--batch", type=int, default=1000, help="runs deleted per transaction")
    p.add_argument("--once", action="store_true", help="delete one batch only")
    p.add_argument("--dry-run", action="store_true", help="list what would be deleted")
    connected(p, store_prune)

    p = subparsers.add_parser(
        "compat",
        help="replay recorded histories with new workflow code",
        description="Replays recent histories from the store (or exported history files) "
        "with the workflow code in --app and reports runs that would fail with a "
        "nondeterminism error. Exits 1 when any run is broken: run it before deploying.",
    )
    add_app_argument(p, required=True)
    p.add_argument(
        "--history",
        action="append",
        type=Path,
        metavar="FILE",
        help="check an exported history file instead of the store (repeatable)",
    )
    p.add_argument("--workflow", action="append", help="only check this workflow (repeatable)")
    p.add_argument("--running-limit", type=int, default=1000)
    p.add_argument("--closed-limit", type=int, default=50, help="closed runs sampled per workflow")
    p.add_argument("--all", action="store_true", help="list passing runs too")
    connected(p, compat)

    # Listed for --help; main() forwards "lint ..." to the linter's own parser before
    # this one ever sees it.
    p = subparsers.add_parser(
        "lint",
        help="check workflow code for non-determinism",
        add_help=False,
        description="Runs the determinism linter; see 'loomwork lint --help'.",
    )
    p.add_argument("lint_args", nargs=argparse.REMAINDER)
    p.set_defaults(handler=lint)

    p = subparsers.add_parser("cron", help="print the next fire times of a cron expression")
    p.add_argument("expression", help="e.g. '*/15 9-17 * * MON-FRI'")
    p.add_argument("--timezone", "--tz", default="UTC")
    p.add_argument("--count", "-n", type=int, default=5)
    p.add_argument("--after", metavar="TIME", help="start from this time (default: now)")
    p.set_defaults(handler=cron)
