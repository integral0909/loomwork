"""``loomwork workflow ...``: start, inspect and control workflow runs."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from loomwork.cli.backend import Operations, OperationsHandle, open_operations
from loomwork.cli.commands import tools
from loomwork.cli.commands.common import (
    EXECUTION_HEADERS,
    add_app_argument,
    add_target_arguments,
    app_from,
    connected,
    event_details,
    event_summary,
    execution_fields,
    execution_json,
    execution_row,
    handle_for,
    open_session,
    store_handle_for,
)
from loomwork.cli.errors import CliError, ExitCode, UsageError
from loomwork.cli.output import Console, to_jsonable
from loomwork.cli.parsing import (
    parse_arguments,
    parse_duration,
    parse_json_value,
    parse_key_values,
    read_at_file,
)
from loomwork.client.client import BatchResult
from loomwork.errors import (
    WorkflowCanceledError,
    WorkflowFailedError,
    WorkflowTerminatedError,
    WorkflowTimedOutError,
)
from loomwork.history import history_to_json
from loomwork.options import IdConflictPolicy, IdReusePolicy, ResetPoint
from loomwork.retry import RetryPolicy
from loomwork.store.records import ExecutionStatus

#: Refuse an un-confirmed batch touching more than this many runs.
_DEFAULT_MAX_BATCH_RUNS = 1000

# --------------------------------------------------------------------------- helpers


def _arguments(args: argparse.Namespace, console: Console) -> list[Any]:
    values: list[str] = list(args.args)
    stdin_text = console.read_stdin() if any(v == "@-" for v in values) else None
    return parse_arguments(values, raw_strings=args.raw, stdin_text=stdin_text)


async def _await_result(handle: OperationsHandle, timeout: float | None, console: Console) -> int:
    """Wait for ``handle`` and print its outcome; the exit code says how it closed."""
    try:
        value = await handle.result(timeout=timeout)
    except TimeoutError:
        raise CliError(f"the run did not close within {timeout:g}s") from None
    except WorkflowFailedError as exc:
        failure = exc.failure
        if console.json_output:
            console.json({"status": "failed", "failure": failure})
        else:
            console.print(f"failed: {failure.type}: {failure.message}")
        return ExitCode.FAILURE
    except WorkflowCanceledError:
        _closed(console, "canceled", "")
        return ExitCode.FAILURE
    except (WorkflowTerminatedError, WorkflowTimedOutError) as exc:
        status = "terminated" if isinstance(exc, WorkflowTerminatedError) else "timed_out"
        _closed(console, status, str(exc))
        return ExitCode.FAILURE
    if console.json_output:
        console.json({"status": "completed", "result": value})
    else:
        console.json(value)
    return ExitCode.OK


def _closed(console: Console, status: str, reason: str) -> None:
    if console.json_output:
        console.json({"status": status, "reason": reason})
    else:
        console.print(f"{status}{': ' + reason if reason else ''}")


# --------------------------------------------------------------------------- handlers


async def start(args: argparse.Namespace, console: Console) -> int:
    positional = _arguments(args, console)
    attributes = parse_key_values(args.attr or [], what="--attr")
    retry = None
    if args.retry_attempts is not None:
        retry = RetryPolicy(max_attempts=args.retry_attempts)
    async with open_operations(args) as ops:
        handle = await ops.start(
            args.workflow,
            *positional,
            id=args.id,
            search_attributes=attributes or None,
            run_timeout=parse_duration(args.run_timeout) if args.run_timeout else None,
            execution_timeout=(
                parse_duration(args.execution_timeout) if args.execution_timeout else None
            ),
            retry_policy=retry,
            id_reuse_policy=IdReusePolicy(args.id_reuse_policy),
            id_conflict_policy=IdConflictPolicy(args.id_conflict_policy),
        )
        if not args.wait:
            if console.json_output:
                console.json({"workflow_id": handle.workflow_id, "run_id": handle.run_id})
            else:
                console.print(f"started {handle.workflow_id} (run {handle.run_id})")
            return ExitCode.OK
        console.note(f"started {handle.workflow_id} (run {handle.run_id}); waiting")
        timeout = parse_duration(args.wait_timeout).total_seconds() if args.wait_timeout else None
        return await _await_result(handle, timeout, console)


async def list_runs(args: argparse.Namespace, console: Console) -> int:
    async with open_operations(args) as ops:
        if args.query:
            if args.workflow or args.status:
                raise UsageError("--query cannot be combined with --workflow or --status")
            records = await ops.query_runs(args.query, limit=args.limit)
        else:
            status = ExecutionStatus(args.status) if args.status else None
            records = await ops.list(workflow=args.workflow, status=status, limit=args.limit)
        console.table(
            EXECUTION_HEADERS,
            [execution_row(r) for r in records],
            records=[execution_json(r, ops.converter) for r in records],
            empty="no runs found",
        )
    return ExitCode.OK


async def describe(args: argparse.Namespace, console: Console) -> int:
    async with open_operations(args) as ops:
        handle = handle_for(args, ops)
        record = await handle.describe()
        fields = execution_fields(record, ops.converter)
        attributes = await handle.search_attributes()
        for key in sorted(attributes):
            fields.append((f"attr.{key}", attributes[key]))
        data = execution_json(record, ops.converter)
        data["search_attributes"] = to_jsonable(attributes)
        console.fields(fields, record=data)
    return ExitCode.OK


async def history(args: argparse.Namespace, console: Console) -> int:
    async with open_operations(args) as ops:
        handle = handle_for(args, ops)
        events = await handle.history()
        converter = ops.converter
    if args.output:
        Path(args.output).write_text(history_to_json(events), encoding="utf-8")
        console.note(f"wrote {len(events)} events to {args.output}")
        return ExitCode.OK
    if args.export:
        console.print(history_to_json(events).rstrip("\n"))
        return ExitCode.OK
    rows = [
        [e.event_id, e.timestamp, e.kind, event_summary(e, converter)]
        for e in events
        if args.kind is None or e.kind in args.kind
    ]
    records = [
        {
            "event_id": e.event_id,
            "timestamp": e.timestamp,
            "kind": e.kind,
            **event_details(e, converter),
        }
        for e in events
        if args.kind is None or e.kind in args.kind
    ]
    console.table(
        ["id", "time", "kind", "details"], rows, records=records, empty="no events", full_last=True
    )
    return ExitCode.OK


async def result(args: argparse.Namespace, console: Console) -> int:
    async with open_operations(args) as ops:
        handle = handle_for(args, ops)
        if not args.wait:
            record = await handle.describe()
            if not record.is_closed:
                raise CliError(
                    f"run {record.run_id} is still running (use --wait to wait for it)",
                    ExitCode.FAILURE,
                )
        timeout = parse_duration(args.timeout_after).total_seconds() if args.timeout_after else None
        return await _await_result(handle, timeout, console)


async def signal(args: argparse.Namespace, console: Console) -> int:
    stdin_text = console.read_stdin() if args.value == "@-" else None
    text = read_at_file(args.value, stdin_text)
    value = text if args.raw else parse_json_value(text, what="signal value")
    async with open_operations(args) as ops:
        handle = handle_for(args, ops)
        await handle.signal(args.channel, value, message_id=args.message_id)
    console.note(f"sent to channel {args.channel!r}")
    return ExitCode.OK


async def cancel(args: argparse.Namespace, console: Console) -> int:
    async with open_operations(args) as ops:
        handle = handle_for(args, ops)
        changed = await handle.cancel(args.reason)
    console.note(
        "cancellation requested" if changed else "the run was already closed or cancelling"
    )
    return ExitCode.OK if changed else ExitCode.FAILURE


async def terminate(args: argparse.Namespace, console: Console) -> int:
    async with open_operations(args) as ops:
        handle = handle_for(args, ops)
        changed = await handle.terminate(args.reason)
    console.note("terminated" if changed else "the run was already closed")
    return ExitCode.OK if changed else ExitCode.FAILURE


async def reset(args: argparse.Namespace, console: Console) -> int:
    # A required mutually-exclusive group already guarantees exactly one of these.
    to = (
        ResetPoint.after_tick(args.after_tick)
        if args.after_tick is not None
        else (
            ResetPoint.first_tick() if args.to == "first-tick" else ResetPoint.before_last_failure()
        )
    )
    async with open_operations(args) as ops:
        handle = handle_for(args, ops)
        new_handle = await handle.reset(to=to, reason=args.reason)
        if not args.wait:
            if console.json_output:
                console.json({"workflow_id": new_handle.workflow_id, "run_id": new_handle.run_id})
            else:
                console.print(f"reset to {new_handle.workflow_id} (run {new_handle.run_id})")
            return ExitCode.OK
        console.note(f"reset to {new_handle.workflow_id} (run {new_handle.run_id}); waiting")
        timeout = parse_duration(args.wait_timeout).total_seconds() if args.wait_timeout else None
        return await _await_result(new_handle, timeout, console)


async def steps(args: argparse.Namespace, console: Console) -> int:
    async with open_session(args) as active:
        handle = store_handle_for(args, active)
        jobs = await handle.pending_steps()
    rows = [
        [
            job.seq,
            job.step_name,
            job.queue,
            job.attempt,
            job.ready_at,
            job.lease_owner,
            f"{job.last_failure.type}: {job.last_failure.message}" if job.last_failure else None,
        ]
        for job in jobs
    ]
    console.table(
        ["seq", "step", "queue", "attempt", "ready at", "worker", "last failure"],
        rows,
        records=[{**to_jsonable(job), "args": None} for job in jobs],
        empty="no pending steps",
    )
    return ExitCode.OK


async def children(args: argparse.Namespace, console: Console) -> int:
    async with open_operations(args) as ops:
        handle = handle_for(args, ops)
        records = await handle.children()
        console.table(
            EXECUTION_HEADERS,
            [execution_row(r) for r in records],
            records=[execution_json(r, ops.converter) for r in records],
            empty="no child workflows",
        )
    return ExitCode.OK


async def query(args: argparse.Namespace, console: Console) -> int:
    app = app_from(args)
    positional = _arguments(args, console)
    async with open_session(args, app) as active:
        handle = store_handle_for(args, active)
        value = await handle.query(args.name, *positional)
    console.json(value)
    return ExitCode.OK


async def update(args: argparse.Namespace, console: Console) -> int:
    positional = _arguments(args, console)
    timeout = parse_duration(args.wait_timeout).total_seconds() if args.wait_timeout else None
    async with open_operations(args) as ops:
        handle = handle_for(args, ops)
        try:
            value = await handle.update(
                args.name, *positional, timeout=timeout, update_id=args.update_id
            )
        except TimeoutError:
            raise CliError(
                "no outcome yet; the update may still run. Repeat with the same --update-id "
                "to keep waiting"
            ) from None
    console.json(value)
    return ExitCode.OK


# --------------------------------------------------------------------------- batch


def _batch_result_summary(result: BatchResult) -> dict[str, Any]:
    return {
        "matched": result.matched,
        "succeeded": result.succeeded,
        "skipped_closed": result.skipped_closed,
        "failed": [{"run_id": f.run_id, "error": f.error} for f in result.failed],
    }


def _print_batch_result(console: Console, result: BatchResult) -> int:
    if console.json_output:
        console.json(_batch_result_summary(result))
    else:
        console.print(
            f"matched {result.matched}, succeeded {result.succeeded}, "
            f"skipped (already closed) {result.skipped_closed}, failed {len(result.failed)}"
        )
        for failure in result.failed:
            console.print(f"  {failure.run_id}: {failure.error}")
    return ExitCode.FAILURE if result.failed else ExitCode.OK


async def _confirm_batch(args: argparse.Namespace, console: Console, ops: Operations) -> bool:
    """Preview the matches and enforce the ``--max-runs``/``--yes`` guard.

    Returns ``True`` when the caller (``--dry-run``) should stop here having only shown
    the preview, ``False`` when it is clear to go ahead with the real operation.
    """
    preview = await ops.query_runs(args.query, limit=args.max_runs + 1)
    if args.dry_run:
        shown = preview[: args.max_runs]
        console.table(
            EXECUTION_HEADERS,
            [execution_row(r) for r in shown],
            records=[execution_json(r, ops.converter) for r in shown],
            empty="no runs match",
        )
        if len(preview) > args.max_runs:
            console.note(f"and more (showing only the first {args.max_runs})")
        else:
            console.note(f"{len(preview)} run(s) match; dry run, nothing was done")
        return True
    if len(preview) > args.max_runs and not args.yes:
        raise UsageError(
            f"this query matches more than --max-runs {args.max_runs} run(s); pass --yes "
            "to proceed anyway, narrow --query, or raise --max-runs"
        )
    return False


async def batch_signal(args: argparse.Namespace, console: Console) -> int:
    stdin_text = console.read_stdin() if args.value == "@-" else None
    text = read_at_file(args.value, stdin_text)
    value = text if args.raw else parse_json_value(text, what="signal value")
    async with open_operations(args) as ops:
        if await _confirm_batch(args, console, ops):
            return ExitCode.OK
        result = await ops.batch_signal(
            args.query,
            args.channel,
            value,
            message_id=args.message_id,
            page_size=args.page_size,
            concurrency=args.concurrency,
        )
    return _print_batch_result(console, result)


async def batch_cancel(args: argparse.Namespace, console: Console) -> int:
    async with open_operations(args) as ops:
        if await _confirm_batch(args, console, ops):
            return ExitCode.OK
        result = await ops.batch_cancel(
            args.query, args.reason, page_size=args.page_size, concurrency=args.concurrency
        )
    return _print_batch_result(console, result)


async def batch_terminate(args: argparse.Namespace, console: Console) -> int:
    async with open_operations(args) as ops:
        if await _confirm_batch(args, console, ops):
            return ExitCode.OK
        result = await ops.batch_terminate(
            args.query, args.reason, page_size=args.page_size, concurrency=args.concurrency
        )
    return _print_batch_result(console, result)


# --------------------------------------------------------------------------- parser


def _add_argument_options(parser: argparse.ArgumentParser, what: str) -> None:
    parser.add_argument(
        "args",
        nargs="*",
        metavar="ARG",
        help=f"{what} arguments as JSON values, or @file.json, or @- for stdin",
    )
    parser.add_argument(
        "--raw", action="store_true", help="pass arguments as plain strings, not JSON"
    )


def register(subparsers: Any) -> None:
    parser = subparsers.add_parser(
        "workflow", aliases=["wf"], help="start, inspect and control workflow runs"
    )
    commands = parser.add_subparsers(dest="workflow_command", metavar="COMMAND", required=True)

    p = commands.add_parser("start", help="start a run")
    p.add_argument("workflow", help="registered workflow name")
    _add_argument_options(p, "workflow")
    p.add_argument("--id", help="workflow id (default: generated)")
    p.add_argument(
        "--attr", action="append", metavar="KEY=VALUE", help="search attribute (repeatable)"
    )
    p.add_argument("--run-timeout", metavar="DURATION", help="close the run after this long")
    p.add_argument(
        "--execution-timeout",
        metavar="DURATION",
        help="bound the whole workflow id, across retries and continue-as-new",
    )
    p.add_argument(
        "--retry-attempts", type=int, metavar="N", help="retry a failed run up to N attempts"
    )
    p.add_argument(
        "--id-reuse-policy",
        choices=[policy.value for policy in IdReusePolicy],
        default=IdReusePolicy.ALLOW_DUPLICATE.value,
        help="what a closed run of this id allows (default: %(default)s)",
    )
    p.add_argument(
        "--id-conflict-policy",
        choices=[policy.value for policy in IdConflictPolicy],
        default=IdConflictPolicy.FAIL.value,
        help="what a currently-running run of this id allows (default: %(default)s)",
    )
    p.add_argument("--wait", action="store_true", help="wait for the result and print it")
    p.add_argument(
        "--wait-timeout", metavar="DURATION", help="with --wait: give up after this long"
    )
    connected(p, start)

    p = commands.add_parser("list", aliases=["ls"], help="list runs")
    p.add_argument("--workflow", help="only runs of this workflow")
    p.add_argument("--status", choices=[s.value for s in ExecutionStatus])
    p.add_argument(
        "--query", "-q", help="visibility query, e.g. \"status = 'failed' AND attr.tier = 'gold'\""
    )
    p.add_argument("--limit", type=int, default=50)
    connected(p, list_runs)

    p = commands.add_parser("describe", aliases=["show"], help="show one run")
    add_target_arguments(p)
    connected(p, describe)

    p = commands.add_parser("history", help="show or export a run's event history")
    add_target_arguments(p)
    p.add_argument("--kind", action="append", help="only events of this kind (repeatable)")
    p.add_argument("--export", action="store_true", help="print the exportable JSON document")
    p.add_argument("--output", "-o", metavar="FILE", help="write the exportable JSON document")
    connected(p, history)

    p = commands.add_parser("result", help="print a closed run's result")
    add_target_arguments(p)
    p.add_argument("--wait", action="store_true", help="wait for a running run to close")
    p.add_argument("--wait-timeout", dest="timeout_after", metavar="DURATION")
    connected(p, result)

    p = commands.add_parser("signal", help="send a message to a channel of a run")
    p.add_argument("workflow_id", help="workflow id")
    p.add_argument("channel", help="channel name")
    p.add_argument("value", help="JSON value, @file.json or @-")
    p.add_argument("--run", dest="run_id", metavar="RUN_ID")
    p.add_argument("--message-id", help="makes delivery idempotent")
    p.add_argument("--raw", action="store_true", help="send the value as a plain string")
    connected(p, signal)

    p = commands.add_parser("cancel", help="ask a run to cancel")
    add_target_arguments(p)
    p.add_argument("--reason", default="", help="recorded with the request")
    connected(p, cancel)

    p = commands.add_parser("terminate", help="stop a run immediately, without cleanup")
    add_target_arguments(p)
    p.add_argument("--reason", required=True, help="recorded in the history")
    connected(p, terminate)

    p = commands.add_parser(
        "reset", help="truncate a run's history at a tick and start a new run from there"
    )
    add_target_arguments(p)
    point = p.add_mutually_exclusive_group(required=True)
    point.add_argument(
        "--to",
        choices=["first-tick", "before-last-failure"],
        help="a named reset point (see docs/operations/recovery.md)",
    )
    point.add_argument(
        "--after-tick", type=int, metavar="N", help="keep the first N completed ticks"
    )
    p.add_argument("--reason", required=True, help="why (recorded on the old and new run)")
    p.add_argument("--wait", action="store_true", help="wait for the new run's result and print it")
    p.add_argument(
        "--wait-timeout", metavar="DURATION", help="with --wait: give up after this long"
    )
    connected(p, reset)

    p = commands.add_parser("steps", help="list a run's pending step jobs")
    add_target_arguments(p)
    connected(p, steps)

    p = commands.add_parser("children", help="list a run's child workflows")
    add_target_arguments(p)
    connected(p, children)

    p = commands.add_parser("query", help="ask a run a read-only question (needs --app)")
    p.add_argument("workflow_id", help="workflow id")
    p.add_argument("name", help="query name")
    _add_argument_options(p, "query")
    p.add_argument("--run", dest="run_id", metavar="RUN_ID")
    add_app_argument(p, required=True)
    connected(p, query)

    p = commands.add_parser("update", help="run an update handler in a run and print its result")
    p.add_argument("workflow_id", help="workflow id")
    p.add_argument("name", help="update name")
    _add_argument_options(p, "update")
    p.add_argument("--run", dest="run_id", metavar="RUN_ID")
    p.add_argument("--update-id", help="makes the request idempotent")
    p.add_argument("--wait-timeout", metavar="DURATION", help="give up waiting after this long")
    connected(p, update)

    batch = commands.add_parser(
        "batch", help="signal, cancel or terminate every run a query matches"
    )
    batch_commands = batch.add_subparsers(dest="batch_command", metavar="COMMAND", required=True)

    def _add_batch_options(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--query",
            "-q",
            required=True,
            help="visibility query selecting the runs to act on (a plain filter: no "
            "ORDER BY or LIMIT of its own)",
        )
        p.add_argument("--page-size", type=int, default=100, help="runs fetched per query page")
        p.add_argument("--concurrency", type=int, default=10, help="operations in flight at once")
        p.add_argument(
            "--dry-run", action="store_true", help="list matching runs without acting on them"
        )
        p.add_argument(
            "--max-runs",
            type=int,
            default=_DEFAULT_MAX_BATCH_RUNS,
            help="refuse to act on more than this many runs without --yes (default: %(default)s)",
        )
        p.add_argument(
            "--yes", action="store_true", help="proceed even if more than --max-runs runs match"
        )

    p = batch_commands.add_parser("signal", help="send a message to a channel of every match")
    p.add_argument("channel", help="channel name")
    p.add_argument("value", help="JSON value, @file.json or @-")
    p.add_argument("--message-id", help="makes delivery idempotent")
    p.add_argument("--raw", action="store_true", help="send the value as a plain string")
    _add_batch_options(p)
    connected(p, batch_signal)

    p = batch_commands.add_parser("cancel", help="ask every match to cancel")
    p.add_argument("--reason", default="", help="recorded with the request")
    _add_batch_options(p)
    connected(p, batch_cancel)

    p = batch_commands.add_parser("terminate", help="terminate every match immediately")
    p.add_argument("--reason", required=True, help="recorded in the history")
    _add_batch_options(p)
    connected(p, batch_terminate)

    tools.register_graph(commands)


__all__ = ["register"]
