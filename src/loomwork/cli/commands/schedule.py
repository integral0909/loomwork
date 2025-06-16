"""``loomwork schedule ...``: create and operate schedules."""

from __future__ import annotations

import argparse
from typing import Any

from loomwork.cli.commands.common import connected, decode_payload, open_session
from loomwork.cli.errors import ExitCode, UsageError
from loomwork.cli.output import Console
from loomwork.cli.parsing import format_duration, parse_arguments, parse_duration, parse_timestamp
from loomwork.codec.converter import DataConverter
from loomwork.schedules.model import OverlapPolicy, SchedulePolicy, ScheduleSpec, ScheduleState


def _describe_when(state: ScheduleState) -> str:
    spec = state.spec
    if spec.cron is not None:
        text = f"cron {spec.cron!r}"
        if spec.timezone != "UTC":
            text += f" ({spec.timezone})"
        return text
    text = f"every {format_duration(spec.interval)}"
    if spec.offset:
        text += f" + {format_duration(spec.offset)}"
    return text


def _row(state: ScheduleState) -> list[Any]:
    return [
        state.schedule_id,
        state.action.workflow_name,
        _describe_when(state),
        "paused" if state.paused else "active",
        state.next_fire_at,
        len(state.running),
        state.total_started,
    ]


_HEADERS = ["schedule", "workflow", "when", "state", "next fire", "running", "started"]


def _fields(state: ScheduleState, converter: DataConverter) -> list[tuple[str, Any]]:
    spec, policy = state.spec, state.policy
    fields: list[tuple[str, Any]] = [
        ("schedule", state.schedule_id),
        ("workflow", state.action.workflow_name),
        ("arguments", [decode_payload(p, converter) for p in state.action.args]),
        ("when", _describe_when(state)),
        ("state", "paused" if state.paused else "active"),
        ("note", state.note or None),
        ("overlap", policy.overlap.value),
        ("catch-up window", policy.catchup_window),
        ("pause on failure", policy.pause_on_failure),
    ]
    if spec.jitter:
        fields.append(("jitter", spec.jitter))
    if spec.start_at or spec.end_at:
        fields.append(("active between", f"{spec.start_at or '-'} .. {spec.end_at or '-'}"))
    fields += [
        ("next fire", state.next_fire_at),
        ("last evaluated", state.last_evaluated),
        ("started", state.total_started),
        ("skipped", state.total_skipped),
        ("created", state.created_at),
        ("updated", state.updated_at),
    ]
    for run in state.running:
        fields.append(("running", f"{run.workflow_id} (run {run.run_id}, for {run.nominal_time})"))
    for run in state.recent[-5:]:
        fields.append(("recent", f"{run.nominal_time}  {run.status}  {run.workflow_id}"))
    return fields


def _show(console: Console, state: ScheduleState, converter: DataConverter) -> None:
    console.fields(_fields(state, converter), record=state)


# --------------------------------------------------------------------------- handlers


async def create(args: argparse.Namespace, console: Console) -> int:
    if (args.cron is None) == (args.every is None):
        raise UsageError("give exactly one of --cron and --every")
    stdin_text = console.read_stdin() if "@-" in args.args else None
    positional = parse_arguments(list(args.args), raw_strings=args.raw, stdin_text=stdin_text)
    async with open_session(args) as active:
        handle = await active.client.schedules.create(
            args.schedule_id,
            args.workflow,
            *positional,
            cron=args.cron,
            interval=parse_duration(args.every) if args.every else None,
            timezone=args.timezone,
            start_at=parse_timestamp(args.start_at) if args.start_at else None,
            end_at=parse_timestamp(args.end_at) if args.end_at else None,
            jitter=parse_duration(args.jitter) if args.jitter else 0,
            overlap=args.overlap,
            catchup_window=parse_duration(args.catchup_window),
            pause_on_failure=args.pause_on_failure,
            paused=args.paused,
            note=args.note,
        )
        state = await handle.describe()
        if console.json_output:
            console.json(state)
        else:
            console.print(f"created schedule {state.schedule_id}; next fire {state.next_fire_at}")
    return ExitCode.OK


async def list_schedules(args: argparse.Namespace, console: Console) -> int:
    async with open_session(args) as active:
        states = await active.client.schedules.list(limit=args.limit)
    console.table(_HEADERS, [_row(s) for s in states], records=states, empty="no schedules")
    return ExitCode.OK


async def describe(args: argparse.Namespace, console: Console) -> int:
    async with open_session(args) as active:
        state = await active.client.schedules.get(args.schedule_id).describe()
        _show(console, state, active.converter)
    return ExitCode.OK


async def pause(args: argparse.Namespace, console: Console) -> int:
    async with open_session(args) as active:
        state = await active.client.schedules.get(args.schedule_id).pause(args.note)
        _show(console, state, active.converter)
    return ExitCode.OK


async def unpause(args: argparse.Namespace, console: Console) -> int:
    async with open_session(args) as active:
        state = await active.client.schedules.get(args.schedule_id).unpause(args.note)
        _show(console, state, active.converter)
    return ExitCode.OK


async def trigger(args: argparse.Namespace, console: Console) -> int:
    async with open_session(args) as active:
        state = await active.client.schedules.get(args.schedule_id).trigger()
    latest = state.recent[-1] if state.recent else None
    if console.json_output:
        console.json(latest)
    elif latest is not None:
        console.print(f"started {latest.workflow_id} (run {latest.run_id})")
    return ExitCode.OK


async def backfill(args: argparse.Namespace, console: Console) -> int:
    start = parse_timestamp(args.start)
    end = parse_timestamp(args.end)
    if end <= start:
        raise UsageError("--to must be after --from")
    async with open_session(args) as active:
        handle = active.client.schedules.get(args.schedule_id)
        before = (await handle.describe()).total_started
        state = await handle.backfill(start, end)
    started = state.total_started - before
    if console.json_output:
        console.json({"started": started, "schedule": state})
    else:
        console.print(f"backfill started {started} run(s)")
    return ExitCode.OK


async def upcoming(args: argparse.Namespace, console: Console) -> int:
    async with open_session(args) as active:
        times = await active.client.schedules.get(args.schedule_id).upcoming(args.count)
    console.table(
        ["fire time"], [[t] for t in times], records=list(times), empty="no upcoming fire times"
    )
    return ExitCode.OK


async def update(args: argparse.Namespace, console: Console) -> int:
    if args.cron is not None and args.every is not None:
        raise UsageError("give at most one of --cron and --every")
    async with open_session(args) as active:
        handle = active.client.schedules.get(args.schedule_id)
        current = await handle.describe()
        spec = None
        if any(v is not None for v in (args.cron, args.every, args.timezone, args.jitter)):
            old = current.spec
            cron = args.cron if args.cron is not None else (None if args.every else old.cron)
            interval = (
                parse_duration(args.every)
                if args.every is not None
                else (None if args.cron else old.interval)
            )
            spec = ScheduleSpec(
                cron=cron,
                interval=interval,
                offset=old.offset if interval == old.interval else 0,
                timezone=args.timezone or old.timezone,
                start_at=old.start_at,
                end_at=old.end_at,
                jitter=parse_duration(args.jitter) if args.jitter is not None else old.jitter,
            )
        policy = None
        if any(v is not None for v in (args.overlap, args.catchup_window, args.pause_on_failure)):
            old_policy = current.policy
            policy = SchedulePolicy(
                args.overlap or old_policy.overlap,
                (
                    parse_duration(args.catchup_window)
                    if args.catchup_window is not None
                    else old_policy.catchup_window
                ),
                (
                    args.pause_on_failure
                    if args.pause_on_failure is not None
                    else old_policy.pause_on_failure
                ),
            )
        if spec is None and policy is None and args.note is None:
            raise UsageError("nothing to change")
        state = await handle.update(spec=spec, policy=policy, note=args.note)
        _show(console, state, active.converter)
    return ExitCode.OK


async def delete(args: argparse.Namespace, console: Console) -> int:
    async with open_session(args) as active:
        deleted = await active.client.schedules.get(args.schedule_id).delete()
    if not deleted:
        raise UsageError(f"schedule {args.schedule_id!r} does not exist")
    console.note(f"deleted schedule {args.schedule_id}")
    return ExitCode.OK


# --------------------------------------------------------------------------- parser


def _schedule_id(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("schedule_id", help="schedule id")


def _bool_flag(parser: argparse.ArgumentParser, name: str, help: str) -> None:
    group = parser.add_mutually_exclusive_group()
    dest = name.replace("-", "_")
    group.add_argument(f"--{name}", dest=dest, action="store_true", default=None, help=help)
    group.add_argument(f"--no-{name}", dest=dest, action="store_false", default=None)


def register(subparsers: Any) -> None:
    parser = subparsers.add_parser(
        "schedule", aliases=["sched"], help="create and operate schedules"
    )
    commands = parser.add_subparsers(dest="schedule_command", metavar="COMMAND", required=True)
    overlaps = [p.value for p in OverlapPolicy]

    p = commands.add_parser("create", help="create a schedule")
    _schedule_id(p)
    p.add_argument("workflow", help="workflow name to start")
    p.add_argument("args", nargs="*", metavar="ARG", help="workflow arguments as JSON values")
    p.add_argument("--raw", action="store_true", help="pass arguments as plain strings")
    p.add_argument("--cron", help="cron expression, e.g. '0 9 * * MON-FRI'")
    p.add_argument("--every", metavar="DURATION", help="fixed interval, e.g. 15m")
    p.add_argument("--timezone", "--tz", default="UTC", help="IANA zone for --cron")
    p.add_argument("--start-at", metavar="TIME", help="no fire times before this")
    p.add_argument("--end-at", metavar="TIME", help="no fire times after this")
    p.add_argument("--jitter", metavar="DURATION", help="spread each start by up to this much")
    p.add_argument("--overlap", choices=overlaps, default=OverlapPolicy.SKIP.value)
    p.add_argument("--catchup-window", metavar="DURATION", default="1m")
    p.add_argument("--pause-on-failure", action="store_true")
    p.add_argument("--paused", action="store_true", help="create the schedule paused")
    p.add_argument("--note", default="")
    connected(p, create)

    p = commands.add_parser("list", aliases=["ls"], help="list schedules")
    p.add_argument("--limit", type=int, default=100)
    connected(p, list_schedules)

    p = commands.add_parser("describe", aliases=["show"], help="show a schedule")
    _schedule_id(p)
    connected(p, describe)

    for name, handler, text in (
        ("pause", pause, "stop starting runs"),
        ("unpause", unpause, "resume starting runs"),
    ):
        p = commands.add_parser(name, help=text)
        _schedule_id(p)
        p.add_argument("--note", default="", help="why (shown by describe)")
        connected(p, handler)

    p = commands.add_parser("trigger", help="start one run now")
    _schedule_id(p)
    connected(p, trigger)

    p = commands.add_parser("backfill", help="start runs for past fire times")
    _schedule_id(p)
    p.add_argument("--from", dest="start", required=True, metavar="TIME")
    p.add_argument("--to", dest="end", default="now", metavar="TIME")
    connected(p, backfill)

    p = commands.add_parser("upcoming", help="print the next fire times")
    _schedule_id(p)
    p.add_argument("--count", "-n", type=int, default=5)
    connected(p, upcoming)

    p = commands.add_parser("update", help="change when or how a schedule fires")
    _schedule_id(p)
    p.add_argument("--cron")
    p.add_argument("--every", metavar="DURATION")
    p.add_argument("--timezone", "--tz")
    p.add_argument("--jitter", metavar="DURATION")
    p.add_argument("--overlap", choices=overlaps)
    p.add_argument("--catchup-window", metavar="DURATION")
    _bool_flag(p, "pause-on-failure", "pause the schedule when a run fails")
    p.add_argument("--note")
    connected(p, update)

    p = commands.add_parser("delete", aliases=["rm"], help="delete a schedule (its runs continue)")
    _schedule_id(p)
    connected(p, delete)
