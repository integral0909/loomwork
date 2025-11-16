"""``loomwork archive`` and ``loomwork replay``, plus ``workflow graph``.

``archive`` writes closed runs to cheap long-term storage and reads them back; ``replay``
runs a recorded history tick by tick against workflow code and explains the first place
it diverges, if any; ``graph`` renders a run's history as a diagram. See
``docs/operations/archiving.md`` and ``docs/debugging.md``.
"""

from __future__ import annotations

import argparse
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loomwork.archive import (
    ArchiveError,
    ArchiveNotFoundError,
    ArchiveReader,
    archive_and_prune,
)
from loomwork.cli.commands.common import (
    EXECUTION_HEADERS,
    add_app_argument,
    add_target_arguments,
    connected,
    execution_fields,
    execution_json,
    execution_row,
    open_session,
    store_handle_for,
)
from loomwork.cli.errors import CliError, ExitCode, UsageError
from loomwork.cli.loader import load_app
from loomwork.cli.output import Console, render_table, to_jsonable
from loomwork.cli.parsing import parse_duration
from loomwork.codec.converter import default_converter
from loomwork.codec.transform import CodecBoundary
from loomwork.debug import (
    DebugError,
    ReplayTrace,
    render_dot,
    render_mermaid,
    render_text,
    trace_replay,
)
from loomwork.history import history_from_json, history_to_json

_SIZE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*(b|kb|mb|gb)?$", re.IGNORECASE)
_SIZE_UNITS = {"b": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3}

_GRAPH_RENDERERS = {"mermaid": render_mermaid, "dot": render_dot, "text": render_text}


def _parse_byte_size(text: str) -> int:
    match = _SIZE_RE.match(text.strip())
    if not match:
        raise UsageError(f"invalid size {text!r}: use bytes or units such as 64MB, 1GB")
    size = int(float(match.group(1)) * _SIZE_UNITS[(match.group(2) or "b").lower()])
    if size <= 0:
        raise UsageError("size must be positive")
    return size


def _archive_cli_error(exc: ArchiveError) -> CliError:
    code = ExitCode.NOT_FOUND if isinstance(exc, ArchiveNotFoundError) else ExitCode.FAILURE
    return CliError(str(exc), code)


# --------------------------------------------------------------------------- archive


async def archive_create(args: argparse.Namespace, console: Console) -> int:
    age = parse_duration(args.older_than)
    if age.total_seconds() <= 0:
        raise UsageError("--older-than must be positive")
    if args.prune and args.query:
        raise UsageError("--prune cannot be combined with --query")
    segment_max_bytes = _parse_byte_size(args.segment_size)
    cutoff = datetime.now(UTC) - age
    async with open_session(args) as active:
        try:
            result = await archive_and_prune(
                active.store,
                args.out_dir,
                cutoff=cutoff,
                source=active.options.target,
                query=args.query,
                schema=active.options.search_attributes,
                segment_max_bytes=segment_max_bytes,
                prune=args.prune,
            )
        except ArchiveError as exc:
            raise UsageError(str(exc)) from None
    manifest = result.manifest
    if console.json_output:
        console.json(
            {
                "directory": str(args.out_dir),
                "cutoff": cutoff,
                "run_count": manifest.run_count,
                "segments": len(manifest.segments),
                "pruned": result.pruned,
            }
        )
    else:
        console.print(
            f"archived {manifest.run_count} run(s) into {len(manifest.segments)} segment(s) "
            f"at {args.out_dir}"
        )
        if args.prune:
            console.note(f"pruned {result.pruned} run(s) from the store")
    return ExitCode.OK


async def archive_verify(args: argparse.Namespace, console: Console) -> int:
    reader = ArchiveReader(args.directory)
    try:
        result = reader.verify()
    except ArchiveError as exc:
        if console.json_output:
            console.json({"ok": False, "error": str(exc)})
        else:
            console.print(f"CORRUPT: {exc}")
        return ExitCode.FAILURE
    if console.json_output:
        console.json({"ok": True, "segments": result.segment_count, "runs": result.run_count})
    else:
        console.print(f"ok: {result.segment_count} segment(s), {result.run_count} run(s)")
    return ExitCode.OK


async def archive_list(args: argparse.Namespace, console: Console) -> int:
    reader = ArchiveReader(args.directory)
    try:
        runs = list(reader.iter_runs(workflow_name=args.workflow))
    except ArchiveError as exc:
        raise _archive_cli_error(exc) from None
    console.table(
        EXECUTION_HEADERS,
        [execution_row(r.record) for r in runs],
        records=[execution_json(r.record, default_converter) for r in runs],
        empty="no archived runs found",
    )
    return ExitCode.OK


async def archive_show(args: argparse.Namespace, console: Console) -> int:
    if not args.workflow_id and not args.run_id:
        raise UsageError("give a workflow id or --run RUN_ID")
    reader = ArchiveReader(args.directory)
    try:
        run = reader.find(workflow_id=args.workflow_id, run_id=args.run_id)
    except ArchiveError as exc:
        raise _archive_cli_error(exc) from None
    if args.export:
        Path(args.export).write_text(history_to_json(run.history), encoding="utf-8")
        console.note(f"wrote {len(run.history)} events to {args.export}")
        return ExitCode.OK
    fields = execution_fields(run.record, default_converter)
    for key in sorted(run.search_attributes):
        fields.append((f"attr.{key}", run.search_attributes[key]))
    fields.append(("history length", len(run.history)))
    data = execution_json(run.record, default_converter)
    data["search_attributes"] = to_jsonable(run.search_attributes)
    data["history_length"] = len(run.history)
    console.fields(fields, record=data)
    return ExitCode.OK


# --------------------------------------------------------------------------- replay


def _tick_rows(trace: ReplayTrace) -> list[list[Any]]:
    return [
        [
            t.tick_number,
            t.tick_event_id,
            t.time,
            t.consumed_through,
            len(t.inputs),
            "; ".join(t.command_descriptions) or "-",
        ]
        for t in trace.ticks
    ]


def _replay_mode(args: argparse.Namespace) -> str:
    """Which history source was given; validates that exactly one, consistently, was."""
    if args.history:
        if args.archive or args.workflow_id or args.run_id:
            raise UsageError("--history cannot be combined with --archive, a workflow id or --run")
        return "history"
    if args.archive:
        if args.workflow_id:
            raise UsageError("--archive takes --run RUN_ID, not a workflow id")
        if not args.run_id:
            raise UsageError("--archive requires --run RUN_ID")
        return "archive"
    if args.workflow_id or args.run_id:
        return "store"
    raise UsageError(
        "give a history source: --history FILE, --archive DIR --run RUN_ID, or a workflow id"
    )


async def replay(args: argparse.Namespace, console: Console) -> int:
    mode = _replay_mode(args)
    app = load_app(args.app)
    queries = [args.query_name] if args.query_name else None

    if mode == "history":
        history = history_from_json(Path(args.history).read_text(encoding="utf-8"))
    elif mode == "archive":
        try:
            run = ArchiveReader(args.archive).find(run_id=args.run_id)
        except ArchiveError as exc:
            raise _archive_cli_error(exc) from None
        history = await CodecBoundary(app.converter.codec).decode(run.history)
    else:
        async with open_session(args, app) as active:
            handle = store_handle_for(args, active)
            history = await CodecBoundary(app.converter.codec).decode(await handle.history())

    try:
        trace = trace_replay(
            history,
            app.registry,
            converter=app.converter,
            until_tick=args.until_tick,
            queries=queries,
        )
    except DebugError as exc:
        raise UsageError(str(exc)) from None

    if console.json_output:
        console.json(
            {
                "workflow_name": trace.workflow_name,
                "workflow_id": trace.workflow_id,
                "run_id": trace.run_id,
                "source_location": trace.source_location,
                "ticks": [
                    {
                        "tick": t.tick_number,
                        "event_id": t.tick_event_id,
                        "time": t.time,
                        "consumed_through": t.consumed_through,
                        "inputs": len(t.inputs),
                        "commands": t.command_descriptions,
                    }
                    for t in trace.ticks
                ],
                "ok": trace.ok,
                "divergence": trace.divergence,
                "closed_by": trace.closed_by.kind if trace.closed_by is not None else None,
                "stopped_at_tick": trace.stopped_at_tick,
                "query_results": trace.query_results,
            }
        )
    else:
        console.print(
            render_table(
                ["tick", "event", "time", "consumed", "inputs", "commands"], _tick_rows(trace)
            )
        )
        if trace.source_location:
            console.note(f"workflow code: {trace.source_location}")
        if trace.closed_by is not None:
            console.note(
                f"run closed by {trace.closed_by.kind} at event {trace.closed_by.event_id}"
            )
        if trace.stopped_at_tick is not None:
            console.note(f"stopped after tick {trace.stopped_at_tick} (--until-tick)")
        if trace.divergence is not None:
            d = trace.divergence
            console.print(f"DIVERGED at event {d.event_id}:")
            console.print(f"  expected: {d.expected}")
            console.print(f"  produced: {d.produced}")
            console.print(f"  {d.explanation}")
        for name, value in trace.query_results.items():
            console.print(f"query {name}:")
            console.json(value)
    return ExitCode.OK if trace.ok else ExitCode.FAILURE


# --------------------------------------------------------------------------- graph


async def workflow_graph(args: argparse.Namespace, console: Console) -> int:
    async with open_session(args) as active:
        handle = store_handle_for(args, active)
        record = await handle.describe()
        history = await handle.history()
    title = f"{record.workflow_id} ({record.workflow_name})"
    diagram = _GRAPH_RENDERERS[args.format](history, title=title)
    if console.json_output:
        console.json(
            {
                "format": args.format,
                "workflow_id": record.workflow_id,
                "run_id": record.run_id,
                "diagram": diagram,
            }
        )
    else:
        console.print(diagram)
    return ExitCode.OK


def register_graph(commands: Any) -> None:
    """Adds ``workflow graph``; called from ``workflow.register()``."""
    p = commands.add_parser("graph", help="render a run's history as a diagram")
    add_target_arguments(p)
    p.add_argument("--format", choices=sorted(_GRAPH_RENDERERS), default="text")
    connected(p, workflow_graph)


# --------------------------------------------------------------------------- parser


def register(subparsers: Any) -> None:
    parser = subparsers.add_parser(
        "archive", help="write closed runs to long-term storage, and read them back"
    )
    commands = parser.add_subparsers(dest="archive_command", metavar="COMMAND", required=True)

    p = commands.add_parser("create", help="archive closed runs older than a cutoff")
    p.add_argument("--older-than", required=True, metavar="DURATION", help="e.g. 30d")
    p.add_argument("--out", required=True, type=Path, metavar="DIR", dest="out_dir")
    p.add_argument(
        "--query", "-q", metavar="QUERY", help="only archive runs matching this visibility query"
    )
    p.add_argument(
        "--prune",
        action="store_true",
        help="delete archived runs from the store afterwards (not with --query)",
    )
    p.add_argument(
        "--segment-size",
        default="64MB",
        metavar="SIZE",
        help="rotate segments at this compressed size (default: 64MB)",
    )
    connected(p, archive_create)

    p = commands.add_parser("verify", help="check an archive's checksums and structure")
    p.add_argument("directory", type=Path, metavar="DIR")
    p.set_defaults(handler=archive_verify)

    p = commands.add_parser("list", help="list runs held in an archive")
    p.add_argument("directory", type=Path, metavar="DIR")
    p.add_argument("--workflow", help="only runs of this workflow")
    p.set_defaults(handler=archive_list)

    p = commands.add_parser("show", help="show (or export) one archived run")
    p.add_argument("directory", type=Path, metavar="DIR")
    add_target_arguments(p)
    p.add_argument(
        "--export", type=Path, metavar="FILE", help="write the run's history as a JSON document"
    )
    p.set_defaults(handler=archive_show)

    p = subparsers.add_parser(
        "replay",
        help="replay a history tick by tick against workflow code",
        description="Replays a history (from a run, an exported history file, or an archive) "
        "tick by tick against the code in --app, printing what each tick consumed and "
        "produced. Exits 1 if the code diverges from the recorded decisions, with a precise "
        "explanation of where and why.",
    )
    add_target_arguments(p)
    p.add_argument("--history", type=Path, metavar="FILE", help="replay an exported history file")
    p.add_argument(
        "--archive", type=Path, metavar="DIR", help="replay a run from an archive (needs --run)"
    )
    add_app_argument(p, required=True)
    p.add_argument("--until-tick", type=int, metavar="N", help="stop after this many ticks")
    p.add_argument(
        "--query",
        dest="query_name",
        metavar="NAME",
        help="show this zero-argument query's result at the stopping point",
    )
    connected(p, replay)


__all__ = ["register", "register_graph"]
