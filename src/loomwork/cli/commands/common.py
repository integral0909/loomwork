"""Helpers shared by the command modules."""

from __future__ import annotations

import argparse
import dataclasses
import json
from collections.abc import Awaitable, Callable
from typing import Any

from loomwork.cli.backend import Operations, OperationsHandle
from loomwork.cli.connection import ConnectionOptions, Session, add_connection_arguments, session
from loomwork.cli.errors import UsageError
from loomwork.cli.loader import AppSpec, load_app
from loomwork.cli.output import Console, format_cell, to_jsonable
from loomwork.client.handle import WorkflowHandle
from loomwork.codec.converter import DataConverter
from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.history.events import Event
from loomwork.store.records import ExecutionRecord

#: What every command handler looks like: parsed arguments in, exit code out.
Handler = Callable[[argparse.Namespace, Console], Awaitable[int]]

#: Fields every event has; left out of one-line summaries.
_EVENT_BASE_FIELDS = frozenset({"event_id", "timestamp"})


def add_target_arguments(parser: argparse.ArgumentParser) -> None:
    """``WORKFLOW_ID [--run RUN_ID]``: the latest run of an id, or one specific run."""
    parser.add_argument("workflow_id", nargs="?", help="workflow id (the latest run of it is used)")
    parser.add_argument("--run", dest="run_id", metavar="RUN_ID", help="a specific run id")


def add_app_argument(parser: argparse.ArgumentParser, *, required: bool) -> None:
    parser.add_argument(
        "--app",
        required=required,
        metavar="REF",
        help="workflow code to load: module, module:attribute or path/to/file.py",
    )


def connected(parser: argparse.ArgumentParser, handler: Handler) -> None:
    """Mark ``parser`` as a command that talks to a store."""
    add_connection_arguments(parser)
    parser.set_defaults(handler=handler)


def handle_for(args: argparse.Namespace, ops: Operations) -> OperationsHandle:
    if not args.workflow_id and not args.run_id:
        raise UsageError("give a workflow id or --run RUN_ID")
    return ops.get_handle(args.workflow_id or None, run_id=args.run_id)


def store_handle_for(args: argparse.Namespace, active: Session) -> WorkflowHandle[Any]:
    """Like :func:`handle_for`, for the few commands (``query``, ``steps``) that always
    need the real, store-backed handle -- because they use methods
    (:meth:`~loomwork.client.handle.WorkflowHandle.query`,
    :meth:`~loomwork.client.handle.WorkflowHandle.pending_steps`) the control API has no
    way to answer, and so are not part of :class:`~loomwork.cli.backend.OperationsHandle`."""
    if not args.workflow_id and not args.run_id:
        raise UsageError("give a workflow id or --run RUN_ID")
    return active.client.get_handle(args.workflow_id or None, run_id=args.run_id)


def app_from(args: argparse.Namespace) -> AppSpec | None:
    reference = getattr(args, "app", None)
    return load_app(reference) if reference else None


def open_session(args: argparse.Namespace, app: AppSpec | None = None) -> Any:
    """``async with open_session(args) as active:`` for the configured store."""
    return session(ConnectionOptions.from_args(args), app)


def decode_payload(payload: Payload | None, converter: DataConverter) -> Any:
    """The JSON value inside ``payload`` for display, or a placeholder when it cannot be
    decoded here (for example because it is encrypted with a key this process lacks)."""
    if payload is None:
        return None
    try:
        return to_jsonable(converter.decode(payload))
    except Exception:
        encoding = payload.encoding or "binary"
        return f"<{len(payload)} bytes, {encoding}>"


async def decode_payload_async(payload: Payload | None, converter: DataConverter) -> Any:
    if payload is None:
        return None
    try:
        return to_jsonable(await converter.decode_async(payload))
    except Exception:
        return decode_payload(payload, converter)


def summarize_value(value: Any, converter: DataConverter) -> Any:
    if isinstance(value, Payload):
        return decode_payload(value, converter)
    if isinstance(value, list) and value and all(isinstance(v, Payload) for v in value):
        return [decode_payload(v, converter) for v in value]
    if isinstance(value, FailureInfo):
        return f"{value.type}: {value.message}"
    return to_jsonable(value)


def event_details(event: Event, converter: DataConverter) -> dict[str, Any]:
    """The event's own fields (not id or time), payloads decoded, empty values dropped."""
    details: dict[str, Any] = {}
    for field in dataclasses.fields(event):
        if field.name in _EVENT_BASE_FIELDS:
            continue
        value = getattr(event, field.name)
        if value in (None, "", [], {}, ()) or value is False:
            continue
        details[field.name] = summarize_value(value, converter)
    return details


def event_summary(event: Event, converter: DataConverter) -> str:
    parts = []
    for key, value in event_details(event, converter).items():
        text = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
        parts.append(f"{key}={text}")
    return " ".join(parts)


def execution_row(record: ExecutionRecord) -> list[Any]:
    return [
        record.workflow_id,
        record.run_id,
        record.workflow_name,
        record.status.value,
        record.created_at,
        record.closed_at,
    ]


EXECUTION_HEADERS = ["workflow id", "run id", "workflow", "status", "created", "closed"]


def execution_fields(record: ExecutionRecord, converter: DataConverter) -> list[tuple[str, Any]]:
    fields: list[tuple[str, Any]] = [
        ("workflow id", record.workflow_id),
        ("run id", record.run_id),
        ("workflow", record.workflow_name),
        ("status", record.status.value),
        ("created", record.created_at),
        ("closed", record.closed_at),
        ("history length", record.last_event_id),
    ]
    if not record.is_closed:
        fields.append(("tick pending", record.tick_pending))
        if record.tick_failures:
            fields.append(("tick failures", record.tick_failures))
            fields.append(("last tick error", record.last_tick_error))
        if record.cancel_requested:
            fields.append(("cancel requested", True))
        if record.lease_owner:
            fields.append(("leased by", record.lease_owner))
    if record.result is not None:
        fields.append(("result", format_cell(decode_payload(record.result, converter))))
    if record.failure is not None:
        fields.append(("failure", f"{record.failure.type}: {record.failure.message}"))
    if record.close_reason:
        fields.append(("close reason", record.close_reason))
    if record.parent_run_id:
        fields.append(("parent run", record.parent_run_id))
    if record.reset_from_run_id:
        fields.append(("reset from", record.reset_from_run_id))
    if record.continued_run_id:
        fields.append(("continued as", record.continued_run_id))
    return fields


def execution_json(record: ExecutionRecord, converter: DataConverter) -> dict[str, Any]:
    data: dict[str, Any] = to_jsonable(record)
    data["result"] = decode_payload(record.result, converter)
    return data
