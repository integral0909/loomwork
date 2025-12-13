"""Mapping between store records and SQL rows, shared by the SQL backends.

Structured columns (payloads, failures, step options, event bodies) are stored as JSON.
Timestamps are handled by each backend (``timestamptz`` in PostgreSQL, integer
microseconds in SQLite) and passed to these helpers already converted to ``datetime``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.history.events import Event
from loomwork.history.serde import event_from_dict, event_to_dict
from loomwork.options import StepOptions, VersioningPolicy
from loomwork.store.records import ExecutionRecord, ExecutionStatus, StepJob

Row = Mapping[str, Any]
Timestamp = datetime | None


def dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def loads(value: str | bytes | Mapping[str, Any] | list[Any] | None) -> Any:
    """Decode a JSON column; drivers that parse JSON already hand over Python objects."""
    if value is None or isinstance(value, Mapping | list):
        return value
    return json.loads(value)


# --------------------------------------------------------------------------- values


def payload_to_json(payload: Payload | None) -> str | None:
    return None if payload is None else dumps(payload.to_dict())


def payload_from_json(value: Any) -> Payload | None:
    data = loads(value)
    return None if data is None else Payload.from_dict(data)


def payloads_to_json(payloads: list[Payload]) -> str:
    return dumps([p.to_dict() for p in payloads])


def payloads_from_json(value: Any) -> list[Payload]:
    return [Payload.from_dict(item) for item in loads(value) or []]


def failure_to_json(failure: FailureInfo | None) -> str | None:
    return None if failure is None else dumps(failure.to_dict())


def failure_from_json(value: Any) -> FailureInfo | None:
    data = loads(value)
    return None if data is None else FailureInfo.from_dict(data)


def options_to_json(options: StepOptions) -> str:
    return dumps(options.to_dict())


def options_from_json(value: Any) -> StepOptions:
    return StepOptions.from_dict(loads(value) or {})


def headers_to_json(headers: Mapping[str, str]) -> str:
    return dumps(dict(headers))


def headers_from_json(value: Any) -> dict[str, str]:
    return dict(loads(value) or {})


# --------------------------------------------------------------------------- events


def event_body_to_json(event: Event) -> str:
    """The event's fields without the id, timestamp and kind (stored in own columns)."""
    body = event_to_dict(event)
    for key in ("id", "ts", "kind"):
        body.pop(key, None)
    return dumps(body)


def event_from_row(run_event_id: int, kind: str, timestamp: datetime, body: Any) -> Event:
    data = dict(loads(body) or {})
    data["id"] = run_event_id
    data["kind"] = kind
    data["ts"] = timestamp.isoformat()
    return event_from_dict(data)


# --------------------------------------------------------------------------- records


EXECUTION_COLUMNS = (
    "run_id",
    "workflow_id",
    "workflow_name",
    "status",
    "created_at",
    "closed_at",
    "last_event_id",
    "last_input_id",
    "consumed_through",
    "tick_pending",
    "tick_ready_at",
    "tick_failures",
    "last_tick_error",
    "lease_owner",
    "lease_expires_at",
    "cancel_requested",
    "result",
    "failure",
    "close_reason",
    "parent_run_id",
    "parent_seq",
    "parent_close_policy",
    "continued_run_id",
    "reset_from_run_id",
    "history_bytes",
    "versioning_policy",
    "pinned_build_id",
)

STEP_JOB_COLUMNS = (
    "job_id",
    "run_id",
    "workflow_id",
    "workflow_name",
    "seq",
    "step_name",
    "args",
    "options",
    "scheduled_at",
    "ready_at",
    "deadline",
    "attempt",
    "lease_owner",
    "lease_expires_at",
    "last_failure",
    "cancel_requested",
    "queue",
    "priority",
    "heartbeat_details",
    "last_heartbeat_at",
    "headers",
    "pinned_build_id",
)


def execution_from_row(row: Row, ts: Any) -> ExecutionRecord:
    """Build a record; ``ts`` converts the backend's timestamp representation."""
    return ExecutionRecord(
        run_id=row["run_id"],
        workflow_id=row["workflow_id"],
        workflow_name=row["workflow_name"],
        status=ExecutionStatus(row["status"]),
        created_at=ts(row["created_at"]),
        closed_at=ts(row["closed_at"]),
        last_event_id=int(row["last_event_id"]),
        last_input_id=int(row["last_input_id"]),
        consumed_through=int(row["consumed_through"]),
        tick_pending=bool(row["tick_pending"]),
        tick_ready_at=ts(row["tick_ready_at"]),
        tick_failures=int(row["tick_failures"]),
        last_tick_error=row["last_tick_error"],
        lease_owner=row["lease_owner"],
        lease_expires_at=ts(row["lease_expires_at"]),
        cancel_requested=bool(row["cancel_requested"]),
        result=payload_from_json(row["result"]),
        failure=failure_from_json(row["failure"]),
        close_reason=row["close_reason"] or "",
        parent_run_id=row["parent_run_id"],
        parent_seq=int(row["parent_seq"]) if row["parent_seq"] is not None else None,
        parent_close_policy=row["parent_close_policy"] or "terminate",
        continued_run_id=row["continued_run_id"],
        reset_from_run_id=row["reset_from_run_id"],
        history_bytes=int(row["history_bytes"]),
        versioning_policy=VersioningPolicy(row["versioning_policy"]),
        pinned_build_id=row["pinned_build_id"],
    )


def step_job_from_row(row: Row, ts: Any) -> StepJob:
    return StepJob(
        job_id=row["job_id"],
        run_id=row["run_id"],
        workflow_id=row["workflow_id"],
        workflow_name=row["workflow_name"],
        seq=int(row["seq"]),
        step_name=row["step_name"],
        args=payloads_from_json(row["args"]),
        options=options_from_json(row["options"]),
        scheduled_at=ts(row["scheduled_at"]),
        ready_at=ts(row["ready_at"]),
        deadline=ts(row["deadline"]),
        attempt=int(row["attempt"]),
        lease_owner=row["lease_owner"],
        lease_expires_at=ts(row["lease_expires_at"]),
        last_failure=failure_from_json(row["last_failure"]),
        cancel_requested=bool(row["cancel_requested"]),
        queue=row["queue"],
        priority=int(row["priority"]),
        heartbeat_details=payload_from_json(row["heartbeat_details"]),
        last_heartbeat_at=ts(row["last_heartbeat_at"]),
        headers=headers_from_json(row["headers"]),
        pinned_build_id=row["pinned_build_id"],
    )
