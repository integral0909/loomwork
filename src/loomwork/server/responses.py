"""JSON views of domain objects for the control API.

Unlike the worker API (which exchanges the lossless wire form from
:mod:`loomwork.remote.wire`), the control API is for callers who may not have Loomwork's
Python types at all. Payloads are decoded to plain JSON when they were encoded with the
default converter (true for every value the control API itself accepted); anything else
(bytes, compressed or encrypted payloads produced by a Python client) falls back to its
metadata plus base64 bytes.
"""

from __future__ import annotations

import contextlib
from typing import Any

from loomwork.codec.converter import default_converter
from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.history.events import Event
from loomwork.history.serde import event_to_dict
from loomwork.schedules.model import ScheduleState
from loomwork.store.records import ExecutionRecord

_DECODABLE = {"json/plain", "binary/null"}


def payload_view(payload: Payload | None) -> Any:
    if payload is None:
        return None
    if payload.encoding in _DECODABLE:
        with contextlib.suppress(Exception):
            return default_converter.decode(payload)
    return payload.to_dict()


def failure_view(failure: FailureInfo | None) -> dict[str, Any] | None:
    if failure is None:
        return None
    return {
        "message": failure.message,
        "type": failure.type,
        "kind": failure.kind,
        "non_retryable": failure.non_retryable,
        "details": failure.details,
        "attributes": dict(failure.attributes),
        "cause": failure_view(failure.cause),
    }


def execution_view(record: ExecutionRecord) -> dict[str, Any]:
    return {
        "run_id": record.run_id,
        "workflow_id": record.workflow_id,
        "workflow_name": record.workflow_name,
        "status": record.status.value,
        "created_at": record.created_at.isoformat(),
        "closed_at": record.closed_at.isoformat() if record.closed_at else None,
        "last_event_id": record.last_event_id,
        "tick_pending": record.tick_pending,
        "tick_failures": record.tick_failures,
        "last_tick_error": record.last_tick_error,
        "lease_owner": record.lease_owner,
        "cancel_requested": record.cancel_requested,
        "result": payload_view(record.result),
        "failure": failure_view(record.failure),
        "close_reason": record.close_reason,
        "parent_run_id": record.parent_run_id,
        "parent_seq": record.parent_seq,
        "continued_run_id": record.continued_run_id,
        "reset_from_run_id": record.reset_from_run_id,
    }


def schedule_view(state: ScheduleState) -> dict[str, Any]:
    """JSON view of a :class:`~loomwork.schedules.model.ScheduleState`: the same shape as
    ``state.to_dict()``, except the action's arguments are decoded like any other payload
    the control API returns, instead of left as wire-form payload dicts."""
    data = state.to_dict()
    data["action"] = {
        "workflow_name": state.action.workflow_name,
        "args": [payload_view(p) for p in state.action.args],
    }
    return data


def event_view(event: Event) -> dict[str, Any]:
    """The event's wire form, decorated with a decoded ``value``/``result`` when it
    carries a JSON payload, for callers reading history without Loomwork's codecs."""
    data = event_to_dict(event)
    for key in ("result", "payload"):
        raw = data.get(key)
        if isinstance(raw, dict) and "data" in raw and "metadata" in raw:
            with contextlib.suppress(Exception):
                data[key] = {"payload": raw, "value": payload_view(Payload.from_dict(raw))}
    return data
