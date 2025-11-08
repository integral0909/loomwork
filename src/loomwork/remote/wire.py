"""JSON wire format for every type that crosses the :class:`~loomwork.store.base.Store`
boundary over HTTP.

Two kinds of values cross that boundary:

* **Data** (:class:`~loomwork.codec.payload.Payload`, :class:`~loomwork.failure.FailureInfo`,
  :class:`~loomwork.options.StepOptions`, history events, execution records, step jobs, ...)
  round-trips exactly and is encoded the same way regardless of direction. These use the
  plain ``*_to_wire`` / ``*_from_wire`` functions, which encode ``datetime`` values as
  ISO-8601 strings.
* **Deadlines carried in a request** (``now``, ``expires_at``, ``ready_at``, ``retry_at``,
  ``cutoff``, a timer's ``fire_at``, a tick's ``time``) are never sent as absolute
  timestamps. Per the server's clock rule (see ``docs/server.md``), a client never asserts
  what time it is on the server. Instead every request captures one reference instant from
  the caller's own clock (``ref``) and expresses every deadline in that request as a
  ``float`` offset in seconds from ``ref``. The server captures its own reference instant
  when it starts handling the request (``server_now``) and reconstructs each deadline as
  ``server_now + offset``. A request that wants "right now" simply sends an offset of
  ``0.0``, which the server resolves to its own idea of "now" -- exactly the clock rule.

  The functions ending in ``_to_request`` / ``_from_request`` apply this transformation;
  they accept ``ref`` (client side) or ``server_now`` (server side) and otherwise share
  field layout with their absolute counterparts.

Every function here is a pure, allocation-only transform: no I/O, no clocks read.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from typing import Any

from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.history.events import Event
from loomwork.history.serde import HistoryFormatError, event_from_dict, event_to_dict
from loomwork.options import IdConflictPolicy, IdReusePolicy, ResetPoint, StepOptions
from loomwork.schedules.model import ScheduleState
from loomwork.store.records import (
    ChildSpec,
    CloseOutcome,
    ExecutionRecord,
    ExecutionStatus,
    HeartbeatResult,
    OutboxAction,
    OutboxMessage,
    StepJob,
    TickOutcome,
    TimerRecord,
)
from loomwork.visibility.schema import AttributeType, SearchAttributes

DtEncoder = Callable[[datetime], Any]
DtDecoder = Callable[[Any], datetime]


class WireFormatError(ValueError):
    """Data received over the wire does not match the expected shape."""


def _require(data: dict[str, Any], key: str) -> Any:
    try:
        return data[key]
    except KeyError:
        raise WireFormatError(f"missing field {key!r}") from None


# --------------------------------------------------------------------------- time


def offset_seconds(ref: datetime, value: datetime) -> float:
    """Express ``value`` as an offset in seconds from ``ref``."""
    return (value - ref).total_seconds()


def apply_offset(ref: datetime, value: float) -> datetime:
    """Reconstruct an absolute time from an offset previously taken from ``ref``."""
    return ref + timedelta(seconds=value)


def offset_seconds_opt(ref: datetime, value: datetime | None) -> float | None:
    return None if value is None else offset_seconds(ref, value)


def apply_offset_opt(ref: datetime, value: float | None) -> datetime | None:
    return None if value is None else apply_offset(ref, value)


def iso_encode(value: datetime) -> str:
    return value.isoformat()


def iso_decode(value: Any) -> datetime:
    if not isinstance(value, str):
        raise WireFormatError(f"expected an ISO-8601 timestamp string, got {value!r}")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise WireFormatError(f"malformed timestamp {value!r}: {exc}") from exc
    if parsed.tzinfo is None:
        raise WireFormatError(f"naive timestamp {value!r} received over the wire")
    return parsed


def iso_encode_opt(value: datetime | None) -> str | None:
    return None if value is None else iso_encode(value)


def iso_decode_opt(value: Any) -> datetime | None:
    return None if value is None else iso_decode(value)


# --------------------------------------------------------------------------- primitives


def payload_to_wire(payload: Payload) -> dict[str, Any]:
    return payload.to_dict()


def payload_from_wire(data: dict[str, Any]) -> Payload:
    try:
        return Payload.from_dict(data)
    except (KeyError, ValueError) as exc:
        raise WireFormatError(f"malformed payload: {exc}") from exc


def payload_opt_to_wire(payload: Payload | None) -> dict[str, Any] | None:
    return None if payload is None else payload_to_wire(payload)


def payload_opt_from_wire(data: Any) -> Payload | None:
    return None if data is None else payload_from_wire(data)


def payload_list_to_wire(payloads: Sequence[Payload]) -> list[dict[str, Any]]:
    return [payload_to_wire(p) for p in payloads]


def payload_list_from_wire(data: Sequence[Any]) -> list[Payload]:
    return [payload_from_wire(p) for p in data]


def failure_to_wire(failure: FailureInfo) -> dict[str, Any]:
    return failure.to_dict()


def failure_from_wire(data: dict[str, Any]) -> FailureInfo:
    try:
        return FailureInfo.from_dict(data)
    except (KeyError, ValueError, TypeError) as exc:
        raise WireFormatError(f"malformed failure: {exc}") from exc


def failure_opt_to_wire(failure: FailureInfo | None) -> dict[str, Any] | None:
    return None if failure is None else failure_to_wire(failure)


def failure_opt_from_wire(data: Any) -> FailureInfo | None:
    return None if data is None else failure_from_wire(data)


def step_options_to_wire(options: StepOptions) -> dict[str, Any]:
    return options.to_dict()


def step_options_from_wire(data: dict[str, Any]) -> StepOptions:
    try:
        return StepOptions.from_dict(data)
    except (KeyError, ValueError, TypeError) as exc:
        raise WireFormatError(f"malformed step options: {exc}") from exc


def event_to_wire(event: Event) -> dict[str, Any]:
    return event_to_dict(event)


def event_from_wire(data: dict[str, Any]) -> Event:
    try:
        return event_from_dict(data)
    except HistoryFormatError as exc:
        raise WireFormatError(str(exc)) from exc


def events_to_wire(events: Sequence[Event]) -> list[dict[str, Any]]:
    return [event_to_wire(e) for e in events]


def events_from_wire(data: Sequence[Any]) -> list[Event]:
    return [event_from_wire(e) for e in data]


def event_opt_to_wire(event: Event | None) -> dict[str, Any] | None:
    return None if event is None else event_to_wire(event)


def event_opt_from_wire(data: Any) -> Event | None:
    return None if data is None else event_from_wire(data)


# --------------------------------------------------------------------------- execution record


def execution_record_to_wire(
    record: ExecutionRecord, *, encode_dt: DtEncoder = iso_encode
) -> dict[str, Any]:
    return {
        "run_id": record.run_id,
        "workflow_id": record.workflow_id,
        "workflow_name": record.workflow_name,
        "status": record.status.value,
        "created_at": encode_dt(record.created_at),
        "closed_at": None if record.closed_at is None else encode_dt(record.closed_at),
        "last_event_id": record.last_event_id,
        "last_input_id": record.last_input_id,
        "consumed_through": record.consumed_through,
        "tick_pending": record.tick_pending,
        "tick_ready_at": None if record.tick_ready_at is None else encode_dt(record.tick_ready_at),
        "tick_failures": record.tick_failures,
        "last_tick_error": record.last_tick_error,
        "lease_owner": record.lease_owner,
        "lease_expires_at": (
            None if record.lease_expires_at is None else encode_dt(record.lease_expires_at)
        ),
        "cancel_requested": record.cancel_requested,
        "result": payload_opt_to_wire(record.result),
        "failure": failure_opt_to_wire(record.failure),
        "close_reason": record.close_reason,
        "parent_run_id": record.parent_run_id,
        "parent_seq": record.parent_seq,
        "parent_close_policy": record.parent_close_policy,
        "continued_run_id": record.continued_run_id,
        "reset_from_run_id": record.reset_from_run_id,
        "history_bytes": record.history_bytes,
    }


def execution_record_from_wire(
    data: dict[str, Any], *, decode_dt: DtDecoder = iso_decode
) -> ExecutionRecord:
    try:
        created_at = data["created_at"]
        return ExecutionRecord(
            run_id=str(_require(data, "run_id")),
            workflow_id=str(_require(data, "workflow_id")),
            workflow_name=str(_require(data, "workflow_name")),
            status=ExecutionStatus(_require(data, "status")),
            created_at=decode_dt(created_at),
            closed_at=None if data.get("closed_at") is None else decode_dt(data["closed_at"]),
            last_event_id=int(data.get("last_event_id", 0)),
            last_input_id=int(data.get("last_input_id", 0)),
            consumed_through=int(data.get("consumed_through", 0)),
            tick_pending=bool(data.get("tick_pending", False)),
            tick_ready_at=(
                None if data.get("tick_ready_at") is None else decode_dt(data["tick_ready_at"])
            ),
            tick_failures=int(data.get("tick_failures", 0)),
            last_tick_error=data.get("last_tick_error"),
            lease_owner=data.get("lease_owner"),
            lease_expires_at=(
                None
                if data.get("lease_expires_at") is None
                else decode_dt(data["lease_expires_at"])
            ),
            cancel_requested=bool(data.get("cancel_requested", False)),
            result=payload_opt_from_wire(data.get("result")),
            failure=failure_opt_from_wire(data.get("failure")),
            close_reason=str(data.get("close_reason", "")),
            parent_run_id=data.get("parent_run_id"),
            parent_seq=data.get("parent_seq"),
            parent_close_policy=str(data.get("parent_close_policy", "terminate")),
            continued_run_id=data.get("continued_run_id"),
            reset_from_run_id=data.get("reset_from_run_id"),
            history_bytes=int(data.get("history_bytes", 0)),
        )
    except (KeyError, ValueError, TypeError) as exc:
        raise WireFormatError(f"malformed execution record: {exc}") from exc


def execution_record_to_request(record: ExecutionRecord, ref: datetime) -> dict[str, Any]:
    return execution_record_to_wire(record, encode_dt=lambda v: offset_seconds(ref, v))


def execution_record_from_request(data: dict[str, Any], server_now: datetime) -> ExecutionRecord:
    return execution_record_from_wire(data, decode_dt=lambda v: apply_offset(server_now, v))


def execution_status_from_wire(value: str) -> ExecutionStatus:
    try:
        return ExecutionStatus(value)
    except ValueError as exc:
        raise WireFormatError(f"unknown execution status {value!r}") from exc


def id_reuse_policy_from_wire(value: str) -> IdReusePolicy:
    try:
        return IdReusePolicy(value)
    except ValueError as exc:
        raise WireFormatError(f"unknown id reuse policy {value!r}") from exc


def id_conflict_policy_from_wire(value: str) -> IdConflictPolicy:
    try:
        return IdConflictPolicy(value)
    except ValueError as exc:
        raise WireFormatError(f"unknown id conflict policy {value!r}") from exc


# --------------------------------------------------------------------------- reset point

_RESET_KIND = "kind"
_RESET_TICK = "tick_count"


def reset_point_to_wire(point: ResetPoint) -> dict[str, Any]:
    return {_RESET_KIND: point.kind, _RESET_TICK: point.tick_count}


def reset_point_from_wire(data: dict[str, Any]) -> ResetPoint:
    try:
        kind = str(_require(data, _RESET_KIND))
        if kind == "first_tick":
            return ResetPoint.first_tick()
        if kind == "after_tick":
            return ResetPoint.after_tick(int(_require(data, _RESET_TICK)))
        if kind == "before_last_failure":
            return ResetPoint.before_last_failure()
        raise WireFormatError(f"unknown reset point kind {kind!r}")
    except (KeyError, ValueError, TypeError) as exc:
        raise WireFormatError(f"malformed reset point: {exc}") from exc


# --------------------------------------------------------------------------- step job


def step_job_to_wire(job: StepJob, *, encode_dt: DtEncoder = iso_encode) -> dict[str, Any]:
    return {
        "job_id": job.job_id,
        "run_id": job.run_id,
        "workflow_id": job.workflow_id,
        "workflow_name": job.workflow_name,
        "seq": job.seq,
        "step_name": job.step_name,
        "args": payload_list_to_wire(job.args),
        "options": step_options_to_wire(job.options),
        "scheduled_at": encode_dt(job.scheduled_at),
        "ready_at": encode_dt(job.ready_at),
        "deadline": None if job.deadline is None else encode_dt(job.deadline),
        "attempt": job.attempt,
        "lease_owner": job.lease_owner,
        "lease_expires_at": (
            None if job.lease_expires_at is None else encode_dt(job.lease_expires_at)
        ),
        "last_failure": failure_opt_to_wire(job.last_failure),
        "cancel_requested": job.cancel_requested,
        "queue": job.queue,
        "priority": job.priority,
        "heartbeat_details": payload_opt_to_wire(job.heartbeat_details),
        "last_heartbeat_at": (
            None if job.last_heartbeat_at is None else encode_dt(job.last_heartbeat_at)
        ),
        "headers": dict(job.headers),
    }


def step_job_from_wire(data: dict[str, Any], *, decode_dt: DtDecoder = iso_decode) -> StepJob:
    try:
        return StepJob(
            job_id=str(_require(data, "job_id")),
            run_id=str(_require(data, "run_id")),
            workflow_id=str(_require(data, "workflow_id")),
            workflow_name=str(_require(data, "workflow_name")),
            seq=int(_require(data, "seq")),
            step_name=str(_require(data, "step_name")),
            args=payload_list_from_wire(data.get("args", [])),
            options=step_options_from_wire(data.get("options", {})),
            scheduled_at=decode_dt(_require(data, "scheduled_at")),
            ready_at=decode_dt(_require(data, "ready_at")),
            deadline=None if data.get("deadline") is None else decode_dt(data["deadline"]),
            attempt=int(data.get("attempt", 0)),
            lease_owner=data.get("lease_owner"),
            lease_expires_at=(
                None
                if data.get("lease_expires_at") is None
                else decode_dt(data["lease_expires_at"])
            ),
            last_failure=failure_opt_from_wire(data.get("last_failure")),
            cancel_requested=bool(data.get("cancel_requested", False)),
            queue=str(data.get("queue", "default")),
            priority=int(data.get("priority", 0)),
            heartbeat_details=payload_opt_from_wire(data.get("heartbeat_details")),
            last_heartbeat_at=(
                None
                if data.get("last_heartbeat_at") is None
                else decode_dt(data["last_heartbeat_at"])
            ),
            headers=dict(data.get("headers") or {}),
        )
    except (KeyError, ValueError, TypeError) as exc:
        raise WireFormatError(f"malformed step job: {exc}") from exc


def step_job_to_request(job: StepJob, ref: datetime) -> dict[str, Any]:
    return step_job_to_wire(job, encode_dt=lambda v: offset_seconds(ref, v))


def step_job_from_request(data: dict[str, Any], server_now: datetime) -> StepJob:
    return step_job_from_wire(data, decode_dt=lambda v: apply_offset(server_now, v))


def step_jobs_to_wire(jobs: Sequence[StepJob]) -> list[dict[str, Any]]:
    return [step_job_to_wire(j) for j in jobs]


def step_jobs_from_wire(data: Sequence[Any]) -> list[StepJob]:
    return [step_job_from_wire(j) for j in data]


def step_jobs_to_request(jobs: Sequence[StepJob], ref: datetime) -> list[dict[str, Any]]:
    return [step_job_to_request(j, ref) for j in jobs]


def step_jobs_from_request(data: Sequence[Any], server_now: datetime) -> list[StepJob]:
    return [step_job_from_request(j, server_now) for j in data]


# --------------------------------------------------------------------------- timers


def timer_to_request(timer: TimerRecord, ref: datetime) -> dict[str, Any]:
    return {
        "run_id": timer.run_id,
        "seq": timer.seq,
        "fire_at": offset_seconds(ref, timer.fire_at),
    }


def timer_from_request(data: dict[str, Any], server_now: datetime) -> TimerRecord:
    try:
        return TimerRecord(
            run_id=str(_require(data, "run_id")),
            seq=int(_require(data, "seq")),
            fire_at=apply_offset(server_now, float(_require(data, "fire_at"))),
        )
    except (KeyError, ValueError, TypeError) as exc:
        raise WireFormatError(f"malformed timer record: {exc}") from exc


def timers_to_request(timers: Sequence[TimerRecord], ref: datetime) -> list[dict[str, Any]]:
    return [timer_to_request(t, ref) for t in timers]


def timers_from_request(data: Sequence[Any], server_now: datetime) -> list[TimerRecord]:
    return [timer_from_request(t, server_now) for t in data]


# --------------------------------------------------------------------------- heartbeat result


def heartbeat_result_to_wire(result: HeartbeatResult) -> dict[str, Any]:
    return {"cancel_requested": result.cancel_requested}


def heartbeat_result_from_wire(data: dict[str, Any]) -> HeartbeatResult:
    return HeartbeatResult(cancel_requested=bool(data.get("cancel_requested", False)))


# --------------------------------------------------------------------------- outbox / close


def outbox_message_to_request(message: OutboxMessage) -> dict[str, Any]:
    return {
        "action": message.action.value,
        "target_run_id": message.target_run_id,
        "target_workflow_id": message.target_workflow_id,
        "event": event_opt_to_wire(message.event),
        "reason": message.reason,
    }


def outbox_message_from_request(data: dict[str, Any]) -> OutboxMessage:
    try:
        return OutboxMessage(
            action=OutboxAction(_require(data, "action")),
            target_run_id=data.get("target_run_id"),
            target_workflow_id=data.get("target_workflow_id"),
            event=event_opt_from_wire(data.get("event")),
            reason=str(data.get("reason", "")),
        )
    except (KeyError, ValueError, TypeError) as exc:
        raise WireFormatError(f"malformed outbox message: {exc}") from exc


def close_outcome_to_request(close: CloseOutcome) -> dict[str, Any]:
    return {
        "status": close.status.value,
        "result": payload_opt_to_wire(close.result),
        "failure": failure_opt_to_wire(close.failure),
        "reason": close.reason,
        "continued_run_id": close.continued_run_id,
    }


def close_outcome_from_request(data: dict[str, Any]) -> CloseOutcome:
    try:
        return CloseOutcome(
            status=execution_status_from_wire(_require(data, "status")),
            result=payload_opt_from_wire(data.get("result")),
            failure=failure_opt_from_wire(data.get("failure")),
            reason=str(data.get("reason", "")),
            continued_run_id=data.get("continued_run_id"),
        )
    except (KeyError, ValueError, TypeError) as exc:
        raise WireFormatError(f"malformed close outcome: {exc}") from exc


def child_spec_to_request(spec: ChildSpec, ref: datetime) -> dict[str, Any]:
    return {
        "record": execution_record_to_request(spec.record, ref),
        "started": event_to_wire(spec.started),
    }


def child_spec_from_request(data: dict[str, Any], server_now: datetime) -> ChildSpec:
    try:
        return ChildSpec(
            record=execution_record_from_request(_require(data, "record"), server_now),
            started=event_from_wire(_require(data, "started")),
        )
    except (KeyError, ValueError, TypeError) as exc:
        raise WireFormatError(f"malformed child spec: {exc}") from exc


# --------------------------------------------------------------------------- tick outcome


def tick_outcome_to_request(outcome: TickOutcome, ref: datetime) -> dict[str, Any]:
    return {
        "worker_id": outcome.worker_id,
        "consumed_through": outcome.consumed_through,
        "time": offset_seconds(ref, outcome.time),
        "decisions": events_to_wire(outcome.decisions),
        "new_steps": step_jobs_to_request(outcome.new_steps, ref),
        "cancel_steps": list(outcome.cancel_steps),
        "new_timers": timers_to_request(outcome.new_timers, ref),
        "cancel_timers": list(outcome.cancel_timers),
        "new_children": [child_spec_to_request(c, ref) for c in outcome.new_children],
        "outbox": [outbox_message_to_request(m) for m in outcome.outbox],
        "close": None if outcome.close is None else close_outcome_to_request(outcome.close),
        "code_version": outcome.code_version,
        "search_attributes": search_attributes_values_to_wire(outcome.search_attributes),
        "continuation": (
            None
            if outcome.continuation is None
            else child_spec_to_request(outcome.continuation, ref)
        ),
        "continuation_ready_at": offset_seconds_opt(ref, outcome.continuation_ready_at),
    }


def tick_outcome_from_request(data: dict[str, Any], server_now: datetime) -> TickOutcome:
    try:
        close = data.get("close")
        continuation = data.get("continuation")
        return TickOutcome(
            worker_id=str(_require(data, "worker_id")),
            consumed_through=int(_require(data, "consumed_through")),
            time=apply_offset(server_now, float(_require(data, "time"))),
            decisions=events_from_wire(data.get("decisions", [])),
            new_steps=step_jobs_from_request(data.get("new_steps", []), server_now),
            cancel_steps=list(data.get("cancel_steps", [])),
            new_timers=timers_from_request(data.get("new_timers", []), server_now),
            cancel_timers=list(data.get("cancel_timers", [])),
            new_children=[
                child_spec_from_request(c, server_now) for c in data.get("new_children", [])
            ],
            outbox=[outbox_message_from_request(m) for m in data.get("outbox", [])],
            close=None if close is None else close_outcome_from_request(close),
            code_version=str(data.get("code_version", "")),
            search_attributes=search_attributes_values_from_wire(data.get("search_attributes")),
            continuation=(
                None if continuation is None else child_spec_from_request(continuation, server_now)
            ),
            continuation_ready_at=apply_offset_opt(server_now, data.get("continuation_ready_at")),
        )
    except (KeyError, ValueError, TypeError) as exc:
        raise WireFormatError(f"malformed tick outcome: {exc}") from exc


# --------------------------------------------------------------------------- search attributes

_ATTR_TYPE_TAG = "__type__"


def _attr_value_to_wire(value: Any) -> Any:
    if isinstance(value, datetime):
        return {_ATTR_TYPE_TAG: "datetime", "value": iso_encode(value)}
    return value


def _attr_value_from_wire(value: Any) -> Any:
    if isinstance(value, dict) and value.get(_ATTR_TYPE_TAG) == "datetime":
        return iso_decode(value["value"])
    return value


def search_attributes_values_to_wire(values: dict[str, Any] | None) -> dict[str, Any] | None:
    """Wire form of a search attribute *value* mapping (``TickOutcome.search_attributes``,
    the return of ``get_search_attributes``). These are opaque business data, not
    deadlines, so they are sent as-is rather than offset from a reference time."""
    if values is None:
        return None
    return {k: _attr_value_to_wire(v) for k, v in values.items()}


def search_attributes_values_from_wire(data: Any) -> dict[str, Any] | None:
    if data is None:
        return None
    return {str(k): _attr_value_from_wire(v) for k, v in data.items()}


def search_attributes_schema_to_wire(schema: SearchAttributes) -> dict[str, str]:
    return {name: attr_type.value for name, attr_type in schema.types.items()}


def search_attributes_schema_from_wire(data: dict[str, Any]) -> SearchAttributes:
    try:
        return SearchAttributes.from_mapping(
            {str(name): AttributeType(value) for name, value in data.items()}
        )
    except (KeyError, ValueError, TypeError) as exc:
        raise WireFormatError(f"malformed search attribute schema: {exc}") from exc


# --------------------------------------------------------------------------- schedules

#: Top-level ``ScheduleState`` fields that are ISO timestamps in ``state.to_dict()``.
_SCHEDULE_DT_FIELDS = ("created_at", "updated_at")
_SCHEDULE_OPT_DT_FIELDS = ("next_fire_at", "last_evaluated", "buffered")
_SCHEDULED_RUN_DT_FIELDS = ("nominal_time", "started_at")


def schedule_state_to_wire(state: ScheduleState) -> dict[str, Any]:
    """Absolute-time wire form, used for values returned to the client."""
    return state.to_dict()


def schedule_state_from_wire(data: dict[str, Any]) -> ScheduleState:
    try:
        return ScheduleState.from_dict(data)
    except (KeyError, ValueError, TypeError) as exc:
        raise WireFormatError(f"malformed schedule: {exc}") from exc


def _shift_run(run: dict[str, Any], shift: Callable[[Any], Any]) -> dict[str, Any]:
    return {**run, **{f: shift(run[f]) for f in _SCHEDULED_RUN_DT_FIELDS}}


def _encode_offset(ref: datetime, iso: Any) -> float:
    return offset_seconds(ref, iso_decode(iso))


def _decode_offset(server_now: datetime, offset: Any) -> str:
    return iso_encode(apply_offset(server_now, float(offset)))


def schedule_state_to_request(state: ScheduleState, ref: datetime) -> dict[str, Any]:
    """``state.to_dict()`` with every timestamp replaced by its offset from ``ref``."""
    data = state.to_dict()

    def shift(iso: Any) -> float:
        return _encode_offset(ref, iso)

    for field_name in _SCHEDULE_DT_FIELDS:
        data[field_name] = shift(data[field_name])
    for field_name in _SCHEDULE_OPT_DT_FIELDS:
        value = data.get(field_name)
        data[field_name] = None if value is None else shift(value)
    for key in ("running", "recent"):
        data[key] = [_shift_run(run, shift) for run in data[key]]
    return data


def schedule_state_from_request(data: dict[str, Any], server_now: datetime) -> ScheduleState:
    """Reverses :func:`schedule_state_to_request` using the server's own clock."""
    data = dict(data)

    def unshift(offset: Any) -> str:
        return _decode_offset(server_now, offset)

    try:
        for field_name in _SCHEDULE_DT_FIELDS:
            data[field_name] = unshift(_require(data, field_name))
        for field_name in _SCHEDULE_OPT_DT_FIELDS:
            value = data.get(field_name)
            data[field_name] = None if value is None else unshift(value)
        for key in ("running", "recent"):
            data[key] = [_shift_run(run, unshift) for run in data.get(key, [])]
        return ScheduleState.from_dict(data)
    except (KeyError, ValueError, TypeError) as exc:
        raise WireFormatError(f"malformed schedule: {exc}") from exc
