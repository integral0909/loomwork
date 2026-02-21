"""The control API: plain-JSON workflow control for callers without Loomwork's Python
types (``curl``, other languages, dashboards).

Arguments and signal/update values are ordinary JSON, encoded to and decoded from
:class:`~loomwork.codec.payload.Payload` with the default converter -- the server never
imports workflow code, so it cannot validate values against a workflow's parameter types
the way an in-process :class:`~loomwork.client.client.Client` does.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from loomwork.codec.converter import default_converter
from loomwork.diagnostics import (
    DEFAULT_HISTORY_WARNING_FRACTION,
    DEFAULT_LIMIT,
    DEFAULT_TICK_FAILURE_THRESHOLD,
    collect_diagnostics,
)
from loomwork.history.events import UpdateCompleted
from loomwork.options import (
    IdConflictPolicy,
    IdReusePolicy,
    ResetPoint,
    VersioningPolicy,
    as_timedelta,
)
from loomwork.retry import RetryPolicy
from loomwork.schedules.model import (
    OverlapPolicy,
    ScheduleAction,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
    validate_schedule_id,
)
from loomwork.schedules.service import ScheduleService
from loomwork.server.auth import audit
from loomwork.server.errors import bad_request
from loomwork.server.responses import (
    event_view,
    execution_view,
    failure_view,
    payload_view,
    schedule_view,
)
from loomwork.server.schemas import (
    BackfillRequest,
    CreateScheduleRequest,
    ReasonRequest,
    RequestUpdateRequest,
    ResetWorkflowRequest,
    ScheduleNoteRequest,
    SignalRequest,
    SignalWithStartRequest,
    StartWorkflowRequest,
    UpdateScheduleRequest,
)
from loomwork.server.security import authenticate, server_state
from loomwork.server.state import NamespaceState
from loomwork.store.records import ExecutionRecord, ExecutionStatus
from loomwork.visibility.attributes import encode_attributes

_M = TypeVar("_M", bound=BaseModel)


def _namespace_name(request: Request) -> str:
    return str(request.path_params["namespace"])


def _namespace(request: Request) -> NamespaceState:
    return server_state(request).namespace(_namespace_name(request))


async def _model(request: Request, model: type[_M]) -> _M:
    try:
        raw = await request.json()
    except Exception as exc:
        raise bad_request(f"invalid JSON body: {exc}") from exc
    try:
        return model.model_validate(raw)
    except ValidationError as exc:
        raise bad_request(str(exc)) from exc


def _audit(request: Request, token_id: str, action: str, target: str = "") -> None:
    audit(token_id=token_id, namespace=_namespace_name(request), action=action, target=target)


# --------------------------------------------------------------------------- workflows


async def start_workflow(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_write"
    )
    ns = _namespace(request)
    body = await _model(request, StartWorkflowRequest)
    args = default_converter.encode_args(body.args)
    retry = (
        RetryPolicy(max_attempts=body.retry_max_attempts)
        if body.retry_max_attempts is not None
        else None
    )
    record = await ns.engine.start_workflow(
        body.name,
        args,
        workflow_id=body.workflow_id,
        search_attributes=body.search_attributes,
        run_timeout=body.run_timeout,
        execution_timeout=body.execution_timeout,
        retry_policy=retry,
        id_reuse_policy=IdReusePolicy(body.id_reuse_policy),
        id_conflict_policy=IdConflictPolicy(body.id_conflict_policy),
    )
    _audit(request, token.id, "start_workflow", record.run_id)
    return JSONResponse(execution_view(record), status_code=201)


async def list_workflows(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_read"
    )
    ns = _namespace(request)
    params = request.query_params
    limit = int(params.get("limit", 100))
    query_text = params.get("query")
    if query_text:
        if "workflow_name" in params or "status" in params:
            raise bad_request("'query' cannot be combined with 'workflow_name' or 'status'")
        records = await ns.engine.query_executions(query_text, limit)
    else:
        status = ExecutionStatus(params["status"]) if "status" in params else None
        records = await ns.engine.list_executions(
            workflow_name=params.get("workflow_name"), status=status, limit=limit
        )
    _audit(request, token.id, "list_workflows")
    return JSONResponse({"executions": [execution_view(r) for r in records]})


async def _describe_view(ns: NamespaceState, record: ExecutionRecord) -> dict[str, Any]:
    body = execution_view(record)
    body["search_attributes"] = await ns.engine.search_attributes(record.run_id)
    return body


async def describe_workflow(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_read"
    )
    ns = _namespace(request)
    record = await ns.engine.resolve(run_id=request.path_params["run_id"])
    _audit(request, token.id, "describe_workflow", record.run_id)
    return JSONResponse(await _describe_view(ns, record))


async def describe_workflow_by_id(request: Request) -> JSONResponse:
    """Resolve the *latest* run of a workflow id: the control-API counterpart of the
    worker API's ``GET /workflows/{workflow_id}/latest``, needed so a caller that only
    knows a workflow id (not one of its run ids) can reach every other ``{run_id}``-keyed
    endpoint below."""
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_read"
    )
    ns = _namespace(request)
    record = await ns.engine.resolve(workflow_id=request.path_params["workflow_id"])
    _audit(request, token.id, "describe_workflow", record.run_id)
    return JSONResponse(await _describe_view(ns, record))


async def whoami(request: Request) -> JSONResponse:
    """What the caller's token may do: its id, roles and namespaces, plus the server's own
    clock.

    Lets a caller (in practice, the CLI) discover whether its token has ``worker`` role
    (so it should use the worker API / ``HttpStore``) or only ``operator``/``reader``
    (so it must use this control API instead) without already knowing which -- the one
    endpoint any correctly-scoped token may call regardless of its role. ``server_time``
    additionally lets ``loomwork doctor`` report the clock skew between the machine it
    runs on and the server, a common cause of confusing timestamps and premature timeouts.
    """
    token = await authenticate(request, namespace=_namespace_name(request), capability=None)
    now = server_state(request).clock.now()
    return JSONResponse(
        {
            "id": token.id,
            "roles": [r.value for r in token.roles],
            "namespaces": list(token.namespaces),
            "server_time": now.isoformat(),
        }
    )


async def diagnostics(request: Request) -> JSONResponse:
    """Aggregate health facts about this namespace's store, for ``loomwork doctor
    --server`` (see :mod:`loomwork.diagnostics`): the same checks the CLI runs directly
    against a local store, computed here since a ``--server`` connection has no store
    access of its own.
    """
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_read"
    )
    ns = _namespace(request)
    params = request.query_params
    limit = int(params.get("limit", DEFAULT_LIMIT))
    tick_failure_threshold = int(
        params.get("tick_failure_threshold", DEFAULT_TICK_FAILURE_THRESHOLD)
    )
    history_warning_fraction = float(
        params.get("history_warning_fraction", DEFAULT_HISTORY_WARNING_FRACTION)
    )
    queues = params.getlist("queue")
    now = server_state(request).clock.now()
    snapshot = await collect_diagnostics(
        ns.store,
        ns.engine,
        now,
        tick_failure_threshold=tick_failure_threshold,
        history_warning_fraction=history_warning_fraction,
        queues=queues,
        limit=limit,
    )
    _audit(request, token.id, "diagnostics")
    return JSONResponse(snapshot.to_dict())


async def workflow_history(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_read"
    )
    ns = _namespace(request)
    run_id = request.path_params["run_id"]
    events = await ns.engine.history(run_id)
    _audit(request, token.id, "workflow_history", run_id)
    return JSONResponse({"run_id": run_id, "events": [event_view(e) for e in events]})


async def workflow_children(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_read"
    )
    ns = _namespace(request)
    run_id = request.path_params["run_id"]
    children = await ns.engine.children(run_id)
    _audit(request, token.id, "workflow_children", run_id)
    return JSONResponse({"children": [execution_view(c) for c in children]})


async def signal_workflow(request: Request) -> Response:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_write"
    )
    ns = _namespace(request)
    run_id = request.path_params["run_id"]
    body = await _model(request, SignalRequest)
    await ns.engine.signal(
        run_id, body.channel, default_converter.encode(body.value), message_id=body.message_id
    )
    _audit(request, token.id, "signal_workflow", run_id)
    return Response(status_code=204)


async def signal_with_start(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_write"
    )
    ns = _namespace(request)
    body = await _model(request, SignalWithStartRequest)
    record = await ns.engine.signal_with_start(
        body.name,
        default_converter.encode_args(body.args),
        workflow_id=body.workflow_id,
        channel=body.channel,
        payload=default_converter.encode(body.value),
        message_id=body.message_id,
    )
    _audit(request, token.id, "signal_with_start", record.run_id)
    return JSONResponse(execution_view(record), status_code=201)


async def request_update(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_write"
    )
    ns = _namespace(request)
    run_id = request.path_params["run_id"]
    body = await _model(request, RequestUpdateRequest)
    update_id = await ns.engine.request_update(
        run_id, body.name, default_converter.encode_args(body.args), update_id=body.update_id
    )
    _audit(request, token.id, "request_update", run_id)
    return JSONResponse({"update_id": update_id}, status_code=202)


async def wait_update(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_read"
    )
    ns = _namespace(request)
    run_id = request.path_params["run_id"]
    update_id = request.path_params["update_id"]
    timeout = float(request.query_params.get("timeout", 20.0))
    timeout = min(timeout, server_state(request).config.limits.long_poll_max_seconds)
    outcome: UpdateCompleted = await ns.engine.wait_update(run_id, update_id, timeout)
    _audit(request, token.id, "wait_update", run_id)
    body: dict[str, Any] = {"update_id": update_id, "status": "completed"}
    if outcome.failure is not None:
        body["failure"] = failure_view(outcome.failure)
    else:
        body["result"] = payload_view(outcome.result)
    return JSONResponse(body)


async def cancel_workflow(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_write"
    )
    ns = _namespace(request)
    run_id = request.path_params["run_id"]
    body = await _model(request, ReasonRequest)
    ok = await ns.engine.request_cancel(run_id, body.reason)
    _audit(request, token.id, "cancel_workflow", run_id)
    return JSONResponse({"ok": ok})


async def terminate_workflow(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_write"
    )
    ns = _namespace(request)
    run_id = request.path_params["run_id"]
    body = await _model(request, ReasonRequest)
    ok = await ns.engine.terminate(run_id, body.reason)
    _audit(request, token.id, "terminate_workflow", run_id)
    return JSONResponse({"ok": ok})


async def reset_workflow(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_write"
    )
    ns = _namespace(request)
    run_id = request.path_params["run_id"]
    body = await _model(request, ResetWorkflowRequest)
    if (body.to is None) == (body.after_tick is None):
        raise bad_request("exactly one of 'to' and 'after_tick' is required")
    to = (
        ResetPoint.after_tick(body.after_tick)
        if body.after_tick is not None
        else (
            ResetPoint.first_tick() if body.to == "first_tick" else ResetPoint.before_last_failure()
        )
    )
    record = await ns.engine.reset(run_id, to=to, reason=body.reason)
    _audit(request, token.id, "reset_workflow", record.run_id)
    return JSONResponse(execution_view(record), status_code=201)


async def workflow_result(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_read"
    )
    ns = _namespace(request)
    run_id = request.path_params["run_id"]
    timeout = float(request.query_params.get("timeout", 20.0))
    timeout = min(timeout, server_state(request).config.limits.long_poll_max_seconds)
    record = await ns.engine.wait_closed(run_id, timeout)
    _audit(request, token.id, "workflow_result", run_id)
    return JSONResponse(execution_view(record))


# --------------------------------------------------------------------------- schedules


def _parse_timestamp(text: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise bad_request(f"invalid timestamp {text!r}: {exc}") from exc
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _schedule_service(ns: NamespaceState) -> ScheduleService:
    return ScheduleService(ns.store, ns.engine.clock, config=ns.engine.config)


async def create_schedule(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_write"
    )
    ns = _namespace(request)
    body = await _model(request, CreateScheduleRequest)
    if (body.cron is None) == (body.interval is None):
        raise bad_request("give exactly one of 'cron' and 'interval'")
    state = ScheduleState(
        schedule_id=validate_schedule_id(body.schedule_id),
        spec=ScheduleSpec(
            cron=body.cron,
            interval=body.interval,
            timezone=body.timezone,
            start_at=_parse_timestamp(body.start_at) if body.start_at else None,
            end_at=_parse_timestamp(body.end_at) if body.end_at else None,
            jitter=body.jitter,
        ),
        action=ScheduleAction(
            body.workflow,
            default_converter.encode_args(body.args),
            id_reuse_policy=(IdReusePolicy(body.id_reuse_policy) if body.id_reuse_policy else None),
            id_conflict_policy=(
                IdConflictPolicy(body.id_conflict_policy) if body.id_conflict_policy else None
            ),
            versioning_policy=(
                VersioningPolicy(body.versioning_policy) if body.versioning_policy else None
            ),
            run_timeout=as_timedelta(body.run_timeout),
            execution_timeout=as_timedelta(body.execution_timeout),
            search_attributes=(
                encode_attributes(body.search_attributes, ns.engine.config.search_attributes)
                if body.search_attributes
                else None
            ),
        ),
        policy=SchedulePolicy(
            OverlapPolicy(body.overlap), body.catchup_window, body.pause_on_failure
        ),
        paused=body.paused,
        note=body.note,
    )
    saved = await _schedule_service(ns).create(state)
    _audit(request, token.id, "create_schedule", saved.schedule_id)
    return JSONResponse(schedule_view(saved), status_code=201)


async def list_schedules(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_read"
    )
    ns = _namespace(request)
    limit = int(request.query_params.get("limit", 100))
    states = await ns.store.list_schedules(limit)
    _audit(request, token.id, "list_schedules")
    return JSONResponse({"schedules": [schedule_view(s) for s in states]})


async def describe_schedule(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_read"
    )
    ns = _namespace(request)
    schedule_id = request.path_params["schedule_id"]
    state = await ns.store.get_schedule(schedule_id)
    _audit(request, token.id, "describe_schedule", schedule_id)
    return JSONResponse(schedule_view(state))


async def _set_paused(request: Request, *, paused: bool, action: str) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_write"
    )
    ns = _namespace(request)
    schedule_id = request.path_params["schedule_id"]
    body = await _model(request, ScheduleNoteRequest)
    state = await _schedule_service(ns).update(schedule_id, paused=paused, note=body.note)
    _audit(request, token.id, action, schedule_id)
    return JSONResponse(schedule_view(state))


async def pause_schedule(request: Request) -> JSONResponse:
    return await _set_paused(request, paused=True, action="pause_schedule")


async def unpause_schedule(request: Request) -> JSONResponse:
    return await _set_paused(request, paused=False, action="unpause_schedule")


async def trigger_schedule(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_write"
    )
    ns = _namespace(request)
    schedule_id = request.path_params["schedule_id"]
    state = await _schedule_service(ns).trigger(schedule_id)
    _audit(request, token.id, "trigger_schedule", schedule_id)
    return JSONResponse(schedule_view(state))


async def backfill_schedule(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_write"
    )
    ns = _namespace(request)
    schedule_id = request.path_params["schedule_id"]
    body = await _model(request, BackfillRequest)
    start, end = _parse_timestamp(body.start), _parse_timestamp(body.end)
    if end <= start:
        raise bad_request("'end' must be after 'start'")
    state = await _schedule_service(ns).backfill(schedule_id, start, end)
    _audit(request, token.id, "backfill_schedule", schedule_id)
    return JSONResponse(schedule_view(state))


async def upcoming_schedule(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_read"
    )
    ns = _namespace(request)
    schedule_id = request.path_params["schedule_id"]
    count = int(request.query_params.get("count", 5))
    times = await _schedule_service(ns).upcoming(schedule_id, count)
    _audit(request, token.id, "upcoming_schedule", schedule_id)
    return JSONResponse({"upcoming": [t.isoformat() for t in times]})


async def update_schedule(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_write"
    )
    ns = _namespace(request)
    schedule_id = request.path_params["schedule_id"]
    body = await _model(request, UpdateScheduleRequest)
    service = _schedule_service(ns)
    current = await service.store.get_schedule(schedule_id)
    changes: dict[str, Any] = {}
    if any(v is not None for v in (body.cron, body.interval, body.timezone, body.jitter)):
        old = current.spec
        cron = (
            body.cron
            if body.cron is not None
            else (None if body.interval is not None else old.cron)
        )
        interval = (
            body.interval
            if body.interval is not None
            else (None if body.cron is not None else old.interval)
        )
        changes["spec"] = ScheduleSpec(
            cron=cron,
            interval=interval,
            offset=old.offset if interval == old.interval else 0,
            timezone=body.timezone or old.timezone,
            start_at=old.start_at,
            end_at=old.end_at,
            jitter=body.jitter if body.jitter is not None else old.jitter,
        )
    if (
        body.overlap is not None
        or body.catchup_window is not None
        or body.pause_on_failure is not None
    ):
        old_policy = current.policy
        changes["policy"] = SchedulePolicy(
            OverlapPolicy(body.overlap) if body.overlap is not None else old_policy.overlap,
            body.catchup_window if body.catchup_window is not None else old_policy.catchup_window,
            body.pause_on_failure
            if body.pause_on_failure is not None
            else old_policy.pause_on_failure,
        )
    if any(
        v is not None
        for v in (
            body.id_reuse_policy,
            body.id_conflict_policy,
            body.versioning_policy,
            body.run_timeout,
            body.execution_timeout,
            body.search_attributes,
        )
    ):
        old_action = current.action
        changes["action"] = ScheduleAction(
            workflow_name=old_action.workflow_name,
            args=old_action.args,
            id_reuse_policy=(
                IdReusePolicy(body.id_reuse_policy)
                if body.id_reuse_policy is not None
                else old_action.id_reuse_policy
            ),
            id_conflict_policy=(
                IdConflictPolicy(body.id_conflict_policy)
                if body.id_conflict_policy is not None
                else old_action.id_conflict_policy
            ),
            versioning_policy=(
                VersioningPolicy(body.versioning_policy)
                if body.versioning_policy is not None
                else old_action.versioning_policy
            ),
            run_timeout=(
                as_timedelta(body.run_timeout)
                if body.run_timeout is not None
                else old_action.run_timeout
            ),
            execution_timeout=(
                as_timedelta(body.execution_timeout)
                if body.execution_timeout is not None
                else old_action.execution_timeout
            ),
            search_attributes=(
                encode_attributes(body.search_attributes, ns.engine.config.search_attributes)
                if body.search_attributes is not None
                else old_action.search_attributes
            ),
        )
    if body.note is not None:
        changes["note"] = body.note
    if not changes:
        raise bad_request("nothing to change")
    state = await service.update(schedule_id, **changes)
    _audit(request, token.id, "update_schedule", schedule_id)
    return JSONResponse(schedule_view(state))


async def delete_schedule(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_write"
    )
    ns = _namespace(request)
    schedule_id = request.path_params["schedule_id"]
    deleted = await ns.store.delete_schedule(schedule_id)
    _audit(request, token.id, "delete_schedule", schedule_id)
    return JSONResponse({"ok": deleted})


routes = [
    Route("/whoami", whoami, methods=["GET"]),
    Route("/diagnostics", diagnostics, methods=["GET"]),
    Route("/workflows", start_workflow, methods=["POST"]),
    Route("/workflows", list_workflows, methods=["GET"]),
    Route("/workflows/signal-with-start", signal_with_start, methods=["POST"]),
    Route("/workflows/by-id/{workflow_id}", describe_workflow_by_id, methods=["GET"]),
    Route("/workflows/{run_id}", describe_workflow, methods=["GET"]),
    Route("/workflows/{run_id}/history", workflow_history, methods=["GET"]),
    Route("/workflows/{run_id}/children", workflow_children, methods=["GET"]),
    Route("/workflows/{run_id}/signal", signal_workflow, methods=["POST"]),
    Route("/workflows/{run_id}/updates", request_update, methods=["POST"]),
    Route("/workflows/{run_id}/updates/{update_id}", wait_update, methods=["GET"]),
    Route("/workflows/{run_id}/cancel", cancel_workflow, methods=["POST"]),
    Route("/workflows/{run_id}/terminate", terminate_workflow, methods=["POST"]),
    Route("/workflows/{run_id}/reset", reset_workflow, methods=["POST"]),
    Route("/workflows/{run_id}/result", workflow_result, methods=["GET"]),
    Route("/schedules", create_schedule, methods=["POST"]),
    Route("/schedules", list_schedules, methods=["GET"]),
    Route("/schedules/{schedule_id}", describe_schedule, methods=["GET"]),
    Route("/schedules/{schedule_id}", delete_schedule, methods=["DELETE"]),
    Route("/schedules/{schedule_id}/pause", pause_schedule, methods=["POST"]),
    Route("/schedules/{schedule_id}/unpause", unpause_schedule, methods=["POST"]),
    Route("/schedules/{schedule_id}/trigger", trigger_schedule, methods=["POST"]),
    Route("/schedules/{schedule_id}/backfill", backfill_schedule, methods=["POST"]),
    Route("/schedules/{schedule_id}/upcoming", upcoming_schedule, methods=["GET"]),
    Route("/schedules/{schedule_id}/update", update_schedule, methods=["POST"]),
]


__all__ = ["routes"]
