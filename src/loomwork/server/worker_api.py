"""The worker API: one route per :class:`~loomwork.store.base.Store` method.

Every handler follows the same shape: authenticate for the ``worker`` capability in the
path's namespace, read a JSON body (for mutating calls), resolve datetimes carried in the
body against the server's own clock (see :mod:`loomwork.remote.wire`), call the matching
store method, and render the result with the matching wire encoder.

``claim_tick`` and ``claim_step`` long-poll: if nothing is ready, the handler waits on the
store's notifier for up to ``min(requested wait, server cap)`` seconds before answering
"nothing ready" (a JSON body with a ``null`` record), so idle remote workers do not have
to busy-poll the server.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from loomwork.history.events import Event, StepCompleted, StepFailed, WorkflowStarted
from loomwork.remote import wire
from loomwork.server.auth import audit
from loomwork.server.errors import bad_request
from loomwork.server.security import authenticate, server_state
from loomwork.server.state import NamespaceState
from loomwork.store.notify import TOPIC_STEPS, TOPIC_TICKS

# --------------------------------------------------------------------------- helpers


async def _body(request: Request) -> dict[str, Any]:
    try:
        data = await request.json()
    except Exception as exc:
        raise bad_request(f"invalid JSON body: {exc}") from exc
    if not isinstance(data, dict):
        raise bad_request("expected a JSON object body")
    return data


def _namespace_name(request: Request) -> str:
    return str(request.path_params["namespace"])


def _namespace(request: Request) -> NamespaceState:
    return server_state(request).namespace(_namespace_name(request))


def _long_poll_wait(request: Request, body: dict[str, Any]) -> float:
    cap = server_state(request).config.limits.long_poll_max_seconds
    requested = float(body.get("wait", 0.0))
    return max(0.0, min(requested, cap))


def _now(server_now: datetime, body: dict[str, Any]) -> datetime:
    """Decode the request's ``now`` offset against ``server_now`` (see
    :mod:`loomwork.remote.wire`). Defaults to ``server_now`` itself (offset 0) for any
    caller that omits it, which is what "the server uses its own clock" means in practice.
    """
    return wire.apply_offset(server_now, float(body.get("now", 0.0)))


def _audit(request: Request, token_id: str, action: str, target: str = "") -> None:
    audit(token_id=token_id, namespace=_namespace_name(request), action=action, target=target)


# --------------------------------------------------------------------------- executions


async def create_execution(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    record = wire.execution_record_from_request(body["record"], server_now)
    started = wire.event_from_wire(body["started"])
    if not isinstance(started, WorkflowStarted):
        raise bad_request("'started' must encode a workflow_started event")
    extra = wire.events_from_wire(body.get("extra_inputs", []))
    id_reuse_policy = wire.id_reuse_policy_from_wire(body.get("id_reuse_policy", "allow_duplicate"))
    id_conflict_policy = wire.id_conflict_policy_from_wire(body.get("id_conflict_policy", "fail"))
    created, event = await ns.store.create_execution(
        record,
        started,
        _now(server_now, body),
        extra_inputs=extra,
        id_reuse_policy=id_reuse_policy,
        id_conflict_policy=id_conflict_policy,
    )
    _audit(request, token.id, "create_execution", created.run_id)
    return JSONResponse(
        {"record": wire.execution_record_to_wire(created), "event": wire.event_to_wire(event)}
    )


async def get_execution(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    record = await ns.store.get_execution(request.path_params["run_id"])
    return JSONResponse({"record": wire.execution_record_to_wire(record)})


async def find_execution(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    record = await ns.store.find_execution(request.path_params["workflow_id"])
    return JSONResponse({"record": wire.execution_record_to_wire(record)})


async def list_executions(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    params = request.query_params
    status = wire.execution_status_from_wire(params["status"]) if "status" in params else None
    records = await ns.store.list_executions(
        workflow_name=params.get("workflow_name"),
        status=status,
        limit=int(params.get("limit", 100)),
    )
    return JSONResponse({"records": [wire.execution_record_to_wire(r) for r in records]})


# --------------------------------------------------------------------------- visibility


async def query_executions(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    schema = wire.search_attributes_schema_from_wire(body.get("schema", {}))
    records = await ns.store.query_executions(
        str(body["query"]), schema, _now(server_now, body), int(body.get("limit", 100))
    )
    return JSONResponse({"records": [wire.execution_record_to_wire(r) for r in records]})


async def get_search_attributes(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    attributes = await ns.store.get_search_attributes(request.path_params["run_id"])
    return JSONResponse({"attributes": wire.search_attributes_values_to_wire(attributes)})


# --------------------------------------------------------------------------- history


async def load_history(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    params = request.query_params
    limit = int(params["limit"]) if "limit" in params else None
    events = await ns.store.load_history(
        request.path_params["run_id"], after_id=int(params.get("after_id", 0)), limit=limit
    )
    return JSONResponse({"events": wire.events_to_wire(events)})


async def append_input(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    event = wire.event_from_wire(body["event"])
    run_id = request.path_params["run_id"]
    stored = await ns.store.append_input(run_id, event, _now(server_now, body))
    _audit(request, token.id, "append_input", run_id)
    return JSONResponse({"event": wire.event_opt_to_wire(stored)})


async def request_cancel(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    ok = await ns.store.request_cancel(
        request.path_params["run_id"], str(body.get("reason", "")), _now(server_now, body)
    )
    _audit(request, token.id, "request_cancel", request.path_params["run_id"])
    return JSONResponse({"ok": ok})


async def terminate(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    ok = await ns.store.terminate(
        request.path_params["run_id"], str(body.get("reason", "")), _now(server_now, body)
    )
    _audit(request, token.id, "terminate", request.path_params["run_id"])
    return JSONResponse({"ok": ok})


async def reset_run(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    old_record, new_record = await ns.store.reset_run(
        request.path_params["run_id"],
        str(body["new_run_id"]),
        wire.reset_point_from_wire(body["to"]),
        str(body.get("reason", "")),
        _now(server_now, body),
    )
    _audit(request, token.id, "reset_run", new_record.run_id)
    return JSONResponse(
        {
            "old_record": wire.execution_record_to_wire(old_record),
            "new_record": wire.execution_record_to_wire(new_record),
        }
    )


# --------------------------------------------------------------------------- ticks


async def claim_tick(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    worker_id = str(body["worker_id"])
    lease = timedelta(seconds=float(body["lease_seconds"]))
    workflow_names = body.get("workflow_names")
    now_offset = float(body.get("now", 0.0))
    wait = _long_poll_wait(request, body)
    notifier = ns.store.notifier
    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait
    while True:
        token_seq = notifier.token(TOPIC_TICKS)
        current_now = wire.apply_offset(ns.engine.clock.now(), now_offset)
        claimed = await ns.store.claim_tick(worker_id, current_now, lease, workflow_names)
        if claimed is not None:
            _audit(request, token.id, "claim_tick", claimed.run_id)
            return JSONResponse({"record": wire.execution_record_to_wire(claimed)})
        remaining = deadline - loop.time()
        if remaining <= 0:
            return JSONResponse({"record": None})
        await notifier.wait(TOPIC_TICKS, token_seq, remaining)


async def complete_tick(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    worker_id = str(body["worker_id"])
    outcome = wire.tick_outcome_from_request(body["outcome"], server_now)
    events = await ns.store.complete_tick(
        request.path_params["run_id"], worker_id, outcome, _now(server_now, body)
    )
    _audit(request, token.id, "complete_tick", request.path_params["run_id"])
    return JSONResponse({"events": wire.events_to_wire(events)})


async def fail_tick(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    retry_at = wire.apply_offset(server_now, float(body["retry_at"]))
    await ns.store.fail_tick(
        request.path_params["run_id"],
        str(body["worker_id"]),
        str(body.get("error", "")),
        retry_at,
        _now(server_now, body),
    )
    _audit(request, token.id, "fail_tick", request.path_params["run_id"])
    return JSONResponse({"ok": True})


# --------------------------------------------------------------------------- steps


async def claim_step(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    worker_id = str(body["worker_id"])
    lease = timedelta(seconds=float(body["lease_seconds"]))
    step_names = body.get("step_names")
    queues = body.get("queues")
    now_offset = float(body.get("now", 0.0))
    wait = _long_poll_wait(request, body)
    notifier = ns.store.notifier
    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait
    while True:
        token_seq = notifier.token(TOPIC_STEPS)
        current_now = wire.apply_offset(ns.engine.clock.now(), now_offset)
        claimed = await ns.store.claim_step(worker_id, current_now, lease, step_names, queues)
        if claimed is not None:
            _audit(request, token.id, "claim_step", claimed.job_id)
            return JSONResponse({"job": wire.step_job_to_wire(claimed)})
        remaining = deadline - loop.time()
        if remaining <= 0:
            return JSONResponse({"job": None})
        await notifier.wait(TOPIC_STEPS, token_seq, remaining)


def _expect_step_outcome(data: dict[str, Any]) -> StepCompleted | StepFailed:
    event: Event = wire.event_from_wire(data)
    if not isinstance(event, StepCompleted | StepFailed):
        raise bad_request("'event' must encode a step_completed or step_failed event")
    return event


async def finish_step(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    event = _expect_step_outcome(body["event"])
    ok = await ns.store.finish_step(
        request.path_params["job_id"], str(body["worker_id"]), event, _now(server_now, body)
    )
    _audit(request, token.id, "finish_step", request.path_params["job_id"])
    return JSONResponse({"ok": ok})


async def retry_step(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    ready_at = wire.apply_offset(server_now, float(body["ready_at"]))
    failure = wire.failure_from_wire(body["failure"])
    now = _now(server_now, body)
    ok = await ns.store.retry_step(
        request.path_params["job_id"], str(body["worker_id"]), ready_at, failure, now
    )
    _audit(request, token.id, "retry_step", request.path_params["job_id"])
    return JSONResponse({"ok": ok})


async def renew_step_lease(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    expires_at = wire.apply_offset(server_now, float(body["expires_at"]))
    ok = await ns.store.renew_step_lease(
        request.path_params["job_id"], str(body["worker_id"]), expires_at
    )
    _audit(request, token.id, "renew_step_lease", request.path_params["job_id"])
    return JSONResponse({"ok": ok})


async def heartbeat_step(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    expires_at = wire.apply_offset(server_now, float(body["expires_at"]))
    details = wire.payload_opt_from_wire(body.get("details"))
    result = await ns.store.heartbeat_step(
        request.path_params["job_id"],
        str(body["worker_id"]),
        expires_at,
        details,
        _now(server_now, body),
    )
    _audit(request, token.id, "heartbeat_step", request.path_params["job_id"])
    return JSONResponse(
        {"result": None if result is None else wire.heartbeat_result_to_wire(result)}
    )


async def release_step(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    ok = await ns.store.release_step(
        request.path_params["job_id"],
        str(body["worker_id"]),
        _now(server_now, body),
        consume_attempt=bool(body["consume_attempt"]),
    )
    _audit(request, token.id, "release_step", request.path_params["job_id"])
    return JSONResponse({"ok": ok})


async def next_step_ready_at(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    ready_at = await ns.store.next_step_ready_at(body.get("step_names"), body.get("queues"))
    return JSONResponse({"ready_at": wire.iso_encode_opt(ready_at)})


async def get_step_jobs(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    jobs = await ns.store.get_step_jobs(request.path_params["run_id"])
    return JSONResponse({"jobs": wire.step_jobs_to_wire(jobs)})


# --------------------------------------------------------------------------- outbox / children


async def relay_outbox(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    processed = await ns.store.relay_outbox(_now(server_now, body), int(body.get("limit", 100)))
    _audit(request, token.id, "relay_outbox")
    return JSONResponse({"processed": processed})


async def outbox_size(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    return JSONResponse({"size": await ns.store.outbox_size()})


async def list_children(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    children = await ns.store.list_children(request.path_params["run_id"])
    return JSONResponse({"children": [wire.execution_record_to_wire(c) for c in children]})


# --------------------------------------------------------------------------- retention / timers


async def delete_closed_before(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    cutoff = wire.apply_offset(server_now, float(body["cutoff"]))
    deleted = await ns.store.delete_closed_before(cutoff, int(body.get("limit", 1000)))
    _audit(request, token.id, "delete_closed_before")
    return JSONResponse({"deleted": deleted})


async def next_timer_at(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    fire_at = await ns.store.next_timer_at()
    return JSONResponse({"fire_at": wire.iso_encode_opt(fire_at)})


async def fire_due_timers(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    fired = await ns.store.fire_due_timers(_now(server_now, body), int(body.get("limit", 100)))
    _audit(request, token.id, "fire_due_timers")
    return JSONResponse({"fired": fired})


# --------------------------------------------------------------------------- schedules


async def create_schedule(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    state = wire.schedule_state_from_request(body["state"], server_now)
    created = await ns.store.create_schedule(state, _now(server_now, body))
    _audit(request, token.id, "create_schedule", created.schedule_id)
    return JSONResponse({"state": wire.schedule_state_to_wire(created)})


async def get_schedule(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    state = await ns.store.get_schedule(request.path_params["schedule_id"])
    return JSONResponse({"state": wire.schedule_state_to_wire(state)})


async def list_schedules(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    limit = int(request.query_params.get("limit", 100))
    states = await ns.store.list_schedules(limit)
    return JSONResponse({"states": [wire.schedule_state_to_wire(s) for s in states]})


async def update_schedule(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    state = wire.schedule_state_from_request(body["state"], server_now)
    updated = await ns.store.update_schedule(
        state, int(body["expected_version"]), _now(server_now, body)
    )
    _audit(request, token.id, "update_schedule", updated.schedule_id)
    return JSONResponse({"state": wire.schedule_state_to_wire(updated)})


async def delete_schedule(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    schedule_id = request.path_params["schedule_id"]
    ok = await ns.store.delete_schedule(schedule_id)
    _audit(request, token.id, "delete_schedule", schedule_id)
    return JSONResponse({"ok": ok})


async def due_schedules(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    states = await ns.store.due_schedules(_now(server_now, body), int(body.get("limit", 100)))
    return JSONResponse({"states": [wire.schedule_state_to_wire(s) for s in states]})


async def fire_schedule(request: Request) -> JSONResponse:
    token = await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    body = await _body(request)
    server_now = ns.engine.clock.now()
    state = wire.schedule_state_from_request(body["state"], server_now)
    starts = [wire.child_spec_from_request(c, server_now) for c in body.get("starts", [])]
    outbox = [wire.outbox_message_from_request(m) for m in body.get("outbox", [])]
    fired = await ns.store.fire_schedule(
        state, int(body["expected_version"]), starts, outbox, _now(server_now, body)
    )
    _audit(request, token.id, "fire_schedule", fired.schedule_id)
    return JSONResponse({"state": wire.schedule_state_to_wire(fired)})


async def next_schedule_at(request: Request) -> JSONResponse:
    await authenticate(request, namespace=_namespace_name(request), capability="worker")
    ns = _namespace(request)
    fire_at = await ns.store.next_schedule_at()
    return JSONResponse({"fire_at": wire.iso_encode_opt(fire_at)})


# --------------------------------------------------------------------------- routes

routes = [
    Route("/executions", create_execution, methods=["POST"]),
    Route("/executions", list_executions, methods=["GET"]),
    Route("/executions/query", query_executions, methods=["POST"]),
    Route("/executions/{run_id}", get_execution, methods=["GET"]),
    Route("/executions/{run_id}/history", load_history, methods=["GET"]),
    Route("/executions/{run_id}/inputs", append_input, methods=["POST"]),
    Route("/executions/{run_id}/cancel", request_cancel, methods=["POST"]),
    Route("/executions/{run_id}/terminate", terminate, methods=["POST"]),
    Route("/executions/{run_id}/reset", reset_run, methods=["POST"]),
    Route("/executions/{run_id}/steps", get_step_jobs, methods=["GET"]),
    Route("/executions/{run_id}/children", list_children, methods=["GET"]),
    Route("/executions/{run_id}/search-attributes", get_search_attributes, methods=["GET"]),
    Route("/workflows/{workflow_id}/latest", find_execution, methods=["GET"]),
    Route("/ticks/claim", claim_tick, methods=["POST"]),
    Route("/ticks/{run_id}/complete", complete_tick, methods=["POST"]),
    Route("/ticks/{run_id}/fail", fail_tick, methods=["POST"]),
    Route("/steps/claim", claim_step, methods=["POST"]),
    Route("/steps/next-ready-at", next_step_ready_at, methods=["POST"]),
    Route("/steps/{job_id}/finish", finish_step, methods=["POST"]),
    Route("/steps/{job_id}/retry", retry_step, methods=["POST"]),
    Route("/steps/{job_id}/lease", renew_step_lease, methods=["POST"]),
    Route("/steps/{job_id}/heartbeat", heartbeat_step, methods=["POST"]),
    Route("/steps/{job_id}/release", release_step, methods=["POST"]),
    Route("/outbox/relay", relay_outbox, methods=["POST"]),
    Route("/outbox/size", outbox_size, methods=["GET"]),
    Route("/retention/delete-closed", delete_closed_before, methods=["POST"]),
    Route("/timers/next", next_timer_at, methods=["GET"]),
    Route("/timers/fire", fire_due_timers, methods=["POST"]),
    Route("/schedules", create_schedule, methods=["POST"]),
    Route("/schedules", list_schedules, methods=["GET"]),
    Route("/schedules/due", due_schedules, methods=["POST"]),
    Route("/schedules/next", next_schedule_at, methods=["GET"]),
    Route("/schedules/{schedule_id}", get_schedule, methods=["GET"]),
    Route("/schedules/{schedule_id}/update", update_schedule, methods=["POST"]),
    Route("/schedules/{schedule_id}/delete", delete_schedule, methods=["POST"]),
    Route("/schedules/{schedule_id}/fire", fire_schedule, methods=["POST"]),
]
