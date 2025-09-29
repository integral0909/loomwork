"""The control API: plain-JSON workflow control for callers without Loomwork's Python
types (``curl``, other languages, dashboards).

Arguments and signal/update values are ordinary JSON, encoded to and decoded from
:class:`~loomwork.codec.payload.Payload` with the default converter -- the server never
imports workflow code, so it cannot validate values against a workflow's parameter types
the way an in-process :class:`~loomwork.client.client.Client` does.
"""

from __future__ import annotations

from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from loomwork.codec.converter import default_converter
from loomwork.history.events import UpdateCompleted
from loomwork.options import IdConflictPolicy, IdReusePolicy, ResetPoint
from loomwork.server.auth import audit
from loomwork.server.errors import bad_request
from loomwork.server.responses import event_view, execution_view, failure_view, payload_view
from loomwork.server.schemas import (
    ReasonRequest,
    RequestUpdateRequest,
    ResetWorkflowRequest,
    SignalRequest,
    SignalWithStartRequest,
    StartWorkflowRequest,
)
from loomwork.server.security import authenticate, server_state
from loomwork.server.state import NamespaceState
from loomwork.store.records import ExecutionStatus

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
    record = await ns.engine.start_workflow(
        body.name,
        args,
        workflow_id=body.workflow_id,
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
    status = ExecutionStatus(params["status"]) if "status" in params else None
    records = await ns.engine.list_executions(
        workflow_name=params.get("workflow_name"),
        status=status,
        limit=int(params.get("limit", 100)),
    )
    _audit(request, token.id, "list_workflows")
    return JSONResponse({"executions": [execution_view(r) for r in records]})


async def describe_workflow(request: Request) -> JSONResponse:
    token = await authenticate(
        request, namespace=_namespace_name(request), capability="control_read"
    )
    ns = _namespace(request)
    record = await ns.engine.resolve(run_id=request.path_params["run_id"])
    _audit(request, token.id, "describe_workflow", record.run_id)
    return JSONResponse(execution_view(record))


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


routes = [
    Route("/workflows", start_workflow, methods=["POST"]),
    Route("/workflows", list_workflows, methods=["GET"]),
    Route("/workflows/signal-with-start", signal_with_start, methods=["POST"]),
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
]


__all__ = ["routes"]
