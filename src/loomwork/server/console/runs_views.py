"""Runs list, run detail, and the signal/cancel/terminate action forms."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable

from starlette.requests import Request
from starlette.responses import Response

from loomwork.codec.converter import default_converter
from loomwork.errors import InvalidResetPointError, PayloadError, WorkflowClosedError
from loomwork.history.events import WorkflowStarted
from loomwork.history.serde import event_to_dict
from loomwork.options import ResetPoint
from loomwork.server.auth import require
from loomwork.server.console.formatting import describe_payload, failure_chain
from loomwork.server.console.render import redirect_with_flash, render
from loomwork.server.console.runtime import (
    ConsoleRuntime,
    accessible_namespaces,
    can_write,
    check_csrf,
    require_session,
    search_attribute_hint,
)
from loomwork.server.console.util import parse_form, parse_int
from loomwork.store.records import ExecutionStatus
from loomwork.visibility.errors import VisibilityQueryError

View = Callable[[Request], Awaitable[Response]]

_DEFAULT_LIMIT = 50
_MAX_LIMIT = 500


def _runs_path(namespace: str) -> str:
    return f"/console/{namespace}/runs"


def _run_path(namespace: str, run_id: str) -> str:
    return f"/console/{namespace}/runs/{run_id}"


def runs_list_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        path = _runs_path(namespace)
        session = await require_session(request, runtime, next_path=path)
        require(session.record, capability="control_read", namespace=namespace)
        ns = runtime.state.namespace(namespace)

        params = request.query_params
        status_text = params.get("status") or ""
        workflow_name = params.get("workflow_name") or ""
        query_text = params.get("q") or ""
        limit = parse_int(
            params.get("limit"), default=_DEFAULT_LIMIT, minimum=1, maximum=_MAX_LIMIT
        )

        query_error: str | None = None
        if query_text.strip():
            try:
                records = await ns.engine.query_executions(query_text, limit)
            except VisibilityQueryError as exc:
                query_error = exc.format()
                records = []
        else:
            status = ExecutionStatus(status_text) if status_text else None
            records = await ns.engine.list_executions(
                workflow_name=workflow_name or None, status=status, limit=limit
            )

        return render(
            runtime,
            request,
            "runs_list.html",
            namespace=namespace,
            namespaces=accessible_namespaces(runtime, session.record),
            records=records,
            status=status_text,
            workflow_name=workflow_name,
            query_text=query_text,
            query_error=query_error,
            limit=limit,
            statuses=[s.value for s in ExecutionStatus],
            search_attributes=search_attribute_hint(runtime, namespace),
            can_write=can_write(session.record, namespace),
            csrf_token=session.csrf,
        )

    return view


def run_detail_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        run_id = request.path_params["run_id"]
        path = _run_path(namespace, run_id)
        session = await require_session(request, runtime, next_path=path)
        require(session.record, capability="control_read", namespace=namespace)
        ns = runtime.state.namespace(namespace)

        record = await ns.engine.describe(run_id)
        history = await ns.engine.history(run_id)
        steps = await ns.engine.pending_steps(run_id)
        children = await ns.engine.children(run_id)
        attributes = await ns.engine.search_attributes(run_id)

        continued_from = ""
        if history and isinstance(history[0], WorkflowStarted):
            continued_from = history[0].continued_from

        events = [event_to_dict(event) for event in history]

        return render(
            runtime,
            request,
            "run_detail.html",
            namespace=namespace,
            record=record,
            result_display=describe_payload(record.result),
            failures=failure_chain(record.failure),
            attributes=attributes,
            steps=steps,
            children=children,
            events=events,
            continued_from=continued_from,
            can_write=can_write(session.record, namespace),
            csrf_token=session.csrf,
        )

    return view


async def _run_action(
    request: Request,
    runtime: ConsoleRuntime,
) -> tuple[str, str, dict[str, str]]:
    """Common prologue for the three action views: checks the session, capability and
    CSRF token, and returns ``(namespace, run_id, form)``."""
    namespace = request.path_params["namespace"]
    run_id = request.path_params["run_id"]
    path = _run_path(namespace, run_id)
    session = await require_session(request, runtime, next_path=path)
    require(session.record, capability="control_write", namespace=namespace)
    form = await parse_form(request)
    check_csrf(session, str(form.get("csrf_token", "")))
    return namespace, run_id, {str(k): str(v) for k, v in form.items()}


def run_signal_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace, run_id, form = await _run_action(request, runtime)
        ns = runtime.state.namespace(namespace)
        channel = form.get("channel", "").strip()
        value_text = form.get("value", "").strip()
        path = _run_path(namespace, run_id)
        if not channel:
            return redirect_with_flash(path, level="error", message="Channel is required.")
        try:
            value = json.loads(value_text) if value_text else None
        except json.JSONDecodeError as exc:
            return redirect_with_flash(
                path, level="error", message=f"Value is not valid JSON: {exc}"
            )
        try:
            payload = default_converter.encode(value)
        except PayloadError as exc:
            return redirect_with_flash(path, level="error", message=f"Cannot encode value: {exc}")
        try:
            await ns.engine.signal(run_id, channel, payload)
        except WorkflowClosedError:
            return redirect_with_flash(
                path, level="error", message="Run is closed; signal was not delivered."
            )
        return redirect_with_flash(
            path, level="success", message=f"Signal sent to channel {channel!r}."
        )

    return view


def run_cancel_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace, run_id, form = await _run_action(request, runtime)
        ns = runtime.state.namespace(namespace)
        reason = form.get("reason", "")
        ok = await ns.engine.request_cancel(run_id, reason)
        path = _run_path(namespace, run_id)
        if ok:
            return redirect_with_flash(path, level="success", message="Cancellation requested.")
        return redirect_with_flash(path, level="warning", message="Run was already closed.")

    return view


def run_terminate_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace, run_id, form = await _run_action(request, runtime)
        ns = runtime.state.namespace(namespace)
        path = _run_path(namespace, run_id)
        if form.get("confirm") != "yes":
            return redirect_with_flash(
                path, level="error", message="Confirm the checkbox to terminate a run."
            )
        reason = form.get("reason", "")
        ok = await ns.engine.terminate(run_id, reason)
        if ok:
            return redirect_with_flash(path, level="success", message="Run terminated.")
        return redirect_with_flash(path, level="warning", message="Run was already closed.")

    return view


def _reset_point_from_form(form: dict[str, str]) -> ResetPoint:
    point = form.get("reset_point", "")
    if point == "first_tick":
        return ResetPoint.first_tick()
    if point == "before_last_failure":
        return ResetPoint.before_last_failure()
    if point == "after_tick":
        try:
            count = int(form.get("after_tick", ""))
        except ValueError:
            raise InvalidResetPointError("give a whole number of ticks to keep") from None
        return ResetPoint.after_tick(count)
    raise InvalidResetPointError(f"unknown reset point {point!r}")


def run_reset_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace, run_id, form = await _run_action(request, runtime)
        ns = runtime.state.namespace(namespace)
        path = _run_path(namespace, run_id)
        try:
            to = _reset_point_from_form(form)
        except InvalidResetPointError as exc:
            return redirect_with_flash(path, level="error", message=str(exc))
        reason = form.get("reason", "")
        if not reason.strip():
            return redirect_with_flash(
                path, level="error", message="A reason is required to reset a run."
            )
        new_record = await ns.engine.reset(run_id, to=to, reason=reason)
        return redirect_with_flash(
            _run_path(namespace, new_record.run_id),
            level="success",
            message=f"Reset: new run {new_record.workflow_id} (run {new_record.run_id}).",
        )

    return view
