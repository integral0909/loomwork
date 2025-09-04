"""Schedules list, schedule detail, and the pause/unpause/trigger action forms."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime

from starlette.requests import Request
from starlette.responses import Response

from loomwork.schedules.model import ScheduleSpec
from loomwork.schedules.service import ScheduleService
from loomwork.server.auth import require
from loomwork.server.console.render import redirect_with_flash, render
from loomwork.server.console.runtime import (
    ConsoleRuntime,
    accessible_namespaces,
    can_write,
    check_csrf,
    require_session,
)
from loomwork.server.console.util import parse_form, parse_int

View = Callable[[Request], Awaitable[Response]]

_DEFAULT_LIMIT = 100
_MAX_LIMIT = 500
_UPCOMING_COUNT = 5


def _schedules_path(namespace: str) -> str:
    return f"/console/{namespace}/schedules"


def _schedule_path(namespace: str, schedule_id: str) -> str:
    return f"/console/{namespace}/schedules/{schedule_id}"


def _upcoming(spec: ScheduleSpec, now: datetime, count: int = _UPCOMING_COUNT) -> list[datetime]:
    times: list[datetime] = []
    current = now
    while len(times) < count:
        nominal = spec.next_after(current)
        if nominal is None:
            break
        times.append(nominal)
        current = nominal
    return times


def schedules_list_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        path = _schedules_path(namespace)
        session = await require_session(request, runtime, next_path=path)
        require(session.record, capability="control_read", namespace=namespace)
        ns = runtime.state.namespace(namespace)
        limit = parse_int(
            request.query_params.get("limit"), default=_DEFAULT_LIMIT, minimum=1, maximum=_MAX_LIMIT
        )
        states = await ns.store.list_schedules(limit)
        return render(
            runtime,
            request,
            "schedules_list.html",
            namespace=namespace,
            namespaces=accessible_namespaces(runtime, session.record),
            states=states,
            limit=limit,
            can_write=can_write(session.record, namespace),
            csrf_token=session.csrf,
        )

    return view


def schedule_detail_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        schedule_id = request.path_params["schedule_id"]
        path = _schedule_path(namespace, schedule_id)
        session = await require_session(request, runtime, next_path=path)
        require(session.record, capability="control_read", namespace=namespace)
        ns = runtime.state.namespace(namespace)
        state = await ns.store.get_schedule(schedule_id)
        upcoming = [] if state.paused else _upcoming(state.spec, ns.engine.clock.now())
        return render(
            runtime,
            request,
            "schedule_detail.html",
            namespace=namespace,
            state=state,
            upcoming=upcoming,
            can_write=can_write(session.record, namespace),
            csrf_token=session.csrf,
        )

    return view


async def _schedule_action(
    request: Request, runtime: ConsoleRuntime
) -> tuple[str, str, dict[str, str]]:
    namespace = request.path_params["namespace"]
    schedule_id = request.path_params["schedule_id"]
    path = _schedule_path(namespace, schedule_id)
    session = await require_session(request, runtime, next_path=path)
    require(session.record, capability="control_write", namespace=namespace)
    form = await parse_form(request)
    check_csrf(session, str(form.get("csrf_token", "")))
    return namespace, schedule_id, {str(k): str(v) for k, v in form.items()}


def schedule_pause_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace, schedule_id, form = await _schedule_action(request, runtime)
        ns = runtime.state.namespace(namespace)
        service = ScheduleService(ns.store, ns.engine.clock)
        await service.update(schedule_id, paused=True, note=form.get("note", ""))
        path = _schedule_path(namespace, schedule_id)
        return redirect_with_flash(path, level="success", message="Schedule paused.")

    return view


def schedule_unpause_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace, schedule_id, form = await _schedule_action(request, runtime)
        ns = runtime.state.namespace(namespace)
        service = ScheduleService(ns.store, ns.engine.clock)
        await service.update(schedule_id, paused=False, note=form.get("note", ""))
        path = _schedule_path(namespace, schedule_id)
        return redirect_with_flash(path, level="success", message="Schedule unpaused.")

    return view


def schedule_trigger_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace, schedule_id, _form = await _schedule_action(request, runtime)
        ns = runtime.state.namespace(namespace)
        service = ScheduleService(ns.store, ns.engine.clock)
        state = await service.trigger(schedule_id)
        path = _schedule_path(namespace, schedule_id)
        latest = state.recent[-1] if state.recent else None
        message = (
            f"Started {latest.workflow_id} (run {latest.run_id})."
            if latest is not None
            else "Triggered."
        )
        return redirect_with_flash(path, level="success", message=message)

    return view
