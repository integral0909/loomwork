"""The batch page: preview a visibility query, then cancel or terminate every match.

Unlike the single-run actions in ``runs_views.py``, a batch action can touch many runs at
once, so it gets its own confirmation step (the preview *is* the confirmation: the query
text is carried through the form, the operator sees exactly what it matches, and only
then submits the actual cancel/terminate) on top of the reason field terminate already
requires.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from urllib.parse import urlencode

from starlette.requests import Request
from starlette.responses import Response

from loomwork.client.client import Client
from loomwork.server.auth import require
from loomwork.server.console.render import redirect_with_flash, render
from loomwork.server.console.runtime import (
    ConsoleRuntime,
    accessible_namespaces,
    can_write,
    check_csrf,
    require_session,
)
from loomwork.server.console.util import parse_form
from loomwork.visibility.errors import VisibilityQueryError

View = Callable[[Request], Awaitable[Response]]

_PREVIEW_LIMIT = 200


def _batch_path(namespace: str) -> str:
    return f"/console/{namespace}/batch"


def batch_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        path = _batch_path(namespace)
        session = await require_session(request, runtime, next_path=path)
        require(session.record, capability="control_read", namespace=namespace)
        ns = runtime.state.namespace(namespace)

        query_text = request.query_params.get("q") or ""
        query_error: str | None = None
        records = []
        truncated = False
        if query_text.strip():
            try:
                records = await ns.engine.query_executions(query_text, _PREVIEW_LIMIT + 1)
            except VisibilityQueryError as exc:
                query_error = exc.format()
            else:
                truncated = len(records) > _PREVIEW_LIMIT
                records = records[:_PREVIEW_LIMIT]

        return render(
            runtime,
            request,
            "batch.html",
            namespace=namespace,
            namespaces=accessible_namespaces(runtime, session.record),
            query_text=query_text,
            query_error=query_error,
            records=records,
            truncated=truncated,
            preview_limit=_PREVIEW_LIMIT,
            can_write=can_write(session.record, namespace),
            csrf_token=session.csrf,
        )

    return view


async def _batch_action(request: Request, runtime: ConsoleRuntime) -> tuple[str, dict[str, str]]:
    namespace = request.path_params["namespace"]
    path = _batch_path(namespace)
    session = await require_session(request, runtime, next_path=path)
    require(session.record, capability="control_write", namespace=namespace)
    form = await parse_form(request)
    check_csrf(session, form.get("csrf_token"))
    return namespace, {str(k): str(v) for k, v in form.items()}


def _redirect_to_preview(namespace: str, query_text: str, *, level: str, message: str) -> Response:
    path = f"{_batch_path(namespace)}?{urlencode({'q': query_text})}"
    return redirect_with_flash(path, level=level, message=message)


def batch_cancel_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace, form = await _batch_action(request, runtime)
        query_text = form.get("query", "")
        reason = form.get("reason", "")
        ns = runtime.state.namespace(namespace)
        client = Client(ns.engine)
        result = await client.batch_cancel(query_text, reason)
        message = (
            f"Cancellation requested for {result.succeeded} of {result.matched} run(s) "
            f"({result.skipped_closed} already closed, {len(result.failed)} failed)."
        )
        level = "success" if not result.failed else "warning"
        return _redirect_to_preview(namespace, query_text, level=level, message=message)

    return view


def batch_terminate_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace, form = await _batch_action(request, runtime)
        query_text = form.get("query", "")
        if form.get("confirm") != "yes":
            return _redirect_to_preview(
                namespace,
                query_text,
                level="error",
                message="Confirm the checkbox to terminate every matching run.",
            )
        reason = form.get("reason", "")
        if not reason.strip():
            return _redirect_to_preview(
                namespace, query_text, level="error", message="A reason is required to terminate."
            )
        ns = runtime.state.namespace(namespace)
        client = Client(ns.engine)
        result = await client.batch_terminate(query_text, reason)
        message = (
            f"Terminated {result.succeeded} of {result.matched} run(s) "
            f"({result.skipped_closed} already closed, {len(result.failed)} failed)."
        )
        level = "success" if not result.failed else "warning"
        return _redirect_to_preview(namespace, query_text, level=level, message=message)

    return view


__all__ = ["batch_cancel_view", "batch_terminate_view", "batch_view"]
