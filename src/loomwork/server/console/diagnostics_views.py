"""The diagnostics page: the same pass/warn/fail report as ``loomwork doctor``.

Read-only, so unlike the run/schedule/batch views there is nothing here to CSRF-protect
-- it only ever answers ``GET`` requests.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from starlette.requests import Request
from starlette.responses import Response

from loomwork.diagnostics import build_findings, collect_diagnostics
from loomwork.server.auth import require
from loomwork.server.console.render import render
from loomwork.server.console.runtime import ConsoleRuntime, accessible_namespaces, require_session

View = Callable[[Request], Awaitable[Response]]


def diagnostics_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        path = f"/console/{namespace}/diagnostics"
        session = await require_session(request, runtime, next_path=path)
        require(session.record, capability="control_read", namespace=namespace)
        ns = runtime.state.namespace(namespace)
        now = ns.engine.clock.now()
        snapshot = await collect_diagnostics(ns.store, ns.engine, now)
        findings = build_findings(snapshot)
        return render(
            runtime,
            request,
            "diagnostics.html",
            namespace=namespace,
            namespaces=accessible_namespaces(runtime, session.record),
            findings=findings,
            snapshot=snapshot,
        )

    return view


__all__ = ["diagnostics_view"]
