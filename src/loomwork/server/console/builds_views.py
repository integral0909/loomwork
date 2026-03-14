"""The Builds page: worker build drain status and compatibility declarations (see
``docs/guides/versioning.md`` and ``docs/adr/0006-worker-build-versioning.md``).

Read-only summary plus two small forms (declare/revoke compatibility); repinning an
individual run lives on the run detail page instead (see ``runs_views.py``), since it
acts on one run, not one build.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Awaitable, Callable

from starlette.requests import Request
from starlette.responses import Response

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

View = Callable[[Request], Awaitable[Response]]


def _builds_path(namespace: str) -> str:
    return f"/console/{namespace}/builds"


def builds_list_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        path = _builds_path(namespace)
        session = await require_session(request, runtime, next_path=path)
        require(session.record, capability="control_read", namespace=namespace)
        ns = runtime.state.namespace(namespace)

        summaries = await ns.engine.versioning.builds_summary()
        compat = await ns.engine.versioning.list_compatibility()
        compatible_with: dict[str, list[str]] = defaultdict(list)
        for c in compat:
            compatible_with[c.build_id].append(c.compatible_with)

        return render(
            runtime,
            request,
            "builds.html",
            namespace=namespace,
            namespaces=accessible_namespaces(runtime, session.record),
            summaries=summaries,
            compatible_with=compatible_with,
            can_write=can_write(session.record, namespace),
            csrf_token=session.csrf,
        )

    return view


def build_declare_compatible_view(runtime: ConsoleRuntime) -> View:
    """``build_id``/``compatible_with`` are form fields, not path segments: unlike the
    revoke form below (whose targets are already known, rendered from the summary
    table), a person declares compatibility by typing a build id that may not yet
    appear anywhere in the table (a build with no live worker and no pinned run yet)."""

    async def view(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        path = _builds_path(namespace)
        session = await require_session(request, runtime, next_path=path)
        require(session.record, capability="control_write", namespace=namespace)
        ns = runtime.state.namespace(namespace)
        form = await parse_form(request)
        check_csrf(session, form.get("csrf_token"))
        build_id = form.get("build_id", "").strip()
        compatible_with = form.get("compatible_with", "").strip()
        if not build_id or not compatible_with:
            return redirect_with_flash(
                path,
                level="error",
                message="Give both a build and the build it is compatible with.",
            )
        await ns.engine.versioning.declare_compatible(build_id, compatible_with)
        return redirect_with_flash(
            path,
            level="success",
            message=f"{build_id} may now claim work pinned to {compatible_with}.",
        )

    return view


def build_revoke_compatible_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        build_id = request.path_params["build_id"]
        compatible_with = request.path_params["compatible_with"]
        path = _builds_path(namespace)
        session = await require_session(request, runtime, next_path=path)
        require(session.record, capability="control_write", namespace=namespace)
        ns = runtime.state.namespace(namespace)
        form = await parse_form(request)
        check_csrf(session, form.get("csrf_token"))
        revoked = await ns.engine.versioning.revoke_compatible(build_id, compatible_with)
        if not revoked:
            return redirect_with_flash(
                path, level="warning", message="That compatibility was already not declared."
            )
        return redirect_with_flash(
            path,
            level="success",
            message=f"{build_id} is no longer compatible with {compatible_with}.",
        )

    return view


__all__ = [
    "build_declare_compatible_view",
    "build_revoke_compatible_view",
    "builds_list_view",
]
