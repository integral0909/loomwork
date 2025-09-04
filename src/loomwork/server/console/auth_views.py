"""Login, logout and the console home page."""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable

from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from loomwork.server.auth import InvalidTokenError
from loomwork.server.console.render import redirect_with_flash, render
from loomwork.server.console.runtime import (
    ConsoleRuntime,
    accessible_namespaces,
    check_csrf,
    require_session,
    start_session,
)
from loomwork.server.console.session import COOKIE_NAME
from loomwork.server.console.util import parse_form, safe_next

View = Callable[[Request], Awaitable[Response]]


def home_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        session = await require_session(request, runtime, next_path="/console/")
        namespaces = accessible_namespaces(runtime, session.record)
        if not namespaces:
            return render(
                runtime,
                request,
                "error.html",
                status_code=403,
                status=403,
                message="This token has no accessible namespaces.",
            )
        return RedirectResponse(url=f"/console/{namespaces[0]}/runs", status_code=303)

    return view


def login_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        if runtime.config.auth.disabled:
            return RedirectResponse(url="/console/", status_code=303)

        if request.method == "GET":
            next_path = safe_next(request.query_params.get("next"))
            return render(runtime, request, "login.html", next_path=next_path, error=None)

        form = await parse_form(request)
        next_path = safe_next(str(form.get("next", "")))
        token_text = str(form.get("token", "")).strip()
        try:
            record = runtime.state.current_tokens().authenticate(token_text)
        except InvalidTokenError:
            return render(
                runtime,
                request,
                "login.html",
                status_code=401,
                next_path=next_path,
                error="Invalid or unknown token.",
            )
        _session_data, cookie_value = start_session(runtime, record.id)
        response = RedirectResponse(url=next_path, status_code=303)
        response.set_cookie(
            COOKIE_NAME,
            cookie_value,
            # ceil, not int(): a sub-second TTL must not truncate to Max-Age=0, which
            # browsers (and httpx's cookie jar) treat as "delete this cookie now".
            max_age=math.ceil(runtime.config.console.session_ttl_seconds),
            httponly=True,
            samesite="strict",
            secure=request.url.scheme == "https",
            path="/console",
        )
        return response

    return view


def logout_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        if runtime.config.auth.disabled:
            return RedirectResponse(url="/console/", status_code=303)
        session = await require_session(request, runtime, next_path="/console/logout")
        form = await parse_form(request)
        check_csrf(session, str(form.get("csrf_token", "")))
        response = redirect_with_flash("/console/login", level="info", message="Signed out.")
        response.delete_cookie(COOKIE_NAME, path="/console")
        return response

    return view
