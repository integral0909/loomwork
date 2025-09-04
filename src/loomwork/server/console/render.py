"""Rendering console pages: templates, flash messages, and error pages.

Flash messages survive a redirect without any server-side session store: the message is
carried in the redirect target's own query string (``?flash=...&level=...``) and read
back on the following ``GET``. It is always rendered through the autoescaped template
engine, so a hostile value echoed back in a flash (for example a signal channel name)
cannot inject markup.
"""

from __future__ import annotations

from urllib.parse import urlencode

from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse

from loomwork.server.console.runtime import ConsoleRuntime

_FLASH_LEVELS = {"info", "success", "warning", "error"}


def render(
    runtime: ConsoleRuntime,
    request: Request,
    template_name: str,
    *,
    status_code: int = 200,
    **context: object,
) -> HTMLResponse:
    template = runtime.env.get_template(template_name)
    html = template.render(
        request=request,
        auth_disabled=runtime.config.auth.disabled,
        flash=flash_from_request(request),
        **context,
    )
    return HTMLResponse(html, status_code=status_code)


def error_page(
    runtime: ConsoleRuntime, request: Request, *, status_code: int, message: str
) -> HTMLResponse:
    return render(
        runtime,
        request,
        "error.html",
        status_code=status_code,
        status=status_code,
        message=message,
    )


def flash_from_request(request: Request) -> tuple[str, str] | None:
    message = request.query_params.get("flash")
    if not message:
        return None
    level = request.query_params.get("level", "info")
    return (level if level in _FLASH_LEVELS else "info", message)


def redirect_with_flash(path: str, *, level: str, message: str) -> RedirectResponse:
    query = urlencode({"flash": message, "level": level})
    return RedirectResponse(url=f"{path}?{query}", status_code=303)
