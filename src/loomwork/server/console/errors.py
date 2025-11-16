"""Turning exceptions raised by console views into HTML responses.

The console is mounted as its own :class:`~starlette.applications.Starlette`
sub-application (see ``app.py``) specifically so it can register its *own* exception
handlers: the outer server's ``loomwork.server.errors.install`` renders JSON, which is
right for the JSON APIs but wrong for a browser. Domain errors familiar from the rest of
the server (a run or schedule that does not exist, a closed run, an invalid visibility
query, ...) become a plain HTML error page instead of a JSON problem body.

:class:`~loomwork.server.console.runtime.NeedsLogin` and
:class:`~loomwork.server.console.runtime.CsrfError` are console-specific control-flow
signals raised by :func:`~loomwork.server.console.runtime.require_session` and
:func:`~loomwork.server.console.runtime.check_csrf`; they are handled here too so every
view can simply call those functions without a ``try``/``except`` of its own.
"""

from __future__ import annotations

import logging
from urllib.parse import quote

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from loomwork.errors import (
    InvalidResetPointError,
    LoomworkError,
    NotFoundError,
    ScheduleConflictError,
    ScheduleExistsError,
    WorkflowAlreadyStartedError,
    WorkflowClosedError,
)
from loomwork.server.auth import ForbiddenError, InvalidTokenError
from loomwork.server.console.render import error_page
from loomwork.server.console.runtime import ConsoleRuntime, CsrfError, NeedsLogin
from loomwork.visibility.errors import VisibilityQueryError

logger = logging.getLogger("loomwork.server")

#: Status code for each domain exception a view may let propagate instead of handling
#: inline. Order does not matter: Starlette dispatches by walking the exception's MRO
#: against the handlers it has registered, most specific first.
_STATUS: dict[type[Exception], int] = {
    NotFoundError: 404,
    ForbiddenError: 403,
    InvalidTokenError: 401,
    CsrfError: 403,
    WorkflowClosedError: 409,
    WorkflowAlreadyStartedError: 409,
    ScheduleExistsError: 409,
    ScheduleConflictError: 409,
    InvalidResetPointError: 422,
    VisibilityQueryError: 400,
    ValueError: 400,
}


def install(app: Starlette, runtime: ConsoleRuntime) -> None:
    async def needs_login(_request: Request, exc: Exception) -> Response:
        assert isinstance(exc, NeedsLogin)
        target = f"/console/login?next={quote(exc.next_path, safe='')}"
        return RedirectResponse(url=target, status_code=303)

    async def domain_error(request: Request, exc: Exception) -> Response:
        status = 500
        for exc_type, code in _STATUS.items():
            if isinstance(exc, exc_type):
                status = code
                break
        message = exc.format() if isinstance(exc, VisibilityQueryError) else str(exc)
        return error_page(runtime, request, status_code=status, message=message or "Error")

    async def unexpected_error(request: Request, _exc: Exception) -> Response:
        logger.exception(
            "unhandled error while serving console %s %s", request.method, request.url.path
        )
        return error_page(runtime, request, status_code=500, message="An internal error occurred.")

    app.add_exception_handler(NeedsLogin, needs_login)
    for exc_type in (
        NotFoundError,
        ForbiddenError,
        InvalidTokenError,
        CsrfError,
        WorkflowClosedError,
        WorkflowAlreadyStartedError,
        ScheduleExistsError,
        ScheduleConflictError,
        InvalidResetPointError,
        VisibilityQueryError,
        ValueError,
    ):
        app.add_exception_handler(exc_type, domain_error)
    # Anything else is an unexpected bug: still render a page, but re-raise afterwards
    # (see loomwork.server.errors) so it is logged as an error and test clients relying
    # on ASGI exception propagation still see it, rather than a silent 500 page.
    app.add_exception_handler(LoomworkError, domain_error)
    app.add_exception_handler(Exception, unexpected_error)
