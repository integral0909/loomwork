"""Turning exceptions into HTTP responses.

Route handlers either raise :class:`ApiError` directly (malformed requests: bad JSON,
missing fields, an unknown query parameter) or let a domain exception
(:class:`~loomwork.errors.LoomworkError` and friends, or a bare ``ValueError``/
``TimeoutError``) propagate. :func:`install` renders both as the same
:class:`~loomwork.remote.errors.ProblemDetail` body that
:class:`~loomwork.remote.http_store.HttpStore` turns back into the matching exception.

Known exception types are registered individually with Starlette's inner
``ExceptionMiddleware``, which sends the response and does not re-raise. Anything else is
an unexpected bug: it is still rendered as a JSON 500 (via a handler for the base
``Exception``, which Starlette instead wires to the outer ``ServerErrorMiddleware``), but
that middleware re-raises afterwards so the ASGI server and test clients still see it,
rather than a programming error quietly turning into a normal-looking error response.
"""

from __future__ import annotations

import logging

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse

from loomwork.errors import LoomworkError
from loomwork.remote.errors import ProblemDetail, problem_for
from loomwork.visibility.errors import VisibilityQueryError

logger = logging.getLogger("loomwork.server")


class ApiError(Exception):
    """A request-level error a route handler raises directly."""

    def __init__(self, problem: ProblemDetail) -> None:
        super().__init__(problem.detail or problem.title)
        self.problem = problem


def bad_request(detail: str, *, code: str = "invalid_request") -> ApiError:
    return ApiError(ProblemDetail(code, "Invalid request", detail, 400))


def unauthorized(detail: str) -> ApiError:
    return ApiError(ProblemDetail("unauthorized", "Unauthorized", detail, 401))


def forbidden(detail: str) -> ApiError:
    return ApiError(ProblemDetail("forbidden", "Forbidden", detail, 403))


#: Registered individually so they are handled (response sent, nothing re-raised)
#: instead of falling through to the 500 path. Order does not matter: Starlette
#: dispatches by walking the raised exception's MRO against this set of keys.
_HANDLED_TYPES: tuple[type[Exception], ...] = (
    ApiError,
    LoomworkError,
    ValueError,
    TimeoutError,
    VisibilityQueryError,
)


async def _render(request: Request, exc: Exception) -> JSONResponse:
    problem = exc.problem if isinstance(exc, ApiError) else problem_for(exc)
    if problem.status >= 500:
        logger.exception("unhandled error while serving %s %s", request.method, request.url.path)
    return JSONResponse(problem.to_wire(), status_code=problem.status)


def install(app: Starlette) -> None:
    """Register exception handlers that turn every error into a problem response."""
    for exc_type in _HANDLED_TYPES:
        app.add_exception_handler(exc_type, _render)
    app.add_exception_handler(Exception, _render)
