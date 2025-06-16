"""Request-level authentication and authorization helpers used by both APIs."""

from __future__ import annotations

from starlette.requests import Request

from loomwork.server.auth import ForbiddenError, InvalidTokenError, Role, TokenRecord, require
from loomwork.server.errors import forbidden, unauthorized
from loomwork.server.state import ServerState

_ANONYMOUS_ADMIN = TokenRecord(
    id="anonymous", salt="", hash="", roles=(Role.ADMIN,), namespaces=("*",)
)


def server_state(request: Request) -> ServerState:
    state: ServerState = request.app.state.server
    return state


async def authenticate(request: Request, *, namespace: str, capability: str) -> TokenRecord:
    """Authenticate the request and check it may use ``capability`` in ``namespace``.

    Raises an :class:`~loomwork.server.errors.ApiError` (401 or 403) on failure.
    """
    server = server_state(request)
    if server.config.auth.disabled:
        return _ANONYMOUS_ADMIN
    header = request.headers.get("authorization", "")
    if not header.lower().startswith("bearer "):
        raise unauthorized("missing bearer token")
    token = header[len("bearer ") :].strip()
    try:
        record = server.current_tokens().authenticate(token)
    except InvalidTokenError as exc:
        raise unauthorized(str(exc)) from exc
    try:
        require(record, capability=capability, namespace=namespace)
    except ForbiddenError as exc:
        raise forbidden(str(exc)) from exc
    return record
