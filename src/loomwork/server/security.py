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


async def authenticate(request: Request, *, namespace: str, capability: str | None) -> TokenRecord:
    """Authenticate the request and check it may use ``capability`` in ``namespace``.

    ``capability`` is ``None`` for endpoints any correctly-scoped token may call
    regardless of role (currently just ``whoami``, which exists so a caller can find out
    its own roles without already knowing them): the namespace scope is still enforced,
    but no specific role is required.

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
    if capability is None:
        if not record.allows_namespace(namespace):
            raise forbidden(f"token {record.id!r} is not scoped to namespace {namespace!r}")
        return record
    try:
        require(record, capability=capability, namespace=namespace)
    except ForbiddenError as exc:
        raise forbidden(str(exc)) from exc
    return record
