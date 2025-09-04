"""Shared console state and the session/CSRF checks every view starts with."""

from __future__ import annotations

import hmac
import secrets
import time
from dataclasses import dataclass

from jinja2 import Environment
from starlette.requests import Request

from loomwork.server.auth import Role, TokenRecord
from loomwork.server.config import ServerConfig
from loomwork.server.console import session as session_mod
from loomwork.server.state import ServerState

#: Used only when ``auth.disabled``: identical in shape to the anonymous admin record
#: ``loomwork.server.security`` grants the JSON APIs in that mode, so the console and the
#: APIs agree on what an unauthenticated request may do while auth is off.
ANONYMOUS_ADMIN = TokenRecord(
    id="anonymous", salt="", hash="", roles=(Role.ADMIN,), namespaces=("*",)
)


@dataclass(slots=True)
class ConsoleRuntime:
    """Built once by :func:`~loomwork.server.console.app.build_console_app`."""

    state: ServerState
    secret: bytes
    env: Environment

    @property
    def config(self) -> ServerConfig:
        return self.state.config


@dataclass(frozen=True, slots=True)
class Session:
    """A verified caller: the token behind the session cookie, plus its CSRF secret."""

    record: TokenRecord
    csrf: str


class NeedsLogin(Exception):
    """Raised by :func:`require_session` when there is no valid session.

    Registered as a console exception handler that redirects to the login page with
    ``next`` set, so every view can simply call :func:`require_session` and not think
    about the unauthenticated case itself.
    """

    def __init__(self, next_path: str) -> None:
        super().__init__("login required")
        self.next_path = next_path


class CsrfError(Exception):
    """Raised when a POSTed form's CSRF token does not match the session's."""


def new_session_secret() -> bytes:
    return secrets.token_urlsafe(32).encode("ascii")


def start_session(runtime: ConsoleRuntime, token_id: str) -> tuple[session_mod.SessionData, str]:
    """Create a fresh session for ``token_id``. Returns the data and its cookie value."""
    data = session_mod.SessionData(
        token_id=token_id,
        csrf=secrets.token_urlsafe(16),
        expires_at=time.time() + runtime.config.console.session_ttl_seconds,
    )
    return data, session_mod.encode_session(runtime.secret, data)


async def require_session(request: Request, runtime: ConsoleRuntime, *, next_path: str) -> Session:
    """The verified caller of this request, or raise :class:`NeedsLogin`.

    ``next_path`` is the console-absolute path (starting ``/console/...``) to return to
    after a successful login; callers pass their own current path since it is cheaper
    and more reliable than reconstructing it from a request that may be behind a mount.
    """
    if runtime.config.auth.disabled:
        return Session(ANONYMOUS_ADMIN, csrf="")
    cookie = request.cookies.get(session_mod.COOKIE_NAME)
    data = session_mod.decode_session(runtime.secret, cookie) if cookie else None
    if data is None:
        raise NeedsLogin(next_path)
    record = runtime.state.current_tokens().tokens.get(data.token_id)
    if record is None:
        raise NeedsLogin(next_path)
    return Session(record, data.csrf)


def check_csrf(session: Session, submitted: str | None) -> None:
    """Raise :class:`CsrfError` unless ``submitted`` matches the session's CSRF token.

    A no-op while auth is disabled: there is no session to bind a CSRF token to, and
    every caller is already an implicit administrator in that mode.
    """
    if not session.csrf:
        return
    if not submitted or not hmac.compare_digest(submitted, session.csrf):
        raise CsrfError("missing or invalid CSRF token; go back and retry")


def accessible_namespaces(runtime: ConsoleRuntime, record: TokenRecord) -> list[str]:
    return sorted(name for name in runtime.state.namespaces if record.allows_namespace(name))


def can_write(record: TokenRecord, namespace: str) -> bool:
    return record.allows_namespace(namespace) and record.grants("control_write")
