"""Signed session cookies for the console.

The console has no server-side session table: a session is entirely represented by its
cookie, an HMAC-signed, JSON-encoded payload of ``{token id, CSRF secret, expiry}``. This
is the same trade-off Django's and Flask's default signed-cookie sessions make -- no
storage to clean up, no state lost between requests to different processes -- and it
composes well with the existing token model: the *authorization* check on every request
re-reads the live token file by id (:meth:`~loomwork.server.state.ServerState.
current_tokens`), so revoking or reducing a token's roles takes effect immediately, the
same as it does for the JSON APIs, even though the session itself remains "logged in"
until it expires.

The cookie never carries the bearer token's secret, only its id: forging a session
without the signing key requires forging the HMAC, and stealing the cookie only grants
whatever the referenced token id currently grants (nothing at all once it is revoked).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass

COOKIE_NAME = "lw_console_session"


@dataclass(frozen=True, slots=True)
class SessionData:
    """The decoded contents of a valid, unexpired session cookie."""

    token_id: str
    csrf: str
    expires_at: float


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("ascii"))


def _signature(secret: bytes, payload: bytes) -> str:
    return _b64url_encode(hmac.new(secret, payload, hashlib.sha256).digest())


def encode_session(secret: bytes, session: SessionData) -> str:
    """Render ``session`` as a ``<payload>.<signature>`` cookie value."""
    payload = json.dumps(
        {"tid": session.token_id, "csrf": session.csrf, "exp": session.expires_at},
        separators=(",", ":"),
    ).encode("utf-8")
    body = _b64url_encode(payload)
    return f"{body}.{_signature(secret, payload)}"


def decode_session(secret: bytes, cookie_value: str) -> SessionData | None:
    """Verify and decode a cookie value. Returns ``None`` for anything invalid, tampered
    with, or expired -- callers treat that identically to "not logged in"."""
    body, separator, signature = cookie_value.partition(".")
    if not separator:
        return None
    try:
        payload = _b64url_decode(body)
    except (ValueError, UnicodeDecodeError):
        return None
    if not hmac.compare_digest(_signature(secret, payload), signature):
        return None
    try:
        data = json.loads(payload)
        session = SessionData(
            token_id=str(data["tid"]), csrf=str(data["csrf"]), expires_at=float(data["exp"])
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if session.expires_at < time.time():
        return None
    return session
