"""Bearer-token authentication and role-based access control for the server.

Tokens look like ``lwk_<id>_<secret>``. Only a salted SHA-256 hash of the secret is ever
stored. The secret is 256 random bits, so unlike a human-chosen password it cannot be
guessed, and a slow key-derivation function would only add latency to every API call
(workers call the server constantly). The plaintext token exists only at the moment it is
generated; if it is lost, generate a new one and revoke the old one.

Roles grant coarse permissions:

* ``admin`` -- every operation in every namespace it lists.
* ``operator`` -- the control API (start, signal, cancel, terminate, ...).
* ``worker`` -- the worker API (claim/complete ticks and steps, timers, outbox).
* ``reader`` -- read-only control endpoints (describe, list, history, children).

A token additionally lists the namespaces it may act in (``"*"`` for all of them). Every
mutating request is written to the ``loomwork.audit`` logger with the token id, namespace,
action and outcome -- never payload contents.
"""

from __future__ import annotations

import base64
import enum
import hashlib
import hmac
import json
import logging
import re
import secrets
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

TOKEN_PREFIX = "lwk"  # noqa: S105 - a format tag, not a credential
_SECRET_BYTES = 32
_SALT_BYTES = 16
# No underscores: they separate the parts of a token.
_ID_RE = re.compile(r"^[A-Za-z0-9-]{1,64}$")

audit_log = logging.getLogger("loomwork.audit")


class AuthError(Exception):
    """Base class for authentication and authorization failures."""


class InvalidTokenError(AuthError):
    """The bearer token is malformed, unknown, or its secret does not match."""


class ForbiddenError(AuthError):
    """The token is valid but lacks the role or namespace needed for this request."""


class Role(enum.StrEnum):
    ADMIN = "admin"
    OPERATOR = "operator"
    WORKER = "worker"
    READER = "reader"


#: What each role may do. ``control_write`` implies ``control_read``.
_ROLE_GRANTS: dict[Role, frozenset[str]] = {
    Role.ADMIN: frozenset({"worker", "control_read", "control_write"}),
    Role.OPERATOR: frozenset({"control_read", "control_write"}),
    Role.WORKER: frozenset({"worker"}),
    Role.READER: frozenset({"control_read"}),
}


@dataclass(frozen=True, slots=True)
class TokenRecord:
    """A token as stored on disk: never the plaintext secret."""

    id: str
    salt: str
    hash: str
    roles: tuple[Role, ...]
    namespaces: tuple[str, ...]
    description: str = ""

    def allows_namespace(self, namespace: str) -> bool:
        return "*" in self.namespaces or namespace in self.namespaces

    def grants(self, capability: str) -> bool:
        return any(capability in _ROLE_GRANTS[role] for role in self.roles)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "salt": self.salt,
            "hash": self.hash,
            "roles": [r.value for r in self.roles],
            "namespaces": list(self.namespaces),
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TokenRecord:
        return cls(
            id=str(data["id"]),
            salt=str(data["salt"]),
            hash=str(data["hash"]),
            roles=tuple(Role(r) for r in data["roles"]),
            namespaces=tuple(str(n) for n in data["namespaces"]),
            description=str(data.get("description", "")),
        )


def _derive(secret: str, salt: bytes) -> str:
    digest = hashlib.sha256(salt + secret.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii")


def generate_token(
    id: str, roles: list[Role] | list[str], namespaces: list[str], *, description: str = ""
) -> tuple[str, TokenRecord]:
    """Create a new token. Returns ``(plaintext, record)``.

    The plaintext is shown to the operator exactly once; only ``record`` is persisted.
    """
    if not _ID_RE.match(id):
        raise ValueError(f"invalid token id {id!r}: use 1-64 letters, digits and -")
    if not roles:
        raise ValueError("a token needs at least one role")
    if not namespaces:
        raise ValueError("a token needs at least one namespace ('*' for all)")
    resolved_roles = tuple(Role(r) for r in roles)
    secret = secrets.token_urlsafe(_SECRET_BYTES)
    salt = secrets.token_bytes(_SALT_BYTES)
    record = TokenRecord(
        id=id,
        salt=base64.urlsafe_b64encode(salt).decode("ascii"),
        hash=_derive(secret, salt),
        roles=resolved_roles,
        namespaces=tuple(namespaces),
        description=description,
    )
    plaintext = f"{TOKEN_PREFIX}_{id}_{secret}"
    return plaintext, record


def _parse_token(plaintext: str) -> tuple[str, str]:
    parts = plaintext.split("_", 2)
    if len(parts) != 3 or parts[0] != TOKEN_PREFIX or not parts[1] or not parts[2]:
        raise InvalidTokenError("malformed bearer token")
    return parts[1], parts[2]


def verify_token(plaintext: str, record: TokenRecord) -> bool:
    """Constant-time check that ``plaintext`` matches ``record``."""
    try:
        token_id, secret = _parse_token(plaintext)
    except InvalidTokenError:
        return False
    if token_id != record.id:
        return False
    salt = base64.urlsafe_b64decode(record.salt.encode("ascii"))
    candidate = _derive(secret, salt)
    return hmac.compare_digest(candidate, record.hash)


@dataclass(slots=True)
class TokenFile:
    """A loaded set of tokens, keyed by id."""

    tokens: dict[str, TokenRecord] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> TokenFile:
        raw = json.loads(path.read_text("utf-8"))
        if not isinstance(raw, list):
            raise ValueError(f"tokens file {path} must contain a JSON array of token objects")
        tokens = {}
        for item in raw:
            record = TokenRecord.from_dict(item)
            tokens[record.id] = record
        return cls(tokens)

    def save(self, path: Path) -> None:
        payload = [record.to_dict() for record in self.tokens.values()]
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def authenticate(self, plaintext: str) -> TokenRecord:
        """Raises :class:`InvalidTokenError` if the token is unknown or wrong."""
        try:
            token_id, _ = _parse_token(plaintext)
        except InvalidTokenError:
            raise
        record = self.tokens.get(token_id)
        if record is None or not verify_token(plaintext, record):
            raise InvalidTokenError("unknown token or secret does not match")
        return record


def require(record: TokenRecord, *, capability: str, namespace: str) -> None:
    """Raises :class:`ForbiddenError` if ``record`` may not use ``capability`` in
    ``namespace``."""
    if not record.allows_namespace(namespace):
        raise ForbiddenError(f"token {record.id!r} is not scoped to namespace {namespace!r}")
    if not record.grants(capability):
        raise ForbiddenError(f"token {record.id!r} does not grant {capability!r}")


def audit(
    *,
    token_id: str,
    namespace: str,
    action: str,
    target: str = "",
    outcome: str = "ok",
) -> None:
    """Record a mutating request. Never logs payload contents or secrets."""
    audit_log.info(
        "token=%s namespace=%s action=%s target=%s outcome=%s",
        token_id,
        namespace,
        action,
        target,
        outcome,
    )
