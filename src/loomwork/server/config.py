"""Server configuration: a TOML file plus environment-variable overrides.

.. code-block:: toml

    [server]
    host = "0.0.0.0"
    port = 8080

    [server.namespaces]
    default = "sqlite:////var/lib/loomwork/default.db"
    billing  = "postgresql://loomwork@db.internal/billing"

    [server.auth]
    tokens_file = "/etc/loomwork/tokens.json"
    # disabled = true   # never set this in production; see docs/server.md

    [server.limits]
    long_poll_max_seconds = 20
    max_request_body_bytes = 10485760

    [server.cors]
    enabled = false

    [server.console]
    enabled = true               # operator web console at /console; see docs/console.md

Any field can be overridden by an environment variable named
``LOOMWORK_SERVER__<SECTION>__<FIELD>`` (double underscores separate nesting levels,
matching the TOML table structure), for example ``LOOMWORK_SERVER__HOST=0.0.0.0`` or
``LOOMWORK_SERVER__NAMESPACES__DEFAULT=sqlite:///./dev.db``. Values are parsed as TOML
scalars, so ``true``/``false``/numbers work without quoting; anything that fails to parse
as a TOML scalar is kept as a plain string.
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from loomwork.visibility.schema import AttributeType, SchemaError, SearchAttributes

ENV_PREFIX = "LOOMWORK_SERVER__"


class AuthConfig(BaseModel):
    """Authentication settings.

    ``disabled`` skips bearer-token checking entirely and grants every capability in
    every namespace to every request. It exists for local development only; the server
    logs a loud warning at startup whenever it is set.
    """

    model_config = {"extra": "forbid"}

    tokens_file: Path | None = None
    disabled: bool = False

    @model_validator(mode="after")
    def _tokens_file_required_unless_disabled(self) -> AuthConfig:
        if not self.disabled and self.tokens_file is None:
            raise ValueError(
                "auth.tokens_file is required unless auth.disabled = true (never set that "
                "in production; see docs/server.md)"
            )
        return self


class LimitsConfig(BaseModel):
    model_config = {"extra": "forbid"}

    #: Upper bound on a client-requested long-poll ``wait``, in seconds.
    long_poll_max_seconds: float = 20.0
    #: Maximum accepted request body size, in bytes.
    max_request_body_bytes: int = 10 * 1024 * 1024

    @field_validator("long_poll_max_seconds")
    @classmethod
    def _positive_poll(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("long_poll_max_seconds must be positive")
        return value

    @field_validator("max_request_body_bytes")
    @classmethod
    def _positive_body(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("max_request_body_bytes must be positive")
        return value


class CorsConfig(BaseModel):
    """CORS is off by default: the worker and control APIs are meant for backend
    clients, not browsers. Enable it explicitly for a browser-based dashboard."""

    model_config = {"extra": "forbid"}

    enabled: bool = False
    allow_origins: list[str] = Field(default_factory=list)


class ConsoleConfig(BaseModel):
    """The operator web console mounted at ``/console`` (see ``docs/console.md``).

    ``session_secret`` signs the console's session cookie with HMAC; leave it unset to
    generate a random per-process key (sessions do not survive a restart). Set it
    explicitly to keep operators logged in across restarts or when running several
    server processes behind a load balancer.

    ``search_attributes`` is a display hint only: it names and types search attributes
    for the runs-list query box so operators do not have to guess them, overriding
    ``[server.search_attributes]`` for this namespace when both are set. What a query may
    actually reference is controlled by ``[server.search_attributes]`` (see
    :class:`ServerConfig`), not by this field.
    """

    model_config = {"extra": "forbid"}

    enabled: bool = True
    session_secret: str | None = None
    session_ttl_seconds: float = 8 * 3600
    search_attributes: dict[str, str] = Field(default_factory=dict)

    @field_validator("session_ttl_seconds")
    @classmethod
    def _positive_ttl(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("session_ttl_seconds must be positive")
        return value


class ServerConfig(BaseModel):
    """Top-level server configuration."""

    model_config = {"extra": "forbid"}

    host: str = "127.0.0.1"
    port: int = 8080
    #: Namespace name -> store URL (see ``loomwork.store.factory.store_from_url``).
    namespaces: dict[str, str] = Field(default_factory=dict)
    #: Namespace name -> declared search attribute types (``{"tier": "string"}``), fed
    #: into that namespace's ``EngineConfig.search_attributes`` (see
    #: :mod:`loomwork.visibility.schema`). Declared per namespace, like the store URL
    #: itself: unrelated namespaces (different tenants, different workflow families)
    #: have no reason to share a search attribute schema. A namespace with no entry gets
    #: an empty schema, so its queries may only use built-in fields (``status``,
    #: ``created_at``, ...), matching the historical default. Used by both the control
    #: API's workflow list/query endpoint and the console's runs-list query box.
    search_attributes: dict[str, dict[str, str]] = Field(default_factory=dict)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    limits: LimitsConfig = Field(default_factory=LimitsConfig)
    cors: CorsConfig = Field(default_factory=CorsConfig)
    console: ConsoleConfig = Field(default_factory=ConsoleConfig)

    @field_validator("namespaces")
    @classmethod
    def _validate_namespace_names(cls, value: dict[str, str]) -> dict[str, str]:
        for name in value:
            if not name or "/" in name:
                raise ValueError(f"invalid namespace name {name!r}")
        return value

    @field_validator("search_attributes")
    @classmethod
    def _validate_search_attributes(
        cls, value: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        for namespace, declared in value.items():
            try:
                search_attributes_from_config(declared)
            except SchemaError as exc:
                raise ValueError(f"search_attributes.{namespace}: {exc}") from exc
        return value


def search_attributes_from_config(declared: dict[str, str]) -> SearchAttributes:
    """``{"tier": "string"}`` as a :class:`SearchAttributes` schema.

    Shared by :class:`ServerConfig` validation and
    :class:`~loomwork.server.state.ServerState`, which builds one of these per namespace
    for its :class:`~loomwork.engine.config.EngineConfig`.
    """
    valid = ", ".join(t.value for t in AttributeType)
    types: dict[str, AttributeType] = {}
    for name, type_name in declared.items():
        try:
            types[name] = AttributeType(type_name)
        except ValueError:
            raise SchemaError(
                f"unknown search attribute type {type_name!r} for {name!r} (use {valid})"
            ) from None
    return SearchAttributes.from_mapping(types)


def _coerce_scalar(text: str) -> Any:
    """Parse an environment variable value the way a TOML scalar would be written.

    Falls back to the raw string when it is not valid TOML (for example a bare host
    name), so ``LOOMWORK_SERVER__HOST=0.0.0.0`` still works even though ``0.0.0.0``
    alone is not a valid TOML document.
    """
    try:
        parsed = tomllib.loads(f"v = {text}")
        return parsed["v"]
    except tomllib.TOMLDecodeError:
        return text


def _set_path(tree: dict[str, Any], path: list[str], value: Any) -> None:
    node = tree
    for key in path[:-1]:
        child = node.setdefault(key, {})
        if not isinstance(child, dict):
            child = {}
            node[key] = child
        node = child
    node[path[-1]] = value


def _apply_env_overrides(tree: dict[str, Any], env: dict[str, str]) -> dict[str, Any]:
    for key, value in env.items():
        if not key.startswith(ENV_PREFIX):
            continue
        suffix = key[len(ENV_PREFIX) :]
        if not suffix:
            continue
        path = [segment.lower() for segment in suffix.split("__") if segment]
        if not path:
            continue
        _set_path(tree, path, _coerce_scalar(value))
    return tree


def load_config(path: str | Path, *, env: dict[str, str] | None = None) -> ServerConfig:
    """Load a :class:`ServerConfig` from a TOML file, applying environment overrides.

    The file's top-level ``[server]`` table is used; a file without one is treated as
    the whole document being the ``server`` table, so short config files do not need
    the extra nesting.
    """
    import os

    data = tomllib.loads(Path(path).read_text("utf-8"))
    tree = data.get("server", data)
    tree = _apply_env_overrides(dict(tree), env if env is not None else dict(os.environ))
    return ServerConfig.model_validate(tree)
