"""Constructing stores from URLs.

=====================================  ==============================================
URL                                    Store
=====================================  ==============================================
``memory://``                          :class:`~loomwork.store.MemoryStore`
``sqlite:///relative/path.db``         :class:`~loomwork.store.sqlite.SQLiteStore`
``sqlite:////absolute/path.db``        (four slashes for an absolute path)
``sqlite://:memory:``                  SQLite in-memory database
``postgresql://user@host:5432/db``     :class:`~loomwork.store.postgres.PostgresStore`
=====================================  ==============================================

Query parameters configure the store: ``busy_timeout`` for SQLite; ``min_size``,
``max_size`` and ``listen`` for PostgreSQL (other parameters are passed to libpq).
"""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from loomwork.store.base import Store
from loomwork.store.memory import MemoryStore

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


class StoreURLError(ValueError):
    """The store URL is malformed or names an unknown backend."""


def _bool(name: str, value: str) -> bool:
    lowered = value.lower()
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False
    raise StoreURLError(f"{name} must be a boolean, got {value!r}")


def _int(name: str, value: str) -> int:
    try:
        return int(value)
    except ValueError:
        raise StoreURLError(f"{name} must be an integer, got {value!r}") from None


def store_from_url(url: str) -> Store:
    """Create (but do not open) the store described by ``url``."""
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    params = dict(parse_qsl(parts.query, keep_blank_values=True))

    if scheme == "memory":
        if params:
            raise StoreURLError("memory:// takes no parameters")
        return MemoryStore()

    if scheme == "sqlite":
        from loomwork.store.sqlite import SQLiteStore

        kwargs: dict[str, Any] = {}
        if "busy_timeout" in params:
            try:
                kwargs["busy_timeout"] = float(params.pop("busy_timeout"))
            except ValueError:
                raise StoreURLError("busy_timeout must be a number") from None
        if params:
            raise StoreURLError(f"unknown sqlite parameters: {sorted(params)}")
        if parts.netloc == ":memory:":
            return SQLiteStore(":memory:", **kwargs)
        if parts.netloc:
            raise StoreURLError(
                "sqlite URLs have no host: use sqlite:///relative or sqlite:////absolute"
            )
        path = parts.path[1:] if parts.path.startswith("/") else parts.path
        if not path:
            raise StoreURLError("sqlite URL is missing a database path")
        return SQLiteStore(path, **kwargs)

    if scheme in {"postgres", "postgresql"}:
        from loomwork.store.postgres import PostgresStore

        pg_kwargs: dict[str, Any] = {}
        for name in ("min_size", "max_size"):
            if name in params:
                pg_kwargs[name] = _int(name, params.pop(name))
        if "listen" in params:
            pg_kwargs["listen"] = _bool("listen", params.pop("listen"))
        dsn = urlunsplit(("postgresql", parts.netloc, parts.path, urlencode(params), ""))
        return PostgresStore(dsn, **pg_kwargs)

    raise StoreURLError(f"unsupported store URL scheme {parts.scheme!r}")


def redact_url(url: str) -> str:
    """``url`` with any password replaced, safe for logs."""
    parts = urlsplit(url)
    if parts.password is None:
        return url
    netloc = parts.netloc.replace(f":{parts.password}@", ":***@", 1)
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
