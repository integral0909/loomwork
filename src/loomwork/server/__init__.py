"""ASGI API server fronting one or more :class:`~loomwork.store.base.Store` backends.

See ``docs/server.md`` for the operator guide (configuration, namespaces, tokens/RBAC,
running remote workers) and the full route table.

The app itself is imported lazily: token management (:mod:`loomwork.server.auth`) and
configuration parsing work without the ``server`` extra installed, which is what lets
``loomwork token`` run on a machine that only administers a server.
"""

from typing import TYPE_CHECKING

from loomwork.server.auth import Role, TokenRecord, generate_token
from loomwork.server.config import ServerConfig, load_config

if TYPE_CHECKING:
    from loomwork.server.app import create_app, serve

__all__ = [
    "Role",
    "ServerConfig",
    "TokenRecord",
    "create_app",
    "generate_token",
    "load_config",
    "serve",
]


def __getattr__(name: str) -> object:
    if name in {"create_app", "serve"}:
        from loomwork.server import app

        return getattr(app, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
