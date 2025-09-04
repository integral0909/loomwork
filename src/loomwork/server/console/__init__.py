"""A server-rendered operator web console for Loomwork's server mode.

Mounted at ``/console`` by :func:`~loomwork.server.app.create_app` when
``config.console.enabled`` (the default). See ``docs/console.md`` for what it does, its
authentication and session model, and its security properties.
"""

from __future__ import annotations

from loomwork.server.console.app import build_console_app

__all__ = ["build_console_app"]
