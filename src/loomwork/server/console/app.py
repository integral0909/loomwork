"""Builds the console's ASGI application.

The console is its own :class:`~starlette.applications.Starlette` instance, mounted at
``/console`` by :func:`loomwork.server.app.create_app`, rather than a plain list of
routes added to the main app. That gives it its own exception handlers (HTML error
pages instead of the JSON problem bodies the APIs return, see ``errors.py``) and its own
middleware (the security headers every response carries, see ``security.py``), both
scoped to the console alone.
"""

from __future__ import annotations

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.routing import Route

from loomwork.server.console import errors as console_errors
from loomwork.server.console.auth_views import home_view, login_view, logout_view
from loomwork.server.console.runs_views import (
    run_cancel_view,
    run_detail_view,
    run_signal_view,
    run_terminate_view,
    runs_list_view,
)
from loomwork.server.console.runtime import ConsoleRuntime, new_session_secret
from loomwork.server.console.schedules_views import (
    schedule_detail_view,
    schedule_pause_view,
    schedule_trigger_view,
    schedule_unpause_view,
    schedules_list_view,
)
from loomwork.server.console.security import SecurityHeadersMiddleware
from loomwork.server.console.static import serve_css
from loomwork.server.console.templating import build_environment
from loomwork.server.state import ServerState


def build_console_app(state: ServerState) -> Starlette:
    """Build the console sub-application backed by ``state``.

    ``state`` is the same :class:`~loomwork.server.state.ServerState` the rest of the
    server uses, opened and closed by the outer app's lifespan; the console holds no
    resources of its own besides its session-signing key and template environment, both
    process-lifetime values computed once here.
    """
    secret = state.config.console.session_secret
    runtime = ConsoleRuntime(
        state=state,
        secret=secret.encode("utf-8") if secret else new_session_secret(),
        env=build_environment(),
    )

    routes = [
        Route("/", home_view(runtime), methods=["GET"]),
        Route("/login", login_view(runtime), methods=["GET", "POST"]),
        Route("/logout", logout_view(runtime), methods=["POST"]),
        Route("/static/console.css", serve_css, methods=["GET"]),
        Route("/{namespace}/runs", runs_list_view(runtime), methods=["GET"]),
        Route("/{namespace}/runs/{run_id}", run_detail_view(runtime), methods=["GET"]),
        Route("/{namespace}/runs/{run_id}/signal", run_signal_view(runtime), methods=["POST"]),
        Route("/{namespace}/runs/{run_id}/cancel", run_cancel_view(runtime), methods=["POST"]),
        Route(
            "/{namespace}/runs/{run_id}/terminate", run_terminate_view(runtime), methods=["POST"]
        ),
        Route("/{namespace}/schedules", schedules_list_view(runtime), methods=["GET"]),
        Route(
            "/{namespace}/schedules/{schedule_id}", schedule_detail_view(runtime), methods=["GET"]
        ),
        Route(
            "/{namespace}/schedules/{schedule_id}/pause",
            schedule_pause_view(runtime),
            methods=["POST"],
        ),
        Route(
            "/{namespace}/schedules/{schedule_id}/unpause",
            schedule_unpause_view(runtime),
            methods=["POST"],
        ),
        Route(
            "/{namespace}/schedules/{schedule_id}/trigger",
            schedule_trigger_view(runtime),
            methods=["POST"],
        ),
    ]
    app = Starlette(routes=routes, middleware=[Middleware(SecurityHeadersMiddleware)])
    console_errors.install(app, runtime)
    return app
