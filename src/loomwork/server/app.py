"""The ASGI application factory.

.. code-block:: python

    from loomwork.server import ServerConfig, create_app

    config = ServerConfig(namespaces={"default": "sqlite:////var/lib/loomwork/db.sqlite3"}, ...)
    app = create_app(config)  # pass to uvicorn, or run via `python -m loomwork.server`

See ``docs/server.md`` for the full route table, the worker/control API split, and the
authentication and namespace model.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from loomwork.clock import Clock
from loomwork.server import control_api, worker_api
from loomwork.server.config import ServerConfig
from loomwork.server.console import build_console_app
from loomwork.server.errors import install as install_error_handlers
from loomwork.server.middleware import RequestContextMiddleware
from loomwork.server.schemas import HealthResponse, ReadyResponse
from loomwork.server.state import ServerState

logger = logging.getLogger("loomwork.server")


async def _healthz(_request: Request) -> JSONResponse:
    return JSONResponse(HealthResponse(status="ok").model_dump())


async def _readyz(request: Request) -> JSONResponse:
    server: ServerState = request.app.state.server
    results = {name: await ns.healthy() for name, ns in server.namespaces.items()}
    ok = all(results.values())
    body = ReadyResponse(status="ok" if ok else "unavailable", namespaces=results)
    return JSONResponse(body.model_dump(), status_code=200 if ok else 503)


def create_app(
    config: ServerConfig,
    *,
    clock: Clock | None = None,
    run_system_workers: bool = True,
) -> Starlette:
    """Build the ASGI application described by ``config``.

    Args:
        clock: overrides the server's time source for every request (used by tests
            with a :class:`~loomwork.clock.ManualClock` shared with the test's
            :class:`~loomwork.remote.http_store.HttpStore`; see ``docs/server.md``).
            Defaults to :class:`~loomwork.clock.SystemClock`.
        run_system_workers: whether each namespace runs an internal worker that fires
            due timers and relays the outbox. Disable it for tests that want to control
            timers and the outbox directly through the store, or when several server
            processes share one namespace and only one of them should run it.
    """
    state = ServerState(config, clock=clock, run_system_workers=run_system_workers)

    @asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        await state.open()
        logger.info("loomwork server ready: namespaces=%s", sorted(state.namespaces))
        try:
            yield
        finally:
            await state.close()
            logger.info("loomwork server stopped")

    middleware = [
        Middleware(RequestContextMiddleware, max_body_bytes=config.limits.max_request_body_bytes)
    ]
    if config.cors.enabled:
        from starlette.middleware.cors import CORSMiddleware

        middleware.append(
            Middleware(
                CORSMiddleware,
                allow_origins=config.cors.allow_origins,
                allow_methods=["*"],
                allow_headers=["*"],
            )
        )

    routes = [
        Route("/healthz", _healthz, methods=["GET"]),
        Route("/readyz", _readyz, methods=["GET"]),
        # The more specific "/control" mount must be listed first: Starlette's router
        # takes the first prefix match, and "/v1/namespaces/{namespace}" is itself a
        # prefix of every control path.
        Mount("/v1/namespaces/{namespace}/control", routes=control_api.routes),
        Mount("/v1/namespaces/{namespace}", routes=worker_api.routes),
    ]
    if config.console.enabled:
        routes.append(Mount("/console", app=build_console_app(state)))

    app = Starlette(routes=routes, middleware=middleware, lifespan=lifespan)
    app.state.server = state
    install_error_handlers(app)
    return app


async def serve(config: ServerConfig, *, clock: Clock | None = None) -> None:
    """Run the server described by ``config`` with uvicorn until cancelled.

    Used by ``python -m loomwork.server``; embed this (or build your own uvicorn
    ``Config``/``Server`` around :func:`create_app`) to host the server inside another
    process's event loop.
    """
    import uvicorn

    app = create_app(config, clock=clock)
    uv_config = uvicorn.Config(app, host=config.host, port=config.port, log_config=None)
    server = uvicorn.Server(uv_config)
    await server.serve()
