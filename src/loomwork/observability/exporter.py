"""A minimal HTTP endpoint that serves a :class:`MetricsRegistry` to Prometheus.

Worker processes usually have no web framework of their own, and pulling one in only to
expose ``/metrics`` would be a heavy dependency for a few lines of text. This server
speaks just enough HTTP/1.1 for scrapers and health checks:

* ``GET /metrics``: the registry in the Prometheus text format.
* ``GET /healthz``: ``200 ok`` while a readiness callback (if any) returns true,
  ``503`` otherwise.

Anything else gets ``404``, and non-GET methods ``405``. Each connection serves one
request and is closed; request headers are read with a size limit and a timeout so a
slow or hostile client cannot tie up the worker.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from http import HTTPStatus

from loomwork.observability.metrics import MetricsRegistry

logger = logging.getLogger("loomwork.metrics")

#: Largest request head accepted, in bytes.
MAX_REQUEST_HEAD = 8192
#: Seconds a client gets to send its request.
REQUEST_TIMEOUT = 5.0


class MetricsServer:
    """Serves ``registry`` on ``host:port`` until :meth:`stop` is called.

    ``port=0`` picks a free port; :attr:`port` holds the actual one after :meth:`start`.
    """

    def __init__(
        self,
        registry: MetricsRegistry,
        host: str = "127.0.0.1",
        port: int = 9464,
        *,
        ready: Callable[[], bool] | None = None,
    ) -> None:
        self.registry = registry
        self.host = host
        self.port = port
        self.ready = ready
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        if self._server is not None:
            return
        self._server = await asyncio.start_server(self._serve, self.host, self.port)
        sockets = self._server.sockets
        if sockets:
            self.port = int(sockets[0].getsockname()[1])
        logger.info("serving metrics on http://%s:%d/metrics", self.host, self.port)

    async def stop(self) -> None:
        server, self._server = self._server, None
        if server is not None:
            server.close()
            await server.wait_closed()

    async def __aenter__(self) -> MetricsServer:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    # ------------------------------------------------------------------ protocol

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            try:
                head = await asyncio.wait_for(
                    reader.readuntil(b"\r\n\r\n"), timeout=REQUEST_TIMEOUT
                )
            except (asyncio.IncompleteReadError, TimeoutError, ConnectionError):
                return
            except asyncio.LimitOverrunError:
                await self._respond(writer, HTTPStatus.REQUEST_HEADER_FIELDS_TOO_LARGE, "")
                return
            if len(head) > MAX_REQUEST_HEAD:
                await self._respond(writer, HTTPStatus.REQUEST_HEADER_FIELDS_TOO_LARGE, "")
                return
            status, body, content_type = self._route(head)
            await self._respond(writer, status, body, content_type)
        except Exception:  # pragma: no cover - defensive: never let a scrape kill the loop
            logger.exception("metrics request failed")
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()

    def _route(self, head: bytes) -> tuple[HTTPStatus, str, str]:
        request_line = head.split(b"\r\n", 1)[0].decode("latin-1")
        parts = request_line.split(" ")
        if len(parts) != 3 or not parts[2].startswith("HTTP/"):
            return HTTPStatus.BAD_REQUEST, "bad request\n", "text/plain"
        method, target, _ = parts
        path = target.split("?", 1)[0]
        if path not in {"/metrics", "/healthz"}:
            return HTTPStatus.NOT_FOUND, "not found\n", "text/plain"
        if method not in {"GET", "HEAD"}:
            return HTTPStatus.METHOD_NOT_ALLOWED, "method not allowed\n", "text/plain"
        if path == "/healthz":
            healthy = self.ready() if self.ready is not None else True
            if healthy:
                return HTTPStatus.OK, "ok\n", "text/plain"
            return HTTPStatus.SERVICE_UNAVAILABLE, "not ready\n", "text/plain"
        return HTTPStatus.OK, self.registry.render(), MetricsRegistry.CONTENT_TYPE

    @staticmethod
    async def _respond(
        writer: asyncio.StreamWriter,
        status: HTTPStatus,
        body: str,
        content_type: str = "text/plain",
    ) -> None:
        payload = body.encode("utf-8")
        head = (
            f"HTTP/1.1 {status.value} {status.phrase}\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(payload)}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode("latin-1")
        writer.write(head + payload)
        with contextlib.suppress(ConnectionError):
            await writer.drain()
