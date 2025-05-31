"""ASGI hardening middleware: request ids and a request body size limit.

Both run at the raw ASGI level (not :class:`starlette.middleware.base.BaseHTTPMiddleware`)
so the body limit is enforced before Starlette buffers anything into memory.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from loomwork.ids import new_id

REQUEST_ID_HEADER = b"x-request-id"


def _header(scope: Scope, name: bytes) -> str | None:
    for key, value in scope.get("headers", ()):
        if key.lower() == name:
            return str(value.decode("latin-1"))
    return None


class RequestContextMiddleware:
    """Echoes or generates ``X-Request-ID`` and rejects oversized request bodies.

    The body check uses the ``Content-Length`` header, which every JSON client
    (including :class:`httpx.AsyncClient`) sends for a buffered request body. A request
    that omits it (chunked transfer-encoding) is not pre-rejected here; handlers still
    bound how much they read in practice by decoding a fixed-size JSON body.
    """

    def __init__(self, app: ASGIApp, *, max_body_bytes: int) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = _header(scope, REQUEST_ID_HEADER) or new_id()
        scope.setdefault("state", {})
        scope["state"]["request_id"] = request_id

        content_length = _header(scope, b"content-length")
        if content_length is not None:
            try:
                too_large = int(content_length) > self.max_body_bytes
            except ValueError:
                too_large = False
            if too_large:
                await _send_too_large(request_id, send)
                return

        async def send_with_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append((REQUEST_ID_HEADER, request_id.encode("latin-1")))
                message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, receive, send_with_id)


async def _send_too_large(request_id: str, send: Callable[[Message], Awaitable[None]]) -> None:
    import json

    body = json.dumps(
        {
            "type": "https://loomwork.dev/errors/payload_too_large",
            "code": "payload_too_large",
            "title": "Request body too large",
            "detail": "the request body exceeds this server's configured limit",
            "status": 413,
        }
    ).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [
                (b"content-type", b"application/json"),
                (REQUEST_ID_HEADER, request_id.encode("latin-1")),
                (b"content-length", str(len(body)).encode("ascii")),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})
