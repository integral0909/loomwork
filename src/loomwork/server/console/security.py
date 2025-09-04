"""Security headers applied to every console response.

A raw ASGI middleware (like :class:`~loomwork.server.middleware.RequestContextMiddleware`)
rather than :class:`starlette.middleware.base.BaseHTTPMiddleware`, so it adds headers to
every response -- including the ones rendered by the console's own exception handlers and
the static stylesheet -- without buffering the body.
"""

from __future__ import annotations

from starlette.types import ASGIApp, Message, Receive, Scope, Send

#: No inline scripts (the console ships none), no framing, no fetching from anywhere but
#: this origin. ``form-action 'self'`` keeps every POST target on the console itself.
CONTENT_SECURITY_POLICY = (
    "default-src 'none'; style-src 'self'; img-src 'self'; form-action 'self'; "
    "base-uri 'none'; frame-ancestors 'none'"
)

SECURITY_HEADERS: tuple[tuple[bytes, bytes], ...] = (
    (b"content-security-policy", CONTENT_SECURITY_POLICY.encode("ascii")),
    (b"x-frame-options", b"DENY"),
    (b"referrer-policy", b"no-referrer"),
    (b"x-content-type-options", b"nosniff"),
)


class SecurityHeadersMiddleware:
    """Adds :data:`SECURITY_HEADERS` to every HTTP response from the console app."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = [*message.get("headers", []), *SECURITY_HEADERS]
                message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, receive, send_with_headers)
