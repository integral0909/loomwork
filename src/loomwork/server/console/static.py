"""The console's one static asset: a hand-written stylesheet, no CDNs.

Read once at import time and served from memory rather than through
:class:`starlette.staticfiles.StaticFiles`, since a single small, trusted file does not
need a directory-serving library -- this also keeps the response headers (in particular
the security headers every console response carries) entirely under this package's
control.
"""

from __future__ import annotations

from pathlib import Path

from starlette.requests import Request
from starlette.responses import Response

_CSS_PATH = Path(__file__).parent / "static" / "console.css"
_CSS = _CSS_PATH.read_text("utf-8")


async def serve_css(_request: Request) -> Response:
    return Response(_CSS, media_type="text/css")
