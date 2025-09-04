"""Small request-parsing helpers shared by the console's view modules."""

from __future__ import annotations

from urllib.parse import parse_qsl

from starlette.requests import Request

_DEFAULT_NEXT = "/console/"


async def parse_form(request: Request) -> dict[str, str]:
    """Parse an ``application/x-www-form-urlencoded`` body.

    Every console form is plain text fields (no uploads), so this avoids depending on
    ``python-multipart`` just to read a handful of ``key=value`` pairs -- the one
    dependency :mod:`starlette`'s own ``Request.form()`` requires even for the
    urlencoded case. An unparseable or absent body simply yields no fields.
    """
    body = await request.body()
    if not body:
        return {}
    pairs = parse_qsl(body.decode("utf-8", errors="replace"), keep_blank_values=True)
    return dict(pairs)


def safe_next(path: str | None) -> str:
    """A ``next`` redirect target, defaulting to the console home for anything that is
    not an absolute path under ``/console`` -- an open-redirect guard, since this value
    comes from a query string or form field a caller controls."""
    if not path or not path.startswith("/console") or path.startswith("//"):
        return _DEFAULT_NEXT
    return path


def parse_int(text: str | None, *, default: int, minimum: int, maximum: int) -> int:
    if text is None or not text.strip():
        return default
    try:
        value = int(text)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))
