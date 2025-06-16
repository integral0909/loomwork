"""Parsing of command-line values: durations, timestamps, JSON arguments."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from loomwork.cli.errors import UsageError

_DURATION_RE = re.compile(r"(\d+(?:\.\d+)?)(ms|w|d|h|m|s)")
_UNIT_SECONDS = {
    "ms": 0.001,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
    "d": 86400.0,
    "w": 604800.0,
}


def parse_duration(text: str) -> timedelta:
    """Parse ``"90"`` (seconds), ``"1.5s"``, ``"250ms"`` or compounds like ``"1h30m"``."""
    value = text.strip().lower()
    if not value:
        raise UsageError("empty duration")
    try:
        seconds = float(value)
    except ValueError:
        pass
    else:
        if seconds < 0:
            raise UsageError(f"duration must not be negative: {text!r}")
        return timedelta(seconds=seconds)
    position = 0
    total = 0.0
    for match in _DURATION_RE.finditer(value):
        if match.start() != position:
            break
        total += float(match.group(1)) * _UNIT_SECONDS[match.group(2)]
        position = match.end()
    if position != len(value) or position == 0:
        raise UsageError(
            f"invalid duration {text!r}: use seconds or units such as 250ms, 30s, 5m, 2h, 7d"
        )
    return timedelta(seconds=total)


def format_duration(value: timedelta | float | None) -> str:
    """The inverse of :func:`parse_duration`, for display: ``timedelta(seconds=5400)`` is
    ``"1h30m"``."""
    if value is None:
        return "-"
    seconds = value.total_seconds() if isinstance(value, timedelta) else float(value)
    if seconds == 0:
        return "0s"
    sign = "-" if seconds < 0 else ""
    seconds = abs(seconds)
    if seconds < 1:
        return f"{sign}{round(seconds * 1000)}ms"
    parts = []
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        count, seconds = divmod(seconds, size)
        if count:
            parts.append(f"{int(count)}{unit}")
    if seconds:
        parts.append(f"{seconds:g}s" if seconds != int(seconds) else f"{int(seconds)}s")
    return sign + "".join(parts)


def parse_timestamp(text: str, *, now: datetime | None = None) -> datetime:
    """Parse an ISO 8601 timestamp (naive values are UTC), ``now``, or a relative time
    such as ``-2h`` or ``+30m``."""
    value = text.strip()
    reference = now or datetime.now(UTC)
    if value == "now":
        return reference
    if value[:1] in {"+", "-"}:
        delta = parse_duration(value[1:])
        return reference + delta if value[0] == "+" else reference - delta
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise UsageError(
            f"invalid time {text!r}: use ISO 8601 (2024-05-01T12:00:00Z), 'now', or -2h/+30m"
        ) from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def parse_json_value(text: str, *, what: str = "argument") -> Any:
    """Parse a JSON value. ``@path`` reads the JSON from a file and ``@-`` from stdin
    (the caller passes stdin's text through :func:`read_at_file`)."""
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise UsageError(f"{what} is not valid JSON ({exc.msg}): {text!r}") from None


def read_at_file(text: str, stdin_text: str | None = None) -> str:
    """Resolve ``@file`` and ``@-`` references; other text is returned unchanged."""
    if not text.startswith("@"):
        return text
    target = text[1:]
    if target == "-":
        if stdin_text is None:
            raise UsageError("@- given but nothing is available on stdin")
        return stdin_text
    path = Path(target)
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise UsageError(f"file not found: {path}") from None
    except OSError as exc:
        raise UsageError(f"cannot read {path}: {exc.strerror}") from None


def parse_arguments(
    values: list[str], *, raw_strings: bool = False, stdin_text: str | None = None
) -> list[Any]:
    """Parse workflow arguments given on the command line.

    Each value is JSON (``42``, ``'"text"'``, ``'{"a": 1}'``), or ``@file.json``. With
    ``raw_strings``, values are passed as plain strings without JSON decoding.
    """
    if raw_strings:
        return [read_at_file(v, stdin_text) for v in values]
    return [
        parse_json_value(read_at_file(v, stdin_text), what=f"argument {i + 1}")
        for i, v in enumerate(values)
    ]


def parse_key_values(pairs: list[str], *, what: str = "attribute") -> dict[str, Any]:
    """Parse ``key=value`` pairs. Values are JSON when they parse as JSON, otherwise
    strings, so ``tier=gold`` and ``amount=12.5`` both do what one expects."""
    result: dict[str, Any] = {}
    for pair in pairs:
        key, sep, raw = pair.partition("=")
        key = key.strip()
        if not sep or not key:
            raise UsageError(f"{what} must look like key=value, got {pair!r}")
        try:
            value: Any = json.loads(raw)
        except json.JSONDecodeError:
            value = raw
        result[key] = value
    return result
