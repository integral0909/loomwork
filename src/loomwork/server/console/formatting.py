"""Turning engine/store values into short, human-readable strings for templates.

Nothing here ever raises on malformed or encrypted data: a payload that cannot be
decoded with the default converter (compressed, encrypted, or simply not JSON) always
renders as a placeholder describing its encoding, never as raw bytes and never as a
crash. This is what lets the run detail page show step arguments, results and history
events uniformly regardless of what codec chain produced them.
"""

from __future__ import annotations

from typing import Any

from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.server.responses import payload_view

_PAYLOAD_KEYS = {"data", "metadata"}


def describe_payload(payload: Payload | None) -> str:
    """Decode ``payload`` with the default converter, or describe it if that fails."""
    if payload is None:
        return "-"
    decoded = payload_view(payload)
    if isinstance(decoded, dict) and set(decoded) == _PAYLOAD_KEYS:
        return f"<{payload.encoding or 'unknown'} payload, {len(payload.data)} bytes>"
    return describe_value(decoded)


def _is_payload_dict(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) >= _PAYLOAD_KEYS
        and isinstance(value.get("metadata"), dict)
    )


def describe_value(value: Any) -> str:
    """Render an arbitrary decoded value (or wire-form payload dict) for display."""
    if _is_payload_dict(value):
        try:
            payload = Payload.from_dict(value)
        except (KeyError, TypeError, ValueError):
            return "<malformed payload>"
        return describe_payload(payload)
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return "[" + ", ".join(describe_value(item) for item in value) + "]" if value else "[]"
    if isinstance(value, dict):
        if not value:
            return "{}"
        return "{" + ", ".join(f"{k}={describe_value(v)}" for k, v in value.items()) + "}"
    return str(value)


def failure_chain(failure: FailureInfo | None) -> list[FailureInfo]:
    """``failure`` followed by every ``cause``, outermost first."""
    chain = []
    current = failure
    while current is not None:
        chain.append(current)
        current = current.cause
    return chain
