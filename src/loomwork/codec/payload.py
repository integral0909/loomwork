"""The :class:`Payload` envelope.

Every user value that crosses the engine boundary (workflow arguments, step results,
failure details) is stored as a payload: opaque bytes plus string metadata describing how
to decode them. The engine never interprets payload bytes, which is what makes end-to-end
payload encryption possible.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from typing import Any

ENCODING_KEY = "encoding"

ENCODING_NULL = "binary/null"
ENCODING_BYTES = "binary/plain"
ENCODING_JSON = "json/plain"


@dataclass(frozen=True, slots=True)
class Payload:
    """Encoded bytes plus metadata."""

    data: bytes
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def encoding(self) -> str:
        return self.metadata.get(ENCODING_KEY, "")

    def with_metadata(self, **updates: str) -> Payload:
        return Payload(self.data, {**self.metadata, **updates})

    def __len__(self) -> int:
        return len(self.data)

    def to_dict(self) -> dict[str, Any]:
        return {
            "data": base64.b64encode(self.data).decode("ascii"),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Payload:
        return cls(
            data=base64.b64decode(data["data"]),
            metadata={str(k): str(v) for k, v in data.get("metadata", {}).items()},
        )

    def __repr__(self) -> str:
        preview = self.data[:32]
        suffix = "..." if len(self.data) > 32 else ""
        return f"Payload({self.encoding or '?'}, {preview!r}{suffix})"


def payloads_to_list(payloads: list[Payload]) -> list[dict[str, Any]]:
    return [p.to_dict() for p in payloads]


def payloads_from_list(items: list[dict[str, Any]]) -> list[Payload]:
    return [Payload.from_dict(item) for item in items]
