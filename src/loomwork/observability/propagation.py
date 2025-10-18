"""W3C Trace Context (``traceparent`` / ``tracestate``) for carrying traces through runs.

A workflow run can outlive every process that touched it, so a trace cannot be carried
in memory from the code that starts a run to the steps that run days later. Instead the
starting caller's trace context is recorded with the run as a header, and each step
attempt continues it. This module is the pure part of that: parsing, validating and
formatting the two headers exactly as the W3C Trace Context specification (level 1)
defines them, and deriving child span ids deterministically.

Determinism matters because workflow code runs again on every replay: a span id created
while a workflow schedules a step must be the same each time, or the recorded history
would differ from the replayed one. :func:`derived_span_id` hashes stable inputs (the run
id and the command's sequence number) instead of drawing random bits.
"""

from __future__ import annotations

import hashlib
import re
import secrets
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

#: Header names, lower case as HTTP/2 requires.
TRACEPARENT = "traceparent"
TRACESTATE = "tracestate"

_TRACEPARENT_RE = re.compile(
    r"^(?P<version>[0-9a-f]{2})-(?P<trace_id>[0-9a-f]{32})-(?P<span_id>[0-9a-f]{16})"
    r"-(?P<flags>[0-9a-f]{2})(?P<rest>.*)$"
)
_INVALID_TRACE_ID = "0" * 32
_INVALID_SPAN_ID = "0" * 16
_SAMPLED = 0x01

# tracestate keys: simple keys, or multi-tenant "tenant@system" keys (spec section 3.3).
_KEY_RE = re.compile(
    r"^(?:[a-z][a-z0-9_\-*/]{0,255}|[a-z0-9][a-z0-9_\-*/]{0,240}@[a-z][a-z0-9_\-*/]{0,13})$"
)
# Printable ASCII except ',' and '=', not ending in a space; up to 256 characters.
_VALUE_RE = re.compile(r"^[\x20-\x2b\x2d-\x3c\x3e-\x7e]{0,255}[\x21-\x2b\x2d-\x3c\x3e-\x7e]$")
MAX_TRACESTATE_MEMBERS = 32


class TraceContextError(ValueError):
    """A header value that does not follow the Trace Context specification."""


@dataclass(frozen=True, slots=True)
class TraceState:
    """An ordered list of vendor ``key=value`` pairs; the first entry is the most recent."""

    entries: tuple[tuple[str, str], ...] = ()

    @classmethod
    def parse(cls, header: str | Iterable[str]) -> TraceState:
        """Parse one or more ``tracestate`` header values (repeated headers concatenate).

        Invalid members make the whole value invalid; per the specification the caller
        then discards the tracestate (not the traceparent). Empty members are allowed.
        """
        values = [header] if isinstance(header, str) else list(header)
        entries: list[tuple[str, str]] = []
        seen: set[str] = set()
        for value in values:
            for raw in value.split(","):
                member = raw.strip(" \t")
                if not member:
                    continue
                key, sep, val = member.partition("=")
                if not sep or not _KEY_RE.match(key) or not _VALUE_RE.match(val):
                    raise TraceContextError(f"invalid tracestate member {member!r}")
                if key in seen:
                    raise TraceContextError(f"duplicate tracestate key {key!r}")
                seen.add(key)
                entries.append((key, val))
        if len(entries) > MAX_TRACESTATE_MEMBERS:
            raise TraceContextError(
                f"tracestate has {len(entries)} members; at most {MAX_TRACESTATE_MEMBERS}"
            )
        return cls(tuple(entries))

    def get(self, key: str) -> str | None:
        for entry_key, value in self.entries:
            if entry_key == key:
                return value
        return None

    def with_entry(self, key: str, value: str) -> TraceState:
        """Add or update ``key`` and move it to the front, as a participant that modified
        the state must (spec section 3.5); the oldest entry is dropped beyond the limit."""
        if not _KEY_RE.match(key):
            raise TraceContextError(f"invalid tracestate key {key!r}")
        if not _VALUE_RE.match(value):
            raise TraceContextError(f"invalid tracestate value {value!r}")
        rest = [(k, v) for k, v in self.entries if k != key]
        entries = ((key, value), *rest)[:MAX_TRACESTATE_MEMBERS]
        return TraceState(entries)

    def without(self, key: str) -> TraceState:
        return TraceState(tuple((k, v) for k, v in self.entries if k != key))

    def format(self) -> str:
        return ",".join(f"{k}={v}" for k, v in self.entries)

    def __bool__(self) -> bool:
        return bool(self.entries)


@dataclass(frozen=True, slots=True)
class TraceContext:
    """One position in a distributed trace: the trace, the parent span, and flags."""

    trace_id: str
    span_id: str
    flags: int = _SAMPLED
    state: TraceState = field(default_factory=TraceState)

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[0-9a-f]{32}", self.trace_id) or self.trace_id == _INVALID_TRACE_ID:
            raise TraceContextError(f"invalid trace id {self.trace_id!r}")
        if not re.fullmatch(r"[0-9a-f]{16}", self.span_id) or self.span_id == _INVALID_SPAN_ID:
            raise TraceContextError(f"invalid span id {self.span_id!r}")
        if not 0 <= self.flags <= 0xFF:
            raise TraceContextError(f"invalid trace flags {self.flags!r}")

    @property
    def sampled(self) -> bool:
        return bool(self.flags & _SAMPLED)

    @classmethod
    def new_root(cls, *, sampled: bool = True) -> TraceContext:
        """A fresh trace. Uses the system's random source: never call it from workflow
        code (use :func:`derived_span_id` there)."""
        return cls(
            trace_id=_nonzero_hex(16),
            span_id=_nonzero_hex(8),
            flags=_SAMPLED if sampled else 0,
        )

    @classmethod
    def parse(cls, traceparent: str, tracestate: str | Iterable[str] | None = None) -> TraceContext:
        """Parse a ``traceparent`` header (and an optional ``tracestate``).

        Follows the specification's forward compatibility rules: a higher version is
        accepted if its first four fields parse, and anything after them must start with
        ``-``; version ``00`` must have nothing after the flags; version ``ff`` is invalid.
        An invalid tracestate is dropped rather than failing the whole context.
        """
        match = _TRACEPARENT_RE.match(traceparent.strip())
        if match is None:
            raise TraceContextError(f"malformed traceparent {traceparent!r}")
        version = match["version"]
        rest = match["rest"]
        if version == "ff":
            raise TraceContextError("traceparent version ff is invalid")
        if version == "00" and rest:
            raise TraceContextError("version 00 traceparent has trailing data")
        if rest and not rest.startswith("-"):
            raise TraceContextError(f"malformed traceparent {traceparent!r}")
        state = TraceState()
        if tracestate:
            try:
                state = TraceState.parse(tracestate)
            except TraceContextError:
                state = TraceState()
        return cls(match["trace_id"], match["span_id"], int(match["flags"], 16), state)

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> TraceContext | None:
        """The context in ``headers`` (case-insensitive names), or ``None`` if absent or
        invalid: a broken incoming header starts no trace instead of failing the call."""
        lowered = {k.lower(): v for k, v in headers.items()}
        traceparent = lowered.get(TRACEPARENT)
        if not traceparent:
            return None
        try:
            return cls.parse(traceparent, lowered.get(TRACESTATE))
        except TraceContextError:
            return None

    def traceparent(self) -> str:
        return f"00-{self.trace_id}-{self.span_id}-{self.flags:02x}"

    def to_headers(self) -> dict[str, str]:
        headers = {TRACEPARENT: self.traceparent()}
        if self.state:
            headers[TRACESTATE] = self.state.format()
        return headers

    def child(self, span_id: str) -> TraceContext:
        """The context a callee sees when ``span_id`` is the caller's new span."""
        return TraceContext(self.trace_id, span_id, self.flags, self.state)


def derived_span_id(*parts: str | int) -> str:
    """A span id computed from ``parts``: the same inputs always give the same id.

    Safe in workflow code, where randomness would differ between the original run and a
    replay. Collisions are as unlikely as for random ids (64 bits of a SHA-256 digest).
    """
    digest = hashlib.sha256("\x1f".join(str(p) for p in parts).encode()).hexdigest()
    span = digest[:16]
    return span if span != _INVALID_SPAN_ID else digest[16:32]


def _nonzero_hex(size: int) -> str:
    while True:
        value = secrets.token_hex(size)
        if value.strip("0"):
            return value


__all__ = [
    "MAX_TRACESTATE_MEMBERS",
    "TRACEPARENT",
    "TRACESTATE",
    "TraceContext",
    "TraceContextError",
    "TraceState",
    "derived_span_id",
]
