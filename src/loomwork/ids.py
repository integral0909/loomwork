"""Identifier generation.

Run ids and job ids are 128-bit, time-ordered identifiers rendered in Crockford base32
(26 characters). Ordering by id approximates ordering by creation time, which keeps
B-tree indexes compact in SQL backends and makes ids pleasant to sort in logs.

Layout: 48 bits of milliseconds since the Unix epoch followed by 80 random bits. Within a
single millisecond the random part is incremented instead of redrawn so that ids created
by one generator are strictly increasing.
"""

from __future__ import annotations

import os
import threading
import time

_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_DECODE = {c: i for i, c in enumerate(_ALPHABET)}
_RANDOM_BITS = 80
_RANDOM_MAX = (1 << _RANDOM_BITS) - 1
ID_LENGTH = 26


def _encode(value: int) -> str:
    chars = []
    for _ in range(ID_LENGTH):
        chars.append(_ALPHABET[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


def decode(identifier: str) -> int:
    """Decode an id produced by :class:`IdGenerator` into its integer value."""
    if len(identifier) != ID_LENGTH:
        raise ValueError(f"expected {ID_LENGTH} characters, got {len(identifier)}")
    value = 0
    for char in identifier.upper():
        try:
            value = (value << 5) | _DECODE[char]
        except KeyError:
            raise ValueError(f"invalid character {char!r} in id") from None
    if value >> 128:
        raise ValueError("id overflows 128 bits")
    return value


def timestamp_ms(identifier: str) -> int:
    """Return the millisecond timestamp embedded in an id."""
    return decode(identifier) >> _RANDOM_BITS


class IdGenerator:
    """Thread-safe generator of monotonic, time-ordered identifiers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_ms = -1
        self._last_random = 0

    def new(self) -> str:
        with self._lock:
            now_ms = time.time_ns() // 1_000_000
            if now_ms <= self._last_ms:
                now_ms = self._last_ms
                self._last_random += 1
                if self._last_random > _RANDOM_MAX:
                    # Random space exhausted within one millisecond: borrow the next one.
                    now_ms += 1
                    self._last_random = int.from_bytes(os.urandom(10), "big") >> 1
            else:
                # Keep headroom below the maximum so increments rarely overflow.
                self._last_random = int.from_bytes(os.urandom(10), "big") >> 1
            self._last_ms = now_ms
            return _encode((now_ms << _RANDOM_BITS) | self._last_random)


_default = IdGenerator()


def new_id() -> str:
    """Return a new time-ordered identifier from the process-wide generator."""
    return _default.new()
