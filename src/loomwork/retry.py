"""Retry policies for steps and tasks."""

from __future__ import annotations

import random
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from loomwork.failure import FailureInfo


def _as_timedelta(value: timedelta | float) -> timedelta:
    return value if isinstance(value, timedelta) else timedelta(seconds=value)


@dataclass(frozen=True, slots=True, init=False)
class RetryPolicy:
    """Exponential backoff with an optional attempt cap.

    Durations accept a ``timedelta`` or a number of seconds.

    Attributes:
        initial_interval: delay before the second attempt.
        backoff: multiplier applied to the delay after every failed attempt.
        max_interval: upper bound for a single delay.
        max_attempts: total attempts including the first; ``0`` means unlimited.
        jitter: fraction (0..1) of each delay that is randomised to avoid thundering
            herds. Jitter is applied by the engine, never inside workflow code.
        non_retryable_types: failure ``type`` names that are never retried.
    """

    initial_interval: timedelta
    backoff: float
    max_interval: timedelta
    max_attempts: int
    jitter: float
    non_retryable_types: frozenset[str]

    def __init__(
        self,
        initial_interval: timedelta | float = timedelta(seconds=1),
        backoff: float = 2.0,
        max_interval: timedelta | float = timedelta(seconds=100),
        max_attempts: int = 5,
        jitter: float = 0.1,
        non_retryable_types: Iterable[str] = (),
    ) -> None:
        initial = _as_timedelta(initial_interval)
        maximum = _as_timedelta(max_interval)
        if initial <= timedelta(0):
            raise ValueError("initial_interval must be positive")
        if maximum < initial:
            raise ValueError("max_interval must be >= initial_interval")
        if backoff < 1.0:
            raise ValueError("backoff must be >= 1.0")
        if max_attempts < 0:
            raise ValueError("max_attempts must be >= 0 (0 means unlimited)")
        if not 0.0 <= jitter <= 1.0:
            raise ValueError("jitter must be between 0 and 1")
        object.__setattr__(self, "initial_interval", initial)
        object.__setattr__(self, "backoff", float(backoff))
        object.__setattr__(self, "max_interval", maximum)
        object.__setattr__(self, "max_attempts", int(max_attempts))
        object.__setattr__(self, "jitter", float(jitter))
        object.__setattr__(self, "non_retryable_types", frozenset(non_retryable_types))

    @classmethod
    def no_retry(cls) -> RetryPolicy:
        """A policy that performs exactly one attempt."""
        return cls(max_attempts=1)

    def delay_for(self, attempt: int, rng: random.Random | None = None) -> timedelta:
        """Delay to wait after ``attempt`` (1-based) failed, before the next attempt."""
        if attempt < 1:
            raise ValueError("attempt numbers start at 1")
        base = self.initial_interval.total_seconds() * (self.backoff ** (attempt - 1))
        base = min(base, self.max_interval.total_seconds())
        if self.jitter and rng is not None:
            spread = base * self.jitter
            base = max(0.0, base - spread + rng.random() * 2 * spread)
        return timedelta(seconds=base)

    def should_retry(self, attempt: int, failure: FailureInfo) -> bool:
        """Whether another attempt should follow failed attempt number ``attempt``."""
        if failure.non_retryable or failure.type in self.non_retryable_types:
            return False
        return self.max_attempts == 0 or attempt < self.max_attempts

    def to_dict(self) -> dict[str, Any]:
        return {
            "initial_interval": self.initial_interval.total_seconds(),
            "backoff": self.backoff,
            "max_interval": self.max_interval.total_seconds(),
            "max_attempts": self.max_attempts,
            "jitter": self.jitter,
            "non_retryable_types": sorted(self.non_retryable_types),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RetryPolicy:
        return cls(
            initial_interval=timedelta(seconds=float(data["initial_interval"])),
            backoff=float(data["backoff"]),
            max_interval=timedelta(seconds=float(data["max_interval"])),
            max_attempts=int(data["max_attempts"]),
            jitter=float(data.get("jitter", 0.0)),
            non_retryable_types=frozenset(data.get("non_retryable_types", ())),
        )
