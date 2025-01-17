"""Per-queue admission control for a worker's step loops.

Limits are *worker-local*: ``rate`` caps how many attempts this worker starts per second
from a queue, ``concurrency`` caps how many run at once. They protect downstream
services from one worker's burst; a fleet-wide limit is the per-worker limit times the
number of workers.

Time comes from :func:`time.monotonic` rather than the engine clock, because limits are
about real throughput, not workflow time.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass


class TokenBucket:
    """Classic token bucket: ``rate`` tokens per second, holding at most ``burst``."""

    def __init__(
        self,
        rate: float,
        burst: float | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if rate <= 0 or math.isinf(rate) or math.isnan(rate):
            raise ValueError("rate must be a positive, finite number")
        self.rate = rate
        self.burst = max(burst if burst is not None else rate, 1.0)
        self._clock = clock
        self._tokens = self.burst
        self._updated = clock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(now - self._updated, 0.0)
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._updated = now

    def try_acquire(self) -> bool:
        self._refill()
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    def peek(self) -> bool:
        """Whether a token is available, without taking it."""
        self._refill()
        return self._tokens >= 1.0

    def refund(self) -> None:
        """Return a token taken for a claim that found no work."""
        self._tokens = min(self.burst, self._tokens + 1.0)

    def seconds_until_token(self) -> float:
        self._refill()
        if self._tokens >= 1.0:
            return 0.0
        return (1.0 - self._tokens) / self.rate


@dataclass(frozen=True, slots=True)
class QueueLimit:
    rate: float | None = None
    burst: float | None = None
    concurrency: int | None = None

    def __post_init__(self) -> None:
        if self.rate is not None and self.rate <= 0:
            raise ValueError("rate must be positive")
        if self.concurrency is not None and self.concurrency < 1:
            raise ValueError("concurrency must be at least 1")


class QueueGate:
    """Decides which queues a step loop may claim from right now."""

    def __init__(
        self,
        queues: Iterable[str] | None,
        limits: Mapping[str, QueueLimit],
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.queues = None if queues is None else tuple(dict.fromkeys(queues))
        unknown = set(limits) - set(self.queues or ())
        if unknown:
            raise ValueError(
                f"limits configured for queues this worker does not poll: {sorted(unknown)}; "
                "list them in task_queues"
            )
        self._limits = dict(limits)
        self._buckets = {
            name: TokenBucket(limit.rate, limit.burst, clock=clock)
            for name, limit in limits.items()
            if limit.rate is not None
        }
        self._running: dict[str, int] = dict.fromkeys(limits, 0)

    @property
    def unrestricted(self) -> bool:
        return not self._limits

    def open_queues(self) -> tuple[str, ...] | None:
        """Queues that currently admit a new attempt (``None`` = every queue)."""
        if self.queues is None:
            return None
        return tuple(q for q in self.queues if self._admits(q))

    def _admits(self, queue: str) -> bool:
        limit = self._limits.get(queue)
        if limit is None:
            return True
        if limit.concurrency is not None and self._running[queue] >= limit.concurrency:
            return False
        bucket = self._buckets.get(queue)
        return bucket is None or bucket.peek()

    def acquire(self, queue: str) -> None:
        """Account for an attempt that was just claimed from ``queue``."""
        if queue in self._running:
            self._running[queue] += 1
        bucket = self._buckets.get(queue)
        if bucket is not None and not bucket.try_acquire():
            # Another loop took the last token between open_queues() and the claim;
            # the overshoot is at most one attempt per loop and is paid back later.
            bucket._tokens -= 1.0

    def release(self, queue: str) -> None:
        if queue in self._running:
            self._running[queue] = max(self._running[queue] - 1, 0)

    def seconds_until_open(self) -> float | None:
        """How long until a rate-limited queue admits again (``None``: not rate-bound)."""
        waits = [
            bucket.seconds_until_token()
            for name, bucket in self._buckets.items()
            if self._limits[name].concurrency is None
            or self._running[name] < (self._limits[name].concurrency or 0)
        ]
        return min(waits) if waits else None

    def running(self, queue: str) -> int:
        return self._running.get(queue, 0)
