"""Engine time sources.

The engine never calls :func:`datetime.now` directly. It reads time through a
:class:`Clock` so that tests can drive time explicitly with :class:`ManualClock`.

All timestamps are timezone-aware UTC ``datetime`` objects.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable


def utcnow() -> datetime:
    """Current wall-clock time as an aware UTC datetime."""
    return datetime.now(UTC)


def ensure_utc(value: datetime) -> datetime:
    """Return ``value`` as an aware UTC datetime; naive values are rejected."""
    if value.tzinfo is None:
        raise ValueError("naive datetimes are not allowed; attach a timezone")
    return value.astimezone(UTC)


@runtime_checkable
class Clock(Protocol):
    """A source of engine time."""

    def now(self) -> datetime:
        """Return the current time as an aware UTC datetime."""
        ...

    async def sleep_until(self, deadline: datetime) -> None:
        """Suspend until the clock reaches ``deadline``."""
        ...


class SystemClock:
    """Clock backed by the operating system's wall clock."""

    def now(self) -> datetime:
        return utcnow()

    async def sleep_until(self, deadline: datetime) -> None:
        delay = (deadline - self.now()).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay)


class ManualClock:
    """A clock that only moves when told to.

    Coroutines blocked in :meth:`sleep_until` wake up as soon as :meth:`advance` or
    :meth:`set` moves the clock past their deadline.
    """

    def __init__(self, start: datetime | None = None) -> None:
        self._now = ensure_utc(start) if start else datetime(2024, 1, 1, tzinfo=UTC)
        self._changed = asyncio.Condition()
        self._version = 0

    def now(self) -> datetime:
        return self._now

    async def sleep_until(self, deadline: datetime) -> None:
        async with self._changed:
            await self._changed.wait_for(lambda: self._now >= deadline)

    async def advance(self, delta: timedelta | float) -> datetime:
        """Move the clock forward by ``delta`` (seconds when given a number)."""
        if not isinstance(delta, timedelta):
            delta = timedelta(seconds=delta)
        if delta < timedelta(0):
            raise ValueError("a ManualClock cannot move backwards")
        return await self.set(self._now + delta)

    async def set(self, value: datetime) -> datetime:
        """Jump to ``value``, which must not be earlier than the current time."""
        value = ensure_utc(value)
        if value < self._now:
            raise ValueError("a ManualClock cannot move backwards")
        async with self._changed:
            self._now = value
            self._version += 1
            self._changed.notify_all()
        return self._now

    @property
    def version(self) -> int:
        """Incremented every time the clock moves; useful in assertions."""
        return self._version
