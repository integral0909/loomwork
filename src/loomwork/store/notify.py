"""In-process wake-up notifications.

Workers look for work, and if there is none, wait until either a notification arrives or
a poll interval elapses. To avoid the lost-wakeup race (work arrives between the check
and the wait), callers take a *token* before checking and pass it to :meth:`wait`, which
returns immediately if anything was published since the token was taken.

Backends with cross-process notification (PostgreSQL ``LISTEN/NOTIFY``) feed remote
notifications into the same :class:`Notifier`.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict

TOPIC_TICKS = "ticks"
TOPIC_STEPS = "steps"
TOPIC_TIMERS = "timers"


def closed_topic(run_id: str) -> str:
    return f"closed:{run_id}"


class Notifier:
    def __init__(self) -> None:
        self._seq: defaultdict[str, int] = defaultdict(int)
        self._waiters: defaultdict[str, set[asyncio.Future[None]]] = defaultdict(set)

    def token(self, topic: str) -> int:
        return self._seq[topic]

    def publish(self, topic: str) -> None:
        self._seq[topic] += 1
        waiters = self._waiters.pop(topic, None)
        if waiters:
            for future in waiters:
                if not future.done():
                    future.set_result(None)

    async def wait(self, topic: str, token: int, timeout: float | None) -> bool:
        """Wait for a publication on ``topic`` after ``token`` was taken.

        Returns ``True`` if one happened, ``False`` on timeout.
        """
        if self._seq[topic] != token:
            return True
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters[topic].add(future)
        try:
            async with asyncio.timeout(timeout):
                await future
            return True
        except TimeoutError:
            return False
        finally:
            waiters = self._waiters.get(topic)
            if waiters is not None:
                waiters.discard(future)
                if not waiters:
                    del self._waiters[topic]

    def discard(self, topic: str) -> None:
        """Forget a topic's sequence number (for short-lived, per-run topics)."""
        if topic not in self._waiters:
            self._seq.pop(topic, None)

    def waiter_count(self, topic: str) -> int:
        return len(self._waiters.get(topic, ()))
