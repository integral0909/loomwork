"""LRU cache of live workflow replayers kept between ticks."""

from __future__ import annotations

from collections import OrderedDict

from loomwork.runtime.replayer import Replayer


class ReplayerCache:
    """Keeps recently used replayers so that a tick only needs the new events.

    Evicted replayers are closed. A size of zero disables caching: every tick replays
    the full history, which is slower but a useful way to shake out determinism bugs.
    """

    def __init__(self, max_size: int) -> None:
        self._max_size = max_size
        self._items: OrderedDict[str, Replayer] = OrderedDict()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def take(self, run_id: str) -> Replayer | None:
        """Remove and return the cached replayer for ``run_id``."""
        replayer = self._items.pop(run_id, None)
        if replayer is None:
            self.misses += 1
        else:
            self.hits += 1
        return replayer

    def put(self, run_id: str, replayer: Replayer) -> None:
        if self._max_size == 0:
            replayer.close()
            return
        previous = self._items.pop(run_id, None)
        if previous is not None and previous is not replayer:
            previous.close()
        self._items[run_id] = replayer
        while len(self._items) > self._max_size:
            _, evicted = self._items.popitem(last=False)
            evicted.close()
            self.evictions += 1

    def discard(self, run_id: str) -> None:
        replayer = self._items.pop(run_id, None)
        if replayer is not None:
            replayer.close()

    def clear(self) -> None:
        for replayer in self._items.values():
            replayer.close()
        self._items.clear()

    def __len__(self) -> int:
        return len(self._items)

    def __contains__(self, run_id: object) -> bool:
        return run_id in self._items
