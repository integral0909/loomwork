"""Killing, restarting, replacing workers and evicting replay-cache entries.

:class:`WorkerChaos` owns a pool of :class:`~loomwork.Worker` objects that all share one
store and registry -- exactly the pattern ``tests/integration/test_chaos.py`` uses by
hand for a single worker death, generalised to an ongoing, seeded schedule of worker
lifecycle events for as many workers as the simulation wants:

* **kill**: ``stop(grace=0)`` a running worker -- no grace period, so in-flight ticks and
  step attempts are abandoned exactly as an ungraceful process death would leave them
  (their leases simply expire later).
* **restart**: bring a killed worker back with the *same* worker id, as a redeployed
  process would.
* **replace**: bring a killed worker back with a *different* worker id, as a freshly
  provisioned node would.
* **evict_cache**: drop one run's cached :class:`~loomwork.runtime.replayer.Replayer`
  from a live worker's replay cache (``Worker.ticks.cache``, both public attributes),
  forcing that run's next tick to replay from history instead of resuming in memory --
  continuously exercising the same code path a cold worker uses for every run.

Every action is drawn from a :class:`random.Random` seeded once at construction. The
*cadence* of actions is paced in real wall-clock time (a seeded interval between
actions, not counted in event-loop turns): a turn count is not actually
backend-independent, since how many scheduler turns are "free" per real second depends
on how much of the work elsewhere is genuinely CPU-bound on this loop (:class:`MemoryStore`)
versus handed off to a background thread (SQLite, through ``aiosqlite``) -- and, worse,
turn-counted pacing accelerates precisely when the system is already struggling (more
retries scheduled with near-zero backoff leave more turns free), compounding a stall
instead of just observing it. See ``docs/simulation-testing.md`` for what this means for
reproducibility.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from loomwork.clock import Clock
from loomwork.engine.config import EngineConfig
from loomwork.sdk.definitions import Registry
from loomwork.store.base import Store
from loomwork.worker.worker import Worker, WorkerOptions

__all__ = ["ChaosEvent", "WorkerChaos"]


@dataclass(frozen=True, slots=True)
class ChaosEvent:
    """One chaos action the schedule actually took, kept for the simulation trace."""

    seq: int
    action: str  # "kill" | "restart" | "replace" | "evict_cache"
    worker_id: str
    detail: str = ""

    def __str__(self) -> str:
        suffix = f" ({self.detail})" if self.detail else ""
        return f"#{self.seq} {self.action} {self.worker_id}{suffix}"


class WorkerChaos:
    """A pool of workers against one store, with a seeded background chaos loop."""

    def __init__(
        self,
        store: Store,
        registry: Registry,
        clock: Clock,
        config: EngineConfig,
        *,
        seed: int,
        worker_options: WorkerOptions,
        num_workers: int,
        kill_rate: float,
        cache_evict_rate: float,
        run_ids: Callable[[], Sequence[str]],
        min_live_workers: int = 1,
        seconds_between_actions: tuple[float, float] = (0.005, 0.03),
    ) -> None:
        if num_workers < min_live_workers:
            raise ValueError("num_workers must be >= min_live_workers")
        self._store = store
        self._registry = registry
        self._clock = clock
        self._config = config
        self._rng = random.Random(seed)
        self._worker_options = worker_options
        self._kill_rate = kill_rate
        self._cache_evict_rate = cache_evict_rate
        self._run_ids = run_ids
        self._min_live = min_live_workers
        self._seconds_between_actions = seconds_between_actions
        self._graveyard: list[str] = []
        self._next_id = 0
        self._seq = 0
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self.events: list[ChaosEvent] = []
        self.workers: list[Worker] = [self._new_worker() for _ in range(num_workers)]

    def _new_worker(self, worker_id: str | None = None) -> Worker:
        if worker_id is None:
            worker_id = f"sim-worker-{self._next_id}"
            self._next_id += 1
        return Worker(
            self._store,
            self._registry,
            clock=self._clock,
            config=self._config,
            options=self._worker_options,
            worker_id=worker_id,
        )

    def _record(self, action: str, worker_id: str, detail: str = "") -> None:
        self._seq += 1
        self.events.append(ChaosEvent(self._seq, action, worker_id, detail))

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        await asyncio.gather(*(w.start() for w in self.workers))
        self._task = asyncio.ensure_future(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
        await asyncio.gather(*(w.stop(grace=0.2) for w in self.workers if w.running))

    # ------------------------------------------------------------------ the chaos loop

    async def _run(self) -> None:
        while not self._stop.is_set():
            interval = self._rng.uniform(*self._seconds_between_actions)
            try:
                async with asyncio.timeout(interval):
                    await self._stop.wait()
                return
            except TimeoutError:
                pass
            await self._act()

    async def _act(self) -> None:
        options: list[str] = []
        weights: list[float] = []
        if self._graveyard:
            options.append("heal")
            weights.append(3.0)
        if len(self.workers) > self._min_live and self._kill_rate > 0:
            options.append("kill")
            weights.append(self._kill_rate)
        if self._cache_evict_rate > 0 and self._run_ids():
            options.append("evict")
            weights.append(self._cache_evict_rate)
        if not options:
            return
        choice = self._rng.choices(options, weights=weights, k=1)[0]
        if choice == "heal":
            await self._heal()
        elif choice == "kill":
            await self._kill()
        else:
            self._evict_cache()

    async def _heal(self) -> None:
        worker_id = self._graveyard.pop(self._rng.randrange(len(self._graveyard)))
        replace = self._rng.random() < 0.5
        new_id = f"sim-worker-{self._next_id}" if replace else worker_id
        if replace:
            self._next_id += 1
        worker = self._new_worker(new_id)
        await worker.start()
        self.workers.append(worker)
        self._record("replace" if replace else "restart", new_id, detail=f"was {worker_id}")

    async def _kill(self) -> None:
        worker = self.workers.pop(self._rng.randrange(len(self.workers)))
        await worker.stop(grace=0)
        self._graveyard.append(worker.worker_id)
        self._record("kill", worker.worker_id)

    def _evict_cache(self) -> None:
        run_ids = self._run_ids()
        if not run_ids or not self.workers:
            return
        worker = self._rng.choice(self.workers)
        run_id = self._rng.choice(list(run_ids))
        worker.ticks.cache.discard(run_id)
        self._record("evict_cache", worker.worker_id, detail=run_id)
