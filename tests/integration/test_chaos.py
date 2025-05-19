"""Fault injection: workflows finish correctly despite a failing store and dying workers.

A proxy store makes a fraction of calls fail, either *before* the call reaches the store
or *after* it took effect (the nastiest case: a commit that succeeded but reported an
error, like a connection dropping while waiting for the reply). Meanwhile a worker is
killed and replaced mid-flight. Every workflow must still complete with the correct
result, every recorded history must replay cleanly, and every step must have run at
least once.
"""

from __future__ import annotations

import asyncio
import random
from collections import Counter
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest

import loomwork as lw
from loomwork.codec import default_converter
from loomwork.runtime import WorkflowInstance, replay_history
from loomwork.store.memory import MemoryStore
from loomwork.worker import WorkerOptions
from tests.helpers import FAST_CONFIG

pytestmark = [pytest.mark.integration, pytest.mark.slow]

#: Store methods whose failure the engine must tolerate. Reads used only by the test
#: harness itself (get_execution, load_history) are not disturbed.
FLAKY_METHODS = {
    "claim_tick",
    "complete_tick",
    "fail_tick",
    "claim_step",
    "finish_step",
    "retry_step",
    "renew_step_lease",
    "heartbeat_step",
    "release_step",
    "fire_due_timers",
    "relay_outbox",
    "next_step_ready_at",
    "next_timer_at",
}


class InjectedFault(ConnectionError):
    pass


class FlakyStore:
    """Delegates to a real store, failing a share of calls before or after they run."""

    def __init__(self, inner: MemoryStore, rate: float, seed: int) -> None:
        self._inner = inner
        self._rate = rate
        self._rng = random.Random(seed)
        self.faults: Counter[str] = Counter()

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if name not in FLAKY_METHODS:
            return attr

        async def flaky(*args: Any, **kwargs: Any) -> Any:
            roll = self._rng.random()
            if roll < self._rate / 2:
                self.faults[f"{name}:before"] += 1
                raise InjectedFault(f"{name} failed before running")
            result = await attr(*args, **kwargs)
            if roll < self._rate:
                self.faults[f"{name}:after"] += 1
                raise InjectedFault(f"{name} failed after running")
            return result

        return flaky


executions: Counter[str] = Counter()


@lw.step(name="chaos.work", retry=lw.RetryPolicy(initial_interval=0.01, max_attempts=0, jitter=0))
async def work(key: str) -> str:
    executions[key] += 1
    await asyncio.sleep(0.001)
    return key.upper()


@lw.workflow(name="chaos.flow")
async def flow(name: str) -> list[str]:
    first = await work(f"{name}.a")
    parallel = await asyncio.gather(work(f"{name}.b"), work(f"{name}.c"))
    await lw.sleep(timedelta(milliseconds=20))
    return [first, *parallel]


CONFIG = replace(
    FAST_CONFIG,
    tick_lease=timedelta(milliseconds=300),
    step_lease_grace=timedelta(milliseconds=300),
    default_step_timeout=timedelta(seconds=2),
    poll_interval=0.01,
)


def worker(store: Any, name: str) -> lw.Loomwork:
    return lw.Loomwork(
        store=store,
        workflows=[flow],
        steps=[work],
        config=CONFIG,
        worker_id=name,
        worker_options=WorkerOptions(workflow_concurrency=4, step_concurrency=8, shutdown_grace=0),
    )


@pytest.mark.parametrize("seed", [1, 2, 3])
async def test_workflows_survive_faults_and_worker_death(seed: int) -> None:
    executions.clear()
    inner = MemoryStore()
    store = FlakyStore(inner, rate=0.15, seed=seed)
    client = lw.Loomwork(store=inner, config=CONFIG, run_worker=False)
    names = [f"wf{seed}-{i}" for i in range(12)]

    async with client:
        handles = [await client.start_workflow(flow, name) for name in names]
        first = worker(store, "first")
        await first.start()
        await asyncio.sleep(0.15)
        # Kill the first worker abruptly (no grace) and replace it.
        await first.stop(grace=0)
        async with worker(store, "second"):
            results = await asyncio.wait_for(
                asyncio.gather(*(h.result() for h in handles)), timeout=30
            )

    for name, result in zip(names, results, strict=True):
        assert result == [f"{name}.A".upper(), f"{name}.B".upper(), f"{name}.C".upper()]
    # Every step ran at least once; faults may have caused re-execution.
    for name in names:
        for part in "abc":
            assert executions[f"{name}.{part}"] >= 1
    assert sum(store.faults.values()) > 0, "the test injected no faults"

    # Decisions were recorded exactly once: every history replays to its result.
    def factory(started: Any) -> WorkflowInstance:
        return WorkflowInstance(flow, started, converter=default_converter)

    for handle in handles:
        history = await inner.load_history(handle.run_id or "")
        replayer = replay_history(history, factory)
        assert replayer.is_closed
        replayer.close()
        scheduled = [e for e in history if e.kind == "step_scheduled"]
        assert len(scheduled) == 3
