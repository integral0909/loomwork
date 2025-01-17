"""Task queues, priorities, heartbeats, cancellation and worker limits."""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest

import loomwork as lw
from loomwork import ManualClock, MemoryStore, StepError
from loomwork.worker import WorkerOptions
from loomwork.worker.limits import QueueLimit
from tests.helpers import FAST_CONFIG, eventually

pytestmark = pytest.mark.integration

ran_on: dict[str, str] = {}
events: list[str] = []
FAST_RETRY = lw.RetryPolicy(initial_interval=0.01, max_attempts=3, jitter=0)


# --------------------------------------------------------------------------- definitions


@lw.step(name="tq.where", queue="gpu")
async def render(frame: int) -> str:
    worker = lw.step_info().run_id
    ran_on[f"frame-{frame}"] = worker
    return f"frame-{frame}"


@lw.step(name="tq.cpu")
async def plain(tag: str) -> str:
    events.append(tag)
    return tag


@lw.workflow(name="tq.pipeline")
async def pipeline() -> list[str]:
    frame = await render(1)
    note = await plain("after-render")
    return [frame, note]


@lw.step(name="tq.ordered", queue="ordered")
async def ordered(tag: str) -> str:
    events.append(tag)
    return tag


@lw.workflow(name="tq.priorities")
async def priorities() -> list[str]:
    calls = [ordered.with_options(priority=p)(f"p{p}") for p in (0, 5, -5, 10)]
    return list(await asyncio.gather(*calls))


@lw.step(name="tq.resumable", heartbeat_timeout=5, retry=FAST_RETRY)
async def resumable(total: int) -> list[int]:
    start = lw.heartbeat_details() or 0
    processed = []
    for item in range(start, total):
        processed.append(item)
        await lw.heartbeat(item + 1)
        if item == 2 and lw.step_info().attempt == 1:
            raise ConnectionError("lost connection mid-batch")
    return processed


@lw.workflow(name="tq.resume")
async def resume(total: int) -> list[int]:
    return await resumable(total)


@lw.step(name="tq.silent", heartbeat_timeout=0.1, timeout=30, retry=FAST_RETRY)
async def silent() -> str:
    if lw.step_info().attempt == 1:
        await asyncio.sleep(30)  # never heartbeats
    return "second attempt"


@lw.workflow(name="tq.silent_wf")
async def silent_wf() -> str:
    return await silent()


cancel_seen = asyncio.Event()


@lw.step(name="tq.long", heartbeat_timeout=5, timeout=60)
async def long_running() -> str:
    try:
        while True:
            await lw.heartbeat()
            await asyncio.sleep(0.01)
    except asyncio.CancelledError:
        cancel_seen.set()
        raise


@lw.workflow(name="tq.abandon")
async def abandon() -> str:
    try:
        async with asyncio.timeout(60):
            await long_running()
    except TimeoutError:
        return "gave up"
    return "finished"


@lw.step(name="tq.limited", queue="limited")
async def limited(i: int) -> float:
    started = time.monotonic()
    events.append(f"start-{i}")
    await asyncio.sleep(0.05)
    events.append(f"end-{i}")
    return started


@lw.workflow(name="tq.fan")
async def fan(n: int) -> list[float]:
    return list(await asyncio.gather(*(limited(i) for i in range(n))))


shutdown_started = asyncio.Event()


@lw.step(name="tq.slow_shutdown", timeout=30)
async def interrupted() -> int:
    if lw.step_info().attempt == 1 and not shutdown_started.is_set():
        shutdown_started.set()
        await asyncio.sleep(30)
    return lw.step_info().attempt


@lw.workflow(name="tq.shutdown")
async def shutdown_wf() -> int:
    return await interrupted()


def app_with(store: MemoryStore, clock: ManualClock, **options: Any) -> lw.Loomwork:
    return lw.Loomwork(
        store=store,
        workflows=[pipeline, priorities, resume, silent_wf, abandon, fan, shutdown_wf],
        steps=[render, plain, ordered, resumable, silent, long_running, limited, interrupted],
        clock=clock,
        config=FAST_CONFIG,
        worker_options=WorkerOptions(shutdown_grace=0.2, **options),
    )


# --------------------------------------------------------------------------- tests


@pytest.fixture(autouse=True)
def _fresh_events() -> None:
    # asyncio.Event binds to the first loop that waits on it; every test has its own.
    global cancel_seen, shutdown_started  # noqa: PLW0603
    cancel_seen = asyncio.Event()
    shutdown_started = asyncio.Event()


async def test_steps_are_routed_to_their_queue(clock: ManualClock) -> None:
    store = MemoryStore()
    general = app_with(store, clock, task_queues=("default",))
    async with general:
        handle = await general.start_workflow(pipeline)
        await eventually(lambda: store.job_count == 1)
        await asyncio.sleep(0.05)
        (job,) = await handle.pending_steps()
        assert job.queue == "gpu"
        assert job.attempt == 0  # nobody polls the gpu queue yet

        gpu = app_with(store, clock, task_queues=("gpu",), workflow_concurrency=0)
        async with gpu:
            assert await handle.result(timeout=5) == ["frame-1", "after-render"]
        assert gpu.worker.steps.steps_completed == 1
        assert general.worker.steps.steps_completed == 1


async def test_priorities_order_claims(clock: ManualClock) -> None:
    events.clear()
    store = MemoryStore()
    # Schedule everything before any worker polls the queue, then drain with one slot.
    producer = app_with(store, clock, task_queues=("default",))
    async with producer:
        handle = await producer.start_workflow(priorities)
        await eventually(lambda: store.job_count == 4)
    consumer = app_with(store, clock, task_queues=("ordered",), step_concurrency=1)
    async with consumer:
        assert sorted(await handle.result(timeout=5)) == ["p-5", "p0", "p10", "p5"]
    assert events == ["p10", "p5", "p0", "p-5"]


async def test_heartbeat_details_let_a_retry_resume(clock: ManualClock) -> None:
    async with app_with(MemoryStore(), clock) as app:
        handle = await app.start_workflow(resume, 6)
        await eventually(lambda: app.worker.steps.retries_scheduled == 1)
        await clock.advance(1)
        # Attempt 1 processed 0..2 and failed; attempt 2 resumes at item 3.
        assert await handle.result(timeout=5) == [3, 4, 5]


async def test_silent_attempt_hits_heartbeat_timeout(clock: ManualClock) -> None:
    async with app_with(MemoryStore(), clock) as app:
        handle = await app.start_workflow(silent_wf)
        await eventually(lambda: app.worker.steps.retries_scheduled == 1, timeout=5)
        await clock.advance(1)
        assert await handle.result(timeout=5) == "second attempt"


async def test_workflow_cancellation_reaches_running_step(clock: ManualClock) -> None:
    async with app_with(MemoryStore(), clock) as app:
        handle = await app.start_workflow(abandon)
        await eventually(lambda: app.worker.steps.attempts_started == 1)
        await eventually(lambda: app.store.timer_count == 1)  # type: ignore[attr-defined]
        await clock.advance(61)
        assert await handle.result(timeout=5) == "gave up"
        # Closing the run deleted the job; the attempt's next heartbeat finds its lease
        # gone and the attempt is cancelled instead of running forever.
        await asyncio.wait_for(cancel_seen.wait(), 5)
        assert app.store.job_count == 0  # type: ignore[attr-defined]
        assert app.worker.steps.steps_failed == 0
        assert app.worker.steps.steps_completed == 0


async def test_concurrency_limit(clock: ManualClock) -> None:
    events.clear()
    options = {
        "task_queues": ("default", "limited"),
        "queue_limits": {"limited": QueueLimit(concurrency=2)},
    }
    async with app_with(MemoryStore(), clock, **options) as app:
        await app.execute(fan, 6, timeout=10)
    running = 0
    peak = 0
    for event in events:
        running += 1 if event.startswith("start") else -1
        peak = max(peak, running)
    assert peak == 2


async def test_rate_limit(clock: ManualClock) -> None:
    events.clear()
    options = {
        "task_queues": ("default", "limited"),
        "queue_limits": {"limited": QueueLimit(rate=20, burst=1)},
    }
    async with app_with(MemoryStore(), clock, **options) as app:
        starts = await app.execute(fan, 5, timeout=10)
    spread = max(starts) - min(starts)
    # Five attempts at 20/s with no burst need at least four 50ms intervals.
    assert spread >= 0.18


async def test_shutdown_releases_attempt_without_counting_it(clock: ManualClock) -> None:
    store = MemoryStore()
    first = app_with(store, clock)
    async with first:
        handle = await first.start_workflow(shutdown_wf)
        await asyncio.wait_for(shutdown_started.wait(), 5)
    assert first.worker.steps.attempts_released == 1
    (job,) = await store.get_step_jobs(handle.run_id or "")
    assert job.lease_owner is None
    assert job.attempt == 0

    second = app_with(store, clock)
    async with second:
        # The released job is claimed again as attempt 1, not attempt 2.
        assert await handle.result(timeout=5) == 1


async def test_with_options_routes_one_call(clock: ManualClock) -> None:
    @lw.workflow(name="tq.override")
    async def override() -> str:
        try:
            return await plain.with_options(queue="nowhere", total_timeout=0.5)("x")
        except StepError as exc:
            return exc.message

    store = MemoryStore()
    app = lw.Loomwork(
        store=store,
        workflows=[override],
        steps=[plain],
        clock=clock,
        config=FAST_CONFIG,
        worker_options=WorkerOptions(shutdown_grace=0.2, task_queues=("default",)),
    )
    async with app:
        handle = await app.start_workflow(override)
        await eventually(lambda: store.job_count == 1)
        (job,) = await handle.pending_steps()
        assert job.queue == "nowhere"
        assert job.deadline is not None
        assert job.attempt == 0  # nobody polls "nowhere"
        await handle.terminate("test done")


def test_step_option_validation() -> None:
    with pytest.raises(ValueError, match="priority"):
        lw.StepOptions(priority=5000)
    with pytest.raises(ValueError, match="invalid queue"):
        lw.StepOptions(queue="has space")
    with pytest.raises(ValueError, match="heartbeat_timeout"):
        lw.StepOptions(heartbeat_timeout=timedelta(0))


async def test_heartbeat_outside_step() -> None:
    with pytest.raises(lw.NotInWorkflowError):
        await lw.heartbeat()
    with pytest.raises(lw.NotInWorkflowError):
        lw.heartbeat_details()


@lw.workflow(name="tq.abandon_and_wait")
async def abandon_and_wait() -> str:
    try:
        async with asyncio.timeout(60):
            await long_running()
    except TimeoutError:
        pass
    await lw.sleep(timedelta(hours=1))  # stays open, so the job is cancelled, not deleted
    return "done"


async def test_open_workflow_cancels_step_through_heartbeat(clock: ManualClock) -> None:
    store = MemoryStore()
    # Advancing the manual clock by a minute must not make the live attempt's lease look
    # expired, otherwise the store would treat the attempt as dead and delete the job.
    config = replace(FAST_CONFIG, step_lease_grace=timedelta(hours=1))
    app = lw.Loomwork(
        store=store,
        workflows=[abandon_and_wait],
        steps=[long_running],
        clock=clock,
        config=config,
        worker_options=WorkerOptions(shutdown_grace=0.2),
    )
    async with app:
        handle = await app.start_workflow(abandon_and_wait)
        await eventually(lambda: app.worker.steps.attempts_started == 1)
        await eventually(lambda: store.timer_count == 1)
        await clock.advance(61)
        await asyncio.wait_for(cancel_seen.wait(), 5)
        await eventually(lambda: app.worker.steps.attempts_released == 1)
        assert store.job_count == 0
        kinds = [e.kind for e in await handle.history()]
        assert "step_cancel_requested" in kinds
        assert "step_completed" not in kinds
        await clock.advance(3600)
        assert await handle.result(timeout=5) == "done"
