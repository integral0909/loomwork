"""Continue-as-new: bounded histories for long-lived workflows."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

import loomwork as lw
from loomwork import ExecutionStatus, ManualClock, MemoryStore
from loomwork.errors import ContinueAsNew
from loomwork.history.events import WorkflowContinuedAsNew
from loomwork.worker import WorkerOptions
from tests.helpers import FAST_CONFIG, eventually, make_instance

pytestmark = pytest.mark.integration


@lw.step(name="can.work")
async def work(item: int) -> int:
    return item * 2


@lw.workflow(name="can.counter")
async def counter(start: int, stop: int, total: int) -> int:
    # Process ten items per run, then hand over to a fresh run.
    batch_end = min(start + 10, stop)
    for item in range(start, batch_end):
        total += await work(item)
    if batch_end < stop:
        lw.continue_as_new(batch_end, stop, total)
    return total


@lw.workflow(name="can.stubborn")
async def stubborn() -> str:
    try:
        lw.continue_as_new()
    except Exception:  # proves ContinueAsNew is not an Exception
        return "swallowed"


@lw.workflow(name="can.parent")
async def parent() -> int:
    return await counter(0, 25, 0)


@lw.workflow(name="can.v1")
async def version_one(n: int) -> str:
    lw.continue_as_new(n + 1, workflow=version_two)


@lw.workflow(name="can.v2")
async def version_two(n: int) -> str:
    info = lw.info()
    return f"v2 got {n} from {info.continued_from_run_id is not None}"


@lw.workflow(name="can.suggested")
async def suggested(rounds: int) -> tuple[bool, int]:
    for _ in range(rounds):
        await work(1)
    return lw.continue_as_new_suggested(), lw.history_length()


def make(clock: ManualClock, store: MemoryStore, *, suggest_after: int = 10_000) -> lw.Loomwork:
    from dataclasses import replace

    return lw.Loomwork(
        store=store,
        workflows=[counter, stubborn, parent, version_one, version_two, suggested],
        steps=[work],
        clock=clock,
        config=replace(FAST_CONFIG, continue_as_new_after_events=suggest_after),
        worker_options=WorkerOptions(shutdown_grace=0.2),
    )


async def test_chain_of_runs_produces_one_result(clock: ManualClock) -> None:
    store = MemoryStore()
    async with make(clock, store) as app:
        handle = await app.start_workflow(counter, 0, 35, 0, id="counter-1")
        assert await handle.result(timeout=10) == sum(i * 2 for i in range(35))
        runs = await store.list_executions(workflow_name="can.counter")
        assert len(runs) == 4
        statuses = sorted(r.status.value for r in runs)
        assert statuses == ["completed", "continued_as_new", "continued_as_new", "continued_as_new"]
        assert {r.workflow_id for r in runs} == {"counter-1"}
        # Each run's history stays small.
        for run in runs:
            assert len(await store.load_history(run.run_id)) < 60
        first = next(r for r in runs if r.run_id == handle.run_id)
        last_event = (await store.load_history(first.run_id))[-1]
        assert isinstance(last_event, WorkflowContinuedAsNew)
        assert last_event.new_run_id == first.continued_run_id


async def test_continue_as_new_is_not_an_exception(clock: ManualClock) -> None:
    assert not issubclass(ContinueAsNew, Exception)
    inst, started = make_instance(stubborn)
    (command,) = inst.activate(started.timestamp, [started], replaying=False)
    assert type(command).__name__ == "ContinueAsNewCommand"


async def test_child_that_continues_as_new_reports_final_result(clock: ManualClock) -> None:
    store = MemoryStore()
    async with make(clock, store) as app:
        assert await app.execute(parent, timeout=10) == sum(i * 2 for i in range(25))


async def test_continue_as_new_into_another_workflow(clock: ManualClock) -> None:
    store = MemoryStore()
    async with make(clock, store) as app:
        handle = await app.start_workflow(version_one, 41)
        assert await handle.result(timeout=5) == "v2 got 42 from True"
        latest = await store.find_execution(handle.workflow_id or "")
        assert latest.workflow_name == "can.v2"


async def test_suggestion_threshold(clock: ManualClock) -> None:
    store = MemoryStore()
    async with make(clock, store, suggest_after=10) as app:
        small = await app.execute(suggested, 1, timeout=5)
        large = await app.execute(suggested, 6, timeout=5)
    assert small[0] is False
    assert large[0] is True
    assert large[1] >= 10


async def test_schedule_follows_continued_runs(clock: ManualClock) -> None:
    @lw.workflow(name="can.looping")
    async def looping(rounds: int) -> None:
        await lw.sleep(timedelta(minutes=30))
        if rounds:
            lw.continue_as_new(rounds - 1)

    store = MemoryStore()
    app = make(clock, store)
    app.register(looping)
    async with app:
        schedule = await app.client.schedules.create("looper", looping, 2, interval=3600)
        await clock.advance(timedelta(hours=1, seconds=1))
        await eventually(lambda: store.run_count == 1)
        # Two hand-overs later the chain is still running when the next hour fires,
        # so the SKIP policy must skip.
        for _ in range(2):
            await eventually(lambda: store.timer_count == 1)
            await clock.advance(timedelta(minutes=30))
            await asyncio.sleep(0.05)
        await clock.advance(timedelta(minutes=1))
        await eventually(lambda: store.timer_count == 1)

        async def skipped() -> bool:
            return (await schedule.describe()).total_skipped >= 1

        await clock.advance(timedelta(hours=1) - timedelta(minutes=31))
        await eventually(skipped)
        state = await schedule.describe()
        assert state.total_started == 1
        runs = await store.list_executions(workflow_name="can.looping")
        assert ExecutionStatus.CONTINUED_AS_NEW in {r.status for r in runs}
