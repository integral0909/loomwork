"""Schedules, child workflows and continue-as-new, all under auto time-skipping."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta

import pytest

import loomwork as lw
from loomwork.testing import WorkflowEnvironment

pytestmark = pytest.mark.integration

WALL_CLOCK_BUDGET = 3.0


@lw.workflow(name="sched.report")
async def daily_report(region: str) -> str:
    return f"report for {region}"


@lw.workflow(name="child.grandchild")
async def grandchild(n: int) -> int:
    await lw.sleep(timedelta(hours=1))
    return n + 1


@lw.workflow(name="child.parent")
async def parent_workflow(n: int) -> int:
    return await grandchild(n)


@lw.step(name="can.work")
async def work(item: int) -> int:
    return item * 2


@lw.workflow(name="can.counter")
async def counter(start: int, stop: int, total: int) -> int:
    batch_end = min(start + 5, stop)
    for item in range(start, batch_end):
        total += await work(item)
        await lw.sleep(timedelta(minutes=10))
    if batch_end < stop:
        lw.continue_as_new(batch_end, stop, total)
    return total


async def _wait_for_fire_count(handle: object, minimum: int, *, timeout: float) -> None:
    """Wait (real time) for a schedule's fire count to reach ``minimum``.

    Auto time-skipping advances the environment's *simulated* clock on its own; this
    only bounds how long the test itself is willing to wait in *real* wall-clock time
    for that background progress to show up.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        state = await handle.describe()  # type: ignore[attr-defined]
        if state.total_started >= minimum:
            return
        if loop.time() > deadline:
            raise AssertionError(
                f"schedule only started {state.total_started} run(s) within {timeout}s"
            )
        await asyncio.sleep(0.005)


async def test_cron_schedule_fires_repeatedly_under_time_skipping() -> None:
    async with WorkflowEnvironment(
        workflows=[daily_report], start_time=datetime(2024, 1, 1, tzinfo=UTC)
    ) as env:
        started = time.monotonic()
        handle = await env.client.schedules.create(
            "daily-emea", daily_report, "emea", cron="0 6 * * *", timezone="UTC"
        )
        await _wait_for_fire_count(handle, 5, timeout=WALL_CLOCK_BUDGET)
        wall = time.monotonic() - started

    assert wall < WALL_CLOCK_BUDGET
    assert env.now() >= datetime(2024, 1, 5, 6, tzinfo=UTC)


async def test_child_workflow_completes_through_a_skipped_sleep() -> None:
    async with WorkflowEnvironment(workflows=[parent_workflow, grandchild]) as env:
        started = time.monotonic()
        result = await env.execute(parent_workflow, 41)
        wall = time.monotonic() - started

    assert result == 42
    assert wall < WALL_CLOCK_BUDGET


async def test_continue_as_new_chain_completes_through_skipped_sleeps() -> None:
    async with WorkflowEnvironment(workflows=[counter], steps=[work]) as env:
        started = time.monotonic()
        # ``handle`` keeps referring to the *first* run of the chain even after
        # .result() follows every continue-as-new hand-over to get the final value.
        handle = await env.start_workflow(counter, 0, 12, 0)
        result = await handle.result(timeout=5)
        wall = time.monotonic() - started

    assert result == sum(item * 2 for item in range(12))
    assert wall < WALL_CLOCK_BUDGET

    first_run_history = await env.history(handle)
    assert "workflow_continued_as_new" in first_run_history.kinds()
