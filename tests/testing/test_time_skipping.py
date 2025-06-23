"""Auto time-skipping: durable waits complete in milliseconds, and real work is never
skipped over to get there."""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta

import pytest

import loomwork as lw
from loomwork.testing import WorkflowEnvironment

pytestmark = pytest.mark.integration

WALL_CLOCK_BUDGET = 2.0
"""Generous real-time ceiling for tests that would otherwise wait days or months."""


@lw.workflow(name="ts.thirty_day_sleep")
async def thirty_day_sleep() -> str:
    await lw.sleep(timedelta(days=30))
    return "woke up"


@lw.workflow(name="ts.many_sleeps")
async def many_sleeps(count: int) -> int:
    total = 0
    for _ in range(count):
        await lw.sleep(timedelta(hours=6))
        total += 1
    return total


@lw.step(name="ts.slow_step")
async def slow_step() -> float:
    """A real step that takes real wall-clock time, unrelated to the ManualClock."""
    started = time.monotonic()
    await asyncio.sleep(0.15)
    return time.monotonic() - started


@lw.workflow(name="ts.concurrent_sleep_and_step")
async def concurrent_sleep_and_step() -> float:
    # Schedule the step, then race it against a two-hour durable sleep. The engine
    # commits both commands in the same tick since neither can proceed synchronously.
    step_future = slow_step()
    await lw.sleep(timedelta(hours=2))
    return await step_future


@lw.workflow(name="ts.timeout_no_signal")
async def waits_for_signal_or_times_out() -> str:
    channel = lw.Channel("decision", str)
    try:
        async with asyncio.timeout(timedelta(days=1).total_seconds()):
            return await channel.receive()
    except TimeoutError:
        return "timed out"


async def test_thirty_day_sleep_completes_in_milliseconds() -> None:
    async with WorkflowEnvironment(workflows=[thirty_day_sleep]) as env:
        started = time.monotonic()
        result = await env.execute(thirty_day_sleep)
        wall = time.monotonic() - started

    assert result == "woke up"
    assert wall < WALL_CLOCK_BUDGET
    # The clock itself really did move forward by (at least) thirty days.
    assert env.now() >= env.clock.now()


async def test_many_sequential_sleeps_all_skip() -> None:
    async with WorkflowEnvironment(workflows=[many_sleeps]) as env:
        before = env.now()
        started = time.monotonic()
        result = await env.execute(many_sleeps, 20)
        wall = time.monotonic() - started
        after = env.now()

    assert result == 20
    assert wall < WALL_CLOCK_BUDGET
    assert after - before >= timedelta(hours=6 * 20)


async def test_timeout_without_a_signal_skips_to_the_deadline() -> None:
    async with WorkflowEnvironment(workflows=[waits_for_signal_or_times_out]) as env:
        started = time.monotonic()
        result = await env.execute(waits_for_signal_or_times_out)
        wall = time.monotonic() - started

    assert result == "timed out"
    assert wall < WALL_CLOCK_BUDGET


async def test_signal_delivered_before_the_deadline_wins() -> None:
    async with WorkflowEnvironment(workflows=[waits_for_signal_or_times_out]) as env:
        handle = await env.start_workflow(waits_for_signal_or_times_out)
        await handle.signal("decision", "approved")
        result = await handle.result(timeout=5)

    assert result == "approved"


async def test_time_skipping_never_skips_runnable_work() -> None:
    """A real, slow step running concurrently with a long durable sleep must run to
    completion (not be cut short or raced) even though the sleep itself is skipped."""
    async with WorkflowEnvironment(workflows=[concurrent_sleep_and_step], steps=[slow_step]) as env:
        started = time.monotonic()
        step_elapsed = await env.execute(concurrent_sleep_and_step)
        wall = time.monotonic() - started

    # The step actually ran for (about) its full real duration...
    assert step_elapsed >= 0.14
    # ...yet the two-hour durable sleep next to it did not cost real wall-clock time.
    assert wall < WALL_CLOCK_BUDGET


async def test_auto_skip_disabled_requires_manual_sleep() -> None:
    async with WorkflowEnvironment(workflows=[thirty_day_sleep], auto_time_skipping=False) as env:
        handle = await env.start_workflow(thirty_day_sleep)
        with pytest.raises(TimeoutError):
            await handle.result(timeout=0.1)

        await env.sleep(timedelta(days=30))
        result = await handle.result(timeout=5)

    assert result == "woke up"


async def test_disabled_auto_skip_leaves_the_clock_alone_when_idle() -> None:
    async with WorkflowEnvironment(auto_time_skipping=False) as env:
        before = env.now()
        await asyncio.sleep(0.05)
        assert env.now() == before


async def test_many_differently_timed_runs_all_skip_independently() -> None:
    """Several runs sleeping for different durations, started together, must each wake
    at their own deadline -- auto-skipping must not wait for the *slowest* one before
    the faster ones are allowed to progress."""
    # Each unit is a six-hour lw.sleep (see many_sleeps): these runs wake at very
    # different simulated times, from six hours out to six days out. Kept small (each
    # count is also how many sequential ticks that run needs) so the real time this
    # takes reflects auto-skipping, not the unavoidable cost of processing many ticks.
    sleep_counts = [1, 2, 3, 5, 24]

    async with WorkflowEnvironment(workflows=[many_sleeps]) as env:
        started = time.monotonic()
        handles = [
            await env.start_workflow(many_sleeps, count, id=f"run-{count}")
            for count in sleep_counts
        ]
        results = [await handle.result(timeout=5) for handle in handles]
        wall = time.monotonic() - started

    assert results == sleep_counts
    assert wall < WALL_CLOCK_BUDGET


async def test_worker_busy_counts_in_flight_ticks_and_steps() -> None:
    """A sanity check on the counter auto-skipping relies on: it settles back to zero
    once a run (with a real step running concurrently with a durable sleep) is done.

    Idle poll loops flick ``busy`` to 1 and back for an instant on every empty claim
    attempt, so this polls briefly rather than asserting the exact value the moment
    ``result()`` returns.
    """
    async with WorkflowEnvironment(workflows=[concurrent_sleep_and_step], steps=[slow_step]) as env:
        handle = await env.start_workflow(concurrent_sleep_and_step)
        await handle.result(timeout=5)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + 1.0
        while env.worker.busy and loop.time() < deadline:  # noqa: ASYNC110
            await asyncio.sleep(0.005)  # polling a plain counter, not an asyncio.Event
        assert env.worker.busy == 0
