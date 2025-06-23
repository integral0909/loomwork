"""Heartbeats, child workflow parent-close policies and sagas inside a
``WorkflowEnvironment`` -- feature coverage beyond the core time-skipping and mocking
behaviour, to make sure nothing about running inside the testkit is second-class."""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta

import pytest

import loomwork as lw
from loomwork import ExecutionStatus
from loomwork.testing import WorkflowEnvironment

pytestmark = pytest.mark.integration

WALL_CLOCK_BUDGET = 2.0


@lw.step(name="adv.heartbeating", heartbeat_timeout=timedelta(seconds=1))
async def heartbeating_step(rounds: int) -> int:
    for i in range(rounds):
        await lw.heartbeat({"round": i})
        await asyncio.sleep(0.02)
    return rounds


@lw.workflow(name="adv.heartbeat_wf")
async def heartbeat_workflow(rounds: int) -> int:
    return await heartbeating_step(rounds)


@lw.workflow(name="adv.sleepy_child")
async def sleepy_child() -> str:
    await lw.sleep(timedelta(days=1))
    return "never gets here"  # pragma: no cover - terminated before this returns


@lw.workflow(name="adv.parent_terminates_child")
async def parent_terminates_child() -> str:
    lw.start_child(sleepy_child, parent_close_policy="terminate")
    return "parent done"


@lw.step(name="adv.charge")
async def charge(order_id: str) -> str:
    return f"ch_{order_id}"


@lw.step(name="adv.refund")
async def refund(charge_id: str) -> None:
    return None


@lw.step(name="adv.reserve")
async def reserve(order_id: str) -> None:
    raise lw.ApplicationError("out of stock", type="OutOfStock", non_retryable=True)


@lw.workflow(name="adv.saga")
async def saga(order_id: str) -> str:
    undo = lw.Compensations()
    charge_id = await charge(order_id)
    undo.add(refund, charge_id)
    try:
        await reserve(order_id)
    except Exception:
        await undo.run()
        raise
    return "reserved"  # pragma: no cover - reserve() always fails in this test


async def test_heartbeating_step_runs_to_completion() -> None:
    async with WorkflowEnvironment(
        workflows=[heartbeat_workflow], steps=[heartbeating_step]
    ) as env:
        started = time.monotonic()
        result = await env.execute(heartbeat_workflow, 5)
        wall = time.monotonic() - started

    assert result == 5
    assert wall < WALL_CLOCK_BUDGET


async def test_parent_close_policy_terminates_child_through_the_outbox() -> None:
    async with WorkflowEnvironment(workflows=[parent_terminates_child, sleepy_child]) as env:
        handle = await env.start_workflow(parent_terminates_child)
        result = await handle.result(timeout=5)
        assert result == "parent done"

        children = await handle.children()
        assert len(children) == 1
        child_run_id = children[0].run_id

        # The termination is delivered by the outbox relay, asynchronously with the
        # parent's own close; give it a brief, bounded moment to land.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + WALL_CLOCK_BUDGET
        while True:
            child = await env.store.get_execution(child_run_id)
            if child.is_closed:
                break
            if loop.time() > deadline:
                raise AssertionError("child was never terminated")
            await asyncio.sleep(0.005)

    assert child.status is ExecutionStatus.TERMINATED


async def test_saga_rolls_back_through_a_mocked_failure() -> None:
    async with WorkflowEnvironment(workflows=[saga], steps=[charge, refund, reserve]) as env:
        refund_mock = env.mock_step(refund, return_value=None)
        with pytest.raises(lw.WorkflowFailedError) as excinfo:
            await env.execute(saga, "order-1")

    refund_mock.assert_called_once()
    refund_mock.assert_called_with("ch_order-1")
    # The real (unmocked) reserve() step is what ultimately failed the run.
    assert excinfo.value.failure.root_cause().type == "OutOfStock"
