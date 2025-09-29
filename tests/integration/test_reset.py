"""Reset: recovering a run broken by a bug, exercised with real workers.

The workflow and step below share a module-level flag: "the bug" is whatever
``BUGGY["charge"]`` says it is when a step or workflow call runs, which lets a single
process stand in for "deploy, hit a bug, fix it, redeploy" without actually reloading
code -- exactly what an operator's reset is meant to recover from.
"""

from __future__ import annotations

import pytest

import loomwork as lw
from loomwork import ExecutionStatus, ManualClock
from tests.conftest import AppFactory

pytestmark = pytest.mark.integration

BUGGY = {"charge": True}


@lw.step(name="reset.charge", retry=lw.RetryPolicy(max_attempts=1))
async def charge(order_id: str) -> str:
    if BUGGY["charge"]:
        raise lw.ApplicationError(f"card declined for {order_id}: bug in charge_card")
    return f"charged:{order_id}"


@lw.workflow(name="reset.process_order")
async def process_order(order_id: str) -> str:
    return await charge(order_id)


@lw.workflow(name="reset.signals")
async def waits_for_a_signal(order_id: str) -> str:
    cancel = lw.Channel("cancel", str)
    reason = await cancel.receive()
    if BUGGY["charge"]:
        raise lw.ApplicationError("bug: forgot to refund before returning")
    return f"cancelled:{order_id}:{reason}"


WORKFLOWS = [process_order, waits_for_a_signal]
STEPS = [charge]


@pytest.fixture(autouse=True)
def _reset_bug_flag() -> None:
    BUGGY["charge"] = True


async def test_reset_recovers_a_run_broken_by_a_step_bug(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app(WORKFLOWS, STEPS)
    handle = await app.start_workflow(process_order, "A-1", id="order-A-1")
    with pytest.raises(lw.WorkflowFailedError):
        await handle.result(timeout=5)

    # "Deploy the fix" and recover the run from before the bad tick.
    BUGGY["charge"] = False
    new_handle = await handle.reset(
        to=lw.ResetPoint.before_last_failure(), reason="fixed the charge_card bug"
    )
    assert await new_handle.result(timeout=5) == "charged:A-1"

    old_record = await handle.describe()
    assert old_record.status is ExecutionStatus.FAILED
    assert old_record.continued_run_id == new_handle.run_id

    new_record = await new_handle.describe()
    assert new_record.reset_from_run_id == handle.run_id
    assert new_record.workflow_id == handle.workflow_id


async def test_reset_result_follows_the_chain_from_the_old_handle(
    make_app: AppFactory, clock: ManualClock
) -> None:
    """A handle obtained before the reset transparently sees the new run's outcome,
    exactly like continue-as-new or an automatic retry: the whole point of reusing
    ``continued_run_id`` (see docs/operations/recovery.md)."""
    app = await make_app(WORKFLOWS, STEPS)
    handle = await app.start_workflow(process_order, "A-2", id="order-A-2")
    with pytest.raises(lw.WorkflowFailedError):
        await handle.result(timeout=5)

    BUGGY["charge"] = False
    await handle.reset(to=lw.ResetPoint.before_last_failure(), reason="fixed")
    assert await handle.result(timeout=5) == "charged:A-2"


async def test_reset_first_tick_redoes_the_whole_run(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app(WORKFLOWS, STEPS)
    handle = await app.start_workflow(process_order, "A-3", id="order-A-3")
    with pytest.raises(lw.WorkflowFailedError):
        await handle.result(timeout=5)

    BUGGY["charge"] = False
    new_handle = await handle.reset(to=lw.ResetPoint.first_tick(), reason="start over")
    assert await new_handle.result(timeout=5) == "charged:A-3"
    history = await new_handle.history()
    # Only the original WorkflowStarted survived the reset; everything else is new.
    assert history[0].kind == "workflow_started"


async def test_reset_redelivers_a_signal_sent_after_the_reset_point(
    make_app: AppFactory, clock: ManualClock
) -> None:
    """A signal is not reproducible by redoing a decision, so unlike a step's outcome it
    must survive a reset even when it arrived after the point being reset to."""
    app = await make_app(WORKFLOWS, STEPS)
    handle = await app.start_workflow(waits_for_a_signal, "A-4", id="order-A-4")
    await handle.signal("cancel", "customer request")
    with pytest.raises(lw.WorkflowFailedError):
        await handle.result(timeout=5)

    BUGGY["charge"] = False
    # Reset to the first tick: the signal was consumed by the (now dropped) failing
    # tick, so without redelivery it would simply vanish.
    new_handle = await handle.reset(to=lw.ResetPoint.first_tick(), reason="fixed the refund bug")
    assert await new_handle.result(timeout=5) == "cancelled:A-4:customer request"


async def test_reset_terminates_a_still_running_run(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app(WORKFLOWS, STEPS)
    BUGGY["charge"] = False
    handle = await app.start_workflow(waits_for_a_signal, "A-5", id="order-A-5")
    # Never signalled: the run is simply still running when it gets reset.
    new_handle = await handle.reset(to=lw.ResetPoint.first_tick(), reason="operator cleanup")
    old_record = await handle.describe()
    assert old_record.status is ExecutionStatus.TERMINATED
    assert old_record.close_reason == "operator cleanup"
    assert old_record.continued_run_id == new_handle.run_id

    await new_handle.signal("cancel", "reason")
    assert await new_handle.result(timeout=5) == "cancelled:A-5:reason"


async def test_reset_invalid_point_raises(make_app: AppFactory, clock: ManualClock) -> None:
    app = await make_app(WORKFLOWS, STEPS)
    handle = await app.start_workflow(process_order, "A-6", id="order-A-6")
    with pytest.raises(lw.WorkflowFailedError):
        await handle.result(timeout=5)
    with pytest.raises(lw.InvalidResetPointError):
        await handle.reset(to=lw.ResetPoint.after_tick(50), reason="")
