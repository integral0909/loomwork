"""Signals, queries, updates, child workflows and sagas, end to end."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pydantic
import pytest

import loomwork as lw
from loomwork import (
    ChildWorkflowError,
    ExecutionStatus,
    ManualClock,
    MemoryStore,
    UpdateFailedError,
    WorkflowClosedError,
)
from loomwork.errors import NotFoundError, PayloadError
from loomwork.history.events import SignalReceived
from loomwork.worker import WorkerOptions
from tests.helpers import FAST_CONFIG, eventually

pytestmark = pytest.mark.integration


class Approval(pydantic.BaseModel):
    approved: bool
    by: str


approvals = lw.Channel("approvals", Approval)
comments = lw.Channel("comments", str)
done_channel = lw.Channel("done", int)


# --------------------------------------------------------------------------- workflows


@lw.step(name="msg.notify")
async def notify(text: str) -> str:
    return f"notified:{text}"


@lw.workflow(name="msg.expense")
async def expense(amount: int) -> dict[str, Any]:
    stage = "waiting"
    notes: list[str] = []

    @lw.query("status")
    def status() -> dict[str, Any]:
        return {"stage": stage, "notes": list(notes), "pending": comments.pending}

    try:
        async with asyncio.timeout(timedelta(days=3).total_seconds()):
            decision = await approvals.receive()
    except TimeoutError:
        stage = "expired"
        return {"stage": stage}
    while comments.pending:
        notes.append(comments.receive_nowait())
    stage = "approved" if decision.approved else "rejected"
    await notify(f"{stage} by {decision.by}")
    return {"stage": stage, "by": decision.by, "notes": notes, "amount": amount}


@lw.workflow(name="msg.cart")
async def cart() -> list[str]:
    items: list[str] = []
    closed = False

    @lw.update("add")
    async def add(item: str) -> int:
        if closed:
            raise lw.ApplicationError("cart is closed", type="CartClosed")
        if not item:
            raise lw.ApplicationError("empty item", type="InvalidItem")
        await notify(f"added {item}")
        items.append(item)
        return len(items)

    @lw.update
    def checkout() -> list[str]:
        nonlocal closed
        closed = True
        return list(items)

    @lw.query
    def count() -> int:
        return len(items)

    while not closed:
        await lw.sleep(timedelta(minutes=1))
    return items


@lw.workflow(name="msg.child")
async def child(n: int) -> int:
    if n < 0:
        raise lw.ApplicationError("negative", type="Negative")
    await notify(f"child {n}")
    return n * n


@lw.workflow(name="msg.parent")
async def parent(values: list[int]) -> list[int]:
    handles = [lw.start_child(child, v) for v in values]
    return list(await asyncio.gather(*handles))


@lw.workflow(name="msg.parent_catches")
async def parent_catches() -> str:
    try:
        await child(-1)  # calling a workflow from a workflow runs it as a child
    except ChildWorkflowError as exc:
        return f"{exc.status}:{exc.__cause__.type if exc.__cause__ else ''}"  # type: ignore[attr-defined]
    return "no error"


@lw.workflow(name="msg.sleeper")
async def sleeper() -> str:
    await lw.sleep(timedelta(days=30))
    return "woke"


@lw.workflow(name="msg.short_parent")
async def short_parent(policy: str) -> str:
    handle = lw.start_child(sleeper, id=f"sleeper-{policy}", parent_close_policy=policy)
    await lw.sleep(1)
    return handle.run_id


@lw.workflow(name="msg.pinger")
async def pinger(target: str, count: int) -> int:
    for i in range(count):
        done_channel.send_to(target, i)
    return count


@lw.workflow(name="msg.collector")
async def collector(expected: int) -> list[int]:
    return [await done_channel.receive() for _ in range(expected)]


compensated: list[str] = []


@lw.step(name="msg.book")
async def book(what: str) -> str:
    if what == "car":
        raise lw.ApplicationError("no cars left", type="SoldOut", non_retryable=True)
    return f"booking:{what}"


@lw.step(name="msg.cancel_booking")
async def cancel_booking(booking: str) -> None:
    compensated.append(booking)


@lw.workflow(name="msg.trip")
async def trip(parts: list[str]) -> str:
    undo = lw.Compensations()
    try:
        for part in parts:
            booking = await book(part)
            undo.add(cancel_booking, booking)
    except lw.StepError:
        await undo.run()
        return "rolled back"
    return "booked"


WORKFLOWS: list[lw.WorkflowDefinition[..., Any]] = [
    expense,
    cart,
    child,
    parent,
    parent_catches,
    sleeper,
    short_parent,
    pinger,
    collector,
    trip,
]
STEPS: list[lw.StepDefinition[..., Any]] = [notify, book, cancel_booking]


def make(clock: ManualClock, store: MemoryStore | None = None) -> lw.Loomwork:
    return lw.Loomwork(
        store=store or MemoryStore(),
        workflows=WORKFLOWS,
        steps=STEPS,
        clock=clock,
        config=FAST_CONFIG,
        worker_options=WorkerOptions(shutdown_grace=0.2),
    )


# --------------------------------------------------------------------------- signals


async def test_signal_and_query(clock: ManualClock) -> None:
    async with make(clock) as app:
        handle = await app.start_workflow(expense, 120)
        await handle.signal(comments, "receipt attached")
        await handle.signal("comments", "urgent")

        async def stage() -> str:
            return str((await handle.query("status"))["stage"])

        await eventually(lambda: app.store.timer_count == 1)  # type: ignore[attr-defined]
        assert (await handle.query("status")) == {"stage": "waiting", "notes": [], "pending": 2}
        await handle.signal(approvals, {"approved": True, "by": "kim"})
        result = await handle.result(timeout=5)
        assert result == {
            "stage": "approved",
            "by": "kim",
            "notes": ["receipt attached", "urgent"],
            "amount": 120,
        }
        # Queries keep working after the run closed (state at completion).
        assert await stage() == "approved"


async def test_signal_validation_and_closed_runs(clock: ManualClock) -> None:
    async with make(clock) as app:
        handle = await app.start_workflow(expense, 1)
        with pytest.raises(PayloadError, match="approvals"):
            await handle.signal(approvals, {"approved": "maybe"})
        await handle.terminate()
        with pytest.raises(WorkflowClosedError):
            await handle.signal(comments, "too late")


async def test_duplicate_message_ids_are_delivered_once(clock: ManualClock) -> None:
    async with make(clock) as app:
        handle = await app.start_workflow(collector, 2)
        for value, message_id in [(1, "a"), (1, "a"), (2, "b")]:
            await handle.signal(done_channel, value, message_id=message_id)
        assert await handle.result(timeout=5) == [1, 2]


async def test_signal_timeout_is_durable(clock: ManualClock) -> None:
    async with make(clock) as app:
        handle = await app.start_workflow(expense, 5)
        await eventually(lambda: app.store.timer_count == 1)  # type: ignore[attr-defined]
        await clock.advance(timedelta(days=3))
        assert await handle.result(timeout=5) == {"stage": "expired"}


async def test_signal_with_start(clock: ManualClock) -> None:
    async with make(clock) as app:
        first = await app.client.signal_with_start(
            collector, 2, id="collect-1", channel=done_channel, value=10
        )
        second = await app.client.signal_with_start(
            collector, 2, id="collect-1", channel=done_channel, value=20
        )
        assert first.run_id == second.run_id
        assert await first.result(timeout=5) == [10, 20]
        history = await first.history()
        assert [e.kind for e in history[:2]] == ["workflow_started", "signal_received"]


async def test_workflows_signal_each_other(clock: ManualClock) -> None:
    async with make(clock) as app:
        target = await app.start_workflow(collector, 3, id="collector-x")
        sender = await app.start_workflow(pinger, "collector-x", 3)
        assert await sender.result(timeout=5) == 3
        assert await target.result(timeout=5) == [0, 1, 2]
        received = [e for e in await target.history() if isinstance(e, SignalReceived)]
        assert {e.sender for e in received} == {sender.workflow_id}


# --------------------------------------------------------------------------- queries / updates


async def test_updates(clock: ManualClock) -> None:
    async with make(clock) as app:
        handle = await app.start_workflow(cart)
        assert await handle.update("add", "book", timeout=5) == 1
        assert await handle.update("add", "pen", timeout=5) == 2
        assert await handle.query("count") == 2
        with pytest.raises(UpdateFailedError) as info:
            await handle.update("add", "", timeout=5)
        assert info.value.failure.type == "InvalidItem"
        # A failed update does not fail the workflow.
        assert (await handle.describe()).status is ExecutionStatus.RUNNING
        assert await handle.update("checkout", timeout=5) == ["book", "pen"]
        with pytest.raises(UpdateFailedError, match="cart is closed"):
            await handle.update("add", "late", timeout=5)
        await clock.advance(60)
        assert await handle.result(timeout=5) == ["book", "pen"]


async def test_update_with_same_id_runs_once(clock: ManualClock) -> None:
    async with make(clock) as app:
        handle = await app.start_workflow(cart)
        first = await handle.update("add", "x", update_id="u-1", timeout=5)
        again = await handle.update("add", "x", update_id="u-1", timeout=5)
        assert first == again == 1
        assert await handle.query("count") == 1


async def test_unknown_update_fails_when_workflow_closes(clock: ManualClock) -> None:
    async with make(clock) as app:
        handle = await app.start_workflow(collector, 1)
        waiter = asyncio.ensure_future(handle.update("nonexistent", timeout=5))
        await asyncio.sleep(0.1)
        await handle.signal(done_channel, 1)
        with pytest.raises(UpdateFailedError, match="without handling"):
            await waiter


async def test_unknown_query(clock: ManualClock) -> None:
    async with make(clock) as app:
        handle = await app.start_workflow(cart)
        await eventually(lambda: app.store.timer_count == 1)  # type: ignore[attr-defined]
        with pytest.raises(NotFoundError, match="no query 'nope'"):
            await handle.query("nope")


# --------------------------------------------------------------------------- children


async def test_child_workflows(clock: ManualClock) -> None:
    async with make(clock) as app:
        handle = await app.start_workflow(parent, [2, 3, 4], id="parent-1")
        assert await handle.result(timeout=10) == [4, 9, 16]
        children = await handle.children()
        assert [c.workflow_id for c in children] == [
            "parent-1/msg.child/1",
            "parent-1/msg.child/2",
            "parent-1/msg.child/3",
        ]
        assert {c.status for c in children} == {ExecutionStatus.COMPLETED}
        assert all(c.parent_run_id == handle.run_id for c in children)


async def test_child_failure_is_raised_in_parent(clock: ManualClock) -> None:
    async with make(clock) as app:
        assert await app.execute(parent_catches, timeout=10) == "failed:Negative"


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        ("terminate", ExecutionStatus.TERMINATED),
        ("request_cancel", ExecutionStatus.CANCELED),
        ("abandon", ExecutionStatus.RUNNING),
    ],
)
async def test_parent_close_policy(
    clock: ManualClock, policy: str, expected: ExecutionStatus
) -> None:
    async with make(clock) as app:
        handle = await app.start_workflow(short_parent, policy)
        await eventually(lambda: app.store.timer_count == 2)  # type: ignore[attr-defined]
        await clock.advance(1)
        child_run = await handle.result(timeout=5)
        child_handle = app.get_handle(run_id=child_run)

        async def child_settled() -> bool:
            return (await child_handle.describe()).status is expected

        await eventually(child_settled)
        assert await app.store.outbox_size() == 0


async def test_child_id_conflict(clock: ManualClock) -> None:
    @lw.workflow(name="msg.conflict")
    async def conflict() -> str:
        try:
            await lw.start_child(sleeper, id="taken")
        except ChildWorkflowError as exc:
            return exc.type
        return "started"

    app = make(clock)
    app.register(conflict)
    async with app:
        await app.start_workflow(sleeper, id="taken")
        assert await app.execute(conflict, timeout=5) == "ChildWorkflowAlreadyStarted"


# --------------------------------------------------------------------------- sagas


async def test_saga_rolls_back_in_reverse(clock: ManualClock) -> None:
    compensated.clear()
    async with make(clock) as app:
        assert await app.execute(trip, ["flight", "hotel", "car"], timeout=10) == "rolled back"
        assert compensated == ["booking:hotel", "booking:flight"]
        compensated.clear()
        assert await app.execute(trip, ["flight", "hotel"], timeout=10) == "booked"
        assert compensated == []


async def test_compensation_errors_are_grouped() -> None:
    calls: list[str] = []

    async def ok(tag: str) -> None:
        calls.append(tag)

    async def bad(tag: str) -> None:
        raise RuntimeError(tag)

    undo = lw.Compensations()
    undo.add(ok, "first")
    undo.add(bad, "second")
    undo.add(ok, "third")
    assert len(undo) == 3
    with pytest.raises(ExceptionGroup) as info:
        await undo.run()
    assert calls == ["third", "first"]
    assert len(info.value.exceptions) == 1
    assert len(undo) == 0

    parallel = lw.Compensations(parallel=True)
    parallel.add(ok, "a")
    parallel.add(ok, "b")
    await parallel.run()
    assert sorted(calls[-2:]) == ["a", "b"]


def test_channel_and_handler_names_are_validated() -> None:
    with pytest.raises(ValueError, match="invalid channel"):
        lw.Channel("bad name")
    assert "Approval" in repr(approvals)


async def test_messaging_apis_outside_workflows() -> None:
    with pytest.raises(lw.NotInWorkflowError):
        await comments.receive()
    with pytest.raises(lw.NotInWorkflowError):
        lw.signal_external("x", comments, "y")
    with pytest.raises(lw.NotInWorkflowError):
        lw.start_child(child, 1)
