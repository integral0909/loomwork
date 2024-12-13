"""End-to-end behaviour of the embedded engine with the memory store."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pydantic
import pytest

import loomwork as lw
from loomwork import (
    ApplicationError,
    ExecutionStatus,
    ManualClock,
    MemoryStore,
    RetryPolicy,
    StepError,
    WorkflowAlreadyStartedError,
    WorkflowCanceledError,
    WorkflowFailedError,
    WorkflowTerminatedError,
)
from loomwork.errors import NotFoundError
from loomwork.history.events import StepCompleted, TickCompleted
from tests.conftest import AppFactory
from tests.helpers import FAST_CONFIG, eventually, kinds

pytestmark = pytest.mark.integration

FAST = RetryPolicy(initial_interval=0.001, max_interval=0.01, max_attempts=3, jitter=0)


class Order(pydantic.BaseModel):
    id: str
    items: list[str]
    total_cents: int


class Receipt(pydantic.BaseModel):
    order_id: str
    charge_id: str
    shipped: bool


# --------------------------------------------------------------------------- definitions

attempts: dict[str, int] = {}


@lw.step(name="orders.charge", retry=FAST)
async def charge(order_id: str, amount: int) -> str:
    attempts[order_id] = attempts.get(order_id, 0) + 1
    if order_id.startswith("flaky") and attempts[order_id] < 3:
        raise ConnectionError("gateway unavailable")
    if order_id.startswith("declined"):
        raise ApplicationError("card declined", type="CardDeclined", non_retryable=True)
    return f"ch_{order_id}_{amount}"


@lw.step(name="orders.ship")
def ship(order_id: str) -> bool:  # synchronous step, runs in a thread
    assert threading.current_thread() is not threading.main_thread()
    return True


@lw.step(name="orders.refund")
async def refund(charge_id: str) -> str:
    return f"refunded:{charge_id}"


@lw.workflow(name="orders.process")
async def process_order(order: Order) -> Receipt:
    charge_id = await charge(order.id, order.total_cents)
    shipped = await ship(order.id)
    return Receipt(order_id=order.id, charge_id=charge_id, shipped=shipped)


@lw.workflow
async def with_timer(minutes: int) -> str:
    start = lw.now()
    await lw.sleep(timedelta(minutes=minutes))
    return f"{(lw.now() - start).total_seconds():.0f}"


@lw.workflow
async def saga(order: Order) -> str:
    charge_id = await charge(order.id, order.total_cents)
    try:
        await lw.sleep(timedelta(days=1))
    except asyncio.CancelledError:
        return await refund(charge_id)
    return "kept"


@lw.step(name="slow", timeout=0.05, retry=RetryPolicy.no_retry())
async def slow() -> None:
    await asyncio.sleep(10)


@lw.workflow
async def calls_slow() -> str:
    try:
        await slow()
    except StepError as exc:
        return f"{exc.type}:{exc.__cause__.type if exc.__cause__ else ''}"  # type: ignore[attr-defined]
    return "no timeout"


@lw.step
async def whoami() -> dict[str, Any]:
    info = lw.step_info()
    return {"step": info.step_name, "attempt": info.attempt, "key": info.idempotency_key}


@lw.workflow
async def introspect() -> dict[str, Any]:
    result = await whoami()
    result["run_id"] = lw.info().run_id
    return result


@lw.workflow
async def fan_out(n: int) -> int:
    results = await asyncio.gather(*(charge(f"fan-{lw.info().run_id}-{i}", i) for i in range(n)))
    return len(results)


broken_code = {"enabled": True}


@lw.workflow
async def eventually_fixed() -> str:
    if broken_code["enabled"]:
        raise RuntimeError("bug in workflow code")
    return "fixed"


@lw.workflow
async def raw_uuid() -> str:
    return str(lw.uuid4())


def order(order_id: str) -> Order:
    return Order(id=order_id, items=["book"], total_cents=1999)


# --------------------------------------------------------------------------- tests


async def test_order_workflow_completes(make_app: AppFactory) -> None:
    app = await make_app([process_order], [charge, ship])
    receipt = await app.execute(process_order, order("o-1"), id="order-o-1", timeout=5)
    assert receipt == Receipt(order_id="o-1", charge_id="ch_o-1_1999", shipped=True)
    handle = app.get_handle("order-o-1")
    record = await handle.describe()
    assert record.status is ExecutionStatus.COMPLETED
    assert kinds(await handle.history()) == [
        "workflow_started",
        "tick_completed",
        "step_scheduled",
        "step_completed",
        "tick_completed",
        "step_scheduled",
        "step_completed",
        "tick_completed",
        "workflow_completed",
    ]


async def test_retries_are_transparent(make_app: AppFactory) -> None:
    app = await make_app([process_order], [charge, ship], use_clock=False)
    handle = await app.start_workflow(process_order, order("flaky-1"))
    receipt = await handle.result(timeout=5)
    assert receipt.charge_id == "ch_flaky-1_1999"
    assert attempts["flaky-1"] == 3
    completed = [e for e in await handle.history() if isinstance(e, StepCompleted)]
    assert completed[0].attempts == 3


async def test_non_retryable_failure_fails_workflow(make_app: AppFactory) -> None:
    app = await make_app([process_order], [charge, ship])
    with pytest.raises(WorkflowFailedError) as info:
        await app.execute(process_order, order("declined-1"), timeout=5)
    failure = info.value.failure
    assert failure.type == "StepError"
    assert failure.attributes["attempts"] == 1
    assert failure.root_cause().type == "CardDeclined"
    assert isinstance(info.value.__cause__, StepError)


async def test_durable_timer_with_manual_clock(make_app: AppFactory, clock: ManualClock) -> None:
    app = await make_app([with_timer])
    handle = await app.start_workflow(with_timer, 90)
    await eventually(lambda: app.store.timer_count == 1)  # type: ignore[attr-defined]
    await clock.advance(timedelta(minutes=89))
    await asyncio.sleep(0.05)
    assert (await handle.describe()).status is ExecutionStatus.RUNNING
    await clock.advance(timedelta(minutes=1))
    assert await handle.result(timeout=5) == "5400"


async def test_cancellation_runs_compensation(make_app: AppFactory) -> None:
    app = await make_app([saga], [charge, refund])
    handle = await app.start_workflow(saga, order("saga-1"))
    await eventually(lambda: app.store.timer_count == 1)  # type: ignore[attr-defined]
    assert await handle.cancel("customer changed their mind")
    assert await handle.result(timeout=5) == "refunded:ch_saga-1_1999"


async def test_uncaught_cancellation(make_app: AppFactory) -> None:
    app = await make_app([with_timer])
    handle = await app.start_workflow(with_timer, 5)
    await eventually(lambda: app.store.timer_count == 1)  # type: ignore[attr-defined]
    await handle.cancel()
    with pytest.raises(WorkflowCanceledError):
        await handle.result(timeout=5)
    assert (await handle.describe()).status is ExecutionStatus.CANCELED


async def test_terminate(make_app: AppFactory) -> None:
    app = await make_app([with_timer])
    handle = await app.start_workflow(with_timer, 5)
    await eventually(lambda: app.store.timer_count == 1)  # type: ignore[attr-defined]
    assert await handle.terminate("ops cleanup")
    with pytest.raises(WorkflowTerminatedError, match="ops cleanup"):
        await handle.result(timeout=5)
    assert not await handle.cancel()


async def test_step_attempt_timeout(make_app: AppFactory) -> None:
    app = await make_app([calls_slow], [slow], use_clock=False)
    assert await app.execute(calls_slow, timeout=5) == "StepError:StepTimeout"


async def test_step_info(make_app: AppFactory) -> None:
    app = await make_app([introspect], [whoami])
    result = await app.execute(introspect, timeout=5)
    assert result["step"] == "whoami"
    assert result["attempt"] == 1
    assert result["key"] == f"{result['run_id']}:1"


async def test_duplicate_workflow_id(make_app: AppFactory) -> None:
    app = await make_app([with_timer])
    await app.start_workflow(with_timer, 5, id="unique")
    with pytest.raises(WorkflowAlreadyStartedError):
        await app.start_workflow(with_timer, 5, id="unique")


async def test_unregistered_workflow_is_rejected(make_app: AppFactory) -> None:
    app = await make_app([with_timer])
    with pytest.raises(NotFoundError, match="not registered"):
        await app.start_workflow(process_order, order("x"))


async def test_many_concurrent_workflows(make_app: AppFactory) -> None:
    app = await make_app([fan_out], [charge], use_clock=False)
    handles = [await app.start_workflow(fan_out, 5) for _ in range(40)]
    results = await asyncio.gather(*(h.result(timeout=20) for h in handles))
    assert results == [5] * 40
    assert await app.client.list(status=ExecutionStatus.RUNNING) == []


async def test_tick_failures_are_retried_until_code_is_fixed(make_app: AppFactory) -> None:
    broken_code["enabled"] = True
    app = await make_app([eventually_fixed], use_clock=False)
    handle = await app.start_workflow(eventually_fixed)

    async def failed_a_few_times() -> bool:
        return (await handle.describe()).tick_failures >= 2

    await eventually(failed_a_few_times)
    record = await handle.describe()
    assert record.status is ExecutionStatus.RUNNING
    assert "bug in workflow code" in (record.last_tick_error or "")
    broken_code["enabled"] = False
    assert await handle.result(timeout=5) == "fixed"


async def test_resume_after_worker_restart(clock: ManualClock) -> None:
    """A new worker on the same store picks up a run mid-flight by replaying it."""
    store = MemoryStore()
    options = lw.WorkerOptions(shutdown_grace=0.5)
    first = lw.Loomwork(
        store=store,
        workflows=[saga],
        steps=[charge, refund],
        clock=clock,
        config=FAST_CONFIG,
        worker_options=options,
    )
    async with first:
        handle = await first.start_workflow(saga, order("restart-1"))
        await eventually(lambda: store.timer_count == 1)
    assert first.worker.ticks.ticks_completed >= 2

    second = lw.Loomwork(
        store=store,
        workflows=[saga],
        steps=[charge, refund],
        clock=clock,
        config=FAST_CONFIG,
        worker_options=options,
    )
    async with second:
        await clock.advance(timedelta(days=1))
        result = await second.get_handle(run_id=handle.run_id).result(timeout=5)
        assert result == "kept"
        # The new worker had no cached instance: it replayed the recorded ticks.
        assert second.worker.ticks.cache.misses >= 1


async def test_without_replay_cache_every_tick_replays(make_app: AppFactory) -> None:
    config = replace(FAST_CONFIG, replay_cache_size=0)
    app = await make_app([process_order], [charge, ship], config=config)
    receipt = await app.execute(process_order, order("nocache-1"), timeout=5)
    assert receipt.shipped
    assert app.worker.ticks.cache.hits == 0


async def test_randomness_is_stable_across_replays(make_app: AppFactory) -> None:
    config = replace(FAST_CONFIG, replay_cache_size=0)
    app = await make_app([raw_uuid], config=config)
    value = await app.execute(raw_uuid, timeout=5)
    assert len(value) == 36


async def test_ticks_share_workflow_time(make_app: AppFactory, clock: ManualClock) -> None:
    app = await make_app([with_timer])
    handle = await app.start_workflow(with_timer, 1)
    await eventually(lambda: app.store.timer_count == 1)  # type: ignore[attr-defined]
    await clock.advance(timedelta(minutes=5))
    await handle.result(timeout=5)
    ticks = [e for e in await handle.history() if isinstance(e, TickCompleted)]
    assert ticks[0].time <= ticks[1].time
    assert ticks[1].time >= ticks[0].time + timedelta(minutes=1)


async def test_client_only_app_does_not_execute(clock: ManualClock) -> None:
    store = MemoryStore()
    client_app = lw.Loomwork(store=store, clock=clock, run_worker=False)
    async with client_app:
        handle = await client_app.start_workflow(
            "orders.process", {"id": "x", "items": [], "total_cents": 1}
        )
        with pytest.raises(TimeoutError):
            await handle.result(timeout=0.1)
        assert (await handle.describe()).tick_pending
