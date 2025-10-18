"""Class-based workflows running on a real worker."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pydantic
import pytest

import loomwork as lw
from loomwork.codec import default_converter
from loomwork.history.events import WorkflowStarted
from loomwork.runtime import WorkflowInstance, replay_history
from loomwork.sdk.definitions import as_definition
from loomwork.testing import WorkflowEnvironment
from loomwork.testing.environment import TESTING_CONFIG

pytestmark = pytest.mark.integration


class Item(pydantic.BaseModel):
    sku: str
    quantity: int = 1


@lw.step(name="classes.reserve")
async def reserve(sku: str, quantity: int) -> str:
    return f"res-{sku}-{quantity}"


@lw.workflow(name="classes.cart")
class Cart:
    def __init__(self, customer: str) -> None:
        self.customer = customer
        self.items: list[Item] = []
        self.reservations: list[str] = []
        self.checked_out = False

    @lw.run
    async def run(self, customer: str) -> dict[str, object]:
        await lw.wait_condition(lambda: self.checked_out, timeout=timedelta(days=7))
        return {
            "customer": customer,
            "items": [item.sku for item in self.items],
            "reservations": self.reservations,
        }

    @lw.signal("add")
    async def add(self, item: Item) -> None:
        # An async handler: the reservation completes before the next message is handled.
        self.reservations.append(await reserve(item.sku, item.quantity))
        self.items.append(item)

    @lw.query
    def count(self) -> int:
        return len(self.items)

    @lw.query("owner")
    def owner_query(self) -> str:
        return self.customer

    @lw.update
    def checkout(self) -> int:
        if not self.items:
            raise lw.ApplicationError("cart is empty", type="EmptyCart")
        self.checked_out = True
        return len(self.items)


@lw.workflow(name="classes.no_init")
class Counter:
    total = 0

    @lw.run
    async def run(self, target: int) -> int:
        await lw.wait_condition(lambda: self.total >= target)
        return self.total

    @lw.signal
    def increment(self, by: int) -> None:
        self.total += by


@lw.workflow(name="classes.parent")
class Parent:
    @lw.run
    async def run(self, target: int) -> int:
        child = lw.start_child(Counter, target, id="child-counter")
        for _ in range(target):
            child.signal(Counter.increment, 1)
        total: int = await child
        return total


@lw.workflow(name="classes.timeout")
class Impatient:
    ready = False

    @lw.run
    async def run(self) -> str:
        try:
            await lw.wait_condition(lambda: self.ready, timeout=60)
        except TimeoutError:
            return "gave up"
        return "ready"

    @lw.signal
    def go(self, _: bool) -> None:
        self.ready = True


@lw.workflow(name="classes.functional_condition")
async def gate() -> list[str]:
    opened: list[str] = []

    @lw.update("open")
    def open_gate(by: str) -> int:
        opened.append(by)
        return len(opened)

    await lw.wait_condition(lambda: len(opened) >= 2)
    return opened


WORKFLOWS = [Cart, Counter, Parent, Impatient, gate]


def env(**kwargs: object) -> WorkflowEnvironment:
    return WorkflowEnvironment(workflows=WORKFLOWS, steps=[reserve], **kwargs)  # type: ignore[arg-type]


async def wait_until_registered(handle: lw.WorkflowHandle[Any]) -> None:
    while (await handle.describe()).tick_pending:  # noqa: ASYNC110
        await asyncio.sleep(0.005)


async def wait_for_count(handle: lw.WorkflowHandle[Any], count: int) -> None:
    """Async signal handlers finish later than the signal is sent (``add`` runs a step)."""
    while await handle.query(Cart.count) < count:  # noqa: ASYNC110
        await asyncio.sleep(0.005)


async def test_signals_queries_updates_and_conditions() -> None:
    async with env(auto_time_skipping=False) as e:
        handle = await e.start_workflow(Cart, "ada", id="cart-1")
        await wait_until_registered(handle)

        await handle.signal(Cart.add, Item(sku="book"))
        await handle.signal(Cart.add, {"sku": "pen", "quantity": 3})
        assert await handle.query(Cart.owner_query) == "ada"
        await wait_for_count(handle, 2)

        count = await handle.update(Cart.checkout, timeout=10)
        assert count == 2
        result = await handle.result(timeout=10)
    assert result == {
        "customer": "ada",
        "items": ["book", "pen"],
        "reservations": ["res-book-1", "res-pen-3"],
    }


async def test_query_on_a_closed_run_sees_final_state() -> None:
    async with env(auto_time_skipping=False) as e:
        handle = await e.start_workflow(Cart, "grace", id="cart-2")
        await wait_until_registered(handle)
        await handle.signal(Cart.add, Item(sku="lamp"))
        await wait_for_count(handle, 1)
        await handle.update(Cart.checkout, timeout=10)
        await handle.result(timeout=10)
        assert await handle.query(Cart.count) == 1


async def test_update_errors_go_to_the_caller_not_the_run() -> None:
    async with env(auto_time_skipping=False) as e:
        handle = await e.start_workflow(Cart, "lin", id="cart-3")
        await wait_until_registered(handle)
        with pytest.raises(lw.UpdateFailedError) as caught:
            await handle.update(Cart.checkout, timeout=10)
        assert caught.value.failure.type == "EmptyCart"
        assert not (await handle.describe()).is_closed
        await handle.signal(Cart.add, Item(sku="mug"))
        await wait_for_count(handle, 1)
        assert await handle.update(Cart.checkout, timeout=10) == 1
        assert (await handle.result(timeout=10))["items"] == ["mug"]


async def test_signals_sent_before_the_first_tick_are_buffered() -> None:
    async with env() as e:
        handle = await e.start_workflow(Counter, 5, id="counter")
        for _ in range(5):
            await handle.signal(Counter.increment, 1)
        assert await handle.result(timeout=10) == 5


async def test_invalid_signal_payload_is_rejected_by_the_sender() -> None:
    async with env(auto_time_skipping=False) as e:
        handle = await e.start_workflow(Cart, "x", id="cart-4")
        with pytest.raises(lw.LoomworkError):
            await handle.signal(Cart.add, {"quantity": "many"})


async def test_class_workflows_as_children() -> None:
    async with env() as e:
        assert await e.execute(Parent, 4, timeout=10) == 4


async def test_wait_condition_timeout_is_durable() -> None:
    async with env() as e:
        assert await e.execute(Impatient, timeout=10) == "gave up"
        history = await e.history(e.get_handle(None, run_id=(await e.list_executions())[0].run_id))
        assert "timer_started" in history.kinds()


async def test_history_replays_without_the_worker_cache() -> None:
    config = replace(TESTING_CONFIG, replay_cache_size=0)
    async with env(auto_time_skipping=False, config=config) as e:
        handle = await e.start_workflow(Cart, "ada", id="cart-5")
        await wait_until_registered(handle)
        await handle.signal(Cart.add, Item(sku="a"))
        await handle.signal(Cart.add, Item(sku="b"))
        await wait_for_count(handle, 2)
        await handle.update(Cart.checkout, timeout=10)
        result = await handle.result(timeout=10)
        history = await handle.history()
    assert result["items"] == ["a", "b"]

    def factory(started: WorkflowStarted) -> WorkflowInstance:
        return WorkflowInstance(as_definition(Cart), started, converter=default_converter)

    replayer = replay_history(history, factory)
    assert replayer.is_closed
    replayer.close()


async def test_wait_condition_in_a_function_style_workflow() -> None:
    async with env(auto_time_skipping=False) as e:
        handle = await e.start_workflow(gate, id="gate")
        await wait_until_registered(handle)
        assert await handle.update("open", "ada", timeout=10) == 1
        assert await handle.update("open", "lin", timeout=10) == 2
        assert await handle.result(timeout=10) == ["ada", "lin"]
