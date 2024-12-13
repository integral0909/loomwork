"""Tests for the deterministic event loop."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from loomwork.errors import DeterminismViolation
from loomwork.runtime.loop import DeterministicEventLoop


class RecordingBridge:
    def __init__(self) -> None:
        self.started: list[asyncio.TimerHandle] = []
        self.cancelled: list[asyncio.TimerHandle] = []

    def start_timer(self, handle: asyncio.TimerHandle) -> None:
        self.started.append(handle)

    def cancel_timer(self, handle: asyncio.TimerHandle) -> None:
        self.cancelled.append(handle)


@pytest.fixture
def bridge() -> RecordingBridge:
    return RecordingBridge()


@pytest.fixture
def loop(bridge: RecordingBridge) -> DeterministicEventLoop:
    return DeterministicEventLoop(bridge, start_time=1000.0)


def run(loop: DeterministicEventLoop, coro: Any) -> asyncio.Task[Any]:
    task = loop.create_task(coro)
    loop.run_until_idle()
    return task


def test_callbacks_run_in_fifo_order(loop: DeterministicEventLoop) -> None:
    order: list[int] = []
    for i in range(5):
        loop.call_soon(order.append, i)
    loop.run_until_idle()
    assert order == [0, 1, 2, 3, 4]
    assert not loop.has_ready


def test_cancelled_handles_are_skipped(loop: DeterministicEventLoop) -> None:
    order: list[int] = []
    loop.call_soon(order.append, 1)
    loop.call_soon(order.append, 2).cancel()
    loop.run_until_idle()
    assert order == [1]


def test_tasks_and_gather(loop: DeterministicEventLoop) -> None:
    async def double(x: int) -> int:
        await asyncio.sleep(0)
        return x * 2

    async def main() -> list[int]:
        return list(await asyncio.gather(*(double(i) for i in range(4))))

    task = run(loop, main())
    assert task.result() == [0, 2, 4, 6]


def test_task_names_are_deterministic(loop: DeterministicEventLoop) -> None:
    async def noop() -> None:
        return None

    names = [loop.create_task(noop()).get_name() for _ in range(3)]
    loop.run_until_idle()
    assert names == ["workflow-task-1", "workflow-task-2", "workflow-task-3"]


def test_running_loop_is_installed_only_while_running(loop: DeterministicEventLoop) -> None:
    seen: list[Any] = []

    async def probe() -> None:
        seen.append(asyncio.get_running_loop())

    run(loop, probe())
    assert seen == [loop]
    assert asyncio.events._get_running_loop() is None


async def test_restores_outer_running_loop(loop: DeterministicEventLoop) -> None:
    outer = asyncio.get_running_loop()
    run(loop, asyncio.sleep(0))
    assert asyncio.get_running_loop() is outer


def test_time_is_workflow_time(loop: DeterministicEventLoop) -> None:
    assert loop.time() == 1000.0
    loop.set_time(1005.5)
    assert loop.time() == 1005.5
    with pytest.raises(ValueError, match="backwards"):
        loop.set_time(1)


def test_sleep_becomes_a_timer(loop: DeterministicEventLoop, bridge: RecordingBridge) -> None:
    async def sleeper() -> str:
        await asyncio.sleep(30)
        return "woke"

    task = run(loop, sleeper())
    assert not task.done()
    assert len(bridge.started) == 1
    handle = bridge.started[0]
    assert handle.when() == 1030.0

    loop.set_time(1030.0)
    loop.fire_timer(handle)
    loop.run_until_idle()
    assert task.result() == "woke"


def test_zero_delay_does_not_create_a_timer(
    loop: DeterministicEventLoop, bridge: RecordingBridge
) -> None:
    fired: list[bool] = []
    loop.call_later(0, fired.append, True)
    loop.call_at(999.0, fired.append, True)
    loop.run_until_idle()
    assert fired == [True, True]
    assert bridge.started == []


def test_timeout_starts_and_cancels_a_timer(
    loop: DeterministicEventLoop, bridge: RecordingBridge
) -> None:
    async def guarded() -> int:
        async with asyncio.timeout(10):
            await asyncio.sleep(0)
            return 1

    task = run(loop, guarded())
    assert task.result() == 1
    assert len(bridge.started) == 1
    assert bridge.cancelled == bridge.started


def test_timeout_expiry(loop: DeterministicEventLoop, bridge: RecordingBridge) -> None:
    never: asyncio.Future[None] = loop.create_future()

    async def guarded() -> str:
        try:
            async with asyncio.timeout(10):
                await never
        except TimeoutError:
            return "timed out"
        return "no"

    task = run(loop, guarded())
    loop.set_time(1010.0)
    loop.fire_timer(bridge.started[0])
    loop.run_until_idle()
    assert task.result() == "timed out"
    # The fired timer must not be reported as cancelled when the scope exits.
    assert bridge.cancelled == []


def test_cancelling_a_fired_timer_is_not_reported(
    loop: DeterministicEventLoop, bridge: RecordingBridge
) -> None:
    handle = loop.call_later(5, lambda: None)
    loop.fire_timer(handle)
    handle.cancel()
    loop.run_until_idle()
    assert bridge.cancelled == []


def test_fire_timer_ignores_cancelled_handles(loop: DeterministicEventLoop) -> None:
    fired: list[bool] = []
    handle = loop.call_later(5, fired.append, True)
    handle.cancel()
    loop.fire_timer(handle)
    loop.run_until_idle()
    assert fired == []


def test_wait_for(loop: DeterministicEventLoop, bridge: RecordingBridge) -> None:
    fut: asyncio.Future[int] = loop.create_future()

    async def waiter() -> int:
        return await asyncio.wait_for(fut, timeout=60)

    task = run(loop, waiter())
    fut.set_result(7)
    loop.run_until_idle()
    assert task.result() == 7
    assert len(bridge.cancelled) == 1


def test_asyncio_primitives_work(loop: DeterministicEventLoop) -> None:
    async def main() -> list[int]:
        queue: asyncio.Queue[int] = asyncio.Queue()
        event = asyncio.Event()
        lock = asyncio.Lock()
        out: list[int] = []

        async def producer() -> None:
            for i in range(3):
                await queue.put(i)
            event.set()

        async def consumer() -> None:
            await event.wait()
            async with lock:
                while not queue.empty():
                    out.append(await queue.get())

        async with asyncio.TaskGroup() as group:
            group.create_task(consumer())
            group.create_task(producer())
        return out

    assert run(loop, main()).result() == [0, 1, 2]


@pytest.mark.parametrize(
    "operation",
    [
        lambda: asyncio.to_thread(lambda: None),
        lambda: asyncio.get_running_loop().run_in_executor(None, lambda: None),
        lambda: asyncio.get_running_loop().getaddrinfo("localhost", 80),
        lambda: asyncio.open_connection("localhost", 80),
        lambda: asyncio.create_subprocess_exec("ls"),
        lambda: asyncio.get_running_loop().sock_recv(None, 1),  # type: ignore[arg-type]
    ],
)
def test_io_is_forbidden_even_if_caught(loop: DeterministicEventLoop, operation: Any) -> None:
    async def sneaky() -> str:
        try:
            await operation()
        except DeterminismViolation:
            return "swallowed"
        return "ran"

    with pytest.raises(DeterminismViolation, match="not allowed in workflow code"):
        run(loop, sneaky())


def test_call_soon_threadsafe_is_forbidden(loop: DeterministicEventLoop) -> None:
    with pytest.raises(DeterminismViolation):
        loop.call_soon_threadsafe(lambda: None)


def test_run_forever_is_rejected(loop: DeterministicEventLoop) -> None:
    with pytest.raises(RuntimeError, match="driven by the executor"):
        loop.run_forever()
    coro = asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="driven by the executor"):
        loop.run_until_complete(coro)
    coro.close()


def test_unhandled_task_errors_are_recorded(loop: DeterministicEventLoop) -> None:
    async def fails() -> None:
        raise ValueError("background")

    loop.call_exception_handler({"message": "boom", "exception": ValueError("x")})
    task = loop.create_task(fails())
    loop.run_until_idle()
    assert isinstance(task.exception(), ValueError)
    assert loop.unhandled_errors[0]["message"] == "boom"


def test_custom_exception_handler(loop: DeterministicEventLoop) -> None:
    seen: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _loop, ctx: seen.append(ctx))
    assert loop.get_exception_handler() is not None
    loop.call_exception_handler({"message": "m"})
    assert seen == [{"message": "m"}]


def test_closed_loop(loop: DeterministicEventLoop) -> None:
    loop.close()
    assert loop.is_closed()
    with pytest.raises(RuntimeError, match="closed"):
        loop.call_soon(lambda: None)
    with pytest.raises(RuntimeError, match="closed"):
        loop.run_until_idle()


def test_not_reentrant(loop: DeterministicEventLoop) -> None:
    errors: list[BaseException] = []

    def reenter() -> None:
        try:
            loop.run_until_idle()
        except RuntimeError as exc:
            errors.append(exc)

    loop.call_soon(reenter)
    loop.run_until_idle()
    assert "not re-entrant" in str(errors[0])


def test_task_factory(loop: DeterministicEventLoop) -> None:
    created: list[str] = []

    def factory(lp: asyncio.AbstractEventLoop, coro: Any, **kwargs: Any) -> asyncio.Task[Any]:
        created.append("x")
        return asyncio.Task(coro, loop=lp, **kwargs)

    loop.set_task_factory(factory)
    assert loop.get_task_factory() is factory
    task = run(loop, asyncio.sleep(0, "done"))
    assert task.result() == "done"
    assert created == ["x"]
    assert task.get_name() == "workflow-task-1"
