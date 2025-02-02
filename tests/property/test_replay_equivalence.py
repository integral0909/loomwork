"""Property: any history produced live replays cleanly and reproduces the outcome.

Hypothesis generates small workflow *programs* (sequences of steps, parallel groups,
sleeps, timeouts and failure handling). Each program runs to completion on a real
worker; then its recorded history is replayed from scratch by a fresh instance, which
must neither raise :class:`NondeterminismError` nor reach a different result.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import loomwork as lw
from loomwork.codec import default_converter
from loomwork.errors import StepError
from loomwork.history.events import WorkflowCompleted
from loomwork.retry import RetryPolicy
from loomwork.runtime import WorkflowInstance, replay_history
from loomwork.worker import WorkerOptions
from tests.helpers import FAST_CONFIG, T0

pytestmark = pytest.mark.slow


@lw.step(retry=RetryPolicy(initial_interval=0.001, max_attempts=2, jitter=0))
async def compute(key: str, fail: bool) -> str:
    if fail:
        raise lw.ApplicationError(f"failed {key}", type="Planned", non_retryable=True)
    return key.upper()


async def _run_op(op: list[Any], index: int) -> Any:
    kind = op[0]
    if kind == "step":
        try:
            return await compute(f"s{index}", op[1])
        except StepError as exc:
            return f"err:{exc.__cause__.type if exc.__cause__ else '?'}"  # type: ignore[attr-defined]
    if kind == "parallel":
        return list(await asyncio.gather(*(compute(f"p{index}.{i}", False) for i in range(op[1]))))
    if kind == "sleep":
        await lw.sleep(timedelta(seconds=op[1]))
        return f"slept@{lw.now().isoformat()}"
    if kind == "timeout":
        try:
            async with asyncio.timeout(op[1]):
                return await compute(f"t{index}", False)
        except TimeoutError:
            return "timeout"
    if kind == "random":
        return lw.random().randint(0, 1000)
    if kind == "child":
        return await program([["step", False], ["random"]] * op[1])
    raise AssertionError(kind)


@lw.workflow
async def program(ops: list[list[Any]]) -> list[Any]:
    return [await _run_op(op, i) for i, op in enumerate(ops)]


OPS = st.one_of(
    st.tuples(st.just("step"), st.booleans()),
    st.tuples(st.just("parallel"), st.integers(1, 4)),
    st.tuples(st.just("sleep"), st.integers(1, 3600)),
    st.tuples(st.just("timeout"), st.integers(30, 600)),
    st.tuples(st.just("random")),
    st.tuples(st.just("child"), st.integers(1, 2)),
).map(list)


async def _execute(ops: list[list[Any]], cache_size: int) -> tuple[Any, list[Any]]:
    clock = lw.ManualClock(T0)
    app = lw.Loomwork(
        workflows=[program],
        steps=[compute],
        clock=clock,
        config=replace(FAST_CONFIG, replay_cache_size=cache_size),
        worker_options=WorkerOptions(shutdown_grace=0.5),
    )
    async with app:
        handle = await app.start_workflow(program, ops)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 20
        while not (await handle.describe()).is_closed:
            if loop.time() > deadline:
                raise AssertionError("program did not finish")
            # Timers only fire when the clock moves; step retries need it too.
            await clock.advance(timedelta(seconds=30))
            await asyncio.sleep(0.005)
        return await handle.result(timeout=1), await handle.history()


@pytest.fixture(autouse=True)
def _close_stray_loop() -> None:
    # pytest-asyncio installs a fresh (unclosed) loop after async tests; close it
    # before this synchronous test creates its own loops with asyncio.run().
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        policy = asyncio.get_event_loop_policy()
        try:
            loop = policy.get_event_loop()
        except RuntimeError:
            return
    if not loop.is_running():
        loop.close()


@settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(ops=st.lists(OPS, min_size=1, max_size=6), cache_size=st.sampled_from([0, 100]))
def test_live_history_replays_to_same_result(ops: list[list[Any]], cache_size: int) -> None:
    result, history = asyncio.run(_execute(ops, cache_size))

    def factory(started: Any) -> WorkflowInstance:
        return WorkflowInstance(program, started, converter=default_converter)

    replayer = replay_history(history, factory)
    assert replayer.is_closed
    replayer.close()
    closing = history[-1]
    assert isinstance(closing, WorkflowCompleted)
    assert default_converter.decode(closing.result) == result
    assert len(result) == len(ops)
