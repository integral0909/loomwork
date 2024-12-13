"""Shared helpers for tests."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from loomwork.clock import ManualClock
from loomwork.codec import default_converter
from loomwork.engine.config import EngineConfig
from loomwork.history.events import (
    Event,
    StepCompleted,
    StepFailed,
    TickCompleted,
    TimerFired,
    WorkflowStarted,
)
from loomwork.retry import RetryPolicy
from loomwork.runtime.instance import WorkflowInstance
from loomwork.sdk.definitions import WorkflowDefinition

T0 = datetime(2024, 1, 1, tzinfo=UTC)

FAST_CONFIG = EngineConfig(
    poll_interval=0.02,
    tick_retry=RetryPolicy(initial_interval=0.01, max_interval=0.05, max_attempts=0, jitter=0),
    default_step_retry=RetryPolicy(initial_interval=0.01, max_interval=0.05, jitter=0),
    step_lease_grace=timedelta(seconds=5),
)


def started_event(
    name: str = "wf",
    args: Sequence[Any] = (),
    *,
    seed: int = 7,
    event_id: int = 1,
    timestamp: datetime = T0,
) -> WorkflowStarted:
    return WorkflowStarted(
        event_id=event_id,
        timestamp=timestamp,
        workflow_name=name,
        workflow_id="wf-id",
        run_id="run-1",
        args=default_converter.encode_args(list(args)),
        random_seed=seed,
    )


def make_instance(
    definition: WorkflowDefinition[..., Any], *args: Any
) -> tuple[WorkflowInstance, WorkflowStarted]:
    started = started_event(definition.name, args)
    return WorkflowInstance(definition, started, converter=default_converter), started


def step_ok(event_id: int, seq: int, value: Any) -> StepCompleted:
    return StepCompleted(
        event_id=event_id, timestamp=T0, seq=seq, result=default_converter.encode(value)
    )


def step_failed(event_id: int, seq: int, failure: Any) -> StepFailed:
    return StepFailed(event_id=event_id, timestamp=T0, seq=seq, failure=failure)


def timer_fired(event_id: int, seq: int) -> TimerFired:
    return TimerFired(event_id=event_id, timestamp=T0, seq=seq)


def tick(event_id: int, consumed_through: int, time: datetime = T0) -> TickCompleted:
    return TickCompleted(
        event_id=event_id, timestamp=time, consumed_through=consumed_through, time=time
    )


def kinds(events: Sequence[Event]) -> list[str]:
    return [e.kind for e in events]


async def eventually(
    predicate: Callable[[], Awaitable[bool]] | Callable[[], bool],
    *,
    timeout: float = 5.0,
    interval: float = 0.01,
    clock: ManualClock | None = None,
    advance: float = 0.0,
) -> None:
    """Poll ``predicate`` until it is true, optionally advancing a manual clock."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        if clock is not None and advance:
            await clock.advance(advance)
        await asyncio.sleep(interval)
