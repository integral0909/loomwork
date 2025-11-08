"""Header plumbing: ``WorkflowStarted``/``StepScheduled``/``ChildScheduled`` headers,
deterministic trace propagation inside :class:`WorkflowInstance`, and the ``Engine``
surface that accepts ``headers=``.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import loomwork as lw
from loomwork.clock import ManualClock
from loomwork.codec import default_converter
from loomwork.engine.engine import Engine
from loomwork.history.events import ChildScheduled, StepScheduled, WorkflowStarted
from loomwork.observability.propagation import TraceContext, derived_span_id
from loomwork.runtime.commands import ScheduleChild, ScheduleStep
from loomwork.runtime.instance import WorkflowInstance
from loomwork.sdk.messaging import start_child
from loomwork.store.memory import MemoryStore

T0 = datetime(2024, 1, 1, tzinfo=UTC)
TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


@lw.step
async def fetch(key: str) -> str:
    return key


@lw.workflow(name="headers.two_steps")
async def two_steps(a: str, b: str) -> list[str]:
    # Both scheduled in one activation, so the test can see both StepScheduled headers
    # at once (a sequential `await fetch(a); await fetch(b)` would split them in two).
    return list(await asyncio.gather(fetch(a), fetch(b)))


@lw.workflow(name="headers.starts_child")
async def starts_child() -> list[str]:
    handle = start_child(two_steps, "a", "b", id="child-1")
    return await handle.result()


def _started(run_id: str, headers: dict[str, str] | None = None) -> WorkflowStarted:
    return WorkflowStarted(
        workflow_name="headers.two_steps",
        workflow_id=f"wid-{run_id}",
        run_id=run_id,
        args=default_converter.encode_args(["a", "b"]),
        headers=headers or {},
    )


def _instance(run_id: str, headers: dict[str, str] | None = None) -> WorkflowInstance:
    return WorkflowInstance(two_steps, _started(run_id, headers), converter=default_converter)


# --------------------------------------------------------------------------- WorkflowInstance


def test_no_propagated_context_means_no_step_headers() -> None:
    instance = _instance("r1")
    assert instance.trace is None
    commands = instance.activate(T0, [_started("r1")], replaying=False)
    assert all(isinstance(c, ScheduleStep) and c.headers == {} for c in commands)


def test_malformed_traceparent_is_ignored_like_from_headers() -> None:
    instance = _instance("r1", {"traceparent": "not-a-traceparent"})
    assert instance.trace is None


def test_step_headers_are_deterministic_children_of_the_run_context() -> None:
    instance = _instance("r1", {"traceparent": TRACEPARENT})
    assert instance.trace == TraceContext.parse(TRACEPARENT)
    commands = instance.activate(
        T0, [_started("r1", {"traceparent": TRACEPARENT})], replaying=False
    )
    scheduled = [c for c in commands if isinstance(c, ScheduleStep)]
    assert len(scheduled) == 2
    for seq, command in enumerate(scheduled, start=1):
        context = TraceContext.from_headers(command.headers)
        assert context is not None
        assert context.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
        assert context.sampled
        assert context.span_id == derived_span_id("r1", "step", seq)
    # Distinct steps never share a span id.
    assert scheduled[0].headers != scheduled[1].headers


def test_child_headers_are_a_deterministic_child_of_the_run_context() -> None:
    instance = WorkflowInstance(
        starts_child,
        _started_for(starts_child, "r1", {"traceparent": TRACEPARENT}),
        converter=default_converter,
    )
    commands = instance.activate(
        T0, [_started_for(starts_child, "r1", {"traceparent": TRACEPARENT})], replaying=False
    )
    (child,) = (c for c in commands if isinstance(c, ScheduleChild))
    context = TraceContext.from_headers(child.headers)
    assert context is not None
    assert context.span_id == derived_span_id("r1", "child", 1)


def _started_for(
    definition: lw.WorkflowDefinition[..., Any],
    run_id: str,
    headers: dict[str, str] | None = None,
) -> WorkflowStarted:
    return WorkflowStarted(
        workflow_name=definition.name,
        workflow_id=f"wid-{run_id}",
        run_id=run_id,
        headers=headers or {},
    )


def test_header_derivation_is_reproducible_across_independent_instances() -> None:
    """The proof that matters for replay: two fresh instances built from the identical
    ``WorkflowStarted`` and fed the identical input produce byte-identical headers, the
    same way a live run and its later replay must (see ``observability/propagation.py``
    and ADR-0002)."""
    started = _started("r1", {"traceparent": TRACEPARENT})
    first = WorkflowInstance(two_steps, started, converter=default_converter)
    second = WorkflowInstance(two_steps, started, converter=default_converter)
    first_commands = [
        c for c in first.activate(T0, [started], replaying=False) if isinstance(c, ScheduleStep)
    ]
    second_commands = [
        c for c in second.activate(T0, [started], replaying=True) if isinstance(c, ScheduleStep)
    ]
    assert [c.headers for c in first_commands] == [c.headers for c in second_commands]


def test_step_scheduled_and_child_scheduled_events_carry_the_command_headers() -> None:
    command = ScheduleStep(seq=1, step_name="fetch", headers={"traceparent": TRACEPARENT})
    event = command.to_event()
    assert isinstance(event, StepScheduled)
    assert event.headers == {"traceparent": TRACEPARENT}

    child_command = ScheduleChild(
        seq=1,
        workflow_name="c",
        workflow_id="c-1",
        run_id="r.c1",
        headers={"traceparent": TRACEPARENT},
    )
    child_event = child_command.to_event()
    assert isinstance(child_event, ChildScheduled)
    assert child_event.headers == {"traceparent": TRACEPARENT}


# --------------------------------------------------------------------------- Engine


async def test_engine_start_workflow_records_headers() -> None:
    store = MemoryStore()
    await store.open()
    try:
        engine = Engine(store, clock=ManualClock(T0))
        record = await engine.start_workflow("wf", [], headers={"traceparent": TRACEPARENT})
        history = await store.load_history(record.run_id)
        assert isinstance(history[0], WorkflowStarted)
        assert history[0].headers == {"traceparent": TRACEPARENT}
    finally:
        await store.close()


async def test_engine_start_workflow_defaults_to_no_headers() -> None:
    store = MemoryStore()
    await store.open()
    try:
        engine = Engine(store, clock=ManualClock(T0))
        record = await engine.start_workflow("wf", [])
        history = await store.load_history(record.run_id)
        assert isinstance(history[0], WorkflowStarted)
        assert history[0].headers == {}
    finally:
        await store.close()


async def test_engine_signal_with_start_records_headers_on_the_new_run() -> None:
    store = MemoryStore()
    await store.open()
    try:
        engine = Engine(store, clock=ManualClock(T0))
        payload = default_converter.encode("hi")
        record = await engine.signal_with_start(
            "wf",
            [],
            workflow_id="wf-1",
            channel="c",
            payload=payload,
            headers={"traceparent": TRACEPARENT},
        )
        history = await store.load_history(record.run_id)
        assert isinstance(history[0], WorkflowStarted)
        assert history[0].headers == {"traceparent": TRACEPARENT}
    finally:
        await store.close()
