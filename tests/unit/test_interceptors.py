"""Interceptor composition: ordering, exception handling, and header mutation.

:func:`wrap_step`/:func:`wrap_call` are exercised directly here (no worker or store
needed); :class:`Client` and :class:`WorkflowHandle` are exercised against a real
``Engine``/``MemoryStore`` for the header-mutation and exception-propagation behaviour
that matters to callers. Step-attempt interceptors wired through a real
:class:`~loomwork.worker.worker.Worker` are covered in
``tests/integration/test_tracing.py`` alongside the OpenTelemetry integration, since both
need a running worker.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest

import loomwork as lw
from loomwork.client.client import Client
from loomwork.clock import ManualClock
from loomwork.engine.engine import Engine
from loomwork.errors import NotFoundError, WorkflowAlreadyStartedError
from loomwork.interceptors import (
    ClientCallContext,
    ClientInterceptor,
    StepAttemptContext,
    StepInterceptor,
    wrap_call,
    wrap_step,
)
from loomwork.store.memory import MemoryStore

T0 = datetime(2024, 1, 1, tzinfo=UTC)


class Recorder:
    def __init__(self) -> None:
        self.events: list[str] = []


class RecordingStepInterceptor:
    def __init__(self, name: str, recorder: Recorder, *, on_error: str | None = None) -> None:
        self.name = name
        self.recorder = recorder
        self.on_error = on_error

    @contextlib.contextmanager
    def intercept_step(self, ctx: StepAttemptContext) -> Iterator[None]:
        self.recorder.events.append(f"before:{self.name}")
        try:
            yield
        except Exception as exc:
            self.recorder.events.append(f"error:{self.name}:{type(exc).__name__}")
            raise
        else:
            self.recorder.events.append(f"after:{self.name}")


class RecordingClientInterceptor:
    def __init__(self, name: str, recorder: Recorder, *, add_header: str | None = None) -> None:
        self.name = name
        self.recorder = recorder
        self.add_header = add_header

    @contextlib.contextmanager
    def intercept_call(self, ctx: ClientCallContext) -> Iterator[None]:
        self.recorder.events.append(f"before:{self.name}:{ctx.kind}")
        if self.add_header:
            ctx.headers[self.add_header] = self.name
        try:
            yield
        except Exception as exc:
            self.recorder.events.append(f"error:{self.name}:{type(exc).__name__}")
            raise
        else:
            self.recorder.events.append(f"after:{self.name}")


# --------------------------------------------------------------------------- wrap_step


def _step_ctx() -> StepAttemptContext:
    info = lw.StepInfo(
        workflow_id="w1",
        run_id="r1",
        workflow_name="wf",
        step_name="s",
        seq=1,
        attempt=1,
        scheduled_at=T0,
    )
    return StepAttemptContext(info=info, args=(), headers={})


def test_wrap_step_runs_outermost_first_and_unwinds_in_reverse() -> None:
    recorder = Recorder()
    interceptors: list[StepInterceptor] = [
        RecordingStepInterceptor("a", recorder),
        RecordingStepInterceptor("b", recorder),
    ]
    with wrap_step(interceptors, _step_ctx()):
        recorder.events.append("body")
    assert recorder.events == ["before:a", "before:b", "body", "after:b", "after:a"]


def test_wrap_step_with_no_interceptors_is_a_no_op() -> None:
    with wrap_step([], _step_ctx()):
        pass  # must not raise


def test_wrap_step_observes_but_does_not_swallow_exceptions() -> None:
    recorder = Recorder()
    interceptors: list[StepInterceptor] = [
        RecordingStepInterceptor("a", recorder),
        RecordingStepInterceptor("b", recorder),
    ]
    with pytest.raises(ValueError, match="boom"), wrap_step(interceptors, _step_ctx()):
        raise ValueError("boom")
    # Both interceptors saw the exception, innermost first, and neither swallowed it.
    assert recorder.events == ["before:a", "before:b", "error:b:ValueError", "error:a:ValueError"]


def test_wrap_step_sees_args_and_headers() -> None:
    seen: list[tuple[tuple[object, ...], dict[str, str]]] = []

    @contextlib.contextmanager
    def intercept_step(ctx: StepAttemptContext) -> Iterator[None]:
        seen.append((ctx.args, dict(ctx.headers)))
        yield

    class Inline:
        def intercept_step(
            self, ctx: StepAttemptContext
        ) -> contextlib.AbstractContextManager[None]:
            return intercept_step(ctx)

    info = lw.StepInfo(
        workflow_id="w1",
        run_id="r1",
        workflow_name="wf",
        step_name="s",
        seq=1,
        attempt=1,
        scheduled_at=T0,
    )
    ctx = StepAttemptContext(info=info, args=(1, "x"), headers={"traceparent": "t"})
    with wrap_step([Inline()], ctx):
        pass
    assert seen == [((1, "x"), {"traceparent": "t"})]


# --------------------------------------------------------------------------- wrap_call


def test_wrap_call_runs_outermost_first_and_unwinds_in_reverse() -> None:
    recorder = Recorder()
    ctx = ClientCallContext(kind="start", workflow_name="wf", workflow_id="w1", headers={})
    interceptors: list[ClientInterceptor] = [
        RecordingClientInterceptor("a", recorder),
        RecordingClientInterceptor("b", recorder),
    ]
    with wrap_call(interceptors, ctx):
        recorder.events.append("call")
    assert recorder.events == ["before:a:start", "before:b:start", "call", "after:b", "after:a"]


def test_wrap_call_lets_each_interceptor_add_headers() -> None:
    ctx = ClientCallContext(kind="start", workflow_name="wf", workflow_id="w1", headers={})
    recorder = Recorder()
    interceptors: list[ClientInterceptor] = [
        RecordingClientInterceptor("auth", recorder, add_header="authorization"),
        RecordingClientInterceptor("trace", recorder, add_header="traceparent"),
    ]
    with wrap_call(interceptors, ctx):
        pass
    assert ctx.headers == {"authorization": "auth", "traceparent": "trace"}


def test_wrap_call_observes_but_does_not_swallow_exceptions() -> None:
    recorder = Recorder()
    ctx = ClientCallContext(kind="start", workflow_name="wf", workflow_id="w1", headers={})
    interceptors: list[ClientInterceptor] = [RecordingClientInterceptor("a", recorder)]
    with pytest.raises(RuntimeError), wrap_call(interceptors, ctx):
        raise RuntimeError("network down")
    assert recorder.events == ["before:a:start", "error:a:RuntimeError"]


# --------------------------------------------------------------------------- Client integration


async def _engine() -> Engine:
    store = MemoryStore()
    await store.open()
    return Engine(store, clock=ManualClock(T0))


async def test_client_start_runs_interceptors_and_records_their_headers() -> None:
    engine = await _engine()
    recorder = Recorder()
    client = Client(
        engine,
        interceptors=[
            RecordingClientInterceptor("outer", recorder),
            RecordingClientInterceptor("inner", recorder, add_header="traceparent"),
        ],
    )
    handle = await client.start("wf", id="w1")
    assert recorder.events == [
        "before:outer:start",
        "before:inner:start",
        "after:inner",
        "after:outer",
    ]
    history = await handle.history()
    assert history[0].headers == {"traceparent": "inner"}  # type: ignore[attr-defined]


async def test_client_start_interceptor_exception_prevents_the_call() -> None:
    engine = await _engine()

    class Explodes:
        def intercept_call(self, ctx: ClientCallContext) -> contextlib.AbstractContextManager[None]:
            raise RuntimeError("no budget left")

    client = Client(engine, interceptors=[Explodes()])
    with pytest.raises(RuntimeError, match="no budget left"):
        await client.start("wf", id="w1")
    with pytest.raises(NotFoundError):  # the run was never created
        await engine.describe("w1")


async def test_client_interceptor_observes_engine_errors_without_swallowing_them() -> None:
    engine = await _engine()
    await engine.start_workflow("wf", [], workflow_id="w1")
    recorder = Recorder()
    client = Client(engine, interceptors=[RecordingClientInterceptor("a", recorder)])
    with pytest.raises(WorkflowAlreadyStartedError):
        await client.start("wf", id="w1")
    assert recorder.events == ["before:a:start", "error:a:WorkflowAlreadyStartedError"]


async def test_handle_signal_and_update_run_interceptors_too() -> None:
    engine = await _engine()
    recorder = Recorder()
    client = Client(engine, interceptors=[RecordingClientInterceptor("a", recorder)])
    handle = await client.start("wf", id="w1")
    await handle.signal("c", "v")
    assert recorder.events == ["before:a:start", "after:a", "before:a:signal", "after:a"]
