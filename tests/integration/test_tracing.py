"""End-to-end trace propagation, header survival, and interceptors against a real
worker: a caller's active span becomes the parent of every step span; headers survive
child workflows, continue-as-new and a worker driven over ``HttpStore``; and replay after
a replay-cache eviction reproduces identical ``StepScheduled`` headers.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import httpx
import pytest
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import loomwork as lw
from loomwork import ManualClock
from loomwork.client.client import Client
from loomwork.engine.engine import Engine
from loomwork.history.events import StepScheduled, WorkflowStarted
from loomwork.interceptors import ClientCallContext, StepAttemptContext
from loomwork.observability import CorrelationFilter, OpenTelemetryTelemetry
from loomwork.observability.otel import TracingClientInterceptor
from loomwork.observability.propagation import TraceContext, derived_span_id
from loomwork.remote.http_store import HttpStore
from loomwork.server.app import create_app
from loomwork.server.config import AuthConfig, ServerConfig
from loomwork.store.memory import MemoryStore
from loomwork.worker.worker import Worker, WorkerOptions
from tests.conftest import AppFactory
from tests.helpers import FAST_CONFIG, eventually

pytestmark = pytest.mark.integration


@lw.step(name="tr.step_a")
async def step_a(x: str) -> str:
    return x


@lw.step(name="tr.step_b")
async def step_b(x: str) -> str:
    return x


@lw.workflow(name="tr.flow")
async def flow(x: str) -> str:
    a = await step_a(x)
    return await step_b(a)


@lw.workflow(name="tr.parent")
async def parent(x: str) -> str:
    child = lw.start_child(flow, x, id=f"{lw.info().workflow_id}-child")
    return await child.result()


@lw.workflow(name="tr.continuing")
async def continuing(x: str, remaining: int) -> str:
    result = await step_a(x)
    if remaining > 0:
        lw.continue_as_new(result, remaining - 1)
    return result


def _provider_and_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def _step_spans(exporter: InMemorySpanExporter, prefix: str = "step tr.") -> list[ReadableSpan]:
    return [s for s in exporter.get_finished_spans() if s.name.startswith(prefix)]


def _run_id(handle: lw.WorkflowHandle[Any]) -> str:
    assert handle.run_id is not None
    return handle.run_id


# --------------------------------------------------------------------------- trace propagation


async def test_caller_span_is_the_parent_of_every_step_span(
    make_app: AppFactory, clock: ManualClock
) -> None:
    provider, exporter = _provider_and_exporter()
    telemetry = OpenTelemetryTelemetry(tracer_provider=provider)
    app = await make_app(
        [flow],
        [step_a, step_b],
        telemetry=telemetry,
        client_interceptors=[TracingClientInterceptor(tracer_provider=provider)],
    )
    tracer = provider.get_tracer("caller")
    with tracer.start_as_current_span("caller") as caller_span:
        caller_trace_id = caller_span.get_span_context().trace_id
        handle = await app.start_workflow(flow, "x")
    assert await handle.result(timeout=5) == "x"

    def seq_of(span: ReadableSpan) -> int:
        assert span.attributes is not None
        return int(span.attributes["loomwork.step.seq"])  # type: ignore[arg-type]

    step_spans = sorted(_step_spans(exporter), key=seq_of)
    assert len(step_spans) == 2
    parent_span_ids: list[int] = []
    for seq, span in enumerate(step_spans, start=1):
        assert span.context is not None
        assert span.context.trace_id == caller_trace_id
        assert span.parent is not None
        assert span.parent.trace_id == caller_trace_id
        assert format(span.parent.span_id, "016x") == derived_span_id(_run_id(handle), "step", seq)
        parent_span_ids.append(span.parent.span_id)
    # Distinct steps hang off distinct, but deterministic, positions in the trace.
    assert parent_span_ids[0] != parent_span_ids[1]


async def test_a_traceparent_header_is_enough_without_an_active_span(
    make_app: AppFactory, clock: ManualClock
) -> None:
    """Callers that already carry a ``traceparent`` (for example from an inbound HTTP
    request) can pass it directly through ``headers=`` instead of an OpenTelemetry span."""
    provider, exporter = _provider_and_exporter()
    telemetry = OpenTelemetryTelemetry(tracer_provider=provider)
    app = await make_app([flow], [step_a, step_b], telemetry=telemetry)
    context = TraceContext.new_root()
    handle = await app.start_workflow(flow, "x", headers=context.to_headers())
    assert await handle.result(timeout=5) == "x"
    step_spans = _step_spans(exporter)
    assert len(step_spans) == 2
    trace_ids = [s.context.trace_id for s in step_spans if s.context is not None]
    assert trace_ids == [int(context.trace_id, 16)] * 2


async def test_step_code_can_read_the_propagated_context(
    make_app: AppFactory, clock: ManualClock
) -> None:
    seen: dict[str, TraceContext | None] = {}

    @lw.step(name="tr.reads_context")
    async def reads_context() -> None:
        seen["step"] = lw.current_trace_context()

    @lw.workflow(name="tr.reads_context_wf")
    async def reads_context_wf() -> None:
        seen["workflow"] = lw.current_trace_context()
        await reads_context()

    app = await make_app([reads_context_wf], [reads_context])
    context = TraceContext.new_root()
    handle = await app.start_workflow(reads_context_wf, headers=context.to_headers())
    await handle.result(timeout=5)
    assert seen["workflow"] == context
    step_context = seen["step"]
    assert step_context is not None
    assert step_context.trace_id == context.trace_id
    assert step_context.span_id == derived_span_id(_run_id(handle), "step", 1)


# --------------------------------------------------------------------------- header survival


async def test_headers_survive_a_child_workflow(make_app: AppFactory, clock: ManualClock) -> None:
    """A child gets a *derived* child of the parent's context (like a step), not a raw
    copy: it is a new position in the same trace, deterministic from the parent run id
    and the child's sequence number (see ``WorkflowInstance._derived_headers``)."""
    app = await make_app([parent, flow], [step_a, step_b])
    context = TraceContext.new_root()
    handle = await app.start_workflow(parent, "x", headers=context.to_headers())
    assert await handle.result(timeout=5) == "x"
    (child,) = await handle.children()
    child_history = await app.engine.history(child.run_id)
    assert isinstance(child_history[0], WorkflowStarted)
    child_context = TraceContext.from_headers(child_history[0].headers)
    assert child_context is not None
    assert child_context.trace_id == context.trace_id
    assert child_context.span_id == derived_span_id(_run_id(handle), "child", 1)


async def test_headers_survive_continue_as_new(make_app: AppFactory, clock: ManualClock) -> None:
    app = await make_app([continuing], [step_a])
    context = TraceContext.new_root()
    handle = await app.start_workflow(continuing, "x", 2, headers=context.to_headers())
    assert await handle.result(timeout=5) == "x"
    record = await app.engine.describe(_run_id(handle))
    # Two continuations: three runs share the same workflow id and the same headers.
    seen_run_ids = {_run_id(handle)}
    while record.continued_run_id:
        record = await app.engine.describe(record.continued_run_id)
        seen_run_ids.add(record.run_id)
    assert len(seen_run_ids) == 3
    for run_id in seen_run_ids:
        history = await app.engine.history(run_id)
        assert isinstance(history[0], WorkflowStarted)
        assert history[0].headers == context.to_headers()


async def test_headers_survive_over_http() -> None:
    config = ServerConfig(namespaces={"default": "memory://"}, auth=AuthConfig(disabled=True))
    server = create_app(config, run_system_workers=False)
    async with server.router.lifespan_context(server):
        transport = httpx.ASGITransport(app=server)
        store = HttpStore("http://test", None, "default", transport=transport, long_poll=0.05)
        await store.open()
        try:
            engine = Engine(store, config=FAST_CONFIG)
            client = Client(engine)
            worker = Worker(
                store,
                lw.Registry(workflows=[flow], steps=[step_a, step_b]),
                config=FAST_CONFIG,
            )
            async with worker:
                context = TraceContext.new_root()
                handle = await client.start(flow, "x", id="http-1", headers=context.to_headers())
                assert await handle.result(timeout=5) == "x"
                history = await handle.history()
                assert isinstance(history[0], WorkflowStarted)
                assert history[0].headers == context.to_headers()
                scheduled = [e for e in history if isinstance(e, StepScheduled)]
                assert len(scheduled) == 2
                for seq, event in enumerate(scheduled, start=1):
                    parsed = TraceContext.from_headers(event.headers)
                    assert parsed is not None
                    assert parsed.trace_id == context.trace_id
                    assert parsed.span_id == derived_span_id(_run_id(handle), "step", seq)
        finally:
            await store.close()


# --------------------------------------------------------------------------- determinism


@lw.workflow(name="tr.two_steps_with_gap")
async def two_steps_with_gap(x: str) -> str:
    a = await step_a(x)
    # A long durable sleep gives the test a reliable point to swap workers at: the run
    # is parked on a timer, not mid-tick, and nothing progresses until the clock moves.
    await lw.sleep(timedelta(hours=1))
    return await step_b(a)


async def test_replay_after_cache_eviction_reproduces_identical_step_headers(
    clock: ManualClock,
) -> None:
    """The second step is scheduled by a *different* worker than the first, one that
    has never seen this run before and so must replay tick 1 from stored history to
    reconstruct the instance (its propagated context, its step sequence counter) before
    it can run live and schedule step 2. Its ``StepScheduled`` still gets the exact
    header a warm worker would have produced -- the property the whole deterministic
    derivation scheme in ``observability/propagation.py`` exists for."""
    store = MemoryStore()
    context = TraceContext.new_root()
    options = WorkerOptions(shutdown_grace=0.5)
    first = lw.Loomwork(
        store=store,
        workflows=[two_steps_with_gap],
        steps=[step_a, step_b],
        clock=clock,
        config=FAST_CONFIG,
        worker_options=options,
    )
    async with first:
        handle = await first.start_workflow(two_steps_with_gap, "x", headers=context.to_headers())
        await eventually(lambda: store.timer_count == 1, clock=clock, advance=0.02)

    # A fresh Loomwork on the same store starts with an empty replay cache.
    second = lw.Loomwork(
        store=store,
        workflows=[two_steps_with_gap],
        steps=[step_a, step_b],
        clock=clock,
        config=FAST_CONFIG,
        worker_options=options,
    )
    async with second:
        await clock.advance(timedelta(hours=1))
        assert await second.get_handle(run_id=handle.run_id).result(timeout=5) == "x"
    assert second.worker.ticks.cache.misses >= 1

    scheduled = [
        e for e in await store.load_history(_run_id(handle)) if isinstance(e, StepScheduled)
    ]
    assert len(scheduled) == 2
    for seq, event in enumerate(scheduled, start=1):
        parsed = TraceContext.from_headers(event.headers)
        assert parsed is not None
        assert parsed.trace_id == context.trace_id
        assert parsed.span_id == derived_span_id(_run_id(handle), "step", seq)


# --------------------------------------------------------------------------- step interceptors


async def test_step_interceptor_sees_args_and_headers_and_observes_failures(
    make_app: AppFactory, clock: ManualClock
) -> None:
    seen: list[StepAttemptContext] = []
    observed_errors: list[str] = []

    class Recording:
        @contextlib.contextmanager
        def intercept_step(self, ctx: StepAttemptContext) -> Iterator[None]:
            seen.append(ctx)
            try:
                yield
            except Exception as exc:
                observed_errors.append(f"{ctx.info.step_name}:{type(exc).__name__}")
                raise

    @lw.step(name="tr.explodes", retry=lw.RetryPolicy(max_attempts=1))
    async def explodes(x: str) -> str:
        raise ValueError("nope")

    @lw.workflow(name="tr.uses_interceptor")
    async def uses_interceptor(x: str) -> str:
        try:
            return await explodes(x)
        except lw.StepError:
            return "recovered"

    app = await make_app(
        [uses_interceptor],
        [explodes],
        worker_options=WorkerOptions(step_interceptors=[Recording()], shutdown_grace=0.5),
    )
    context = TraceContext.new_root()
    handle = await app.start_workflow(uses_interceptor, "x", headers=context.to_headers())
    assert await handle.result(timeout=5) == "recovered"
    assert len(seen) == 1
    assert seen[0].args == ("x",)
    assert TraceContext.from_headers(seen[0].headers) == TraceContext(
        context.trace_id, derived_span_id(_run_id(handle), "step", 1)
    )
    assert observed_errors == ["tr.explodes:ValueError"]


# --------------------------------------------------------------------------- client interceptors


async def test_client_interceptor_ordering_around_a_real_start(
    make_app: AppFactory, clock: ManualClock
) -> None:
    events: list[str] = []

    def make(name: str) -> object:
        class _Interceptor:
            @contextlib.contextmanager
            def intercept_call(self, ctx: ClientCallContext) -> Iterator[None]:
                events.append(f"before:{name}")
                yield
                events.append(f"after:{name}")

        return _Interceptor()

    app = await make_app(
        [flow], [step_a, step_b], client_interceptors=[make("outer"), make("inner")]
    )
    handle = await app.start_workflow(flow, "x")
    assert await handle.result(timeout=5) == "x"
    assert events == ["before:outer", "before:inner", "after:inner", "after:outer"]


# --------------------------------------------------------------------------- logging correlation


async def test_correlation_filter_tags_records_from_steps_and_workflows(
    make_app: AppFactory, clock: ManualClock, caplog: pytest.LogCaptureFixture
) -> None:
    plain_logger = logging.getLogger("tr.plain")
    plain_logger.addFilter(CorrelationFilter())

    @lw.step(name="tr.logs")
    async def logs_from_step(x: str) -> str:
        plain_logger.info("in step")
        return x

    @lw.workflow(name="tr.logs_wf")
    async def logs_wf(x: str) -> str:
        plain_logger.info("in workflow")
        return await logs_from_step(x)

    app = await make_app([logs_wf], [logs_from_step])
    context = TraceContext.new_root()
    with caplog.at_level(logging.INFO, logger="tr.plain"):
        handle = await app.start_workflow(logs_wf, "x", headers=context.to_headers())
        assert await handle.result(timeout=5) == "x"
    records = {r.message: r for r in caplog.records if r.name == "tr.plain"}
    assert records["in workflow"].run_id == handle.run_id  # type: ignore[attr-defined]
    assert records["in workflow"].trace_id == context.trace_id  # type: ignore[attr-defined]
    assert records["in step"].step_name == "tr.logs"  # type: ignore[attr-defined]
    assert records["in step"].trace_id == context.trace_id  # type: ignore[attr-defined]
