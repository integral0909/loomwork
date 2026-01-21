"""Trace a document pipeline end to end with OpenTelemetry, a custom step
interceptor and correlated logging.

Production needs the ``otel`` extra (``pip install "loomwork[otel]"``, which installs
``opentelemetry-api``) and a real OpenTelemetry SDK provider wired to a real exporter
(OTLP, Jaeger, ...) -- Loomwork's core never imports ``opentelemetry`` itself; only
``loomwork.observability.otel`` does. This example uses the SDK's in-memory exporter
directly (``opentelemetry-sdk`` is a dev dependency of this repository, not something
Loomwork depends on) purely so it can print the resulting spans without a collector.

.. code-block:: console

    uv run python -m examples.traced_pipeline.main

See ``README.md`` for what this demonstrates.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Iterator, Sequence

from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import loomwork as lw
from examples.support import rule
from examples.traced_pipeline.steps import classify_document, extract_text, store_result
from examples.traced_pipeline.workflows import process_document
from loomwork.interceptors import StepAttemptContext
from loomwork.observability import CorrelationFilter, OpenTelemetryTelemetry
from loomwork.observability.otel import TracingClientInterceptor
from loomwork.worker import WorkerOptions


class TenantAuditLog:
    """A custom step interceptor: for every step attempt, records which tenant touched
    which step -- a compliance audit trail, kept without changing a single step or
    workflow. Every step in this pipeline takes ``tenant_id`` as its first argument, so
    ``ctx.args`` is enough; a real audit sink would append to a durable log instead of
    an in-process list.
    """

    def __init__(self) -> None:
        self.entries: list[tuple[str, str, str]] = []

    @contextlib.contextmanager
    def intercept_step(self, ctx: StepAttemptContext) -> Iterator[None]:
        tenant_id = str(ctx.args[0]) if ctx.args else "unknown"
        self.entries.append((tenant_id, ctx.info.step_name, ctx.info.run_id))
        yield


class _RecordingHandler(logging.Handler):
    """Collects log records for the demo to print afterwards, standing in for
    whatever structured-logging sink (stdout JSON, a log shipper) a real deployment
    would use -- ``CorrelationFilter`` works the same way regardless of the handler."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _print_trace(spans: Sequence[ReadableSpan], trace_id: int) -> None:
    """Print every span belonging to ``trace_id``, indented under its parent where that
    parent was itself exported. A "step ..." span's real parent is a deterministic but
    *synthetic* position (``derived_span_id``, see ``docs/guides/interceptors.md``) --
    exactly like a span crossing a real network hop, nothing ever exports a span object
    at that exact id, so a real trace viewer (Jaeger, Tempo, ...) draws it as a child of
    that position anyway, purely from the id it carries; this printer does the same.
    """
    in_trace = [s for s in spans if s.context is not None and s.context.trace_id == trace_id]
    exported_ids = {s.context.span_id for s in in_trace if s.context is not None}
    by_parent: dict[int | None, list[ReadableSpan]] = {}
    synthetic_parent: set[int] = set()
    for span in in_trace:
        real_parent_id = span.parent.span_id if span.parent is not None else None
        assert span.context is not None
        if real_parent_id is not None and real_parent_id not in exported_ids:
            synthetic_parent.add(span.context.span_id)
            real_parent_id = None  # nothing to nest it under; show it at the top level
        by_parent.setdefault(real_parent_id, []).append(span)

    def walk(parent_id: int | None, depth: int) -> None:
        children = sorted(by_parent.get(parent_id, []), key=lambda s: s.start_time or 0)
        for span in children:
            assert span.context is not None
            note = (
                "  (synthetic parent position)" if span.context.span_id in synthetic_parent else ""
            )
            print(f"{'  ' * depth}{span.name}{note}")
            walk(span.context.span_id, depth + 1)

    walk(None, 0)


async def main() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("traced_pipeline.caller")

    handler = _RecordingHandler()
    handler.addFilter(CorrelationFilter())
    correlated_logger = logging.getLogger("traced_pipeline")
    correlated_logger.addHandler(handler)
    correlated_logger.setLevel(logging.INFO)
    correlated_logger.propagate = False  # keep the demo's own output from doubling up

    audit_log = TenantAuditLog()
    app = lw.Loomwork(
        workflows=[process_document],
        steps=[extract_text, classify_document, store_result],
        # OpenTelemetryTelemetry does not (yet) implement every method the Telemetry
        # protocol has grown since (history_size_warning, pinned_runs_waiting); see
        # README.md's "A note on typing" for the discrepancy this works around.
        telemetry=OpenTelemetryTelemetry(tracer_provider=provider),
        client_interceptors=[TracingClientInterceptor(tracer_provider=provider)],
        worker_options=WorkerOptions(step_interceptors=[audit_log], shutdown_grace=0.2),
    )
    async with app:
        print(rule("Starting a run inside a caller span"))
        with tracer.start_as_current_span("handle-upload") as caller_span:
            caller_trace_id = caller_span.get_span_context().trace_id
            result = await app.execute(process_document, "tenant-a", "doc-1", id="doc-1", timeout=5)
        print(f"result: {result}")

    print(rule("The resulting span tree -- every step is a child of the caller's trace"))
    finished = exporter.get_finished_spans()
    _print_trace(finished, caller_trace_id)
    tick_spans = [
        s for s in finished if s.context is not None and s.context.trace_id != caller_trace_id
    ]
    print(
        f"(plus {len(tick_spans)} 'workflow.tick' spans, each its own untraced span --"
        " tick telemetry is per-worker bookkeeping, not part of the propagated trace)"
    )

    print(rule("The audit log a custom step interceptor kept, per tenant"))
    for tenant_id, step_name, run_id in audit_log.entries:
        print(f"  tenant={tenant_id} step={step_name} run={run_id}")

    print(rule("Correlated log records -- a plain logger, tagged by CorrelationFilter"))
    for record in handler.records:
        print(
            f"  {record.getMessage()!r} run_id={getattr(record, 'run_id', None)} "
            f"step_name={getattr(record, 'step_name', None)} "
            f"trace_id={getattr(record, 'trace_id', None)}"
        )


if __name__ == "__main__":
    asyncio.run(main())
