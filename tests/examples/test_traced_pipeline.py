"""Tests for ``examples.traced_pipeline``."""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from examples.traced_pipeline.main import TenantAuditLog, _RecordingHandler
from examples.traced_pipeline.steps import classify_document, extract_text, store_result, stored
from examples.traced_pipeline.workflows import process_document
from loomwork import Loomwork
from loomwork.observability import CorrelationFilter, OpenTelemetryTelemetry
from loomwork.observability.otel import TracingClientInterceptor
from loomwork.worker import WorkerOptions


@pytest.fixture(autouse=True)
def _clear_store() -> Iterator[None]:
    stored.clear()
    yield
    stored.clear()


def _provider_and_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


async def test_every_step_span_descends_from_the_callers_trace() -> None:
    provider, exporter = _provider_and_exporter()
    tracer = provider.get_tracer("test-caller")
    app = Loomwork(
        workflows=[process_document],
        steps=[extract_text, classify_document, store_result],
        telemetry=OpenTelemetryTelemetry(tracer_provider=provider),
        client_interceptors=[TracingClientInterceptor(tracer_provider=provider)],
        worker_options=WorkerOptions(shutdown_grace=0.5),
    )
    async with app:
        with tracer.start_as_current_span("caller") as caller_span:
            caller_trace_id = caller_span.get_span_context().trace_id
            result = await app.execute(process_document, "tenant-a", "doc-1", id="doc-1", timeout=5)

    assert result == {"document_id": "doc-1", "label": "invoice", "location": "tenant-a/doc-1.json"}
    step_spans = [s for s in exporter.get_finished_spans() if s.name.startswith("step ")]
    assert len(step_spans) == 3
    for span in step_spans:
        assert span.context is not None
        assert span.context.trace_id == caller_trace_id
    assert stored["tenant-a/doc-1.json"] == ("tenant-a", "invoice")


async def test_audit_log_records_every_step_by_tenant() -> None:
    audit_log = TenantAuditLog()
    app = Loomwork(
        workflows=[process_document],
        steps=[extract_text, classify_document, store_result],
        worker_options=WorkerOptions(step_interceptors=[audit_log], shutdown_grace=0.5),
    )
    async with app:
        handle = await app.start_workflow(process_document, "tenant-b", "doc-2", id="doc-2")
        await handle.result(timeout=5)

    assert [entry[:2] for entry in audit_log.entries] == [
        ("tenant-b", "traced_pipeline.extract_text"),
        ("tenant-b", "traced_pipeline.classify_document"),
        ("tenant-b", "traced_pipeline.store_result"),
    ]
    assert len({run_id for _, _, run_id in audit_log.entries}) == 1


async def test_correlation_filter_tags_logs_from_the_workflow_and_its_steps() -> None:
    handler = _RecordingHandler()
    handler.addFilter(CorrelationFilter())
    logger = logging.getLogger("traced_pipeline")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        app = Loomwork(
            workflows=[process_document],
            steps=[extract_text, classify_document, store_result],
            worker_options=WorkerOptions(shutdown_grace=0.5),
        )
        async with app:
            handle = await app.start_workflow(process_document, "tenant-c", "doc-3", id="doc-3")
            await handle.result(timeout=5)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(logging.NOTSET)

    messages = {record.getMessage(): record for record in handler.records}
    workflow_record = messages["processing doc-3 for tenant-c"]
    assert workflow_record.run_id == handle.run_id  # type: ignore[attr-defined]
    step_record = messages["extracting text from doc-3"]
    assert step_record.step_name == "traced_pipeline.extract_text"  # type: ignore[attr-defined]
    assert step_record.run_id == handle.run_id  # type: ignore[attr-defined]
