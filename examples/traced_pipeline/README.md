# Traced pipeline

OpenTelemetry trace propagation end to end, a custom step interceptor, and correlated
logging -- the three pieces `docs/guides/interceptors.md` covers together, since Loomwork
uses the same interceptor mechanism for all of them.

## Scenario

`process_document` (in `workflows.py`) is an ordinary three-step pipeline (extract,
classify, store) with nothing tracing-specific in it at all. `main.py` wires tracing,
auditing and log correlation on from the outside:

1. A caller span, `handle-upload`, is made current with the real OpenTelemetry SDK
   (`TracerProvider` + an in-memory exporter) before starting the run.
   `TracingClientInterceptor` (registered as a `client_interceptor`) captures that
   active span and carries it into the run's `WorkflowStarted.headers`.
2. `OpenTelemetryTelemetry` (registered as the app's `telemetry`) reports a `step ...`
   span for every step attempt, parented on that propagated context via a
   deterministic, replay-stable derived position (`derived_span_id`) rather than a
   randomly generated one -- the same position every replay produces, on any worker.
3. `TenantAuditLog`, a small custom `StepInterceptor`, records which tenant touched
   which step -- a stand-in for a compliance audit trail, added without changing a
   single step or workflow.
4. `CorrelationFilter` is attached to a plain `logging.getLogger("traced_pipeline")`
   logger used by both the workflow and its steps; every record it tags picks up
   `run_id`, `step_name` and `trace_id` automatically.

`main.py` then prints the resulting span tree, the audit log, and the correlated log
records.

## Loomwork features used

- `loomwork.observability.OpenTelemetryTelemetry` and
  `loomwork.observability.otel.TracingClientInterceptor` for end-to-end trace
  propagation (see `docs/guides/interceptors.md`'s "Trace propagation").
- A custom `loomwork.interceptors.StepInterceptor` (`TenantAuditLog`) wrapping every
  step attempt.
- `loomwork.observability.CorrelationFilter` for structured-logging correlation on an
  ordinary logger.

## Production versus this example

Production needs the `otel` extra (`pip install "loomwork[otel]"`, which installs
`opentelemetry-api`) and a real OpenTelemetry SDK `TracerProvider` wired to a real
exporter (OTLP, Jaeger, ...) -- Loomwork's core never imports `opentelemetry` itself;
only `loomwork.observability.otel` does, so tracing support costs nothing unless it is
used. This example additionally uses `opentelemetry-sdk`'s in-memory exporter directly,
which is a dev dependency of this repository (not something Loomwork depends on),
purely so the demo can print the resulting spans without standing up a collector.

## Running it

```console
uv run python -m examples.traced_pipeline.main
```

Runs against the real clock and a `MemoryStore` (nothing here waits on durable time),
finishing in a fraction of a second.

## Tests

`tests/examples/test_traced_pipeline.py` checks:

- every `step ...` span shares the caller's trace id;
- `TenantAuditLog` records exactly one entry per step, in order, all under the same
  tenant and run id;
- `CorrelationFilter` tags log records from both the workflow and its steps with the
  run id, and step records additionally with the step name.
