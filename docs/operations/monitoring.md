# Monitoring

## Metrics

`MetricsTelemetry` records Loomwork's standard metrics into a `MetricsRegistry`, which
renders the Prometheus text format:

| Metric | Type | Labels | What to watch |
| ------ | ---- | ------ | ------------- |
| `loomwork_ticks_total` | counter | `workflow`, `outcome` | `outcome="failed"` rising: nondeterminism or bugs in workflow code |
| `loomwork_tick_duration_seconds` | histogram | `workflow` | slow ticks: large histories without cache, store latency |
| `loomwork_replayed_ticks_total` | counter | `workflow` | high relative to ticks: the replay cache is too small |
| `loomwork_replay_cache_total` | counter | `result` | hit ratio |
| `loomwork_step_attempts_total` | counter | `step`, `queue`, `outcome` | `retrying`/`failed` by step |
| `loomwork_step_attempt_duration_seconds` | histogram | `step`, `queue` | step latency |
| `loomwork_step_queue_delay_seconds` | histogram | `queue` | growing delay: not enough step workers for a queue |
| `loomwork_step_attempts_running` | gauge | `queue` | saturation against `step_concurrency` / queue limits |
| `loomwork_schedule_runs_total` | counter | `result` | `skipped` rising: overlap policy or outages |
| `loomwork_outbox_messages_total` | counter | | cross-run traffic |
| `loomwork_worker_errors_total` | counter | `loop` | any value: check logs |

`loomwork worker --metrics-port 9464` serves them at `http://127.0.0.1:9464/metrics`, with
a `/healthz` readiness probe next to it. In your own process, `MetricsServer` does the same
without a web framework:

```python
from loomwork.observability import MetricsServer, MetricsTelemetry

metrics = MetricsTelemetry()
app = Loomwork(..., telemetry=metrics)
async with MetricsServer(metrics.registry, host="0.0.0.0", port=9464), app:
    ...
```

Or expose them from your application's existing HTTP server:

```python
metrics = MetricsTelemetry()
app = Loomwork(..., telemetry=metrics)

async def metrics_endpoint(request):
    return Response(metrics.registry.render(), media_type=MetricsRegistry.CONTENT_TYPE)
```

### Store-level health

Some signals live in the store rather than in one worker:

- **Outbox backlog**: `await store.outbox_size()` should stay near zero; a growing
  outbox means no worker has `run_outbox=True`. `loomwork store info` prints it along with
  the number of running and failed runs.
- **Stuck runs**: `await client.query_runs("status = 'running' AND tick_failures > 5")`
  finds runs whose ticks keep failing; `last_tick_error` explains why.
- **Old running runs**: `created_at < now() - interval '30 days' AND status = 'running'`.

## Tracing

`OpenTelemetryTelemetry` (install `loomwork[otel]`) creates:

- a `workflow.tick <name>` span per tick with its real duration and outcome;
- a `step <name>` span per attempt, made current while the step's code runs, so
  instrumented HTTP clients and database drivers inside the step become its children.

Configure exporters with the OpenTelemetry SDK as usual; Loomwork only uses the API.

### End-to-end propagation

A run started inside an active span, or one started with a `traceparent` passed as
`headers=`, makes every step that run schedules a child of that trace, all the way
through child workflows, continue-as-new and retries, even across a worker restart or a
replay days later. `step <name>` spans are parented on it automatically once
`OpenTelemetryTelemetry` is in use; no extra configuration is needed beyond starting
runs with a propagated context. The built-in `TracingClientInterceptor` does that
automatically from the caller's active span:

```python
from loomwork.observability import OpenTelemetryTelemetry, TracingClientInterceptor

app = lw.Loomwork(
    ...,
    telemetry=OpenTelemetryTelemetry(),
    client_interceptors=[TracingClientInterceptor()],
)

with tracer.start_as_current_span("checkout"):
    await app.start_workflow(process_order, order_id)
```

See [Interceptors and context propagation](../guides/interceptors.md) for how the
propagated context is derived deterministically (so replay reproduces it exactly), how
to read it from workflow or step code, and the client/step middleware API it is built
on.

## Logs

- `loomwork.engine.ticks`: tick failures (nondeterminism at ERROR, other failures at
  WARNING, with the traceback).
- `loomwork.engine.steps`: retries (INFO) and permanent failures (WARNING).
- `loomwork.worker`: start/stop and unexpected loop errors.
- `loomwork.audit` (server mode): every mutating API request.
- `loomwork.user`: `loomwork.logger` from workflow and step code, tagged with
  `workflow_id`, `run_id` and step identity, and silent during replay.
- `loomwork.telemetry`: one record per tick and step attempt when `LoggingTelemetry` is
  used; fields are in `extra`, ready for JSON formatters.

### Correlating your own logs

`loomwork.logger` tags its own records, but application code inside a step (or a
library it calls) usually logs through its own `logging.getLogger(__name__)`, which by
default carries none of that context. `CorrelationFilter` closes the gap: attach it to
a handler or a logger and matching records gain `workflow_id`, `run_id`, `step_name`,
`step_attempt`, `trace_id` and `span_id` (whichever apply) while a workflow or step is
running:

```python
from loomwork.observability import CorrelationFilter

logging.getLogger().addFilter(CorrelationFilter())
```

Fields that do not apply (for example `step_name` inside a workflow, or any of them
outside a run) are left unset rather than `None`; a JSON formatter picks up whatever is
there. `trace_id`/`span_id` come from the same propagated context described above, so
log lines and trace spans for the same step attempt share an id even without an
OpenTelemetry exporter configured.
