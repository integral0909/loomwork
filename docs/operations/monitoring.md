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

Expose them from your application's HTTP server:

```python
metrics = MetricsTelemetry()
app = Loomwork(..., telemetry=metrics)

async def metrics_endpoint(request):
    return Response(metrics.registry.render(), media_type=MetricsRegistry.CONTENT_TYPE)
```

### Store-level health

Some signals live in the store rather than in one worker:

- **Outbox backlog**: `await store.outbox_size()` should stay near zero; a growing
  outbox means no worker has `run_outbox=True`.
- **Stuck runs**: `await client.query_runs("status = 'running' AND tick_failures > 5")`
  finds runs whose ticks keep failing; `last_tick_error` explains why.
- **Old running runs**: `created_at < now() - interval '30 days' AND status = 'running'`.

## Tracing

`OpenTelemetryTelemetry` (install `loomwork[otel]`) creates:

- a `workflow.tick <name>` span per tick with its real duration and outcome;
- a `step <name>` span per attempt, made current while the step's code runs, so
  instrumented HTTP clients and database drivers inside the step become its children.

Configure exporters with the OpenTelemetry SDK as usual; Loomwork only uses the API.

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
