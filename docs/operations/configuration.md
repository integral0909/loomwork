# Configuration reference

## `EngineConfig`

Shared by the engine, workers and clients of one deployment. Pass the same object to
`Loomwork(config=...)`, `Worker(config=...)` and `Engine(config=...)`.

| Field | Default | Description |
| ----- | ------- | ----------- |
| `tick_lease` | 30s | How long a worker may hold a run while executing a tick before another worker may take over. Ticks are normally milliseconds; the lease only matters when a worker dies mid-tick. |
| `tick_retry` | 1s, ×2, max 5m, unlimited, jitter 0.2 | Backoff between attempts of a failing tick (nondeterminism, bugs). Attempts are unlimited so that a fixed deploy resumes the run. |
| `default_step_timeout` | 5m | Per-attempt timeout for steps that do not set one. |
| `default_step_retry` | `RetryPolicy()` | Retry policy for steps that do not set one (5 attempts, 1s initial, ×2, max 100s). |
| `step_lease_grace` | 30s | Added to a step's timeout (or heartbeat window) to form its lease, so a slow but live worker is not raced by a second attempt. |
| `replay_cache_size` | 1000 | Live workflow instances each worker keeps between ticks. `0` disables the cache: every tick replays the full history. |
| `poll_interval` | 1.0s | Upper bound on how long an idle dispatcher waits before checking for work when no notification arrives. |
| `timer_batch_size` | 100 | Timers fired, and outbox messages relayed, per store call. |
| `continue_as_new_after_events` | 10000 | History size at which `continue_as_new_suggested()` returns `True`. |
| `search_attributes` | empty | Declared search attribute types; queries may only reference declared attributes. |

## `WorkerOptions`

| Field | Default | Description |
| ----- | ------- | ----------- |
| `workflow_concurrency` | 4 | Workflow ticks executed concurrently. `0` disables workflow execution (a pure step worker). |
| `step_concurrency` | 16 | Step attempts executed concurrently across all queues. `0` disables step execution. |
| `run_timers` | `True` | Fire due timers. One timer firer per deployment is enough; more are safe. |
| `run_outbox` | `True` | Relay cross-run messages (child results, parent-close policies, workflow-to-workflow signals). |
| `run_schedules` | `True` | Evaluate schedules and start their runs. |
| `shutdown_grace` | 10s | Time in-flight work gets on `stop()` before it is cancelled and handed back. |
| `task_queues` | `None` (all) | Queues whose steps this worker executes. |
| `queue_limits` | `{}` | Per-queue `QueueLimit(rate, burst, concurrency)`; limited queues must be listed in `task_queues`. |

## Stores

Stores are created from URLs with `loomwork.store.store_from_url`:

| URL | Store |
| --- | ----- |
| `memory://` | In-process, not durable. Tests and demos. |
| `sqlite:///relative/path.db`, `sqlite:////absolute/path.db` | SQLite (`loomwork[sqlite]`). Query parameter: `busy_timeout` (seconds). |
| `sqlite://:memory:` | SQLite in memory. |
| `postgresql://user@host:5432/db` | PostgreSQL (`loomwork[postgres]`). Query parameters: `min_size`, `max_size` (pool), `listen` (`on`/`off`); anything else is passed to libpq, e.g. `sslmode=require`. |

Never put passwords in configuration files: use `PGPASSFILE`, environment variables or
your platform's secret store. `loomwork.store.redact_url` removes passwords from URLs
before they are logged.

### Sizing the PostgreSQL pool

A worker needs roughly one connection per concurrently executing tick or step attempt
that is inside a store call, plus one for each service loop (timers, outbox, schedules)
and one for `LISTEN`. Store calls are short, so `max_size` well below
`workflow_concurrency + step_concurrency` is usually enough; start with
`max(10, (workflow_concurrency + step_concurrency) // 2)` and watch
`loomwork_tick_duration_seconds` for queueing.

## Payload codecs

```python
from loomwork.codec import CodecChain, CompressionCodec, DataConverter, EnvironmentKeyRing
from loomwork.codec.encryption import EncryptionCodec

converter = DataConverter(
    codec=CodecChain([CompressionCodec(threshold=1024), EncryptionCodec(EnvironmentKeyRing())])
)
app = Loomwork(..., converter=converter)
```

Every process that reads or writes payloads (workers, clients, the CLI) needs the same
codec configuration. See [payload encryption](../payload-encryption.md).

## Telemetry

```python
from loomwork.observability import CompositeTelemetry, LoggingTelemetry, MetricsTelemetry

metrics = MetricsTelemetry()
app = Loomwork(..., telemetry=CompositeTelemetry([metrics, LoggingTelemetry()]))
text = metrics.registry.render()            # Prometheus exposition format
```

See [monitoring](monitoring.md).
