# Task queues

By default every worker executes every step it has code for. **Task queues** route steps
to specific workers: machines with GPUs, workers holding payment credentials, a pool
sized for a rate-limited partner API.

## Routing steps

```python
@lw.step(queue="gpu", timeout=timedelta(minutes=30), heartbeat_timeout=60)
async def render_video(job: RenderJob) -> str: ...


@lw.step(queue="payments")
async def charge(order: Order) -> str: ...
```

Workers subscribe to queues:

```python
# General-purpose workers: workflows and default-queue steps.
Worker(store, registry, options=WorkerOptions(task_queues=("default",)))

# GPU machines: only render steps, no workflow ticks.
Worker(store, Registry(steps=[render_video]),
       options=WorkerOptions(task_queues=("gpu",), workflow_concurrency=0))
```

`task_queues=None` (the default) means every queue. A step's queue can also be chosen
per call: `await render_video.with_options(queue="gpu-large")(job)`.

## Priorities

Within a queue, higher priorities are claimed first (`-1000` to `1000`, default `0`):

```python
await send_email.with_options(priority=100)(password_reset)
await send_email.with_options(priority=-50)(newsletter)
```

## Rate and concurrency limits

Limits protect downstream systems from a worker's bursts. They are per worker; a
fleet-wide limit is the per-worker limit times the number of workers.

```python
from loomwork.worker.limits import QueueLimit

WorkerOptions(
    task_queues=("default", "partner-api"),
    queue_limits={"partner-api": QueueLimit(rate=5, burst=5, concurrency=2)},
)
```

`rate` is attempts per second (a token bucket holding up to `burst`), `concurrency` is
attempts running at once. Every limited queue must be listed in `task_queues`.

## Heartbeats and lost workers

Each attempt holds a **lease** on its job. If a worker dies, the lease expires and another
worker retries the step.

- Without `heartbeat_timeout` the lease covers the whole attempt timeout, so a lost
  attempt is detected only when `timeout` (plus a grace period) has passed.
- With `heartbeat_timeout` the lease covers one heartbeat interval and every
  `loomwork.heartbeat()` renews it, so a lost attempt of a six-hour step is detected
  within a minute.

Heartbeat details are stored and handed to the next attempt through
`loomwork.heartbeat_details()`, which is how a retried long step resumes from its last
checkpoint instead of from the beginning.

## Graceful shutdown

`worker.stop(grace)` stops claiming new work and gives in-flight attempts `grace`
seconds. Attempts still running after that are cancelled and handed back without
counting against their retry policy, so another worker can start them immediately.
