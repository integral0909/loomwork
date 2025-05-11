# Workflows and steps

## Declaring workflows

```python
import loomwork as lw


@lw.workflow(name="billing.monthly_invoice")
async def monthly_invoice(customer_id: str, month: str) -> Invoice:
    usage = await collect_usage(customer_id, month)
    invoice = await render_invoice(customer_id, usage)
    await send_invoice(invoice)
    return invoice
```

- **Names are part of the history.** They default to the function name; set them
  explicitly for anything long-lived so renaming the Python function is safe.
- **Arguments and results** are converted to JSON payloads. Pydantic models,
  dataclasses, `datetime`, `UUID`, `Decimal`, enums, lists and dicts all round-trip, and
  are decoded into the annotated parameter types.
- **Keyword-only parameters are not supported**: arguments are recorded positionally.
- `@workflow(fail_on=[InvalidInvoice])` makes additional exception types fail the run
  instead of just the tick (see [failures](../concepts.md#failures)).

## Declaring steps

```python
@lw.step(
    name="billing.collect_usage",
    retry=lw.RetryPolicy(initial_interval=1, backoff=2, max_interval=60, max_attempts=10),
    timeout=30,            # per attempt, seconds or timedelta
    total_timeout=600,     # across all attempts and backoff delays
)
async def collect_usage(customer_id: str, month: str) -> Usage:
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{USAGE_API}/{customer_id}/{month}")
        response.raise_for_status()
        return Usage.model_validate(response.json())
```

Calling a step inside a workflow schedules it and returns an awaitable for its recorded
result. Calling it anywhere else runs the function directly, so steps stay easy to unit
test.

### Retries

`RetryPolicy` controls retries with exponential backoff:

| Field                 | Default | Meaning                                           |
| --------------------- | ------- | ------------------------------------------------- |
| `initial_interval`    | 1s      | Delay before the second attempt                   |
| `backoff`             | 2.0     | Multiplier per failed attempt                     |
| `max_interval`        | 100s    | Upper bound for one delay                         |
| `max_attempts`        | 5       | Total attempts including the first; `0` = unlimited |
| `jitter`              | 0.1     | Fraction of each delay randomised                 |
| `non_retryable_types` | none    | Failure types that stop retrying immediately      |

Raise `ApplicationError(..., non_retryable=True)` from a step for failures that retrying
cannot fix (a declined card, invalid input).

### Per-call options

```python
invoice = await render_invoice.with_options(timeout=120, priority=10)(customer_id, usage)
```

### Idempotency

Steps run at least once. Use the idempotency key, which is stable across retries of the
same scheduled step:

```python
@lw.step
async def charge(order_id: str, cents: int) -> str:
    key = lw.step_info().idempotency_key          # "<run_id>:<seq>"
    return await payments.create_charge(order_id, cents, idempotency_key=key)
```

### Long-running steps and heartbeats

```python
@lw.step(timeout=timedelta(hours=6), heartbeat_timeout=60)
async def reindex(collection: str) -> int:
    start = lw.heartbeat_details() or 0          # resume after a crash
    for batch in batches(collection, start):
        await index(batch)
        await lw.heartbeat(batch.end)           # cheap; throttled automatically
    return total
```

`heartbeat_timeout` makes a silent attempt count as lost after a minute instead of six
hours, and heartbeats tell the step when its workflow no longer wants the result: the
step then sees `asyncio.CancelledError` at its next `await`.

## Time inside workflows

```python
await lw.sleep(timedelta(days=3))           # durable
await lw.sleep_until(invoice.due_date)      # durable
async with asyncio.timeout(3600):           # durable deadline
    decision = await approvals.receive()
```

`lw.now()` is the tick's recorded time. `lw.random()` and `lw.uuid4()` are seeded per run
and identical on replay. `lw.logger` suppresses log records while replaying, so each line
is logged once.

## Concurrency inside a workflow

Standard asyncio composition works:

```python
charges = await asyncio.gather(*(charge(item) for item in order.items))

async with asyncio.TaskGroup() as group:
    group.create_task(notify_warehouse(order))
    group.create_task(notify_customer(order))

done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
```

Steps started together are scheduled in the same tick and run in parallel on workers.

## Starting and awaiting runs

```python
async with lw.Loomwork(store=store, workflows=[monthly_invoice], steps=[...]) as app:
    handle = await app.start_workflow(monthly_invoice, "cust-7", "2024-05", id="invoice-cust-7-2024-05")
    invoice = await handle.result(timeout=60)

    # Later, from anywhere with access to the store:
    handle = app.get_handle("invoice-cust-7-2024-05")
    record = await handle.describe()        # status, timestamps, failure
    history = await handle.history()        # recorded events
    await handle.cancel("customer request") # cooperative: the workflow sees CancelledError
    await handle.terminate("stuck")         # immediate, no workflow code runs
```

`handle.result()` raises `WorkflowFailedError`, `WorkflowCanceledError` or
`WorkflowTerminatedError` when the run did not complete.

## Cancellation

`handle.cancel()` delivers `asyncio.CancelledError` into the workflow at its current
`await`. The workflow may clean up, including running compensating steps, before it
finishes:

```python
try:
    await lw.sleep(timedelta(days=30))
except asyncio.CancelledError:
    await release_reservation(reservation_id)
    raise
```

## Long-lived workflows

Histories grow with every step. A workflow that runs for months, or loops over millions
of items, should periodically **continue as new**: it ends the run and starts a fresh one
with the same workflow id and an empty history.

```python
@lw.workflow
async def subscription(customer_id: str, cycle: int) -> None:
    await bill(customer_id, cycle)
    await lw.sleep(timedelta(days=30))
    lw.continue_as_new(customer_id, cycle + 1)
```

`lw.continue_as_new_suggested()` becomes true once the history passes the engine's
`continue_as_new_after_events` threshold.
