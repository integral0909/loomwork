# Order fulfillment

An e-commerce saga: reserve inventory, charge the customer, then ship -- unwinding
cleanly if anything goes wrong along the way, and giving the customer a window to
change their mind before the parcel goes out.

## Scenario

`process_order` (in `workflows.py`) runs an `Order` through three steps, each backed by
a small in-memory stand-in for an external system (`steps.py`):

1. **Reserve inventory** at a warehouse.
2. **Charge the customer's card** through a payment gateway that fails the first two
   attempts for every charge (simulating a flaky network) before succeeding on the
   third -- unless the card is on a permanent decline list, in which case it never
   succeeds. The step is scheduled with a `RetryPolicy`, so Loomwork retries it
   automatically; the gateway itself is keyed by the step's
   [`step_info().idempotency_key`](../../src/loomwork/info.py), so a retried attempt
   that actually reached the gateway before losing its response is never charged twice.
3. Give the customer up to `cancellation_window` (two hours, by default) to cancel
   through a typed `Channel` before the order ships.
4. **Ship** the order and return a tracking number.

If a step fails permanently (its retries are exhausted, or it raises a non-retryable
`ApplicationError` such as a declined card), the workflow runs every compensation
registered so far, in reverse order, via `loomwork.Compensations` -- releasing the
warehouse reservation and, if the charge had already gone through, refunding it. The
same unwind runs if the customer cancels: nothing about "undo what already succeeded"
needs to know *why* the order didn't ship.

A `@loomwork.query("status")` handler exposes the order's current stage
(`reserving` -> `charging` -> `awaiting_shipment` -> `shipped`/`cancelled`/`failed`) so a
caller (or, here, the demo script) can watch progress without waiting for the final
result.

## Loomwork features used

- `@loomwork.step` with a `RetryPolicy` and `step_info().idempotency_key` for a durable,
  idempotent call to an unreliable external system.
- `loomwork.ApplicationError(..., non_retryable=True)` for a business failure that
  should never be retried.
- `loomwork.Channel` for the customer's cancellation signal, raced against a durable
  timeout with `asyncio.timeout`.
- `loomwork.Compensations` for saga-style rollback.
- `@loomwork.query` for read-only progress into a running workflow.
- Pydantic models (`Order`, `OrderResult`, `LineItem`) as workflow arguments and
  results, converted automatically by the default `DataConverter`.

## Running it

```console
uv run python -m examples.order_fulfillment.main
```

The demo uses `loomwork.ManualClock` instead of the real wall clock. The workflow code
itself uses realistic durations (a two-hour cancellation window, real retry backoffs);
the demo simply advances the manual clock through them with
`examples.support.advance_until`, so all three scenarios settle in well under a second
of real time instead of taking hours.

## Tests

`tests/examples/test_order_fulfillment.py` drives the workflow directly (against a
`Loomwork` app backed by `MemoryStore` and a `ManualClock`) and checks:

- the happy path ships and charges exactly once despite two gateway failures;
- a mid-flight cancellation refunds the charge and releases the reservation;
- a declined card fails without ever charging the customer, and releases the
  reservation that had already been made.
