# Child workflows and sagas

## Child workflows

A workflow can start other workflows as **children**. Children have their own history,
so they are the way to split a large process into independently observable parts, fan
work out across many runs, or reuse a workflow from several parents.

The simplest form calls the workflow like a function. It starts the child and waits for
its result:

```python
@lw.workflow
async def onboard(customer: Customer) -> None:
    account = await create_account(customer)            # a child workflow
    await send_welcome_pack(account)
```

`lw.start_child` returns a handle, which lets you start many children, signal them, and
choose what happens to them if the parent finishes first:

```python
@lw.workflow
async def import_catalog(pages: int) -> int:
    handles = [
        lw.start_child(import_page, page, id=f"catalog-page-{page}")
        for page in range(pages)
    ]
    counts = await asyncio.gather(*handles)
    return sum(counts)
```

- **Ids.** A child's workflow id defaults to `<parent id>/<child name>/<n>`. Its run id is
  derived from the parent's run id, so both are known immediately.
- **Failures.** A child that fails, is cancelled or is terminated raises
  `ChildWorkflowError` in the parent; `exc.status` tells which, and `__cause__` holds the
  child's own failure. A child whose workflow id is already running fails to start with
  type `ChildWorkflowAlreadyStarted`.
- **Atomicity.** The child is created in the same transaction as the parent's decision
  to start it.
- **Continue-as-new.** A child that continues as new keeps its place: the parent receives
  the result of the final run in the chain.

### Parent-close policies

| Policy           | When the parent closes first, the child is... |
| ---------------- | --------------------------------------------- |
| `terminate`      | terminated (default)                          |
| `request_cancel` | asked to cancel; it may run compensation logic |
| `abandon`        | left running                                  |

```python
lw.start_child(nightly_cleanup, parent_close_policy="abandon")
```

Policies are applied through the engine's outbox shortly after the parent closes
([ADR-0004](../adr/0004-cross-run-outbox.md)).

## Sagas

A saga is a sequence of steps where each successful step has a compensating action
that undoes it if a later step fails. `lw.Compensations` keeps the undo stack:

```python
@lw.workflow
async def book_trip(trip: Trip) -> str:
    undo = lw.Compensations()
    try:
        flight = await book_flight(trip)
        undo.add(cancel_flight, flight)
        hotel = await book_hotel(trip)
        undo.add(cancel_hotel, hotel)
        await charge(trip)
    except (lw.StepError, asyncio.CancelledError):
        await undo.run()            # runs cancel_hotel, then cancel_flight
        raise
    return "booked"
```

Compensations are ordinary steps, so they are durable and retried like any other step.
If some compensations fail, the others still run and `undo.run()` raises an
`ExceptionGroup` at the end. `Compensations(parallel=True)` runs them concurrently.

Because cancellation is delivered as `asyncio.CancelledError`, the same handler also
rolls back when the run is cancelled by an operator.
