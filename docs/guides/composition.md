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

### Id reuse and conflict policies for children

`lw.start_child` takes the same `id_reuse_policy` and `id_conflict_policy` arguments as
`start_workflow` (see [id reuse and conflict policies](workflows.md#id-reuse-and-conflict-policies)
for the policies themselves), governing the child's workflow id exactly the same way --
with one difference. Starting a top-level run can raise straight into the caller;
starting a child cannot, since nothing synchronously awaits the store call that creates
it. Every outcome besides a plain create is instead recorded as a decision in the
*parent's* history and delivered to it as an ordinary child failure or result, so replay
sees the same thing every time:

```python
try:
    handle = lw.start_child(
        send_receipt, order, id=f"receipt-{order.id}",
        id_reuse_policy=lw.IdReusePolicy.REJECT_DUPLICATE,
    )
    await handle
except lw.ChildWorkflowError as exc:
    if exc.type == "ChildWorkflowIdReuseRejected":
        ...  # a receipt for this order was already sent
```

| Outcome | What the parent sees |
| ------- | --------------------- |
| `id_conflict_policy=FAIL` and the id is running | `ChildWorkflowError` (`type="ChildWorkflowAlreadyStarted"`), as today |
| `id_reuse_policy` rejects a closed id | `ChildWorkflowError` (`type="ChildWorkflowIdReuseRejected"`) |
| `id_conflict_policy=USE_EXISTING` | the *existing* run's eventual result or failure, once it closes |
| `id_conflict_policy=TERMINATE_EXISTING` | the *new* run's own result or failure, as usual |

**`USE_EXISTING` does not create anything.** The run it finds does not have to be one of
this workflow's own children, or a child at all -- it can be any run (started by another
parent, or a plain top-level `start_workflow`) that happens to occupy the target workflow
id right now. Attaching only asks to be told the result; it does **not** confer
ownership: the attaching parent's own parent-close policy has no effect on that run (it
was never *its* child), and if the run was itself started as someone else's child, that
other parent's close policy is what still governs it. If the run continues as new (or is
retried, or reset) before it finally closes, the attachment follows the chain to whichever
run closes it. If the target already closed by the time the attachment is resolved (a
race with the read that decided to attach), the parent gets a fresh, ordinary child
instead, exactly as `id_conflict_policy=FAIL` would when nothing is running -- because at
that point nothing *is* running for the id, and the default `id_reuse_policy` allows it.

**`TERMINATE_EXISTING` terminates the running execution and starts the child in its
place**, atomically: no worker ever observes both active for the id, the same guarantee
`start_workflow`'s own `TERMINATE_EXISTING` makes.

### Versioning policy for children

A child resolves its own [versioning policy](versioning.md#patching-vs-pinning-which-to-use)
independently of its parent -- from engine configuration, exactly as a top-level
`start_workflow` call would -- unless overridden explicitly:

```python
lw.start_child(rewrite_heavy_workflow, versioning_policy=lw.VersioningPolicy.PINNED)
```

A `PINNED` parent commonly starts `AUTO_UPGRADE` children, and vice versa: coupling them
would make one workflow's deploy strategy dictate an unrelated one's (see
[ADR-0006](../adr/0006-worker-build-versioning.md)).

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
