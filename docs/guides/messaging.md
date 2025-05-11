# Messaging: channels, queries and updates

Running workflows can receive messages (**channels**), answer questions (**queries**)
and accept requests that change them and return a result (**updates**).

| Mechanism | Direction        | Changes the run | Caller gets          | Recorded in history |
| --------- | ---------------- | --------------- | -------------------- | ------------------- |
| Channel   | caller → workflow | yes             | nothing (fire and forget) | yes           |
| Query     | caller ← workflow | no              | the handler's answer | no                  |
| Update    | caller ↔ workflow | yes             | the handler's result or error | yes        |

## Channels

A channel is a typed mailbox, declared next to the workflow that reads it:

```python
class Approval(pydantic.BaseModel):
    approved: bool
    by: str


approvals = lw.Channel("approvals", Approval)


@lw.workflow
async def expense_report(report: Report) -> str:
    try:
        async with asyncio.timeout(timedelta(days=3).total_seconds()):
            decision = await approvals.receive()
    except TimeoutError:
        return "expired"
    return "approved" if decision.approved else "rejected"
```

Sending:

```python
await handle.signal(approvals, Approval(approved=True, by="kim"))
await handle.signal(approvals, {"approved": True, "by": "kim"}, message_id="approval-7")
```

- Values are **validated** against the channel type before they are sent.
- Messages that arrive before the workflow calls `receive()` are **buffered**.
- A `message_id` makes delivery **idempotent**: a second message with the same id is
  ignored by the workflow.
- `channel.pending` and `channel.receive_nowait()` inspect and drain the buffer;
  `async for msg in channel.stream()` receives forever.

### Signal-with-start

Deliver a message to a workflow, starting it first if it is not running. The start and
the message are atomic:

```python
handle = await client.signal_with_start(
    shopping_cart, customer_id, id=f"cart-{customer_id}", channel=cart_events, value=event
)
```

### Between workflows

```python
@lw.workflow
async def order(order_id: str) -> None:
    ...
    inventory_events.send_to("inventory-manager", Reserved(order_id=order_id))
```

The send is recorded in the sender's history and delivered through the engine's outbox,
so it happens exactly once from the sender's point of view even if a worker crashes.

## Queries

Queries read workflow state without changing it. Register handlers inside the workflow
body so that replay re-registers them:

```python
@lw.workflow
async def import_job(source: str) -> int:
    processed = 0

    @lw.query("progress")
    def progress() -> dict[str, int]:
        return {"processed": processed}

    for page in range(100):
        processed += await import_page(source, page)
    return processed
```

```python
print(await handle.query("progress"))
```

Queries are answered by replaying the committed history in the calling process, so they
need the workflow code there (`Client(engine, registry=...)`, which `Loomwork` sets up
automatically). They also work on closed runs, returning the state at completion.
Handlers must be plain functions: anything that would schedule work is rejected.

## Updates

Updates change a running workflow and return a result:

```python
@lw.workflow
async def cart(customer_id: str) -> list[str]:
    items: list[str] = []
    checked_out = False

    @lw.update("add")
    async def add(item: str) -> int:
        if checked_out:
            raise lw.ApplicationError("cart is closed", type="CartClosed")
        await reserve_stock(item)          # updates may run steps and sleep
        items.append(item)
        return len(items)

    @lw.update
    def checkout() -> list[str]:
        nonlocal checked_out
        checked_out = True
        return items

    while not checked_out:
        await lw.sleep(timedelta(minutes=1))
    return items
```

```python
count = await handle.update("add", "book", timeout=10)
try:
    await handle.update("add", "pen")
except lw.UpdateFailedError as exc:
    print(exc.failure.type)              # "CartClosed"
```

- An error raised by an update handler is returned to the caller; it does **not** fail
  the workflow.
- Passing the same `update_id` twice runs the handler once; the second call waits for the
  same outcome. Use this to retry safely after a timeout.
- Updates that arrive before their handler is registered wait for it. If the workflow
  closes without handling an update, the caller gets `UpdateFailedError` with type
  `UpdateNotHandled`.
