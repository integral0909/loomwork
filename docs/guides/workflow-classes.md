# Class-based workflows

A workflow that reacts to several kinds of messages reads better as a class: state lives
in attributes, and each signal, query and update is a method.

```python
import loomwork as lw


@lw.workflow(name="support.ticket")
class Ticket:
    def __init__(self, ticket_id: str) -> None:
        self.status = "open"
        self.comments: list[str] = []

    @lw.run
    async def run(self, ticket_id: str) -> str:
        try:
            await lw.wait_condition(lambda: self.status == "closed", timeout=timedelta(days=30))
        except TimeoutError:
            self.status = "expired"
        await notify_customer(ticket_id, self.status)       # a step
        return self.status

    @lw.signal
    def comment(self, text: str) -> None:
        self.comments.append(text)

    @lw.signal("close")
    async def close_ticket(self, reason: str) -> None:
        await archive_thread(reason)                        # handlers may run steps
        self.status = "closed"

    @lw.query
    def summary(self) -> dict[str, object]:
        return {"status": self.status, "comments": len(self.comments)}

    @lw.update
    def reopen(self) -> str:
        if self.status != "closed":
            raise lw.ApplicationError("ticket is not closed", type="NotClosed")
        self.status = "open"
        return self.status
```

## The pieces

| Decorator | On | Meaning |
| --------- | -- | ------- |
| `@lw.workflow` / `@lw.workflow(name=...)` | the class | declares the workflow; the name defaults to the class name |
| `@lw.run` | exactly one `async def` method | the entry point; receives the workflow's arguments |
| `@lw.signal` / `@lw.signal("name")` | a method taking one message | handles every message sent to that channel |
| `@lw.query` / `@lw.query("name")` | a plain method | answers queries; must only read state |
| `@lw.update` / `@lw.update("name")` | a plain or `async` method | handles updates; its result or error goes to the caller |

The workflow's arguments are passed to `@run`, and also to `__init__` if it accepts
parameters, so state is initialised before any handler runs, including handlers for
messages that were sent before the run's first tick.

Methods without a decorator are ordinary helpers. Every method of the class runs as
workflow code: the same determinism rules apply, and `loomwork lint` checks all of them.

## Handler semantics

- **Signals** are handled one message at a time per signal, in the order they arrived.
  An `async` handler finishes, including any steps it runs, before the next message of
  the same signal is handled. Different signals are handled concurrently.
- **Queries** run against the state after the last completed tick, including on closed
  runs.
- **Updates** run as workflow code. An error raised by the handler is returned to the
  caller and does not fail the run.
- Because an async handler can still be running when the next request arrives, an update
  can see state from before a signal's handler finished. When order matters, have the
  update wait for it: `await lw.wait_condition(lambda: len(self.items) == expected)`.

## `wait_condition`

```python
await lw.wait_condition(lambda: self.approved or self.rejected, timeout=timedelta(days=3))
```

The predicate is evaluated immediately and again after every signal or update handler
finishes. It must only read workflow state. `timeout` is a durable timer; when it
elapses, `TimeoutError` is raised. If other workflow code changes state that a condition
depends on, call `lw.notify_conditions()` afterwards so waiting conditions re-check it.

`wait_condition` works in function-style workflows too, for state changed by update
handlers.

## From the client

Attributes of the decorated class name its handlers, so they can be passed directly to a
workflow handle:

```python
handle = await client.start(Ticket, "T-1001", id="ticket-T-1001")
await handle.signal(Ticket.comment, "customer called back")   # the channel, type-checked
print(await handle.query(Ticket.summary))                      # "summary"
await handle.update(Ticket.reopen)                             # "reopen"
```

`Ticket.comment` is the signal's `lw.Channel`, whose type comes from the handler's
parameter annotation, so the value is validated before it is sent. Query and update
attributes are their names. Other attributes fall through to the class.

## Histories and versioning

A class-based workflow records exactly the same events as the equivalent function with
nested handlers, so the two styles can be mixed freely and converted into each other
without breaking running workflows, as long as the sequence of commands stays the same.
The code fingerprint recorded with each tick covers every method of the class, not only
`@run`, since a change to any handler can change what the workflow does. The usual
versioning tools (`lw.patched()`, `loomwork compat`) apply unchanged; see
[versioning](versioning.md).
