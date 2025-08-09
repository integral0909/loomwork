# Expense approval

A human-in-the-loop approval flow: a manager has three days to approve or reject an
expense report, nudged by a reminder every 24 hours; if the window lapses without a
decision, the report escalates to a director for the same three-day, reminder-backed
window. The employee can attach receipts at any point before a decision is recorded.

## Scenario

`expense_report` (in `workflows.py`) waits on a typed `Channel` for a `Decision`,
racing it against a durable deadline built from `asyncio.timeout`. The waiting and
reminder logic lives in `_await_decision`, a plain (undecorated) helper function that
the workflow calls twice -- once for the manager, once for the director if the manager
never answers:

```python
while remaining > timedelta(0):
    wait = min(remaining, REMINDER_INTERVAL)
    try:
        async with asyncio.timeout(wait.total_seconds()):
            return await decision_channel.receive()
    except TimeoutError:
        remaining -= wait
        if remaining > timedelta(0):
            await send_reminder(report_id, approver)
```

Because it only sleeps, waits on a channel and calls steps -- all of which are safe to
replay -- it needs no decorator of its own; the determinism linter follows the call
from the workflow into it the same way it would follow any other same-module helper.

While a decision is pending, `@loomwork.update("add_receipt")` lets the employee attach
a `Receipt` (this runs as ordinary workflow code, so it can validate against the
in-flight state), and `@loomwork.query("status")` reports the current stage and how
many receipts have been attached.

## Loomwork features used

- `loomwork.Channel` for the approval decision, raced against a durable timeout with
  `asyncio.timeout` -- the same pattern used for the reminder loop.
- `@loomwork.update` for adding a receipt while the workflow is still running.
- `@loomwork.query` for the current approval stage.
- A same-module helper function called from workflow code (not a step): the
  determinism linter checks it exactly as if its code were inlined into the workflow.

## Running it

```console
uv run python -m examples.expense_approval.main
```

The workflow's own logic uses the real durations described above (a three-day window,
24-hour reminders) -- nothing is shortened. The demo drives a `loomwork.ManualClock`
through those durations with `examples.support.advance_until`, so all three scenarios
(quick approval, escalation after two missed reminders, and an expired report that
nobody ever answers) finish in a fraction of a second of real time.

## Tests

`tests/examples/test_expense_approval.py` checks:

- a manager's prompt approval, including a receipt attached beforehand;
- escalation to the director after the manager's three-day window lapses, and the
  director's approval afterwards;
- a report that nobody ever answers, expiring after both windows lapse.
