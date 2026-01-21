# Support ticket

A support ticket with SLA timers, escalation and a customer reply thread -- modeled as
a **class-based workflow**, where a signal, a query and an update each read naturally as
a method instead of a branch inside one long function.

## Scenario

`Ticket` (in `workflows.py`) is a workflow class:

- `__init__` and `@lw.run`'s `run` both receive the ticket's starting arguments
  (`ticket_id`, `subject`, `assignee`); state -- `status`, `assignee`, the reply thread --
  lives in attributes.
- `run` waits for the ticket to close with `lw.wait_condition(lambda: self.status ==
  "closed", timeout=...)`, once for each of two SLA stages:
  1. `FIRST_RESPONSE_SLA` (four hours). If it lapses, the ticket **escalates**: an
     `escalate_ticket` step pages the on-call lead, who becomes the new assignee.
  2. The remainder of `RESOLUTION_SLA` (three days total). If that also lapses without
     a `close`, the ticket closes itself as `"expired"`.
  A reply alone never closes a ticket -- only an explicit `close` signal (an agent's or
  the customer's decision) does -- so the SLA clock keeps running even through a reply
  that does not resolve the issue.
- `@lw.signal reply(text)` appends to the reply thread.
- `@lw.signal("close") close_ticket(reason)` closes the ticket, which
  `lw.wait_condition`'s predicate notices as soon as the signal handler finishes.
- `@lw.query summary()` reports status, assignee, reply count and whether the ticket
  escalated -- readable at any time, including after the ticket has closed.
- `@lw.update reassign(new_assignee)` hands the ticket to someone else, **validating**
  the request first: a blank assignee, reassigning to the current assignee, or
  reassigning a closed ticket all raise an `ApplicationError` that goes back to the
  caller of the update without failing the run.

`main.py` runs four scenarios: closed quickly (no escalation), escalated then closed,
left unanswered until it expires, and reassigned (with both a valid reassignment and two
rejected ones).

## Loomwork features used

- `@lw.workflow` on a **class**, with `@lw.run`, `@lw.signal`, `@lw.signal("name")`,
  `@lw.query` and `@lw.update` (see `docs/guides/workflow-classes.md`).
- `lw.wait_condition(predicate, timeout=...)` for a durable wait on workflow state,
  chained across two SLA stages.
- Sending a message through the class's own attributes from the client
  (`handle.signal(Ticket.reply, ...)`, `handle.query(Ticket.summary)`,
  `handle.update(Ticket.reassign, ...)`), type-checked against each handler's own
  parameter annotation.
- `lw.ApplicationError` raised from an update handler, surfaced to the caller as
  `lw.UpdateFailedError` without touching the run's outcome.

## Running it

```console
uv run python -m examples.support_ticket.main
```

Uses `loomwork.ManualClock`: the workflow code itself uses realistic SLA durations (a
four-hour first-response window, a three-day resolution window), and the demo advances
the manual clock through them with `examples.support`'s helpers, so all four scenarios
settle in well under a second of real time.

## Tests

`tests/examples/test_support_ticket.py` uses `loomwork.testing.WorkflowEnvironment`
(automatic time skipping, so no manual clock driving is needed) and `env.mock_step` to
check:

- a ticket closed before the first-response SLA never escalates, and the mocked
  `escalate_ticket` step is never called;
- a ticket that misses the first-response SLA escalates exactly once, to the assignee
  the (mocked) `escalate_ticket` step returns, and can still be closed afterwards;
- a ticket nobody ever answers expires, and `notify_customer` is called with
  `"expired"`;
- `reassign` accepts a valid new assignee and rejects a blank one, a no-op
  reassignment, and a reassignment after the ticket has closed, each with the specific
  `ApplicationError` type the workflow raises.
