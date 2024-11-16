# ADR-0002: Tick-delimited history

- Status: accepted
- Date: 2024-11-16

## Context

Replay must reproduce not only *which* inputs a workflow saw but *which inputs it saw
together*. Code such as

```python
done, _ = await asyncio.wait({a, b}, return_when=asyncio.FIRST_COMPLETED)
```

behaves differently if the results of `a` and `b` were delivered in the same activation
or in two separate ones. The history therefore has to record activation boundaries.

Inputs can be appended at any time (a step finishes while a worker is busy processing
the workflow), so the boundary cannot simply be "everything before the decisions".

## Decision

A **tick** is one activation of the workflow:

1. A workflow worker leases the run and loads its history.
2. It delivers every input event the workflow has not yet consumed, at a single workflow
   time, and runs the event loop until no callback is ready.
3. It commits a `TickCompleted` event followed immediately by the decision events
   produced from the tick's commands, in **one atomic append**.

`TickCompleted` records:

- `consumed_through`: the id of the last input event delivered in this tick. Inputs
  appended while the worker was busy have higher ids and are delivered by the next tick.
- `time`: the workflow time used for the tick (`workflow.now()`), which also seeds
  `loop.time()`.

Nothing is written when a tick starts. A tick that crashes or fails leaves no trace in the
history; its lease expires and another worker retries it. Tick failures (for example a
nondeterminism error) are recorded on the execution record, not in the history.

### Replay algorithm

```
pending_inputs = []
for event in history:
    if event is input:
        pending_inputs.append(event)
    elif event is TickCompleted(consumed_through, time):
        batch = [e for e in pending_inputs if e.id <= consumed_through]
        remove batch from pending_inputs
        commands = instance.activate(time, batch)
        expected = decision events following this TickCompleted
        match(commands, expected)            # NondeterminismError on mismatch
final live tick: activate(now, pending_inputs) -> new commands
```

## Consequences

- Exactly one write per tick; commits cannot conflict with concurrently appended inputs.
- The execution record tracks `consumed_through` and a `tick_pending` flag, set whenever
  an input arrives with a higher id.
- Workers can cache live workflow instances between ticks: if the cached instance has
  processed history up to event *N*, only events after *N* need to be fed.
