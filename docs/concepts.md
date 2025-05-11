# Concepts

This page explains how Loomwork makes ordinary `async` functions durable, and what that
means for the code you write. Read it once before writing your first real workflow; the
rules at the end follow directly from the model.

## Workflows, steps and runs

A **workflow** is an `async def` function decorated with `@loomwork.workflow`. It
orchestrates: it decides what happens next, waits for things, and returns a result.

A **step** is a function decorated with `@loomwork.step`. It does the actual work that
touches the outside world: HTTP calls, database writes, sending email. Steps may be
`async def` or plain `def` (plain functions run in a thread).

A **run** is one execution of a workflow. It has a **run id** (unique, generated) and a
**workflow id** (chosen by you, for example `order-42`). Only one run per workflow id can
be running at a time, which makes starting a workflow idempotent from the caller's point
of view.

## The history

Every run owns an append-only **history** of events. Two kinds matter most:

- **Input events** describe things that happened *to* the workflow: it was started, a
  step completed or failed, a timer fired, a message arrived, cancellation was requested.
- **Decision events** record what the workflow *decided*: schedule this step, start this
  timer, start this child, complete with this result.

The history is the single source of truth. A run's status, its pending steps and timers
are all projections of it.

```
 1  workflow_started      args=["order-42", 1999]
 2  tick_completed        consumed_through=1
 3  step_scheduled        seq=1 step=charge_card
 4  step_completed        seq=1 result="ch_123"
 5  tick_completed        consumed_through=4
 6  timer_started         seq=1 fire_at=...+1h
 7  timer_fired           seq=1
 8  tick_completed        consumed_through=7
 9  step_scheduled        seq=2 step=ship
10  step_completed        seq=2
11  tick_completed        consumed_through=10
12  workflow_completed    result="shipped"
```

## Ticks and replay

Workflow code runs in **ticks**. A tick delivers every input the workflow has not seen
yet, runs the code until it cannot make progress (it is waiting for a step, a timer or a
message), and commits the decisions it made, all in one atomic write.

The workflow's memory is not persisted between ticks. Instead, whenever a worker needs
to continue a run it does not have in memory (after a restart, or on a different machine),
it **replays** the history: it runs the workflow function from the beginning and feeds it
the recorded inputs. When the code reaches `await charge_card(...)`, the step is not
executed again. Its recorded result is returned immediately. When the history is
exhausted, the code continues live and new decisions are recorded.

Workers keep recently used workflow instances in memory (the **replay cache**), so
usually only the new events are fed. Replay is the fallback, not the common path.

## Determinism

Replay only works if the workflow code makes the **same decisions** when it sees the same
inputs. During replay Loomwork checks every decision the code makes against the recorded
decision events; a mismatch is a **nondeterminism error**. The tick fails and is retried
later, so a fixed deploy can resume the run without losing it.

Loomwork makes the common sources of non-determinism safe or impossible:

| Instead of                    | Use                                     | Why                              |
| ----------------------------- | --------------------------------------- | -------------------------------- |
| `datetime.now()`, `time.time()` | `loomwork.now()`                      | The tick's recorded time         |
| `time.sleep()`                | `loomwork.sleep()` or `asyncio.sleep()` | Becomes a durable timer          |
| `random.random()`, `uuid.uuid4()` | `loomwork.random()`, `loomwork.uuid4()` | Seeded per run                 |
| HTTP, files, databases        | a step                                  | Executed once, result recorded   |
| `os.environ`                  | a step, or a workflow argument          | May differ between replays       |
| iterating a `set`             | `sorted(...)`                           | Order depends on the hash seed   |

The workflow's event loop rejects I/O (`run_in_executor`, sockets, subprocesses) at
runtime, even if the code catches the error. The [determinism linter](lint-rules.md)
finds the rest statically:

```bash
python -m loomwork.lint src/
```

## Time

`loomwork.now()` returns the time at which the current tick started. It only moves
between ticks, never backwards, and is identical on replay. `asyncio.timeout`,
`asyncio.wait_for` and `asyncio.sleep` inside workflows are all durable: they create
timers that are recorded in the history and fired by the engine.

## Failures

- A **step failure** is retried according to its retry policy. When retries are
  exhausted, the workflow receives a `StepError` (whose `__cause__` is the step's last
  error) and can handle it like any exception.
- An `ApplicationError`, `StepError`, `ChildWorkflowError` or `TimeoutError` that escapes
  the workflow **fails the run**. That is a recorded, final outcome.
- **Any other exception** escaping the workflow (a `KeyError` from a bug, say) fails only
  the current **tick**. The run stays open and the tick is retried with backoff, so
  deploying a fix resumes the run. Use `@workflow(fail_on=[...])` to make other
  exception types final.

## Delivery guarantees

- Workflow decisions are recorded **exactly once**: a tick commits atomically or not at
  all.
- Steps run **at least once**. A worker can crash after a step's side effect but before
  its result is recorded; the step then runs again. Make steps idempotent, for example
  with `loomwork.step_info().idempotency_key`, which is stable across retries.
- Messages sent with a `message_id` are delivered to the workflow once, even if sent
  twice.

## Where to go next

- [Workflows and steps](guides/workflows.md)
- [Messaging: channels, queries and updates](guides/messaging.md)
- [ADR-0002](adr/0002-tick-delimited-history.md) for the precise replay algorithm
