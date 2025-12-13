# Internals

This page is for people changing Loomwork itself: how the pieces fit, where each concern
lives, and the invariants a change must keep. The design decisions behind it are in the
[ADRs](adr/); the user-facing model is in [concepts](concepts.md).

## Layers

```
 sdk/            @workflow, @step, channels, queries, updates, lw.sleep ...   (user API)
 runtime/        deterministic event loop, WorkflowInstance, Replayer         (runs workflow code)
 engine/         Engine, TickProcessor, StepExecutor, config, replay cache    (orchestration)
 worker/         Worker: dispatchers, bounded pools, service loops            (process model)
 store/          Store contract; memory, SQL (SQLite/PostgreSQL); notifier   (durability)
 remote/, server/  HttpStore and the HTTP API that serves it                  (network edge)
 client/, app.py   Client, WorkflowHandle, schedules, the Loomwork facade     (callers)
```

Dependencies point downwards only. `runtime` knows nothing about stores; `store` knows
nothing about workflow code; `engine` is where the two meet. The SDK functions called
from workflow code (`lw.sleep`, `Channel.receive`, ...) find the running
`WorkflowInstance` through the event loop (`runtime.context.current_instance`), not
through globals, which is why they fail with `NotInWorkflowError` anywhere else.

## The life of a run

1. **Start.** `Engine.start_workflow` builds an `ExecutionRecord` and a `WorkflowStarted`
   event and calls `Store.create_execution`, which inserts both atomically and marks the
   run as having a pending tick. The id conflict rules are enforced inside that
   transaction.
2. **Claim.** A worker's workflow dispatcher calls `Store.claim_tick`, which leases one
   run whose tick is pending and ready (`tick_ready_at` has passed) to this worker for
   `tick_lease`. SQL stores do this with a single `UPDATE ... RETURNING` over a
   `FOR UPDATE SKIP LOCKED` subquery, so concurrent workers never block on each other.
3. **Replay.** `TickProcessor` takes the run's `Replayer` from the replay cache, or
   creates one, and feeds it the history it has not seen yet
   (`load_history(after_id=...)`). The replayer re-runs the workflow function tick by
   tick, matching the commands the code produces against the recorded decision events.
   A mismatch raises `NondeterminismError`.
4. **Live tick.** `Replayer.live_tick(now)` delivers every unconsumed input event at one
   workflow time and runs the deterministic loop until nothing is ready. The result is a
   list of **commands** (`ScheduleStep`, `StartTimer`, `CompleteWorkflow`, ...).
5. **Commit.** `TickProcessor` turns commands into a `TickOutcome`: the `TickCompleted`
   marker plus decision events, new step jobs, timers, children, outbox messages, search
   attribute changes and possibly a close. `Store.complete_tick` applies all of it in one
   transaction, and only if this worker still holds the lease; otherwise it raises
   `LeaseLostError` and nothing changes.
6. **Steps.** A step dispatcher claims a `StepJob` (`claim_step`) and `StepExecutor` runs
   the step function. `finish_step` appends `StepCompleted`/`StepFailed` to the history
   as an input event and marks the run's tick pending again; `retry_step` schedules the
   next attempt instead. Heartbeats renew the lease and store checkpoint details.
7. **Timers and the outbox.** Service loops fire due timers (`fire_due_timers` appends
   `TimerFired`) and relay outbox messages to other runs (signals to other workflows,
   child results to parents), each as an input event on the target run.
8. **Close.** A tick whose outcome closes the run sets the final status, result or
   failure, cancels remaining step jobs and timers, and, for continue-as-new or a
   workflow retry, creates the next run in the same transaction.

Every arrow between these stages goes through the store. Workers keep no state that
another worker needs; the replay cache is only an optimisation, and a worker that dies
mid-tick leaves nothing behind but a lease that expires.

## Invariants

These are what the conformance and property tests check; a change that breaks one is a
correctness bug, however small it looks.

- **Histories are append-only**, and event ids within a run are dense and increasing.
- **A tick commits entirely or not at all.** `TickCompleted` and its decisions are
  appended together with every projection change (jobs, timers, status) in one
  transaction.
- **Only the lease holder commits.** `complete_tick`, `fail_tick`, `finish_step`,
  `retry_step`, heartbeats and lease renewals all check the lease inside their
  transaction.
- **Input ordering is recorded, not inferred.** `TickCompleted.consumed_through` says
  which inputs a tick saw; inputs appended during a tick belong to the next one
  ([ADR-0002](adr/0002-tick-delimited-history.md)).
- **Workflow time is recorded.** `TickCompleted.time` is what `lw.now()` and
  `loop.time()` return during that tick, on every replay.
- **Cross-run effects go through the outbox**
  ([ADR-0004](adr/0004-cross-run-outbox.md)): a tick may *create* runs (children, a
  continuation) in its own transaction, but it never appends to another existing run's
  history. Signals to other workflows and child results reach their target when the
  outbox relay appends them, in a transaction of their own.
- **Lock order** in SQL stores is the execution row first, then its jobs and timers.
- **A worker with no build id is eligible for every run and step, pinned or not.**
  `claim_tick`/`claim_step`'s `build_id` filter is additive: passing `None` reproduces
  the exact query that existed before worker build versioning, so an unversioned
  deployment cannot be affected by this feature (see [ADR-0006](
  adr/0006-worker-build-versioning.md)). A versioned worker's eligibility for a pinned
  run or a pinned-queue step is exactly "unpinned, matches, or declared compatible" --
  never anything looser.
- **A run's pin, once set, only changes through `repin_run`.** `complete_tick` sets
  `pinned_build_id` at most once per run (the first tick of a `PINNED` run processed by
  a worker with a build id); every later tick's commit leaves it untouched.

## Stores

`store/base.py` defines the contract. `MemoryStore` implements it with dictionaries and
one lock; `SQLStore` (`store/sql/base.py`) implements it once for both SQL dialects, with
`SQLiteStore` and `PostgresStore` supplying connections, transactions, timestamp encoding
and a few dialect hooks ([ADR-0003](adr/0003-shared-sql-store.md)). `HttpStore` implements
the same contract over the server's worker API.

`tests/store_conformance.py` is one test class that every backend runs, including
`HttpStore` against in-process servers. `tests/property/test_store_equivalence.py` drives
the memory and SQLite stores through random operation sequences and requires identical
observable results after every step.

Adding a store operation means: the abstract method with a precise docstring, an
implementation in `MemoryStore` and `SQLStore` (plus a migration if the schema changes),
the HTTP route in `server/worker_api.py` and its client in `remote/http_store.py`, and
conformance tests. Migrations are append-only and checksummed; never edit one that has
shipped.

**Wake-ups.** Stores publish topics (`ticks`, `steps`, `timers`, `outbox`, `schedules`)
through a `Notifier` after each commit. Workers wait on these instead of sleeping for a
full poll interval. PostgreSQL relays them between processes with `LISTEN/NOTIFY`;
`HttpStore` long-polls the server, which waits on its own store's notifications; between
separate SQLite processes, polling at `poll_interval` is the fallback.

## The runtime

- `runtime/loop.py`, `DeterministicEventLoop`: FIFO callbacks, workflow time from the
  tick, `call_at` turned into durable timers, I/O refused.
- `runtime/instance.py`, `WorkflowInstance`: one run's workflow task, its pending steps,
  timers, channels, children and handlers; turns SDK calls into commands and input
  events into resolved futures.
- `runtime/replayer.py`, `Replayer`: feeds histories to an instance tick by tick,
  matches commands against decisions, and produces the next live tick.
- `runtime/commands.py`: the command types and how each maps onto a decision event.

Anything under `runtime/` and `sdk/` that runs inside a workflow must be deterministic:
no wall clock, no randomness except the seeded workflow RNG, no iteration over sets or
unordered dicts in ways that affect commands, and no I/O.

## Payloads and codecs

Workflow code and step code see Python values. The `DataConverter` turns them into
`Payload`s (JSON plus metadata). If a codec chain is configured, `CodecBoundary`
encodes payloads on their way into the store and decodes them on the way out, at the
edges of the engine: in `TickProcessor` around the history and commands, and in
`StepExecutor` around arguments and results. Stores, the server and the outbox only
ever handle encoded payloads, which is what makes end-to-end encryption hold.

## Where to add things

| Change | Touch |
| ------ | ----- |
| New workflow API (like `lw.sleep`) | `sdk/`, a command in `runtime/commands.py` and its event in `history/events.py`, handling in `runtime/instance.py`, projection in `engine/ticks.py` and the stores; a golden history test |
| New event field | `history/events.py` with a default value so old histories still decode; serde round-trip tests |
| New store operation | see *Stores* above |
| New CLI command | a module under `cli/commands/` registered in `cli/main.py`; tests under `tests/cli/`; `docs/cli.md` |
| New lint rule | `lint/rules/`, registered in `lint/registry.py`; documented in `docs/lint-rules.md` (a test checks the two agree) |
| New metric | `observability/telemetry.py` (`MetricsTelemetry`) and the table in `docs/operations/monitoring.md` |

## Test suites

| Directory | What it covers |
| --------- | -------------- |
| `tests/unit/` | modules in isolation, including the store conformance suite per backend |
| `tests/integration/` | real workers against real stores: messaging, children, timeouts, chaos |
| `tests/property/` | Hypothesis: store equivalence, serde round trips |
| `tests/server/` | the HTTP API and `HttpStore` |
| `tests/cli/`, `tests/deploy/` | the command line and the deployment manifests |
| `tests/testing/`, `tests/examples/` | the testkit and the example applications |

`pytest -m "not slow"` is the quick loop; CI runs everything, with PostgreSQL.
