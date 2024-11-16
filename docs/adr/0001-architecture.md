# ADR-0001: Overall architecture

- Status: accepted
- Date: 2024-11-16

## Context

Loomwork runs long-lived business processes written as ordinary `async` Python functions.
Processes must survive worker crashes, deploys and restarts without the author writing
checkpointing code. We want one programming model that works both inside an existing
application process (no extra infrastructure) and as a horizontally scaled service.

## Decision

### Event sourcing with replay

Each workflow run owns an append-only **history** of events. The history is the single
source of truth; every other piece of state (execution status, pending step jobs, timers)
is a projection that can be rebuilt from it.

Workflow code never persists its own state. To resume a run, a worker re-executes the
workflow function from the beginning and feeds it the recorded history. Operations whose
outcome is already recorded (a step result, a fired timer) resolve immediately from the
history instead of being performed again. Once the history is exhausted, the workflow
continues live and its new decisions are recorded.

### Workflow code runs on a deterministic event loop

Workflow coroutines run on `DeterministicEventLoop`, an `asyncio.AbstractEventLoop`
implementation that:

- runs callbacks strictly in FIFO order and only when driven by the executor;
- reports *workflow time* from `loop.time()`: the timestamp of the current tick, never
  the wall clock;
- turns `call_at` / `call_later` into **durable timer commands**, so `asyncio.sleep`,
  `asyncio.timeout` and `asyncio.wait_for` become durable without special APIs;
- refuses I/O (`run_in_executor`, sockets, subprocesses) with a clear error.

Standard asyncio composition (`gather`, `wait`, `TaskGroup`, `Event`, `Queue`) therefore
works unchanged inside workflows.

### Commands and events

Running workflow code produces **commands** (`ScheduleStep`, `StartTimer`,
`CompleteWorkflow`, ...). The engine turns accepted commands into **decision events** in
the history and performs their side effects. External outcomes (a step completed, a timer
fired, cancellation requested) arrive as **input events**.

During replay, commands are matched one-to-one against the recorded decision events. A
mismatch means the code changed incompatibly and raises `NondeterminismError`.

### Ticks

One deterministic advance of a workflow is a **tick**; see ADR-0002.

### Steps vs tasks

- **Steps** are side-effecting async functions executed by step workers. Their result is
  journaled so they are never re-executed on replay. They carry retry and timeout policy.
- **Tasks** (milestone 3) are steps routed to named queues and executed by any worker
  subscribed to that queue, with heartbeats and rate limits.

### Store protocol

All persistence goes through a narrow, transactional `Store` protocol. Every method is
atomic, and the protocol is shaped so that PostgreSQL can implement it with row locks,
`SKIP LOCKED` leasing and `LISTEN/NOTIFY` wake-ups. The in-memory store implements the
same contract for tests and embedded use; SQLite provides durable single-node storage.

### Two deployment modes

- **Embedded:** `Loomwork(...)` hosts the engine and the workers inside the application
  process. No network hop.
- **Server:** an ASGI API fronts the engine; workers poll it over HTTP. Workflow and step
  code is identical in both modes.

## Consequences

- Workflow code must be deterministic. Guard rails (runtime checks and an AST linter)
  are first-class subsystems rather than documentation.
- History size grows with the number of steps a run performs; snapshots and
  continue-as-new (milestone 6) bound it.
- The event model is a compatibility contract and is versioned explicitly.
