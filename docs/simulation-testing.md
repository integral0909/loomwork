# Simulation testing

`loomwork.testing.simulation` runs many workflows through real `Worker` objects and a
real store, under a seeded, reproducible schedule of store faults, worker deaths and
replay-cache evictions, and checks the engine's invariants continuously and at the end.
It is the kind of testing that finds the bugs example-based tests (`tests/integration/`)
miss: not "does this one scenario work", but "does every invariant still hold across
thousands of random schedules of failure."

```python
from loomwork.testing.simulation import SimulationConfig, simulate

report = simulate(seed=1234, config=SimulationConfig(num_runs=50))
if not report.ok:
    print(report.format_text())
```

```console
$ uv run pytest tests/simulation                        # the default (fast) suite
$ uv run pytest tests/simulation -m simulation           # the larger soak suite
$ python -m loomwork.testing.simulation --seeds 1..200 --store sqlite
```

## What it checks

Every run the harness starts is checked, as soon as it closes, against:

- **Replays cleanly.** The recorded history replays against the workload's own code
  (`loomwork.versioning.check_history`) without a `NondeterminismError`.
- **Append-only, dense history.** Event ids are contiguous and increasing; no event is
  ever missing, duplicated or out of order.
- **Monotonic `consumed_through`.** Every `TickCompleted` marker's `consumed_through`
  is greater than or equal to the previous one -- a tick never "forgets" input a
  previous tick already consumed.
- **At-least-once execution, exactly-once recording.** Each workload logs every attempt
  of its steps to an in-memory counter (`SideEffectLog`), independent of the history;
  the checker requires that counter to be at least 1 for every step the plan expected to
  run (a step may be attempted many times under fault injection, but it must run at
  least once), and separately requires the *recorded* outcome (`StepCompleted`/
  `StepFailed`) to appear in the history exactly once per step, however many attempts it
  took to get there.
- **The expected outcome.** Every scripted run comes with an `ExpectedOutcome` --
  completed with a specific result, failed with a specific kind of failure, or cancelled
  -- computed from its arguments *before* the run ever executes (see "Adding a
  workload" below). A mismatch is the harness catching a real behavioural difference,
  not a coincidence of timing.
- **No orphaned step jobs.** A closed run has no outstanding step jobs (checked through
  `Store.get_step_jobs`, the public contract every backend implements).

Once every run the harness itself started has closed, it also sweeps the *whole* store
(`Store.list_executions`) for every other execution it did not directly drive -- child
workflows, continue-as-new successors -- and checks the same replay/history/orphaned-job
invariants against those too. Finally it checks two store-wide, closed-world invariants
that only make sense once every run has closed:

- **No leftover timers.** `Store.next_timer_at()` is `None`. The `Store` contract has no
  per-run timer listing, but since every execution the store holds was created by this
  one simulation, an outstanding timer once everything has closed can only be a leaked
  one.
- **The outbox drains.** `Store.outbox_size()` reaches zero (given a short, real-time
  grace period for the relay loop's own poll cadence to catch up).

None of these checks reach into store internals: everything above goes through the
public `Store` contract (`store/base.py`) and `loomwork.versioning`, exactly what a real
operator or a CI job could run against a production store.

## Running it

```console
uv run pytest tests/simulation                # a handful of seeds; part of the quick loop
uv run pytest tests/simulation -m slow         # the same, plus a 50-seed soak and a SQLite run
uv run pytest tests/simulation -m simulation   # just the soak test
```

The `simulation` marker (registered in `pyproject.toml` alongside `slow` and
`integration`) exists so a soak run can be selected on its own; every simulation test
that runs more than a handful of seeds is marked both `slow` and `simulation`.

For a long soak run outside of pytest:

```console
python -m loomwork.testing.simulation --seeds 1..500
python -m loomwork.testing.simulation --seeds 1..200 --store sqlite --fault-rate 0.03 \
    --kill-rate 0.05 --cache-evict-rate 0.05
```

`--seeds` takes a single seed or an inclusive range. `--quiet` prints only failing seeds;
`--verbose` shows the worker's own logging (every injected fault is a handled, expected
error it logs and recovers from -- informative when investigating a failure, noisy
otherwise). See `python -m loomwork.testing.simulation --help` for the rest (fault and
chaos rates, worker count, wall-clock timeout).

The SQLite example above uses gentler rates than the (memory-tuned) defaults on purpose;
see "The limits of determinism" for why.

## Reproducing a failing seed

```python
from loomwork.testing.simulation import SimulationConfig, simulate

report = simulate(1234, SimulationConfig(num_runs=50))   # whatever the failing report printed
```

A failing `SimulationReport.format_text()` prints exactly this call, ready to paste. Two
things are always deterministic from `seed` and `config` alone, so re-running it is the
first and usually only thing you need:

- **Every workload's arguments and expected outcome.** `SimulationConfig`'s own
  `num_runs`, cycled across the built-in workloads, each seeded with a
  `random.Random` derived from `seed`; nothing about a workload's args, its script (what
  signals it sends and when) or what it *should* do depends on real time or real
  randomness.
- **The set of `(workload, args) -> outcome` pairs.** Every run the harness drove reaches
  the same actual outcome on a second call, so a run that violated an invariant does so
  again. `tests/simulation/test_simulation.py::test_simulation_is_reproducible` checks
  exactly this.

What is *not* guaranteed to be byte-for-byte identical: the exact sequence and count of
injected faults, which of several workers happens to claim a given tick, and the real
wall-clock timing everything takes. See below for why, and what to do about it if you
need a fully deterministic trace to step through.

## Adding a workload

A workload (`loomwork/testing/simulation/workloads.py`) is a `Workload`: the workflow
(and any child workflows) and steps to register, plus a `plan(rng, index) -> RunPlan`
function. `RunPlan` carries the arguments for one run, an `ExpectedOutcome` computed
*from those arguments alone* (never by predicting what the fault schedule will do), the
step-log keys that run's steps should hit at least once, and optionally a `drive`
coroutine describing exactly how to interact with the run (send a signal, an update, a
cancellation) and a `post_check` for anything extra worth asserting once it closes.

The one rule that matters: **the expected outcome must be computable without running any
workflow code**, and any interaction the plan drives must itself be scripted (never "wait
and see what happens"), so that the same seed always produces the same script and the
same predicted outcome. Two patterns the existing workloads lean on:

- **Race a signal, not a coincidence.** A plan that races a signal against a durable
  timeout decides in advance which one should win (`signaled = rng.random() < 0.5`) and
  scripts the driver to guarantee it: send the signal as the very first thing after the
  run starts if it should win, or never send it at all if the timeout should. See
  `docs/testing.md`'s note on why interacting with a run across more than one `await`
  risks a race with time-skipping, and `sim.signal_race` for how the harness avoids it
  (whichever call could still race the deadline goes first, with nothing else awaited in
  between).
- **Unlimited retries for anything that is not the thing under test.** A step whose
  business logic should always succeed gets an unlimited retry budget
  (`_UNLIMITED_RETRY` in `workloads.py`); otherwise the fault schedule's own
  lease-abandonment pressure can exhaust a production-sized retry budget (5 attempts) on
  pure infrastructure flakiness, which is a fault-injection artifact, not the business
  outcome the plan predicted. A step whose failure *is* the thing under test
  (`sim.saga`'s "declined" charge) stays retryable for transient abandonment but marks
  its specific failure type non-retryable, so it still fails fast and deterministically
  when it is supposed to.

`build_workloads(log, names=...)` builds every workload fresh per simulation run, closing
over a private `SideEffectLog` -- never share workflow/step definitions or their closed-
over state across `simulate()` calls.

## The limits of determinism

The harness seeds four independent things from one integer: the fault schedule
(`FaultyStore`), worker chaos (`WorkerChaos`), workload generation (arguments, scripts),
and the event loop's own callback order (`SimEventLoop`, below). Investigating exactly
how far that determinism reaches -- and where it stops -- turned up real, structural
boundaries worth knowing before you rely on this tool.

### A seeded event loop

`asyncio`'s real event loop runs whichever callbacks are already ready, in FIFO order
(`BaseEventLoop._run_once`, in `asyncio/base_events.py`, pops them from a deque with
`popleft`). That hides most of the interleavings of concurrently-ready code that would
actually find a bug: two worker dispatch loops woken by the same notification, or the
branches of an `asyncio.gather`, always resume in the same order.

`SimEventLoop` (`loop.py`) changes exactly this: it replaces that deque with one whose
`popleft` returns a seeded-random element instead of the oldest one. `_run_once` never
does anything else with it (only `.append()`, `.popleft()`, `len()` and truthiness, all
still supported), so nothing else about the loop's semantics changes. Given the same
seed, the same set of callbacks that become ready together resume in the same relative
order every time, on the same Python version and platform -- this is what makes it a
custom event loop rather than a monkeypatch: it changes which of several *already
correct* interleavings gets explored by a given seed, without touching timers, I/O or
task semantics at all.

What it does not, and structurally cannot, make reproducible:

- **Callbacks scheduled through `call_later`/`call_at`** -- a real `asyncio.sleep` with a
  positive delay, a socket becoming readable -- are ordered by wall-clock time and the
  OS's own I/O readiness, neither of which the seed controls. The worker's own dispatch
  loops wait this way (`Notifier.wait(topic, token, poll_interval)`; see
  `worker/worker.py`), so exactly when an idle worker wakes up to notice new work is
  real-time-dependent, seed or no seed.
- **Real ids.** `loomwork.ids.new_id()` (run ids, job ids) mixes 48 bits of wall-clock
  time with 80 random bits from OS entropy -- deliberately not seeded, since production
  ids must not be predictable. Several stores use an id as an explicit tie-breaker for
  "which of several equally-ready things gets claimed first"
  (`MemoryStore.claim_tick`'s `(record.tick_ready_at, record.run_id)` comparison, for
  one): when two runs are ready at the exact same instant -- routine, since this harness
  starts every scripted run at once -- *which one* a worker claims first depends on ids
  the seed never touched.

The practical effect, confirmed empirically while building this harness: two calls to
`simulate(seed, config)` do **not** produce byte-identical fault/chaos traces, and do
**not** necessarily close their runs in the same order. What they do produce, every time
observed across several hundred repeated calls: the exact same set of
`(workload, arguments) -> outcome`. Every run's actual behaviour is reproducible; the
blow-by-blow trace of how it got there is not. `test_simulation_is_reproducible`
(`tests/simulation/test_simulation.py`) checks precisely this, and deliberately excludes
`run_id` from the comparison for the reason above.

If you need a fully deterministic, single-steppable trace (for example while working out
exactly why a run misbehaved), remove every source of real-time contention: run a single
workload (`workloads=frozenset({"the_one"})`), a single run (`num_runs=1`) and a single
worker (`num_workers=1`). With no second run or worker to race, the id-based tie-breaks
above never come into play.

Very rarely (observed once in several hundred repeated seeds during development, not
reproduced on demand from that same seed afterwards) a run reached a *different* final
outcome on a repeat call under heavy concurrent chaos and fault load. This is consistent
with the same real-time tie-breaking above having a wider effect than usual under
specific OS scheduling timing, rather than with a bug that a fixed seed alone
reproduces; if you see this, the most useful next step is the single-workload/
single-run/single-worker isolation above, which removes the contention that theory
depends on.

### SQLite: no clock to skip

Against `MemoryStore`, the harness drives a `ManualClock` and fast-forwards it the moment
nothing can make progress (`_skip_idle_time`, the same idea as
`WorkflowEnvironment`'s auto time-skipping, generalised to a pool of workers, plus one
extra deadline source: the lease expiry of a tick or step job that a fault made a worker
silently abandon -- see the docstring on `_leased_deadlines` for why `next_tick_ready_at`
and `next_step_ready_at` alone cannot see that). This is what makes the default (memory)
suite fast regardless of how long a workload's simulated timers are.

Against `SQLiteStore` there is no clock to skip: it uses a real `SystemClock`, so every
retry backoff and every lease costs real wall-clock time. Two consequences worth knowing:

- **A soak run against SQLite needs a correspondingly generous `--wall-clock-timeout`**
  and gentler fault/chaos rates than the memory-tuned defaults -- real I/O is slower, so
  the same rates that comfortably finish in well under a second against memory can
  legitimately need tens of seconds against SQLite.
- **The worker-chaos schedule's cadence is real-time paced** (`WorkerChaos`, a seeded
  interval between actions, not a count of scheduler turns) precisely because a
  turns-per-second measure is not backend-independent: SQLite hands most of a tick's
  work to a background thread (`aiosqlite`), which leaves far more scheduler turns "free"
  per real second than the same workload against MemoryStore, and worse, a turn-counted
  cadence would accelerate exactly when the system is already struggling (a stall leaves
  even more turns free), compounding rather than just observing a stall.

## Design notes

- **`FaultyStore`** (`faults.py`) delegates every call to a real store and only ever
  either lets it through unchanged or does not reach it at all -- it never corrupts the
  inner store, only lies about the outcome the caller sees. Three kinds of lie: the call
  never reaches the store ("before"); it reaches the store, commits, and the reply is
  lost anyway ("after" -- the hardest case a real deployment has to tolerate, and the one
  most likely to find a bug); and lease-expiry pressure, which shrinks the lease
  `claim_tick`/`claim_step` hand out so the "only the lease holder commits" invariant
  gets exercised continuously rather than only when a worker happens to die slowly.
- **`WorkerChaos`** (`chaos.py`) owns a pool of workers sharing one store: kill (no
  grace period), restart (same worker id, as a redeployed process), replace (a new id,
  as a freshly provisioned node) and replay-cache eviction (`Worker.ticks.cache.discard`,
  forcing a cold replay on the next tick).
- **The harness never retries a *workload's* own client calls silently** -- a worker
  retries its own operations as the whole point of the engine, but a signal or update
  sent directly from a workload's `drive` callable is a one-shot call from application
  code, so the harness wraps it (`_ReliableHandle`) exactly the way a production caller
  is expected to: retry on a client-side fault, with a stable idempotency key so a
  retried send is never double-applied.

If a simulation finds a real engine bug (not a harness or workload issue -- see the
sections above for what to rule out first), record it precisely: the failing seed and
config from the report, a minimal workflow reproducing it, and a test marked
`xfail(strict=True)` referencing the bug, in `tests/simulation/`, so it starts passing
(and needs its `xfail` removed) the moment the underlying issue is fixed. None were open
at the time this was written; see the accompanying report for what was investigated and
ruled out.
