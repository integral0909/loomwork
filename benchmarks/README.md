# Benchmarks

A benchmark here is a **scenario**: setup/run/teardown against a store backend
(`memory`, `sqlite`, `postgres`, or `http` -- an in-process HTTP server plus
`HttpStore`), parameterised over things like run count, step count, fan-out width,
payload size, worker concurrency and codec chain. Every scenario runs a warmup trial or
two (discarded), then several measured trials, and reports either a **throughput**
(operations/second, median across trials with a bootstrap confidence interval) or a
**latency** (seconds, pooled samples with p50/p95/p99 and a confidence interval around
the median). See `benchmarks/stats.py` for the statistics and `benchmarks/harness.py`
for the scenario framework itself.

## Running

```bash
uv run python -m benchmarks list                                    # every scenario
uv run python -m benchmarks run --quick                              # memory + sqlite, cheap parameters
uv run python -m benchmarks run --store memory,sqlite,postgres,http  # everything
uv run python -m benchmarks run --scenario throughput --scenario tick_latency
uv run python -m benchmarks run --out results.json                   # also write JSON
uv run python -m benchmarks compare old.json new.json --threshold 10%
```

`--quick` cuts run counts, trial counts and history lengths down; `memory` and `sqlite`
together finish in well under a minute on a laptop, which is what CI runs on every push
to `main` (informational only -- see the `benchmarks` job in
`.github/workflows/ci.yml`). Drop `--quick` for numbers worth writing down; several
scenarios (`store_contention`, `visibility`, `throughput`) are far more informative at
realistic sizes than at quick-mode sizes.

`--store postgres` needs a reachable PostgreSQL server. It never touches whatever
database `--postgres-dsn` names -- it always creates and uses a dedicated
`loomwork_bench` database in that same cluster (see `benchmarks/stores.py`), so it is
safe to point it at a shared or CI database's connection info, including
`LOOMWORK_TEST_POSTGRES_DSN` itself.

## Scenarios

| Scenario | What it measures | Why it matters |
| --- | --- | --- |
| `throughput` | Workflows/second for a one-step and a 20-step workflow, started and awaited in bulk | The number most people ask first: how many runs can one worker process |
| `dispatch_latency` | `ready -> attempt start` delay for step jobs, under a fan-out at varying step concurrency | Whether `step_concurrency` is keeping up with how fast steps become ready |
| `tick_latency` | Per-tick duration on a long chain, replay cache on vs off | The replay cache's payoff, and the cost of undersizing it |
| `timer_accuracy` | Delay between a durable timer's deadline and its observed firing, alone and under load | Whether `lw.sleep()` fires close to on time as concurrent timers pile up |
| `replay` | Cold `replay_history()` cost (no cache) vs history length | What every tick pays without a cache hit, and what `continue_as_new()` avoids paying forever |
| `messaging` | Update round trip (exact) and signal round trip (poll-observed) | Latency budget for interactive workflows (approvals, chat-like flows) |
| `continuation` | Throughput through a chain of `continue_as_new` generations | The overhead a long-lived entity pays for staying bounded |
| `history_limit` | Tick cost near `history_limit_events`, and the cost of the tick that finally exceeds it | Confirms the limit check is cheap (checked before replay, not after) |
| `codec_overhead` | Throughput with no codec, compression, encryption, and both plus offload | The price of `docs/payload-encryption.md` and `docs/large-payloads.md` |
| `store_contention` | Throughput draining pre-seeded ticks/steps with 1/4/16/64 concurrent claimers | Whether `FOR UPDATE SKIP LOCKED` is actually keeping claimers from blocking each other |
| `visibility` | `query_runs()` latency vs number of runs already in the store | Whether visibility queries stay fast as a deployment accumulates history |

Every scenario's module docstring (`benchmarks/scenarios/*.py`) explains its specific
setup and what to read into the numbers; the table above is the map, not the territory.

## Reading the results

- **A confidence interval is not a error bar to eyeball away.** `compare` only calls
  something a regression when the point estimates differ by more than `--threshold`
  *and* the two confidence intervals do not overlap. A single run's console table
  shows the interval so you can see how much noise is in play before trusting the
  point estimate on its own.
- **Quick mode's confidence intervals are not reliable.** With one or two trials,
  bootstrap resampling has almost nothing to resample from, so the interval collapses
  toward the point estimates and `compare` becomes oversensitive. Use full runs (drop
  `--quick`, or pass more trials) before trusting a `compare` verdict, or before
  reporting a number anywhere but a quick sanity check.
- **The replay cache turns "linear in history" into "flat."** Compare `tick_latency`'s
  `cache=on` and `cache=off` cases on the same history length: without the cache every
  tick replays everything seen so far (see `replay`'s numbers by history length for
  what that costs directly); with it, only new events since the last tick are fed in.
- **`store_contention` is where backends actually differ.** `MemoryStore` claims under
  one process-wide lock by design -- it is the "no real concurrency" baseline, not a
  regression when it does not scale with claimer count the way SQLite and PostgreSQL's
  `SKIP LOCKED` claiming does.
- **`http` measures the server/serialization edge**, not a particular backing store:
  it always runs against an in-process server backed by `MemoryStore`. The gap between
  `memory` and `http` on the same scenario is roughly what running workers over the
  network, rather than embedded, costs on top of the store itself.
- **Numbers are only comparable on the same machine**, with nothing else competing for
  CPU, and ideally the same store version. `compare`'s environment metadata (Python
  version, platform, CPU count, `loomwork` version, git commit) is there so you notice
  when it isn't the same machine before drawing conclusions.

## Adding a scenario

Subclass `benchmarks.harness.Scenario`: set `name`, `description`, `metric` (`
"throughput"` or `"latency"`), `unit`, and implement `cases(quick)` (the parameter sets)
and `run_trial(ctx, case)` (one trial, returning a `Trial` with either
`operations`/`seconds` or `samples` filled in). Register it in
`benchmarks/scenarios/__init__.py`. Reuse the shared workflows and steps in
`benchmarks/workloads.py` rather than defining new ones unless the scenario genuinely
needs a different workload shape -- see that module's docstring for why.

See `docs/performance.md` for the sizing guidance this suite's numbers support, and
`docs/internals.md` for the engine internals the scenarios are aimed at.
