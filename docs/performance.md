# Performance

Guidance for sizing a Loomwork deployment, backed by `benchmarks/` -- run it yourself
before trusting a number here on different hardware, a different store version, or a
different codec configuration; see `benchmarks/README.md` for what each scenario
measures and how to read confidence intervals. This page's own numbers (below) were
measured once, on one machine, and are illustrative, not a promise about yours.

## Sizing workers

- **`workflow_concurrency` bounds how many runs one worker ticks at once**, not how
  fast any single tick is. A tick that only awaits steps and timers (no CPU-bound work
  in workflow code, which is the deterministic-loop contract anyway) is cheap; the
  ceiling is usually store round trips, not CPU. Start at the default (4) and raise it
  if `loomwork_ticks_total` keeps growing relative to `loomwork_tick_duration_seconds`
  staying flat -- see `docs/operations/monitoring.md`.
- **`step_concurrency` should be sized to your fan-out, not just your machine.**
  `dispatch_latency` makes this concrete: fanning out 20 steps at once showed no
  benefit from raising `step_concurrency` past 4 (there was nothing waiting), but
  fanning out 200 showed `concurrency=4` costing noticeably more wall time than
  `concurrency=16` on SQLite (0.354s vs 0.273s median) -- once fan-out width exceeds
  `step_concurrency`, jobs queue and dispatch latency grows. If your workflows
  routinely `asyncio.gather` N steps, keep `step_concurrency` at or above N per worker,
  or spread N across more workers.
- **A pure step worker (`workflow_concurrency=0`) or a pure tick worker
  (`step_concurrency=0`) is a legitimate topology**, not just a benchmark trick --
  `store_contention` uses exactly this to isolate `claim_tick` from `claim_step`
  scaling, and it is the same shape a production deployment uses to give steps that
  need special credentials or hardware their own worker pool (`task_queues`).
- **More claimers is not always more throughput.** On `MemoryStore`, `claim_tick`
  throughput rose from 1 to 16 claimers and then fell back at 64 (one process-wide
  lock plus more coroutines contending for it costs more than it gains past a point).
  On SQLite, throughput barely moved with claimer count at all: SQLite serializes
  every writer transaction through one connection lock (`docs/internals.md`, "Stores"),
  so `FOR UPDATE SKIP LOCKED` avoids *deadlocks*, not *contention*, on that backend.
  More claimers only pays off against PostgreSQL, and even there, past the point where
  claimers outnumber available work, added claimers add contention rather than
  throughput. Measure your own store and workload rather than assuming "more workers"
  always helps.

## Choosing a store

| Store | When | What the numbers show |
| --- | --- | --- |
| `memory` | Tests, demos, anything that can lose its state on restart | Fastest by a wide margin (roughly 6-10x a durable store on the same machine for simple throughput) because there is no I/O; not durable |
| `sqlite` | One process, one machine, needs to survive a restart | Durable at a real cost per operation; several processes may share the file but only learn about each other's work by polling |
| `postgres` | Anything that scales out, or wants `LISTEN/NOTIFY` wake-ups across processes | Similar per-operation cost to SQLite alone, but scales across machines and claimers in a way SQLite cannot (see "Sizing workers" above) |
| `http` (`HttpStore` + server) | Workers on a different host or in a different language runtime than the store | Adds the network/serialization edge on top of whichever store backs the server; budget for that separately from the store's own cost |

**Visibility queries are where the store choice matters most as history accumulates.**
`MemoryStore.query_executions` is a linear scan over every stored run
(`src/loomwork/store/memory.py`); `visibility`'s numbers show it growing with run count
(0.4ms at 100 runs, 4ms at 1,000, 28ms at 5,000) while SQLite's indexed `status` query
stayed flat to noisy-but-not-growing across the same range (under 3ms throughout,
occasionally *faster* at 5,000 than at 100 -- page-cache warmth, not a real trend). If
your deployment runs `query_runs`/the operator console against a store with more than a
few thousand runs, that alone is a reason to prefer a SQL store over `MemoryStore` even
before durability enters the discussion.

## Replay cache sizing

`replay_cache_size` (default 1000 live instances) is what turns "every tick replays the
whole history" into "every tick feeds in only what changed since the last one." The
`tick_latency` scenario measures this directly on a 300-step chain: with the cache on, a
tick cost about 0.1ms (memory), 2.1ms (postgres) or 2.1ms (sqlite); with it off, the
same tick cost 5.2ms, 11.4ms and 11.8ms respectively -- **7x to 45x**, and that gap only
widens with history length, since a cache miss replays the *entire* history (`replay`'s
numbers: cold replay cost is roughly linear in step count, from under a millisecond at
10 steps to 30-60ms at 1,000 steps depending on store).

Sizing it is a memory/hit-rate trade-off: each cached entry is one live workflow
instance (roughly proportional to how much state the workflow keeps in local
variables, plus its pending futures), and a worker only benefits from the cache for
runs it ticks more than once while they're still in the cache. Size it to comfortably
hold your typical number of *concurrently active* runs per worker, not your total run
count -- a run that closes, or that this worker never ticks again, gets no benefit from
staying cached, and `docs/operations/monitoring.md`'s `loomwork_replay_cache_total`
hit-ratio metric is the honest way to tell whether the size you picked is working
rather than guessing from history length alone. Setting it to `0` disables the cache
entirely; useful for shaking out determinism bugs (every tick is a fresh replay,
maximally sensitive to nondeterminism), never for production.

## Payload size and codec advice

`codec_overhead` runs the same one-step workflow through no codec, compression,
encryption, and both, at a 4 KiB compressible payload. On `MemoryStore`, where codec CPU
cost is not hidden behind I/O, the ordering is exactly what you'd expect -- none fastest,
each codec added costing more (roughly 20-30% off throughput for compression, encryption,
or both combined, relative to no codec at all). Once a durable store is in the loop, the
picture changes: **the store's per-statement cost dominates codec CPU cost**, matching
this suite's other durable-store numbers (see `benchmarks/README.md`, "Durable stores
are bound by statements per operation") -- the difference between codec configurations
on SQLite or PostgreSQL was within this run's noise floor, not a clear ordering.

Practical guidance that follows:

- **Turn on compression and encryption by default against a durable store**; the
  marginal cost is small next to what the store already costs per operation, and
  `docs/payload-encryption.md`'s threat model is worth having regardless.
- **`OffloadCodec` (`docs/large-payloads.md`) is about history size, not raw throughput.**
  Its own overhead is one length check for anything under its threshold; reach for it
  when a step's arguments or results are tens of kilobytes or more, so histories (which
  are replayed in full and kept for the life of a run) stay small, not because it makes
  a single call faster.
- **Payload size matters more than codec choice.** A workflow argument or result in the
  tens-of-kilobytes range is a bigger lever on tick and replay cost (more bytes to
  encode, decode, and store per event) than whether compression is on. Keep routine
  workflow/step payloads small (references, ids, small structs) and use `OffloadCodec`
  for the exceptions rather than growing every history to accommodate them.

## Measured results

Apple M1, 8 cores, macOS 26.2.0 (Darwin 25.2.0), Python 3.12.9, local PostgreSQL 16.10,
loomwork 0.1.0.dev0, git `a1f4b82`. Produced by `python -m benchmarks run` (not
`--quick`) with nothing else competing for the machine except, for `store_contention`,
`codec_overhead` and `visibility`, each other -- three scenario runs happened to
overlap on this machine, which is why their PostgreSQL numbers are noisier than the
rest (see "Caveats" below); rerun those three in isolation before treating their exact
figures as anything more than directional.

| Scenario | Store | Case | Point | 95% CI | Unit |
| --- | --- | --- | --- | --- | --- |
| throughput | memory | simple | 929 | [473, 1091] | workflows/s |
| throughput | memory | step_heavy (20 steps) | 124 | [95.8, 137] | workflows/s |
| throughput | sqlite | simple | 90.9 | [82.1, 96.3] | workflows/s |
| throughput | sqlite | step_heavy | 8.4 | [4.7, 10.8] | workflows/s |
| throughput | postgres | simple | 155 | [132, 161] | workflows/s |
| throughput | postgres | step_heavy | 22.1 | [20.3, 22.8] | workflows/s |
| tick_latency | memory | cache=on / cache=off | 0.115ms / 5.23ms | -- | seconds |
| tick_latency | sqlite | cache=on / cache=off | 2.08ms / 11.8ms | -- | seconds |
| tick_latency | postgres | cache=on / cache=off | 1.61ms / 11.4ms | -- | seconds |
| replay | memory | steps=1000 | 60.6ms | [45.2, 77.4] | seconds |
| replay | sqlite | steps=1000 | 37.4ms | [36.2, 44.7] | seconds |
| replay | postgres | steps=1000 | 31.9ms | [29.0, 45.7] | seconds |
| dispatch_latency | sqlite | width=200,concurrency=4 | 0.354 | [0.330, 0.379] | seconds |
| dispatch_latency | sqlite | width=200,concurrency=16 | 0.273 | [0.259, 0.286] | seconds |
| timer_accuracy | memory | alone / load=50 | 4.7ms / 17.5ms | -- | seconds |
| timer_accuracy | sqlite | alone / load=50 | 15.2ms / 100.9ms | -- | seconds |
| timer_accuracy | postgres | alone / load=50 | 25.2ms / 86.1ms | -- | seconds |
| messaging | memory | update / signal | 0.22ms / 1.91ms | -- | seconds |
| messaging | sqlite | update / signal | 3.77ms / 4.14ms | -- | seconds |
| messaging | postgres | update / signal | 3.41ms / 6.27ms | -- | seconds |
| continuation | memory | generations=40 | 1237 | [959, 1301] | steps/s |
| continuation | sqlite | generations=40 | 192 | [177, 196] | steps/s |
| history_limit | memory | limit_events=150 | 0.093ms | [0.090, 0.096] | seconds |
| history_limit | sqlite | limit_events=150 | 1.80ms | [1.75, 1.86] | seconds |
| store_contention | memory | claim_tick, 1/4/16/64 claimers | 3081 / 4675 / 5161 / 4382 | -- | ops/s |
| store_contention | sqlite | claim_tick, 1/4/16/64 claimers | 337 / 398 / 375 / 333 | -- | ops/s |
| codec_overhead | memory | none / compression / encryption | 894 / 724 / 660 | -- | workflows/s |
| visibility | memory | runs=100 / 1000 / 5000 | 0.38ms / 4.08ms / 28.5ms | -- | seconds |
| visibility | sqlite | runs=100 / 1000 / 5000 | 2.92ms / 2.31ms / 1.75ms | -- | seconds |

This is a representative subset (every scenario, not every store/case combination it
ran); regenerate the full table yourself with `python -m benchmarks run --out
results.json` and `benchmarks.report.console_table`/`markdown_table`.

## Caveats

- **These are single-run point estimates from one machine**, not a distribution over
  many machines or many days. Re-run before making a capacity-planning decision, and
  prefer `compare`'s confidence-interval-aware verdict over eyeballing two numbers.
- **`store_contention`, `codec_overhead` and `visibility` overlapped in time** on the
  machine that produced this table (a scheduling convenience, not a recommended way to
  benchmark): three processes competing for 8 cores adds CPU noise to every backend's
  numbers from that window, and their shared `loomwork_bench` PostgreSQL database (see
  `benchmarks/stores.py`) adds a second, PostgreSQL-specific source of noise on top --
  one PostgreSQL `visibility` case (`runs=5000`) even hit a genuine deadlock between the
  three concurrently-running processes and has no figure above it. This is a benchmark
  methodology artifact, not a Loomwork bug, but it is why this table's PostgreSQL
  figures for those three scenarios are the least trustworthy numbers on this page.
  Don't run multiple benchmark processes against the same PostgreSQL database at once;
  run scenarios sequentially, or point separate invocations at separate
  `--postgres-dsn` clusters.
- **`--quick` numbers are not these numbers.** Quick mode's 1-2 trials exist to catch
  a scenario that broke, not to measure anything; see `benchmarks/README.md`'s note on
  why quick mode's confidence intervals are not trustworthy.
