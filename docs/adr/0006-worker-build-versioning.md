# ADR-0006: Worker build versioning (pinned runs)

- Status: accepted
- Date: 2025-12-13

## Context

`loomwork.patched()` and the compatibility checker (ADR predates this document; see
`docs/guides/versioning.md`) let a single fleet of workers, all running the *same*
deploy, keep replaying old histories correctly by branching on a recorded marker. That
works well when the new code is a superset of the old: both branches exist in the same
binary.

It does not work when the safe move is to *not run the new code against old histories at
all* -- a rewrite that changed so much of a workflow's shape that expressing it as
`patched()` branches would mean keeping the entire old implementation alongside the new
one indefinitely, or a library upgrade that changes step behaviour in a way no branch in
*workflow* code can express. Operators in that position today have two bad options: hold
the deploy until every old run drains (which can be days or months for long-running
workflows), or accept a wave of `NondeterminismError`s while old runs retry against code
that can never replay them.

## Decision

Workers may declare a **build id** (`WorkerOptions.build_id`, `loomwork worker
--build-id`). A run's **versioning policy** -- `AUTO_UPGRADE` (default) or `PINNED` --
decides whether its ticks care about build ids at all:

- `AUTO_UPGRADE` is today's behaviour, unchanged: any worker may claim any tick.
- `PINNED` means the run's ticks are claimed only by a worker whose build matches the
  build that ran the run's **first** tick, or one an operator has declared **compatible**
  with it. The pin is recorded on the run (`ExecutionRecord.pinned_build_id`) and on
  every `TickCompleted` (`build_id`), set atomically in the transaction that commits the
  first tick.

An **unversioned worker** (`build_id=None`, the default) is eligible for every run,
pinned or not -- exactly today's behaviour. This is the load-bearing property of the
whole design: a deployment that never sets a build id anywhere behaves identically to
one running a Loomwork release before this feature existed. Versioning by build id is
opt-in per worker, not a mode switch on the engine or the store.

Policy is resolved per run at creation (`EngineConfig.default_versioning_policy`,
`EngineConfig.workflow_versioning_policy` per workflow name, or an explicit
`versioning_policy=` argument to `start_workflow`, in that order of precedence, lowest
to highest). It is fixed for the run's lifetime; only the pin (which build) can change
later, via `repin`.

### Compatibility sets

Two builds can be declared compatible: `declare_build_compatible(build_id, compatible_
with)` lets workers of `build_id` claim runs and pinned-queue steps pinned to
`compatible_with`. This is directed (B compatible with A does not imply A compatible
with B) and is how a same-day hotfix -- a build that touched no workflow code, only a
step's implementation or a dependency -- can drain the build it replaces without
runs staying pinned to a build nobody runs any more.

### Why not extend `patched()` instead

`patched()` operates *inside* one build: it lets one worker process make different
decisions for old and new histories. Build pinning operates *between* builds: it lets
two builds run side by side, each only ever seeing the histories it can safely replay.
They compose rather than compete -- a `PINNED` deploy can still use `patched()` for
small changes within a build, and an `AUTO_UPGRADE` deploy already relies on `patched()`
for everything. The trade-off:

| | `patched()` | Build pinning (`PINNED`) |
| --- | --- | --- |
| Old and new code | Same process, same deploy | Separate deploys, run side by side |
| What changes safely | Anything expressible as an `if` branch on a marker | Anything -- the old build never sees new-code decisions at all |
| Cost | A permanent branch (until `deprecate_patch`/removal) for every change | Running two (or more) builds until old runs drain |
| Operator visibility | Compatibility checker, golden histories | `builds_summary()`: pinned run counts and live workers per build |
| Best for | Frequent, small, forward-compatible changes | Infrequent, large rewrites; library upgrades; anything `patched()` would make unreadable |

See the updated `docs/guides/versioning.md` for the operator-facing version of this
comparison.

### Steps are not pinned by default

Steps are never replayed, so nothing about correctness requires pinning them: a step
scheduled by a pinned run's workflow code can be executed by any worker without risking
a nondeterminism error. Pinning a *queue* (`EngineConfig.pinned_queues`) is therefore a
deployment choice, not a correctness one: it exists for workers that hold build-specific
credentials, binaries or drivers, where running an old run's step on a new build's
worker (or vice versa) would be wrong for reasons outside the workflow engine's model.
Opting a queue in copies the run's pin onto every step job scheduled for that queue, at
the same commit that would pin the run itself, so a pinned run's first tick and its
first pinned-queue step end up pinned together.

### Claiming: the SQL filter

`claim_tick`/`claim_step` gain an optional `build_id` parameter (the claiming worker's
own build). The added predicate, in both SQLite and PostgreSQL:

```sql
AND (pinned_build_id IS NULL
     OR pinned_build_id = :build_id
     OR pinned_build_id IN (SELECT compatible_with FROM lw_build_compatibility
                             WHERE build_id = :build_id))
```

skipped entirely when the caller passes `build_id=None`. `lw_executions.pinned_build_id`
and `lw_step_jobs.pinned_build_id` each get a partial index (`WHERE ... IS NOT NULL`),
and `lw_build_compatibility`'s primary key `(build_id, compatible_with)` makes the
sub-query an index lookup on a tiny table (one row per declared compatibility, not per
run). `MemoryStore` applies the same three-way check in Python.

### Presence and drain visibility

An operator retiring a build needs to know two things: how many runs are still pinned
to it, and whether any live worker could still process them. `count_pinned_runs()`
answers the first from the indexed `pinned_build_id` column. The second needs *some*
notion of which builds are alive, which is not otherwise observable (a build with zero
running workers looks, from the store alone, identical to one between deploys).

Workers therefore heartbeat their build id into `lw_worker_presence`
(`worker_id` primary key, `build_id`, `last_seen_at`, `expires_at`) on a low-frequency
loop (default every 30s; `WorkerOptions.build_heartbeat_interval`), one upsert per
heartbeat. This is the cheap design the presence mechanism needed:

- **No claiming dependency.** Presence is read only by `builds_summary()` and the drain
  telemetry hook; it never gates `claim_tick`/`claim_step`. A worker with a stale or
  missing presence row still claims work normally -- presence failing open, rather than
  workers losing eligibility because a heartbeat was delayed.
- **One row per worker, not per tick.** Unlike lease renewal (per run, per step), this
  is one upsert per worker per interval regardless of how much work it does.
- **No cleanup loop.** Expired rows (`expires_at <= now`) are simply excluded by
  `list_worker_builds(now)`'s `WHERE expires_at > now`; they are overwritten by that
  worker's next heartbeat or left as harmless dead rows otherwise (`lw_worker_presence`
  is one row per worker process that has ever run, not per tick -- it does not grow
  with run volume).
- **Redundant checking is fine.** Any worker with `report_build_drain=True` (default)
  independently recomputes `builds_summary()` on the same interval and calls
  `Telemetry.pinned_runs_waiting(build_id, count)` for every orphaned build (a nonzero
  pinned count with no live or compatible worker). Several workers doing this is
  duplicate work, not a correctness hazard -- there is no election, because electing a
  leader is more machinery than the check is worth.

### Interaction with other engine mechanisms

- **Continue-as-new and workflow retries.** The successor run inherits the *policy*
  (`versioning_policy`) but starts **unpinned**: its first tick pins it fresh, to
  whichever build is live at that moment. A long continue-as-new chain therefore keeps
  draining forward onto current code rather than perpetually re-pinning itself to a
  build an operator is trying to retire -- the whole point of `PINNED` is to bound how
  long a *single run* can hold code back, and a run is one link in that chain, not the
  whole chain.
- **Reset.** The same rule: a reset run (`Store.reset_run`) inherits the original's
  policy and starts unpinned, for the same reason -- a reset is explicitly "try this
  history against current code."
- **Child workflows.** Not inherited. A child is its own workflow type, resolved fresh
  from `EngineConfig` exactly as a top-level `start_workflow` call would be. A `PINNED`
  parent commonly starts `AUTO_UPGRADE` children (and vice versa); coupling them would
  make an unrelated workflow's deploy strategy dictate this one's.
- **Schedules.** Runs a schedule starts go through the same `ExecutionRecord`
  construction as any other run and default to `AUTO_UPGRADE` today; wiring
  `ScheduleService` through `EngineConfig.resolve_versioning_policy` is straightforward
  follow-up work, tracked separately since `schedules/` is outside this change's scope.

## Consequences

- Unversioned deployments (no worker ever sets `build_id`) are provably unaffected:
  every new predicate is skipped, every new column defaults to `NULL`/`auto_upgrade`,
  and the conformance suite runs the full existing suite with `build_id=None` throughout.
- Operators get a third tool alongside `patched()` and the compatibility checker, for
  the case neither fits: a rewrite too large to branch on, given time to drain naturally
  while new runs get new code immediately.
- Running two builds side by side costs real infrastructure (two worker fleets, however
  briefly) that `patched()` does not. `PINNED` is a deliberate choice per workflow type,
  not a default.
- A `PINNED` run with no live or compatible worker for its pinned build is stuck, by
  design -- that is the safety property. `builds_summary()` and
  `Telemetry.pinned_runs_waiting` exist precisely so this is visible well before it is
  surprising, and `repin` is the escape hatch when a build truly is not coming back.
