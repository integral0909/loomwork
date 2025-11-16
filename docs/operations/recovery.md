# Recovering and operating on running fleets

Tools for the operator's side of running Loomwork: undoing the effects of a bug without
losing history, acting on many runs at once, and keeping a single run's history bounded.

## Resetting a run

A bug can make a workflow schedule the wrong step, follow the wrong branch, or otherwise
record decisions nobody wanted. Fixing the code does not undo those decisions: replay
still matches them against the (buggy) history. **Reset** truncates a run's history at a
tick boundary and starts a new run of the same workflow id from there, so the fixed code
gets to make the decision again.

```python
import loomwork as lw

handle = app.get_handle("order-A-17")
new_handle = await handle.reset(
    to=lw.ResetPoint.before_last_failure(),
    reason="bug in charge_card retry logic, fixed in deploy 214",
)
result = await new_handle.result()
```

The same operation is available without writing Python: `loomwork workflow reset
order-A-17 --to before-last-failure --reason "..."` (see the [CLI reference](../cli.md)),
or from the operator console's run detail page (`/console/{namespace}/runs/{run_id}`),
which also shows the reset chain (`reset_from_run_id` backwards, `continued_run_id`
forwards) on both the old and new run's pages. All three -- Python, CLI and console --
call the same `Engine.reset`; a CLI or console session with only an `operator` token
(no `worker` role) still works, through the server's control API (see
[server mode](../server.md)).

### Choosing a reset point

`ResetPoint` names a tick boundary; the new run's history is everything up to and
including that tick, replayed and continued live:

| Point | Keeps | Use it when |
| --- | --- | --- |
| `ResetPoint.first_tick()` | Only `WorkflowStarted`. The new run replays from scratch. | The bug affects the very first decision, or you just want a clean re-run with the original arguments. |
| `ResetPoint.after_tick(n)` | The first `n` completed ticks. | You know exactly which tick introduced the bad decision (from `handle.history()`) and want to keep everything before it. `after_tick(0)` is the same as `first_tick()`. |
| `ResetPoint.before_last_failure()` | Every completed tick except the run's last one. | The common case: a run failed, was cancelled, terminated or timed out, and the bug is in whatever the *last* tick decided. Since a tick that raises a nondeterminism error is never committed to history (ADR-0002), the tick that produced a run's recorded, final outcome is always its last committed one -- so "before the last failure" and "before the last tick" are the same thing. A run with zero or one completed ticks resets to `first_tick()`. |

### What actually happens

1. If the run is still **running**, it is terminated first (`reason` recorded on it,
   exactly like `handle.terminate(reason)`). If it had already **closed** (failed,
   cancelled, terminated, timed out, or even completed), its outcome is left untouched --
   reset only adds a forward link, it never rewrites what already happened.
2. A new run is created for the same workflow id. Its history, up to the chosen tick
   boundary, is byte-for-byte the old run's history (same event ids, same content): no
   remapping, so nothing about determinism or replay changes.
3. The old run's `continued_run_id` is set to the new run's id, reusing the field
   continue-as-new and automatic failure-retries already use to chain runs together.
   This means `handle.result()` on the *old* handle (or on a handle obtained before the
   reset) transparently follows the chain and returns the new run's outcome -- resetting a
   run is, from a caller's point of view, exactly like a continue-as-new or a retry that
   an operator triggered instead of the workflow or a retry policy. The new run's
   `reset_from_run_id` names the run it was reset from, so you can always find the
   original by walking backwards.
4. Nothing is lost. Any input the old run received after the reset point -- a signal, an
   update request, a step or timer outcome that had already arrived before the bug was
   noticed -- is re-delivered to the new run as a freshly appended input event (a new
   event id, the original content). The new run's next tick sees exactly the same
   pending work the old run would have, just without the bad decision in between.
5. Steps and timers the reset point leaves **outstanding** (scheduled by a kept tick,
   with no completion recorded anywhere in the old run's history) are recreated for the
   new run, so a step worker or the timer service can actually finish them. None of the
   old run's step jobs or timers are reused; they are deleted with it when it closes.
   A step or timer whose outcome *did* arrive (even after the reset point -- see point 4)
   is not recreated: it resolves normally once its redelivered input is consumed.

### Why `continued_run_id`, not a dedicated field

An earlier design considered a dedicated `reset_to_run_id` on the old run, parallel to
`reset_from_run_id` on the new one. `continued_run_id` already means "this workflow id's
story continues in that other run" for continue-as-new and for the automatic retry that
follows a workflow-level `RetryPolicy`; a reset is the same kind of handoff, just
initiated by an operator instead of the workflow. Reusing it means every existing piece
of code that follows a run's chain to its true final outcome (`WorkflowHandle.result()`,
the CLI, the console) gets reset support for free, and there is exactly one field to
check for "is this run's result actually somewhere else." `reset_from_run_id` is the one
genuinely new piece of information: which run a reset run came from.

### Concurrency and atomicity

`Engine.reset` / `WorkflowHandle.reset` call the store's `reset_run`, which is atomic per
run touched: closing the old run (if it was running) is one transaction, creating the new
run is another, never both at once (a lock-ordering rule shared with every other
cross-run operation in Loomwork -- see ADR-0004). Between the two, the old run is
immutable (closed runs reject every further append), so the second transaction always
reads a stable history. The reset point is validated against the run's history *before*
anything is touched, so an invalid `after_tick(n)` (higher than the number of ticks the
run actually completed) raises `InvalidResetPointError` without side effects.

If the workflow id already has an unrelated running execution by the time the new run
would be created (someone started it again independently, or a second reset raced this
one), `reset_run` raises `WorkflowAlreadyStartedError` rather than create a second running
execution for the id -- the same invariant `id_conflict_policy` enforces on a normal start.

## Batch operations

`Client.batch_signal`, `Client.batch_cancel` and `Client.batch_terminate` apply one
operation to every run matching a [visibility query](../visibility-queries.md):

```python
summary = await client.batch_terminate(
    "status = 'running' AND workflow_name = 'orders.process' "
    "AND created_at < timestamp '2024-01-01T00:00:00Z'",
    reason="superseded by the 2024 pricing migration",
)
print(summary)
# BatchResult(matched=812, succeeded=810, skipped_closed=2, failed=())
```

```python
summary = await client.batch_signal(
    "status = 'running' AND attr.customer_tier = 'gold'",
    cancel_channel, "customer requested a refund",
)

summary = await client.batch_cancel("status = 'running' AND attr.region = 'eu-west'")
```

`query` must be a plain filter: it must not carry its own `ORDER BY` or `LIMIT`, because
batch methods page through matches themselves (a keyset cursor on `(created_at, run_id)`,
stable even while earlier pages close or otherwise change the runs they touched). Paging
and applying the operation both happen client-side, with bounded concurrency
(`concurrency=10` by default) so a batch of thousands of runs does not open thousands of
concurrent store calls at once; `page_size` (default 100) controls how many runs are
fetched per query round trip.

A `BatchResult` never raises for individual runs: `matched` is how many the query
returned; `succeeded` were applied; `skipped_closed` had already closed between being
matched and being acted on (not an error -- signalling or cancelling a closed run is a
no-op); anything else lands in `failed` as a `BatchError(run_id, error)`, and the batch
keeps going. Raise the concurrency for large fleets, lower it if the store is a
bottleneck; lower `page_size` if runs are closing quickly enough that a keyset page might
otherwise skip one (an edge case only visible with pages in the tens of thousands and no
`status = 'running'` filter to shrink the result set as you go).

The CLI exposes the same three operations as `loomwork workflow batch signal|cancel|
terminate --query Q [...]`, with `--dry-run` to preview matches before acting,
`--page-size`/`--concurrency` matching the Python parameters above, and a confirmation
guard (`--max-runs`, default 1000; pass `--yes` to act on more) since a broad query is
easy to write by mistake and hard to undo. The operator console's batch page
(`/console/{namespace}/batch`) gives the same preview-then-act flow for cancel and
terminate (not signal, which needs a channel and value the console has no generic form
for) with the query text carried through as the confirmation. See the
[CLI reference](../cli.md) and [console](../console.md).

## History size limits

Every event a workflow's decisions and inputs produce is permanent; a run that never
continues as new keeps growing. `EngineConfig` bounds this in two ways:

| Field | Default | Effect |
| --- | --- | --- |
| `history_warning_events` | 10000 | The tick where the run's history first reaches this many events logs one warning and calls `Telemetry.history_size_warning(run_id, workflow_name, size, "events")`. |
| `history_limit_events` | 50000 | The run is failed cleanly, before the tick runs, with a `HistoryLimitExceeded` failure recommending continue-as-new. |
| `history_warning_bytes` / `history_limit_bytes` | disabled (`None`) | The same two checks, evaluated against the store's running total of encoded event bytes instead of event count. Enable both together; there is no useful "limit without a warning first." |

The limit check runs against the history size **as it was after the last committed
tick**, before the current tick's workflow code executes -- never mid-tick, and never
based on anything the workflow decides live. That makes the failure deterministic: a
worker retrying the same tick (after a crash, a lease loss, a nondeterminism error before
the fix landed) sees the same limit breached and produces the same clean failure every
time, and a tick is never left half-written by discovering the limit partway through.

Handle it like any other durable failure:

```python
@lw.workflow
async def long_running(...) -> None:
    while not done():
        await do_work_chunk()
        if lw.continue_as_new_suggested():
            lw.continue_as_new(...)
```

If `continue_as_new_suggested()` (driven by `continue_as_new_after_events`, a separate,
lower threshold meant to be checked *before* things get bad) is heeded, `history_limit_*`
should never trigger in practice; it exists as the backstop for workflows that ignore the
hint or where continue-as-new was added after some runs had already grown past it. Once a
run does fail with `HistoryLimitExceeded`, [reset](#resetting-a-run) it to a point before
the history grew unbounded, if there is a reasonable one, or accept its failure and start
a fresh run.
