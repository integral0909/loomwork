# Incident recovery

Operator recovery from a bug in production (`docs/operations/recovery.md`): fix the
code, reset one high-priority run individually, and batch-terminate the rest -- plus an
idempotent restart while the incident is still open.

## Scenario

`process_claim` (in `workflows.py`) validates a claim, waits out a review window, then
computes its payout. `compute_payout` (in `steps.py`) has a real bug: it rejects any
claim over $1,000, not just fraud-flagged ones -- a wrong condition, not a business
rule. The workflow does not catch the step's failure, so the bug fails the *run itself*,
cleanly (see `docs/operations/recovery.md`'s "when a run breaks anyway"): this is the
common case `ResetPoint.before_last_failure()` exists for.

`main.py` runs the incident end to end:

1. Three claims come in: **claim-VIP** (a large, legitimate, urgent claim -- a short,
   ten-minute review window) and **claim-A**/**claim-B** (smaller, ordinary claims, on
   the realistic six-hour window).
2. A webhook retries claim-A's submission before it has finished. Starting it again
   with `IdConflictPolicy.USE_EXISTING` returns a handle to the *same* running
   execution instead of raising `WorkflowAlreadyStartedError` or starting a duplicate.
3. claim-VIP's short window elapses; the bug rejects its (perfectly legitimate)
   quarter-million-dollar payout. claim-A and claim-B are unaffected -- their window
   has not elapsed, so they are still running.
4. **The bug is fixed**: a new `Loomwork` app is brought up with `compute_payout`'s
   body corrected (`compute_payout_fixed`) -- the workflow itself is unchanged, so
   nothing about this redeploy needs `patched()` (see the compatibility table in
   `docs/guides/versioning.md`: a step's implementation is always safe to change).
5. claim-VIP is worth recovering individually: `handle.reset(to=ResetPoint.
   before_last_failure())` drops the tick that recorded the bad decision and lets the
   fixed code make it again, from the same point -- validation is not repeated.
6. Everything else still in flight (claim-A and claim-B) is paused for the incident in
   one call: `client.batch_terminate(query, reason=...)` over a visibility query
   (`status = 'running'`), rather than resetting each one individually.

## Loomwork features used

- `handle.reset(to=loomwork.ResetPoint.before_last_failure(), reason=...)`.
- `client.batch_terminate(query, reason=...)` over a
  [visibility query](../../docs/visibility-queries.md).
- `loomwork.IdConflictPolicy.USE_EXISTING` for an idempotent restart against a
  currently running execution.
- Swapping a step's implementation across a redeploy while keeping the workflow code,
  and its recorded decisions, unchanged.

## Running it

```console
uv run python -m examples.incident_recovery.main
```

Uses `loomwork.ManualClock` and `MemoryStore` (durability isn't the point here --
operator tooling is); claim-VIP's short window and claim-A/B's realistic six-hour one
are advanced with `examples.support`'s helpers, so the whole incident settles in a
fraction of a second.

## Tests

`tests/examples/test_incident_recovery.py` checks:

- a run that failed under the buggy step recovers cleanly once reset against an app
  running the fixed step -- it completes with the correct payout, and validation is
  not re-run;
- `batch_terminate` over `status = 'running'` matches and terminates every run still
  in flight, each with the given reason;
- `IdConflictPolicy.USE_EXISTING` returns the same run id for a duplicate submission,
  and the claim is validated only once.
