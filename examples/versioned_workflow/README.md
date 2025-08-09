# Versioned workflow

Evolving a workflow's code while runs are in flight, and checking a change against
recorded history *before* deploying it.

## Scenario

`workflows.py` defines four versions of one onboarding workflow, all registered under
the same name, `versioned_workflow.onboard` (only one is ever handed to a running
deployment at a time -- see `main.py`):

- **v1** always sends a welcome email.
- **v2** wants to switch to SMS, guarded by `loomwork.patched("welcome-sms")`.
- **v2 (unsafe)** makes the same change *without* the guard.
- **v3** is v2 once no run predating the patch is left: `loomwork.deprecate_patch`
  replaces the `if`.

`main.py` runs the story in order:

1. Starts a run under v1. It sends its welcome email immediately, then goes to sleep
   for an hour -- so its history already commits to the email branch before anything
   else happens.
2. **Before deploying anything**, checks both v2 candidates against that recorded
   history with `loomwork.versioning.check_compatibility`: the guarded version reports
   `ok`; the unsafe one reports a `NondeterminismError`, naming the exact event where a
   replay would diverge (it tried to schedule an SMS step where the history already
   recorded an email step).
3. Deploys the *guarded* v2 and starts a second run. Once both are done: the first run
   -- which never called `patched()` under v1 -- still resolves to `False` on replay
   for that call (because its history doesn't yet contain a `PatchRecorded` marker) and
   sends email, exactly as it originally would have; the second, brand new run resolves
   `patched()` to `True` and sends SMS.
4. Checks both now-closed histories against v2 again, confirming both are compatible
   going forward.

## Loomwork features used

- `loomwork.patched()` / `loomwork.deprecate_patch()` for evolving workflow logic
  safely.
- `loomwork.versioning.check_compatibility()`, replaying recorded (and in-flight)
  histories against a candidate `Registry` and reporting exactly where an incompatible
  change would diverge.
- `TickCompleted.code_version` (via `WorkflowDefinition.fingerprint`), recorded on every
  tick and surfaced in a compatibility report so you can see which code version wrote
  which history.

## Running it

```console
uv run python -m examples.versioned_workflow.main
```

Uses `loomwork.ManualClock` for the workflow's hour-long durable sleep, advanced
instantly with `examples.support.advance_until`; nothing here waits in real time, so the
whole four-part story finishes in a fraction of a second.

## Tests

`tests/examples/test_versioned_workflow.py` checks:

- `check_compatibility` reports the guarded v2 as fully compatible with a v1 history in
  flight;
- it reports the unsafe v2 as incompatible, with `nondeterministic=True` and a
  `diverged_at_event` pointing at the actual mismatch;
- after deploying the guarded v2, a run that predates the patch still takes the old
  (email) branch, while a fresh run takes the new (SMS) one;
- `check_history` confirms v3 (with `deprecate_patch`) still replays a history that
  already recorded the patch decision.
