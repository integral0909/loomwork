# Troubleshooting

## A run is not making progress

Start with the execution record:

```python
record = await handle.describe()
print(record.status, record.tick_pending, record.tick_failures, record.last_tick_error)
print(await handle.pending_steps())
```

| Symptom | Likely cause | What to do |
| ------- | ------------ | ---------- |
| `tick_failures > 0`, `last_tick_error` starts with `NondeterminismError` | Workflow code changed incompatibly | Roll back, or add `loomwork.patched()`; see [versioning](../guides/versioning.md) |
| `last_tick_error` with another exception | A bug in workflow code raised | Fix and deploy; the tick retries automatically |
| `tick_pending` true, no failures, nothing happens | No worker registers this workflow | Start a worker with the workflow in its registry |
| Pending step jobs with `attempt == 0` | No worker polls the step's queue or knows the step | Check `task_queues` and step registration |
| Pending step job with a lease far in the future | Its worker is alive but slow, or died and the lease has not expired | Wait for the lease; use `heartbeat_timeout` for long steps |
| Waiting on a timer that should have fired | No worker has `run_timers=True` | Enable timers on at least one worker |
| Child result never arrives | No worker relays the outbox | Enable `run_outbox` on at least one worker |

## Nondeterminism errors

The error names the first divergence:

```
NondeterminismError: history event 9 records StepScheduled(seq=2, step='orders.send_email'),
but the workflow code produced ScheduleStep(seq=2, step='orders.send_sms')
```

Reproduce locally from the exported history:

```python
from loomwork.history import history_to_json
Path("broken.json").write_text(history_to_json(await handle.history()))
```

```python
from loomwork.versioning import check_history_file
print(check_history_file("broken.json", Registry([order])))
```

Common causes: reading the clock or randomness directly, iterating a `set`, calling
`os.environ`, conditional logic on data fetched outside a step, or reordering steps.
Run `python -m loomwork.lint` on the workflow module.

## `DeterminismViolation`

Workflow code attempted I/O (`asyncio.to_thread`, sockets, subprocesses) on the
workflow's event loop. Move the operation into a step.

## A step keeps running after its workflow finished

Steps only learn that their result is no longer needed through heartbeats. Long steps
should call `loomwork.heartbeat()` regularly; they are then cancelled at their next
`await`.

## Payload errors

`PayloadError: payload does not match <type>` means a recorded value cannot be decoded
into the annotated type, usually because a model changed incompatibly. Make new fields
optional with defaults, or version the model.

`IntegrityError` from the encryption codec means a payload was encrypted with a key that
is not in the key ring, was tampered with, or (with `require_encryption=True`) is not
encrypted at all.

## `WorkflowAlreadyStartedError`

A run with that workflow id is still running. This is how Loomwork makes starts
idempotent: catch it and use `app.get_handle(workflow_id)` to attach to the existing run,
or use `signal_with_start`.
