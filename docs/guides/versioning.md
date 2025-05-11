# Changing workflow code safely

Runs can live for days or months, and the code that replays them is whatever you
deployed most recently. A change is **safe** if every running history replays under the
new code to the same decisions. This guide explains which changes are safe, how to make
unsafe changes safe with patches, and how to check before deploying.

## What is safe to change

Replay matches each decision the code makes against the recorded decision event, by
**kind, sequence number and identity** (the step name, the child workflow name, the
timer's sequence).

| Change                                                  | Safe? |
| ------------------------------------------------------- | ----- |
| Step implementation (its body)                          | yes   |
| Arguments passed to a step, a sleep's duration          | yes   |
| Logging, comments, refactoring that keeps decisions     | yes   |
| Code after the last decision any running history reached | yes  |
| Calling a different step at the same point              | **no** |
| Adding, removing or reordering steps, timers or children before existing ones | **no** |
| Changing a workflow or step **name**                    | **no** |

## Patches

`loomwork.patched()` lets old and new behaviour coexist:

```python
@lw.workflow
async def order(order: Order) -> None:
    await charge(order)
    if lw.patched("sms-receipts"):
        await send_sms(order)       # new behaviour
    else:
        await send_email(order)     # what old runs already recorded
    await ship(order)
```

- **New runs**, and running ones that reach this point after the deploy, take the new
  branch; a marker is recorded in their history.
- **Runs that already passed this point** replay their recorded decision: `patched()`
  returns `False` while replaying history without the marker.

When no run that predates the patch remains (check with a visibility query or wait out
your retention period), remove the old branch but keep the call as `deprecate_patch`, so
histories that recorded the marker still replay:

```python
    lw.deprecate_patch("sms-receipts")
    await send_sms(order)
```

Once those runs are gone too, delete the `deprecate_patch` call.

## Code fingerprints

Every tick records a fingerprint of the workflow code that executed it
(`TickCompleted.code_version`). Fingerprints ignore comments, docstrings, type
annotations and formatting, so they change only when the logic does. Use them to see
which deploy made each decision of a run:

```python
for event in await handle.history():
    if event.kind == "tick_completed":
        print(event.time, event.code_version)
```

## Checking compatibility before deploy

The compatibility checker replays recorded histories with the code you are about to
deploy and reports every run that would break:

```python
from loomwork.versioning import check_compatibility

report = await check_compatibility(store, Registry(workflows=[order, ...]))
print(report.format_text())
if not report.ok:
    raise SystemExit(1)
```

```
[FAIL] orders.process: 118 compatible, 3 incompatible
  run 01J2... (running, recorded by code 3f9a1c2e0b7d) at event 9: history event 9
  records StepScheduled(seq=2, step='orders.send_email'), but the workflow code produced
  ScheduleStep(seq=2, step='orders.send_sms')
```

It checks all running executions (they will replay on the new code) and a sample of
recent closed runs of each workflow (they show the paths the code takes).

### Golden histories

For checks without database access, export representative histories once and commit
them:

```python
from loomwork.history import history_to_json
Path("tests/golden/order-happy-path.json").write_text(history_to_json(await handle.history()))
```

```python
from loomwork.versioning import check_history_file

def test_order_code_replays_golden_histories() -> None:
    for path in Path("tests/golden").glob("*.json"):
        assert check_history_file(path, Registry(workflows=[order])).ok, path
```

## When a run breaks anyway

A run whose history no longer replays does not fail. Its tick fails with a
`NondeterminismError` and is retried with backoff (see `last_tick_error` on the
execution). Roll back, or deploy a fix that uses `patched()`, and the run resumes.
