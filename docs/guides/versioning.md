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

## Patching vs. pinning: which to use

`patched()` is not the only way to deploy incompatible workflow code. **Worker build
versioning** (see [ADR-0006](../adr/0006-worker-build-versioning.md)) lets you run two
builds of your worker fleet side by side: old runs keep going to the old build, new runs
go to the new build, and no run ever sees a decision from code it cannot safely replay.

```python
from loomwork.options import VersioningPolicy

# Runs of this workflow stay on whichever build ran their first tick.
config = EngineConfig(
    workflow_versioning_policy={"orders.process": VersioningPolicy.PINNED},
)
```

```bash
loomwork worker --app myapp.workflows --build-id "$(git rev-parse --short HEAD)"
```

| Use `patched()` when...                                   | Use pinning when...                                      |
| ----------------------------------------------------------- | --------------------------------------------------------- |
| The change is small: a step swapped, a branch added         | The change is too large or too frequent to express as `if` branches |
| You want one worker fleet, one deploy                       | You can run two builds side by side, even briefly          |
| Old runs finish in hours, not months                        | Old runs can safely take as long as they need to drain     |
| You will remove the branch once old runs are gone           | You never want the new build to run old decisions at all   |

They compose: a `PINNED` workflow can still call `patched()` for small changes within
one build, and switching a workflow to `PINNED` does not require removing existing
patches.

To pin a run explicitly (overriding the workflow's configured default) or to check on
a build's drain progress:

```python
handle = await client.start(process_order, order, versioning_policy=VersioningPolicy.PINNED)

for build in await client.versioning.builds_summary():
    print(build.build_id, build.pinned_running_runs, build.live_workers, build.orphaned)
```

A build with `pinned_running_runs == 0` has no runs left pinned to it and can be
retired. One with `orphaned=True` has runs waiting for a build (or a build declared
compatible with it) that has no live workers -- join one, declare a compatible build, or
`await client.versioning.repin(run_id, other_build_id)` to move it. See
`docs/operations/deployment.md` for the full rolling-deploy walkthrough.

### Child workflows and schedules

A child workflow's versioning policy is **not inherited** from its parent: it resolves
fresh from engine configuration, exactly as a top-level `start_workflow` call would,
unless the parent overrides it explicitly:

```python
lw.start_child(rewrite_heavy_workflow, versioning_policy=VersioningPolicy.PINNED)
```

A `PINNED` parent commonly starts `AUTO_UPGRADE` children and vice versa -- coupling them
would make one workflow's deploy strategy dictate an unrelated one's. The same applies to
runs a schedule starts: `client.schedules.create(..., versioning_policy=...)` overrides
the policy for every run that schedule fires; left unset, each resolves independently,
exactly like an unspecified top-level start (see `docs/guides/composition.md` and
`docs/guides/schedules.md`).

## When a run breaks anyway

A run whose history no longer replays does not fail. Its tick fails with a
`NondeterminismError` and is retried with backoff (see `last_tick_error` on the
execution). Roll back, or deploy a fix that uses `patched()`, and the run resumes.
