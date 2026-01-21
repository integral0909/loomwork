# Blue/green deploy

Worker build versioning ([ADR-0006](../../docs/adr/0006-worker-build-versioning.md)):
running two incompatible builds of the same workflow side by side, against one shared
store, instead of holding a deploy until every in-flight run drains or rewriting the
change as a `loomwork.patched()` branch.

## Scenario

`blue_green_deploy.process_widget` (in `workflows.py`) has two versions, both registered
under the same workflow name:

- **v1** (`process_widget_v1`) reserves a widget with `reserve_widget`, waits out a
  one-hour pick window, then ships it.
- **v2** (`process_widget_v2`) is a rewrite too large for `patched()` to express
  cleanly in a small example: it calls a *different* step, `reserve_widget_v2`, at the
  same position -- unsafe for any run whose history already committed to v1's step (see
  the compatibility table in `docs/guides/versioning.md`).

`main.py` runs the whole rolling-deploy story end to end against one `SQLiteStore`:

1. **Blue** (`WorkerOptions(build_id="blue-...")`) takes two orders. Each pins to the
   blue build the moment its first tick commits (`EngineConfig.workflow_versioning_policy`
   marks this workflow `VersioningPolicy.PINNED`).
2. Blue is retired -- a bug was just found in its `ship_widget` step, and the fleet must
   be replaced before the two orders finish. `client.versioning.builds_summary()` shows
   blue's two pinned runs with `live_workers=0` and `orphaned=True`: they are stuck,
   visibly, until a matching (or compatible) build shows up.
3. **Green** (v2) comes up and immediately takes two *new* orders, which pin to it.
4. A **same-day hotfix** of blue -- v1's workflow code, completely unchanged, only
   `ship_widget`'s body fixed (a placeholder tracking number replaced with a real one) --
   joins and is declared compatible with the original blue build:
   `client.versioning.declare_compatible(hotfix_build, blue_build)`. This is the case
   ADR-0006 calls out by name: a build that touched no workflow code can drain the build
   it replaces without any run staying pinned to a build nobody runs any more.
5. All four orders finish: the first two, still pinned to blue, ship through the
   hotfixed step; the new two run entirely on v2. `builds_summary()` afterwards shows
   blue fully drained (no pinned runs, nothing left to report) and green still serving.

## Loomwork features used

- `WorkerOptions(build_id=...)` and `EngineConfig(workflow_versioning_policy={...:
  VersioningPolicy.PINNED})` (`loomwork.options.VersioningPolicy`).
- `client.versioning.builds_summary()` -- pinned run counts, live worker counts, and the
  `orphaned` flag for a build with nothing left to serve it.
- `client.versioning.declare_compatible(build_id, compatible_with)` for a same-day
  hotfix build that can safely finish draining the build it replaces.
- Several `Loomwork` apps, each its own build, sharing one `SQLiteStore`.

## A note on ordering

Each order is started, and given a moment to pin, while only the build it is meant to
pin to is running. `docs/operations/deployment.md` puts it plainly: "new runs pin to
whichever build's worker processes their first tick -- normally the new build, *if it
has spare capacity*, since both are eligible until a run is pinned." That "normally" is
a real race in production (resolved by capacity, not by which client started the run);
this example resolves it the way a deterministic demo (and test) needs to, by making
sure only one build-carrying worker is ever competing for a given run's first tick.

## Running it

```console
uv run python -m examples.blue_green_deploy.main
```

Uses `loomwork.ManualClock` for the pick window, advanced with `examples.support`'s
helpers; a worker's build-presence heartbeat is otherwise real-time paced
(`WorkerOptions.build_heartbeat_interval`), so blue's is set short enough to visibly
lapse once its workers stop, and green/the hotfix build's are set long enough to survive
the rest of the demo -- a real deployment leaves the (real-time) default alone.

## Tests

`tests/examples/test_blue_green_deploy.py` checks:

- an order started under blue stays pinned to it, becomes visibly `orphaned` once blue
  is retired, and resumes (through the hotfixed step) once a compatible build joins;
- a new order started under green pins to green and is unaffected by blue's backlog;
- `builds_summary()` reflects each phase correctly, including a fully drained build no
  longer appearing at all;
- each build's recorded history replays cleanly under its own code
  (`loomwork.versioning.check_history`) and -- the property worker build pinning exists
  to guarantee -- does *not* replay under the other build's code.
