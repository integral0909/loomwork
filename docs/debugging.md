# Debugging: the replay debugger and history graphs

Two tools for understanding what a run actually did: `loomwork replay` steps through a
history tick by tick against your workflow code, and `loomwork workflow graph` draws its
steps, timers, children and waits as a diagram. Both work on a running store, an
[exported history file](cli.md#loomwork-workflow), or an [archive](operations/archiving.md)
-- the same history, wherever it lives.

## The replay debugger

```
loomwork replay WORKFLOW_ID [--run RUN_ID] --app REF
loomwork replay --history FILE --app REF
loomwork replay --archive DIR --run RUN_ID --app REF
```

This replays the history tick by tick, exactly as a worker would (see
[ADR-0002](adr/0002-tick-delimited-history.md)): each tick's recorded inputs are
delivered to your workflow code, and the commands it produces are compared one by one
against the decision events actually recorded. The output is a table, one row per tick:

```
$ loomwork replay order-42 --app myapp.workflows
TICK  EVENT  TIME                  CONSUMED  INPUTS  COMMANDS
1     2      2024-06-01T09:12:44Z  1         1       ScheduleStep(seq=1, step='charge_card')
2     5      2024-06-01T09:12:45Z  4         1       StartTimer(seq=1, fire_at=...)
3     8      2024-06-01T09:13:45Z  7         1       CompleteWorkflow
workflow code: /app/myapp/workflows.py:14
```

A run whose code has since changed incompatibly stops at the first mismatch and explains
it precisely instead of just failing:

```
$ loomwork replay order-42 --app myapp.workflows  # after renaming a step
TICK  EVENT  TIME                  CONSUMED  INPUTS  COMMANDS
1     2      2024-06-01T09:12:44Z  1         1       ScheduleStep(seq=1, step='charge_card_v2')
DIVERGED at event 3:
  expected: StepScheduled(seq=1, step='charge_card')
  produced: ScheduleStep(seq=1, step='charge_card_v2')
  history event 3 records StepScheduled(seq=1, step='charge_card'), but the workflow
  code produced ScheduleStep(seq=1, step='charge_card_v2')
workflow code: /app/myapp/workflows.py:14
```

The command exits 1 when it diverges, so it composes with `loomwork compat` (which
answers "would any run break", across many runs) when you already know *which* run broke
and want to see exactly where. `--history FILE` replays an exported history document
without a store at all; `--archive DIR --run RUN_ID` replays a run pulled from an
archive, decoding its still-encoded payloads with the codec configured on `--app` exactly
as a live run would.

### Stopping early and inspecting queries

```
loomwork replay order-42 --app myapp.workflows --until-tick 2 --query state
```

`--until-tick N` stops after the Nth activation instead of replaying to the end. With the
workflow instance still live at that point, `--query NAME` calls one of its registered
`@lw.query` handlers (zero-argument queries only) and prints the result -- the same
mechanism `loomwork workflow query` uses against a live run
(`loomwork.engine.queries.QueryRunner`), except here it can look at any point in the
run's past, not just its current state.

### Programmatic API

```python
from loomwork.debug import trace_replay

trace = trace_replay(history, registry, converter=converter, until_tick=5, queries=["state"])
if not trace.ok:
    print(trace.divergence.explanation)
for tick in trace.ticks:
    print(tick.tick_number, tick.command_descriptions)
```

`trace_replay` never raises on a mismatch (unlike
`loomwork.runtime.replayer.Replayer`, which a worker relies on failing fast): it records
a `Divergence` and stops, so a caller can show everything that replayed correctly and
explain the first thing that did not. It raises `DebugError` only for a history that
cannot be replayed at all (empty, not starting with `WorkflowStarted`, or naming a
workflow the given registry does not define).

## History graphs

```
loomwork workflow graph WORKFLOW_ID [--run RUN_ID] --format mermaid|dot|text
```

Renders a run's steps, timers, children and waits (time spent blocked on a signal, an
update or a cancellation request) as one of:

- **`mermaid`** -- a `gantt` diagram, one section per lane, ready to paste into a
  Markdown file or ticket that renders Mermaid.
- **`dot`** -- a Graphviz `digraph`, one cluster per lane, for `dot -Tsvg` or any tool
  that reads DOT.
- **`text`** -- a compact, terminal-friendly timeline; the default, and the fastest way
  to see what a run spent its time on without leaving the shell.

```
$ loomwork workflow graph order-42 --format text
2024-06-01T09:12:44+00:00  step   charge_card               completed   -> 2024-06-01T09:12:45+00:00
2024-06-01T09:12:45+00:00  timer  timer 1                   fired       -> 2024-06-01T09:13:45+00:00
2024-06-01T09:13:45+00:00  wait   channel 'cancel'           resolved    -> 2024-06-01T09:13:46+00:00
```

These are pure functions of the event history (`loomwork.debug.graph`): they read event
kinds, sequence numbers and timestamps only, never payload contents, so they work
identically on a live run, an exported history file, or an archived one -- including one
whose payloads are still encrypted.

### Programmatic API

```python
from loomwork.debug import build_timeline, render_mermaid, render_dot, render_text

entries = build_timeline(history)          # list[TimelineEntry]: lane, label, start, end, status
diagram = render_mermaid(history, title="order-42")
```
