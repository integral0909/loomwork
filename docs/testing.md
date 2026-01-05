# Testing workflows

`loomwork.testing` gives workflows and steps a fast, deterministic place to run in a
test suite. Its centerpiece is `WorkflowEnvironment`: an in-memory `Loomwork` app whose
clock a test never has to drive by hand, because the environment skips durable waits
automatically. A workflow that sleeps for thirty days, or a schedule that fires daily
for a year, completes in a handful of milliseconds of real wall-clock time.

This replaces the pattern Loomwork's own integration tests used before this package
existed: a `ManualClock` paired with a hand-rolled polling loop (`tests/helpers.eventually`)
that a test advances step by step, guessing when it is safe to move time forward without
racing real work. `WorkflowEnvironment` makes that guesswork -- and the loop -- entirely
unnecessary.

```python
import loomwork as lw
from loomwork.testing import WorkflowEnvironment


@lw.step
async def charge_card(order_id: str, cents: int) -> str: ...


@lw.workflow(name="orders.process")
async def process_order(order_id: str, cents: int) -> str:
    return await charge_card(order_id, cents)


async def test_process_order() -> None:
    async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card]) as env:
        env.mock_step(charge_card, return_value="ch_123")
        result = await env.execute(process_order, "order-1", 1999)
        assert result == "ch_123"
```

## `WorkflowEnvironment`

Construct one with the workflows and steps under test, exactly as you would a
`Loomwork` app:

```python
env = WorkflowEnvironment(
    workflows=[process_order],
    steps=[charge_card, ship],
    start_time=datetime(2024, 1, 1, tzinfo=UTC),  # optional; defaults to 2024-01-01 UTC
)
async with env:
    ...
```

It is an async context manager. Entering it starts an embedded worker against a fresh,
non-durable `MemoryStore` and a `ManualClock`; exiting it stops the worker and cancels
the background task that watches for idle time. Everything you would do with a
`Loomwork` app is available on it directly:

- `await env.execute(workflow, *args, id=..., timeout=...)` -- start a run and wait for
  its result.
- `await env.start_workflow(workflow, *args, id=...)` -- start a run and get a handle
  back immediately.
- `env.get_handle(workflow_id, ..., result_type=...)` -- a handle by workflow id (always
  follows that id's *latest* run) or run id.
- `await env.signal(workflow_id, channel, value)`, `await env.query(workflow_id, name, *args)`,
  `await env.update(workflow_id, name, *args)`, `await env.cancel(workflow_id, reason)`,
  `await env.terminate(workflow_id, reason)`, `await env.list_executions(...)` -- shortcuts
  for the same call on `env.get_handle(workflow_id)` (or, for listing, `env.client.list(...)`),
  for whenever a workflow id is all you have.
- `env.register(*definitions)` -- add more workflows or steps after construction.
- `env.client`, `env.worker`, `env.registry` -- lower-level access for anything not
  wrapped directly (schedules, `signal_with_start`, inspecting the worker's `.busy`
  counter, ...).

`timeout=` on `execute`/`result` is a real wall-clock number of seconds -- a safety net
against a test hanging forever (for example on a signal nothing ever sends), not a
setting that interacts with the simulated clock in any way.

A subtlety worth calling out: `signal`/`query`/`update`/`cancel` all assume the run is
still open by the time they run. If a workflow's only durable wait is a timeout that
auto time-skipping is free to jump straight past (see below) the moment nothing else is
happening, interacting with it across more than one `await` risks racing that jump --
send the signal (or send whatever the test needs to send) as the very next thing after
starting the run, or build that environment with `auto_time_skipping=False` and drive
the clock yourself for tests that need a run to stay open across several separate steps.

### Auto time-skipping

By default (`auto_time_skipping=True`), a background task continuously watches every
run the environment hosts. The moment *nothing* can make progress at the current
instant -- no worker loop is mid-tick or mid-step, no step is ready to claim, no timer or
schedule is already due, and no outbox message is waiting to be relayed -- it jumps the
`ManualClock` straight to the next such deadline. As soon as that unblocks something (a
timer fires, a schedule's action starts a run, a step's retry backoff ends), real
workers pick the work up immediately, exactly as they would in production.

This means:

```python
@lw.workflow(name="reminders.send")
async def send_reminder() -> str:
    await lw.sleep(timedelta(days=30))
    return "sent"


async def test_thirty_day_reminder() -> None:
    async with WorkflowEnvironment(workflows=[send_reminder]) as env:
        result = await env.execute(send_reminder)  # returns in milliseconds
    assert result == "sent"
    assert env.now() >= env.clock.now()  # the clock really did move forward 30 days
```

runs in milliseconds, not thirty days, and the same is true of a cron schedule that
needs to fire five times, a step retrying with exponential backoff, or a signal-or-timeout
race where nothing ever sends the signal.

**What it never does** is skip past real, runnable work. A step that does something
genuinely slow in real time (calls a test double that itself sleeps, waits on a real
socket, ...) is only ever waited on for real -- the worker executing it holds the
environment "busy" for as long as it takes, and the clock does not move until it is
done. `tests/testing/test_time_skipping.py::test_time_skipping_never_skips_runnable_work`
is a regression test of exactly this: a step that takes 150ms of real time, scheduled
alongside a two-hour durable sleep, both completes in full *and* the overall test still
finishes in well under a second, because only the sleep was skipped.

Disable it for tests that care about exactly when real wake-ups happen (for example
checking a worker's polling behaviour), and drive the clock explicitly instead:

```python
async with WorkflowEnvironment(workflows=[send_reminder], auto_time_skipping=False) as env:
    handle = await env.start_workflow(send_reminder)
    with pytest.raises(TimeoutError):
        await handle.result(timeout=0.1)     # nothing has happened yet
    await env.sleep(timedelta(days=30))      # advance the clock by hand
    assert await handle.result(timeout=5) == "sent"
```

`env.now()` returns the environment's current simulated time; `await env.sleep(duration)`
advances the `ManualClock` by `duration` (a `timedelta` or a number of seconds) and
returns the new time. Both work regardless of `auto_time_skipping`.

## Mocking steps

`env.mock_step(step, *, return_value=..., side_effect=...)` replaces a step's
implementation for that environment only -- it never touches the `StepDefinition` object
you passed in, so the same object keeps its real behaviour in production code and in
any other environment or test running concurrently. Steps you never mock keep running
for real.

```python
mock = env.mock_step(charge_card, return_value="ch_123")
...
mock.assert_called_once()
mock.assert_called_with("order-1", 1999)
```

`return_value` is used for every call. `side_effect` is more flexible:

```python
# Raise on every call -- retries apply exactly as they would to a real step raising it.
env.mock_step(charge_card, side_effect=ConnectionError("gateway down"))

# Compute the result from the (decoded) arguments.
env.mock_step(charge_card, side_effect=lambda order_id, cents: f"ch_{order_id}")

# A different outcome per call -- handy for "fails twice, then succeeds".
env.mock_step(charge_card, side_effect=[ConnectionError("down"), ConnectionError("down"), "ch_123"])
```

Every mock records its calls in order:

```python
mock.calls            # list[StepCall], each with .args (the decoded positional arguments)
mock.call_count
mock.assert_called()
mock.assert_not_called()
mock.assert_called_once()
mock.assert_called_with(*args)   # checks the *last* call
mock.assert_any_call(*args)      # checks that *some* call matched
```

Argument decoding is driven by the step's own type hints (mocking preserves the
original function's signature via `functools.update_wrapper`), so a mocked step sees the
same argument types a real one would.

## Inspecting and asserting on history

`await env.history(handle)` returns a `HistoryView`: iterate it, index it, call
`.kinds()` for the shape of the history (a list of event kinds in order), `.of_kind(kind)`
to filter, or `.pretty()` for a human-readable, one-line-per-event dump useful in a
failed assertion's output or just while writing a test.

`await env.assert_history(handle)` (or the standalone `loomwork.testing.assert_history`,
which also accepts a `HistoryView` or a plain list of events) returns a fluent
`HistoryAssertion`. Every method raises `AssertionError` with the mismatch spelled out on
failure, and returns `self` on success, so checks chain:

```python
assertion = await env.assert_history(handle)
assertion.contains_step("charge_card", times=1).completed_with("ch_123")
```

Available checks:

- `.contains_step(name, *, times=None)` -- the step was scheduled (optionally exactly
  `times` times; once per call site the workflow reached, regardless of retries).
- `.step_failed(name)` -- the step recorded a permanent failure.
- `.completed_with(value)` -- the run completed and its decoded result equals `value`.
- `.failed_with(exc_type)` -- the run failed with a failure matching `exc_type`
  (reconstructed via `FailureInfo.to_exception()`, so `ApplicationError`, `StepError`,
  a plain exception type by name, or any of your own `ApplicationError` subclasses all
  work).
- `.canceled()` -- the run was cancelled.
- `.no_nondeterminism(*workflows)` -- the history still replays cleanly against
  `workflows` (or, with no arguments, against the environment's own registry) --
  see below.

## Golden histories

A golden history is a JSON export of a run's history (the same format
`loomwork.history.history_to_json` produces), checked into a repository so that a later
change to workflow code can be checked against it without a live store, run or
environment.

```python
# Once, to record the golden file (commit its output):
async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card]) as env:
    handle = await env.start_workflow(process_order, "order-1", 1999)
    await handle.result()
    await record_golden(handle, "tests/golden/process_order_v1.json")

# In every test run afterwards, with no environment or store at all:
def test_process_order_still_replays() -> None:
    assert_replays("tests/golden/process_order_v1.json", process_order)
```

`assert_replays` (and the `check_history_file` it is built on, from
`loomwork.versioning`) raises a readable `AssertionError` -- naming the event at which
history and code diverge -- the moment a code change would break runs recorded by the
old code. Re-record the golden file deliberately when a workflow's recorded shape is
meant to change.

`replay_check(handle_or_history, *workflows)` is the same check without a file: it takes
a history directly (a handle, a `HistoryView`, or a plain event list) and replays it
against a *different* set of workflow definitions than the ones that recorded it --
useful within a single test that wants to check whether a proposed rewrite would still
handle an existing run.

`WorkflowEnvironment` has convenience wrappers -- `env.record_golden(handle, path)`,
`env.assert_replays(path, *workflows)`, `await env.replay_check(source, *workflows)` --
that default to every workflow the environment was built with when `workflows` is
omitted.

## pytest fixtures

Loomwork ships no `pytest11` entry point (installing it never changes what a project's
test suite collects on its own). Opt into the `loomwork_env` fixture and the
`loomwork_start_time` marker from your own `conftest.py`:

```python
# conftest.py
pytest_plugins = ["loomwork.testing.pytest_plugin"]
```

```python
async def test_process_order(loomwork_env) -> None:
    env = await loomwork_env(workflows=[process_order], steps=[charge_card])
    env.mock_step(charge_card, return_value="ch_123")
    assert await env.execute(process_order, "order-1", 1999) == "ch_123"
```

`loomwork_env` is a factory: every environment it builds is entered immediately and
exited automatically (in reverse order of creation) when the test ends. Set the default
`start_time` for every environment a test builds with the marker:

```python
@pytest.mark.loomwork_start_time(datetime(2030, 1, 1, tzinfo=UTC))
async def test_something_dated(loomwork_env) -> None:
    env = await loomwork_env(workflows=[...])
    assert env.now() == datetime(2030, 1, 1, tzinfo=UTC)
```

`start_time=` on a specific `loomwork_env(...)` call overrides the marker for that one
environment.

## Notes and limits

- `WorkflowEnvironment` always uses the in-memory store: it is for fast, deterministic
  unit and integration tests of workflow *logic*, not a substitute for the store
  conformance suite or an end-to-end test against SQLite or PostgreSQL.
- The engine configuration it uses by default (`loomwork.testing.TESTING_CONFIG`) tunes
  poll intervals and backoffs down to milliseconds so that whatever real time a test
  does spend (an actual notification wake-up, or the environment's own idle checks) stays
  negligible; pass `config=` to override it. None of this affects workflow-visible
  timing, which is driven entirely by the `ManualClock`.
- Mocking reaches into the environment's own `Registry` to swap a step's implementation;
  it never mutates a `StepDefinition` object shared with other code, so the same step
  can be mocked differently (or not at all) in a different environment running at the
  same time.

For fault injection, worker chaos and invariant checking across many workflows and
workers at once -- the kind of testing that finds the bugs example-based tests like the
ones on this page miss -- see [simulation testing](simulation-testing.md).
