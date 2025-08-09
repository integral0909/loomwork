# Loomwork examples

Runnable example applications that double as documentation for idiomatic Loomwork
usage, and as regression tests of the public API (see `tests/examples/`). Each is a
self-contained package: `workflows.py` (and `steps.py` where that separation helps),
`main.py` (runnable with `uv run python -m examples.<name>.main`), and its own
`README.md` describing the scenario, the Loomwork features it uses, and how to run it.

| Example | Scenario | Key features |
| --- | --- | --- |
| [`order_fulfillment`](order_fulfillment/README.md) | An e-commerce saga: reserve, charge, ship, with a customer cancellation window | Steps with retries and an idempotency key, `Compensations`, `Channel` raced against a timeout, `@query` |
| [`expense_approval`](expense_approval/README.md) | Human-in-the-loop approval with reminders and escalation | `Channel` + `asyncio.timeout`, `@update`, `@query`, a same-module helper the linter follows |
| [`bulk_import`](bulk_import/README.md) | Paging through a large dataset | Child workflows for bounded fan-out/fan-in, `continue_as_new`, `heartbeat`/`heartbeat_details` |
| [`scheduled_reports`](scheduled_reports/README.md) | Recurring reports in more than one time zone | Cron schedules with time zones, `info().scheduled_time`, overlap policy, pause/backfill |
| [`task_queues`](task_queues/README.md) | Routing GPU-heavy work to a dedicated worker | `@step(queue=...)`, `WorkerOptions(task_queues=..., queue_limits=...)`, two workers sharing one store |
| [`encrypted_payments`](encrypted_payments/README.md) | Payload encryption for sensitive data | `CodecChain`, `EncryptionCodec`, `StaticKeyRing` key rotation |
| [`versioned_workflow`](versioned_workflow/README.md) | Evolving a running workflow safely | `patched()`, `deprecate_patch()`, `loomwork.versioning.check_compatibility` |

## Running an example

```console
uv run python -m examples.order_fulfillment.main
```

Every example finishes in well under a few seconds. Where the real scenario involves
hours or days (a cancellation window, a reminder schedule, a cron job), the demo uses
`loomwork.ManualClock` and advances it programmatically (see `examples/support.py`)
instead of a shortened, unrealistic duration -- the workflow code itself uses the same
durations a production deployment would.

Examples that are specifically about durability or encryption
(`task_queues`, `encrypted_payments`) use a real `SQLiteStore` in a temporary directory;
every other example uses the non-durable `MemoryStore`, since durability isn't the point
of the scenario.

## Tests

`tests/examples/` has one test module per example (exercising both a happy path and at
least one failure or edge case through the workflow code directly), a test that runs
every example's `main.py` and checks it exits cleanly, and a test that runs
`loomwork.lint.lint_paths(["examples"])` and asserts every example's workflow code is
clean under the determinism linter.

```console
uv run pytest tests/examples
uv run mypy examples tests/examples
uv run ruff check examples tests/examples
```
