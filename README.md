# Loomwork

**Durable, event-sourced workflows for Python.** Write long-running business processes as
ordinary `async` functions. Loomwork records every decision they make, so a run survives
crashes, restarts and deploys by replaying its history, whether it takes seconds or months.

```python
import asyncio
from datetime import timedelta

import loomwork as lw


@lw.step(retry=lw.RetryPolicy(max_attempts=5))
async def charge_card(order_id: str, cents: int) -> str:
    # Talk to the payment provider. Retried with backoff; idempotency key is stable.
    return await payments.charge(order_id, cents, key=lw.step_info().idempotency_key)


@lw.step
async def ship(order_id: str) -> None: ...


@lw.step
async def refund(charge_id: str) -> None: ...


cancel = lw.Channel("cancel", str)


@lw.workflow(name="orders.process")
async def process_order(order_id: str, cents: int) -> str:
    charge_id = await charge_card(order_id, cents)
    try:
        # Give the customer an hour to change their mind. Durable: survives restarts.
        async with asyncio.timeout(timedelta(hours=1).total_seconds()):
            reason = await cancel.receive()
        await refund(charge_id)
        return f"cancelled: {reason}"
    except TimeoutError:
        await ship(order_id)
        return "shipped"


async def main() -> None:
    async with lw.Loomwork(workflows=[process_order], steps=[charge_card, ship, refund]) as app:
        print(await app.execute(process_order, "order-42", 1999, id="order-42"))
```

## Why Loomwork

- **Plain Python, durable semantics.** Workflows are `async def` functions, or classes
  whose methods handle signals, queries and updates. `asyncio.gather`, `asyncio.timeout`,
  `asyncio.wait` and `TaskGroup` work inside them, and every `asyncio.sleep` becomes a
  durable timer.
- **Embedded or server.** Run the engine inside your application on SQLite or PostgreSQL
  with no extra infrastructure, or run `loomwork server` and connect workers over HTTP.
  The workflow code is identical.
- **PostgreSQL-native coordination.** Workers lease work with `FOR UPDATE SKIP LOCKED` and
  wake up through `LISTEN/NOTIFY`. No broker, no separate queue.
- **Safety built in.** A deterministic event loop rejects I/O inside workflows at runtime;
  a static linter finds non-deterministic code before deploy; a compatibility checker
  replays recorded histories against new code; `loomwork.patched()` changes logic safely
  while runs are in flight, or pinned runs let incompatible builds run side by side.
- **Encrypted payloads.** A codec chain compresses and encrypts every argument, result and
  checkpoint before it reaches the store. Database administrators see ciphertext.
- **Operable.** Visibility queries (`status = 'failed' AND attr.tier = 'gold'`), a CLI and
  a web console, reset and batch operations for recovering from bugs, `loomwork doctor`,
  Prometheus metrics, OpenTelemetry traces carried from caller to every step, archives.

## Features

| Area            | What you get                                                                      |
| --------------- | --------------------------------------------------------------------------------- |
| Programming model | `@workflow` functions or classes, `@step`, `wait_condition`, deterministic `now()`, `random()`, `uuid4()`, durable `sleep()` |
| Steps           | Retries with backoff, per-attempt and total timeouts, heartbeats with checkpoints, cancellation |
| Task queues     | Route steps to specialised workers, priorities, per-queue rate and concurrency limits |
| Messaging       | Typed channels (signals), queries, updates, signal-with-start, workflow-to-workflow signals |
| Composition     | Child workflows with parent-close and id policies, sagas with `Compensations`, continue-as-new |
| Scheduling      | Cron (seconds, `L`, `#`, time zones, DST-aware) and intervals; overlap policies, backfill |
| Stores          | In-memory, SQLite, PostgreSQL, or any of them behind the HTTP server; checksummed migrations |
| Payloads        | Compression, AES-256-GCM encryption with key rotation, offloading large payloads to filesystem or S3-compatible storage |
| Versioning      | `patched()`, pinned runs per worker build, code fingerprints per tick, compatibility checks |
| Operations      | Id reuse and conflict policies, reset to a tick, batch signal/cancel/terminate, history limits, archives |
| Visibility      | Search attributes and a small, safe query language compiled to SQL |
| Security        | Encrypted payloads; token auth with hot reload, RBAC and namespaces in server mode; a script-free console |
| Observability   | Telemetry hooks, Prometheus exposition, OpenTelemetry with end-to-end trace propagation, interceptors, correlated logs |
| Tooling         | `loomwork` CLI, web console, determinism linter, replay debugger, testkit with time skipping, deterministic simulation testing, benchmarks |

## Installation

Loomwork requires Python 3.12 or newer.

```bash
pip install loomwork                 # core (in-memory store)
pip install "loomwork[sqlite]"       # durable single-node store
pip install "loomwork[postgres]"     # production store
pip install "loomwork[crypto]"       # payload encryption
pip install "loomwork[server]"       # HTTP server, web console and remote workers
pip install "loomwork[client]"       # remote workers and CLI against a server
pip install "loomwork[s3]"           # S3-compatible blob storage for large payloads
pip install "loomwork[otel]"         # OpenTelemetry integration
```

## Command line

```bash
loomwork worker --app myapp.workflows --store sqlite:///loomwork.db
loomwork workflow start orders.checkout '{"order_id": "A-17"}' --id order-A-17 --wait
loomwork workflow list -q "status = 'failed'"
loomwork workflow history order-A-17 -o order-A-17.json
loomwork compat --app myapp.workflows        # will the new code replay recorded runs?
loomwork schedule create nightly reports.nightly --cron "0 3 * * *" --tz Europe/Stockholm
loomwork workflow reset order-A-17 --to before-last-failure --reason "fixed in 1.4.2"
loomwork doctor                              # what is wrong with this deployment?
```

See the [CLI reference](docs/cli.md).

## Documentation

Start with the [tutorial](docs/tutorial.md) and [concepts](docs/concepts.md); the
[documentation index](docs/index.md) lists everything:

- Guides: [workflows and steps](docs/guides/workflows.md),
  [class-based workflows](docs/guides/workflow-classes.md), [messaging](docs/guides/messaging.md),
  [child workflows and sagas](docs/guides/composition.md), [schedules](docs/guides/schedules.md),
  [task queues](docs/guides/task-queues.md), [versioning](docs/guides/versioning.md),
  [interceptors and tracing](docs/guides/interceptors.md), [testing](docs/testing.md),
  [debugging](docs/debugging.md)
- Operations: [deployment](docs/operations/deployment.md) (and [`deploy/`](deploy/README.md)),
  [configuration](docs/operations/configuration.md), [monitoring](docs/operations/monitoring.md),
  [recovery](docs/operations/recovery.md), [archiving](docs/operations/archiving.md),
  [troubleshooting](docs/operations/troubleshooting.md), [performance](docs/performance.md)
- Server: [server mode](docs/server.md), [web console](docs/console.md)
- Reference: [command line](docs/cli.md), [visibility queries](docs/visibility-queries.md),
  [payload encryption](docs/payload-encryption.md), [large payloads](docs/large-payloads.md),
  [lint rules](docs/lint-rules.md), [architecture decisions](docs/adr/),
  [internals](docs/internals.md)
- [Examples](examples/README.md): twelve runnable applications, each with tests

## Development

```bash
uv sync                 # create the virtualenv with all extras and dev tools
uv run pytest           # run the tests (set LOOMWORK_TEST_POSTGRES_DSN for PostgreSQL)
uv run ruff check .     # lint
uv run mypy             # strict type checking
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the ground rules and [SECURITY.md](SECURITY.md)
for reporting vulnerabilities.

## License

Apache License 2.0. See [LICENSE](LICENSE).
