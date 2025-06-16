# Server mode

Embedded mode (`Loomwork(...)`) hosts the engine and workers inside one process with
direct access to the store. Server mode splits that in two: an ASGI API
(`loomwork.server`) fronts one or more stores, and remote workers and clients talk to it
over HTTP through `loomwork.remote.http_store.HttpStore` -- a `Store` implementation, so
`Worker(HttpStore(url, token), registry)` runs workflows exactly like
`Worker(MemoryStore(), registry)` does. The same is true of `Engine` and `Client`: give
either one an `HttpStore` and it drives the server instead of a local store, unchanged.

```
                          ┌─────────────────────────────┐
   Worker (any host)  ───▶│                              │
   Engine/Client       ───▶  loomwork server (ASGI app)  │───▶  Store (per namespace)
   (Python, HttpStore) ───▶│  worker API + control API   │      memory / SQLite / Postgres
                          │                              │
   curl / other lang  ───▶│  control API only           │
                          └─────────────────────────────┘
```

## Two APIs, one server

- **Worker API** (`/v1/namespaces/{namespace}/...`): one endpoint per `Store` method.
  This is what `HttpStore` speaks, so it is what both `Worker` and `Engine`/`Client` use
  from Python. It exchanges the lossless wire form of `Store` types
  (`loomwork.remote.wire`) and requires the `worker` role.
- **Control API** (`/v1/namespaces/{namespace}/control/...`): plain JSON for callers
  without Loomwork's Python types -- `curl`, another language, a dashboard. Arguments and
  signal/update values are ordinary JSON, encoded and decoded with the default data
  converter. It requires the `operator` role for writes, `reader` or `operator` for reads.
  It cannot answer queries (`WorkflowHandle.query`): those need the workflow code, which
  the server never imports.

A Python caller almost always wants the worker API (i.e. `HttpStore`); reach for the
control API only when there is no Loomwork Python process on that side of the connection.

## Namespaces

Each namespace is an independent store, addressed by `/v1/namespaces/{namespace}/...`.
This is how one server isolates tenants (or environments, or workflow families) without
separate schemas or deployments: namespace `billing` might be a PostgreSQL database while
`scratch` is `memory://`. A token is scoped to the namespaces it may use (`"*"` for all).

## The clock rule

A client never asserts what time it is on the server: `HttpStore` never sends an
absolute timestamp. Every request captures one reference instant from the caller's own
clock and expresses every datetime in that request (the tick's `now`, a step's retry
time, a lease's expiry, ...) as a `float` offset in seconds from that reference. The
server captures its own reference instant when it starts handling the request and
reconstructs each value as `server_time + offset`. Sending an offset of `0.0` means
"right now", which the server resolves with its own clock -- so a client and a server
with unsynchronized wall clocks still agree on every deadline's *meaning* (a lease that
expires in 30 seconds), even though they would disagree about its literal timestamp. See
the module docstring of `loomwork.remote.wire` for the exact mechanism, and
`tests/server/test_http_store_conformance.py` for how tests get exact-value assertions to
hold despite this (a `ManualClock` shared, in-process, between the test's `HttpStore` and
the server it talks to).

## Running a server

```toml
# server.toml
[server]
host = "0.0.0.0"
port = 8080

[server.namespaces]
default = "sqlite:////var/lib/loomwork/default.db"
billing  = "postgresql://loomwork@db.internal/billing"

[server.auth]
tokens_file = "/etc/loomwork/tokens.json"

[server.limits]
long_poll_max_seconds = 20
max_request_body_bytes = 10485760

[server.cors]
enabled = false
```

```
loomwork server --config server.toml            # or: python -m loomwork.server --config ...
```

`--host` and `--port` override the file; see the [CLI reference](cli.md).

Programmatically:

```python
from loomwork.server import ServerConfig, create_app, serve

config = ServerConfig(namespaces={"default": "memory://"}, auth=AuthConfig(disabled=True))
app = create_app(config)          # a Starlette ASGI app: mount it however you like
await serve(config)                # or run it with uvicorn directly
```

### Configuration reference

| Field | Default | Description |
| ----- | ------- | ----------- |
| `host` | `127.0.0.1` | Bind address for `loomwork server`. |
| `port` | `8080` | Bind port. |
| `namespaces` | `{}` | Namespace name → store URL (`loomwork.store.store_from_url`). |
| `auth.tokens_file` | none | Path to the JSON tokens file. Required unless `auth.disabled`. |
| `auth.disabled` | `False` | Skip authentication; every request is an administrator everywhere. **Development only** -- the server logs a loud warning at startup. |
| `limits.long_poll_max_seconds` | `20` | Upper bound on a client-requested `claim_tick`/`claim_step`/update/result wait; a longer request is capped, not rejected. |
| `limits.max_request_body_bytes` | `10 MiB` | Requests over this size get `413` before the body is read. |
| `cors.enabled` | `False` | The worker and control APIs are for backend callers, not browsers; enable explicitly for a browser-based dashboard. |
| `cors.allow_origins` | `[]` | Origins to allow when CORS is enabled. |

Every field can be overridden by an environment variable
`LOOMWORK_SERVER__<SECTION>__<FIELD>` (double underscores mirror the TOML nesting), for
example `LOOMWORK_SERVER__HOST=0.0.0.0` or
`LOOMWORK_SERVER__NAMESPACES__DEFAULT=sqlite:///./dev.db`. Values are parsed as TOML
scalars, so `true`/`false`/numbers work unquoted.

Store URLs are never logged with their password (`loomwork.store.redact_url`); see
[configuration](operations/configuration.md) for the store URL reference itself.

### System workers

Each namespace also starts an internal worker with an empty registry
(`workflow_concurrency=0`, `step_concurrency=0`, `run_timers=run_outbox=run_schedules=
True`), so timers fire, the outbox is relayed and schedules are evaluated without any
external worker needing to run those loops. Pass `run_system_workers=False` to
`create_app` to disable this (for example when several server processes share one
namespace and only one of them should run it, or in tests that want to fire timers and
relay the outbox explicitly through the store without a background loop racing them).

### Health checks

- `GET /healthz` -- liveness; always `200` once the process is up.
- `GET /readyz` -- readiness; `200` if every namespace's store answered a cheap call,
  `503` (with a per-namespace breakdown) otherwise.

Both are unauthenticated, as is conventional for orchestrator health probes.

## Tokens and RBAC

Bearer tokens look like `lwk_<id>_<secret>`. Only a salted SHA-256 hash of the secret is
ever stored (a fresh random salt per token). The secret is 256 random bits, so it cannot be
guessed, and a deliberately slow password hash would only add latency to every API call.
The plaintext token is shown once, when it is generated. Token ids may contain letters,
digits and `-` (not `_`, which separates the parts of a token).

```
loomwork token create ci-worker --role worker --namespace default -f tokens.json
loomwork token list -f tokens.json
loomwork token revoke ci-worker -f tokens.json
```

The server re-reads the tokens file when it changes (checked at most once a second), so
creating, rotating (`token create --replace`) and revoking tokens take effect without a
restart. A file that fails to parse, for example one caught half-written by an editor, is
ignored with an error in the log and the previous tokens stay in force. The CLI writes the
file atomically with owner-only permissions.

Or from Python:

```python
from loomwork.server.auth import Role, TokenFile, generate_token

plaintext, record = generate_token("ci-worker", [Role.WORKER], ["default"])
print(plaintext)  # lwk_ci-worker_<random> -- show this once, then discard it

tokens = TokenFile({record.id: record})
tokens.save("tokens.json")
```

`tokens.json` holds records, never secrets:

```json
[
  {
    "id": "ci-worker",
    "salt": "...",
    "hash": "...",
    "roles": ["worker"],
    "namespaces": ["default"],
    "description": ""
  }
]
```

### Roles

| Role | Grants |
| ---- | ------ |
| `admin` | Everything, in every namespace the token lists. |
| `operator` | The control API: start, signal, cancel, terminate, request/wait updates, list, describe, history, children. |
| `worker` | The worker API: everything `HttpStore` calls, i.e. what `Worker` and Python `Engine`/`Client` need. |
| `reader` | Read-only control API: describe, list, history, children, wait for a result. |

A token additionally lists the namespaces it may act in (`"*"` for all). Requests are
checked in this order: valid token → correct role for the capability → allowed namespace;
a token scoped to `default` gets the same `403` for `other` whether or not `other` is
configured, so it cannot learn what other namespaces exist.

**Python callers need the `worker` role**, not `operator`: `Engine`/`Client` over
`HttpStore` go through the worker API exactly like `Worker` does. `operator`/`reader` are
for the plain-JSON control API used by non-Python callers.

### Disabling auth

`auth.disabled = true` skips token checking entirely and treats every request as an
administrator in every namespace. It exists for local development; the server logs a
warning naming the risk every time it starts with this set. Never use it in production.

### Audit log

Every mutating request is logged via the `loomwork.audit` logger:

```
token=ci-worker namespace=default action=claim_tick target=run-01H... outcome=ok
```

Never the payload contents, arguments, or secrets -- only token id, namespace, action and
the run/workflow id involved.

## Running a remote worker

```python
from loomwork.remote import HttpStore
from loomwork.worker import Worker, WorkerOptions

store = await connect("https://loomwork.internal", token, namespace="default")
# or: store = HttpStore("https://loomwork.internal", token, "default"); await store.open()

worker = Worker(store, registry, options=WorkerOptions(workflow_concurrency=8))
async with worker:
    await asyncio.Event().wait()  # run until told to stop
```

`HttpStore(base_url, token, namespace="default", *, timeout=30.0, long_poll=20.0,
max_retries=3, clock=None)`:

- `long_poll`: how long `claim_tick`/`claim_step` ask the server to hold the connection
  open while nothing is ready (capped server-side by `limits.long_poll_max_seconds`), so
  an idle worker is not busy-polling. `0` disables it (every claim answers immediately) --
  what the conformance suite uses so "nothing to claim" assertions are not
  timing-dependent.
- `max_retries`: connection errors and `5xx` responses to idempotent (`GET`) requests are
  retried with jittered exponential backoff. Mutating calls are never retried
  automatically, since re-sending e.g. `complete_tick` after an ambiguous failure could
  double-apply effects; the owning tick/step loop's own retry (a fresh claim) handles that
  instead.
- `clock`: the local clock used to compute the offsets above. Defaults to the system
  clock; tests share one `ManualClock` between the store and the server.

Its `notifier` is local to the process: `HttpStore` publishes to it whenever one of its
own calls is known to have changed something (a run created, a tick committed new steps
or timers, a run closed, ...), mirroring what the embedded stores publish for the same
effects. It cannot learn about another process's changes that way -- long-polling on
claims is what keeps latency low for those.

### Convenience helpers

```python
from loomwork.remote import connect, engine_for, client_for

store = await connect(base_url, token, namespace="default")
engine = engine_for(store)
client = client_for(store, registry=registry)
```

## Route table

All worker-API paths are under `/v1/namespaces/{namespace}`; control-API paths add
`/control`. `wait`/`timeout` query or body parameters are capped by
`limits.long_poll_max_seconds`.

### Worker API (`Store` methods)

| Method | Path | `Store` method |
| ------ | ---- | -------------- |
| POST | `/executions` | `create_execution` |
| GET | `/executions` | `list_executions` |
| POST | `/executions/query` | `query_executions` |
| GET | `/executions/{run_id}` | `get_execution` |
| GET | `/executions/{run_id}/history` | `load_history` |
| POST | `/executions/{run_id}/inputs` | `append_input` |
| POST | `/executions/{run_id}/cancel` | `request_cancel` |
| POST | `/executions/{run_id}/terminate` | `terminate` |
| GET | `/executions/{run_id}/steps` | `get_step_jobs` |
| GET | `/executions/{run_id}/children` | `list_children` |
| GET | `/executions/{run_id}/search-attributes` | `get_search_attributes` |
| GET | `/workflows/{workflow_id}/latest` | `find_execution` |
| POST | `/ticks/claim` | `claim_tick` (long-polls) |
| POST | `/ticks/{run_id}/complete` | `complete_tick` |
| POST | `/ticks/{run_id}/fail` | `fail_tick` |
| POST | `/steps/claim` | `claim_step` (long-polls) |
| POST | `/steps/next-ready-at` | `next_step_ready_at` |
| POST | `/steps/{job_id}/finish` | `finish_step` |
| POST | `/steps/{job_id}/retry` | `retry_step` |
| POST | `/steps/{job_id}/lease` | `renew_step_lease` |
| POST | `/steps/{job_id}/heartbeat` | `heartbeat_step` |
| POST | `/steps/{job_id}/release` | `release_step` |
| POST | `/outbox/relay` | `relay_outbox` |
| GET | `/outbox/size` | `outbox_size` |
| POST | `/retention/delete-closed` | `delete_closed_before` |
| GET | `/timers/next` | `next_timer_at` |
| POST | `/timers/fire` | `fire_due_timers` |
| POST | `/schedules` | `create_schedule` |
| GET | `/schedules` | `list_schedules` |
| POST | `/schedules/due` | `due_schedules` |
| GET | `/schedules/next` | `next_schedule_at` |
| GET | `/schedules/{schedule_id}` | `get_schedule` |
| POST | `/schedules/{schedule_id}/update` | `update_schedule` |
| POST | `/schedules/{schedule_id}/delete` | `delete_schedule` |
| POST | `/schedules/{schedule_id}/fire` | `fire_schedule` |

### Control API (plain JSON)

| Method | Path | Effect |
| ------ | ---- | ------ |
| POST | `/control/workflows` | Start a workflow: `{name, args: [...], workflow_id?}` → `201` with the execution. |
| GET | `/control/workflows` | List: `?workflow_name=&status=&limit=`. |
| POST | `/control/workflows/signal-with-start` | `{name, args, workflow_id, channel, value, message_id?}`. |
| GET | `/control/workflows/{run_id}` | Describe. |
| GET | `/control/workflows/{run_id}/history` | History as JSON events. |
| GET | `/control/workflows/{run_id}/children` | Child executions. |
| POST | `/control/workflows/{run_id}/signal` | `{channel, value, message_id?}` → `204`. |
| POST | `/control/workflows/{run_id}/updates` | `{name, args, update_id?}` → `202` with `update_id`. |
| GET | `/control/workflows/{run_id}/updates/{update_id}` | `?timeout=`; long-polls for the outcome. |
| POST | `/control/workflows/{run_id}/cancel` | `{reason?}`. |
| POST | `/control/workflows/{run_id}/terminate` | `{reason?}`. |
| GET | `/control/workflows/{run_id}/result` | `?timeout=`; long-polls for the run to close. |

### Top-level

| Method | Path | Effect |
| ------ | ---- | ------ |
| GET | `/healthz` | Liveness (unauthenticated). |
| GET | `/readyz` | Readiness: stores reachable (unauthenticated). |

## Errors

Every error is a JSON body (loosely `RFC 7807`) with a machine-readable `code`:

```json
{
  "type": "https://loomwork.dev/errors/lease_lost",
  "code": "lease_lost",
  "title": "Lease lost",
  "detail": "worker w1 does not hold the lease on run r1",
  "status": 409
}
```

| `code` | Status | Meaning |
| ------ | ------ | ------- |
| `not_found` | 404 | Run, workflow id or schedule does not exist. |
| `workflow_already_started` | 409 | Extra fields `workflow_id`, `run_id` of the running execution. |
| `lease_lost` | 409 | The caller no longer holds the tick/step lease. |
| `workflow_closed` | 409 | The run closed before the operation could apply. |
| `schedule_exists` | 409 | Creating a schedule whose id is taken. |
| `schedule_conflict` | 409 | Optimistic-concurrency check on a schedule failed. |
| `payload_error` | 422 | A value could not be encoded or decoded. |
| `registration_error` | 422 | (Reserved; the server has no workflow registry of its own.) |
| `query_syntax_error`, `query_type_error` | 400 | A visibility query failed to parse or type-check; extra fields carry `source`, `line`, `column`, `offset` so `str()`/`.format()` on the reconstructed exception still points at the offending token. |
| `invalid_request` | 400 | Malformed JSON, a missing field, or any other `ValueError`. |
| `timeout` | 408 | A long-poll (update/result wait) elapsed. |
| `unauthorized` | 401 | Missing or invalid bearer token. |
| `forbidden` | 403 | Valid token, wrong role or namespace. |
| `payload_too_large` | 413 | Request body over `limits.max_request_body_bytes`. |
| `internal_error` | 500 | An unhandled exception; the ASGI server also sees it re-raised (unlike every other code above), so it is logged and does not masquerade as a normal error response. |

`HttpStore` turns each of these back into the exact exception type an embedded store
would raise (`loomwork.remote.errors.exception_for`), so calling code cannot tell whether
it is talking to a local or a remote store.

## Hardening

- **Request size**: bodies over `limits.max_request_body_bytes` are rejected by
  `Content-Length` before the app reads them.
- **JSON only**: every body is parsed as JSON; anything else is `400`.
- **Request ids**: `X-Request-ID` is echoed if the caller sends one, generated
  otherwise, and always present on the response.
- **Structured errors**: see above; no stack traces or internal details leak into `4xx`
  bodies (a `500` also hides its detail).
- **Graceful shutdown**: the ASGI lifespan closes every namespace's system worker and
  store on shutdown.
- **CORS off by default**: the APIs are for backend callers.

## Testing against a server

`tests/server/test_http_store_conformance.py` runs the full
`tests.store_conformance.StoreConformance` suite against `HttpStore` → an in-process
server (`httpx.ASGITransport`, no real socket) → first `MemoryStore`, then
`SQLiteStore`. The suite is unmodified; the fixtures share one `ManualClock`, frozen at a
fixed `T0`, between the test's `HttpStore` and the server's `create_app(..., clock=...)`,
so the clock-rule offsets always round-trip to the exact value the conformance suite
asserts on (see that module's docstring for the full argument), and disable the
namespace's system worker (`run_system_workers=False`) so it does not race the suite's own
direct calls to `fire_due_timers`/`relay_outbox`.
