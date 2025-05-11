# Deployment

Loomwork runs in two shapes. The workflow and step code is the same in both.

## Embedded

The engine runs inside your application process. Workers are coroutines in the same
event loop. There is nothing extra to deploy.

```python
store = store_from_url(os.environ["LOOMWORK_STORE_URL"])     # sqlite:/// or postgresql://
app = Loomwork(store=store, workflows=WORKFLOWS, steps=STEPS)
await store.open()
async with app:
    await serve_http_api(app)                                 # your application
```

- **SQLite** suits a single application instance: an internal tool, a desktop app, a
  small service. Several processes may share the file (WAL mode), but they only learn
  about each other's work by polling.
- **PostgreSQL** suits anything that scales out: every application instance runs a
  worker, and they coordinate through the database. `LISTEN/NOTIFY` wakes idle workers
  within milliseconds.

## Dedicated workers

The same store, but workflow and step execution moves to separate processes. Your web
application only starts runs and reads results:

```python
# Web application: a client only.
app = Loomwork(store=store, run_worker=False)

# Worker processes (as many as you need):
worker = Worker(store, Registry(WORKFLOWS, STEPS), options=WorkerOptions(step_concurrency=64))
async with worker:
    await stop_signal.wait()
```

Split further with task queues: general workers for workflows and default steps,
specialised workers for `gpu` or `payments` queues ([task queues](../guides/task-queues.md)).

## Server mode

`loomwork server` puts an HTTP API in front of the stores. Workers and clients connect
over HTTP with bearer tokens instead of holding database credentials, and non-Python
services can start workflows and send signals with plain JSON. See
[server mode](../server.md).

## Rolling deploys

1. **Check compatibility** of the new workflow code against recorded histories
   ([versioning](../guides/versioning.md)); fail the pipeline if runs would break.
2. **Lint** workflow code for non-determinism (`python -m loomwork.lint`).
3. Roll workers gradually. Old and new workers can run side by side. If an old worker
   picks up a run whose history was extended by new code, its replay fails with a
   nondeterminism error, which fails only that tick; the tick is retried and soon lands
   on a new worker. Every tick records the fingerprint of the code that executed it, so
   you can see which version made each decision.
4. Register new steps on workers **before** deploying workflow code that calls them.
   Otherwise the step jobs simply wait until a worker that knows the step is running.

Stopping a worker (`SIGTERM` in your process manager should call `worker.stop()`) stops
claiming work, lets in-flight ticks and step attempts finish within `shutdown_grace`, and
hands back any attempt that is still running so another worker starts it immediately.

## Database migrations

Stores apply their schema migrations when opened. Migrations are append-only and
checksummed; PostgreSQL runs them under an advisory lock, so many processes can start at
once. A database whose schema is newer than the installed Loomwork, or whose recorded
migrations differ from the known ones, is refused with `MigrationError` instead of being
used.

## Retention

Closed runs keep their histories until you delete them:

```python
deleted = await store.delete_closed_before(now - timedelta(days=30))
```

Run this periodically (a schedule is a good place). Deleting a run also deletes its
search attributes. Running executions are never touched.
