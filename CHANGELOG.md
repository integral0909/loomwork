# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Project scaffolding: packaging, CI, lint and type-check configuration, security policy.
- Event-sourced history model with tick-delimited activations.
- Deterministic asyncio event loop that maps `asyncio.sleep`, `asyncio.timeout` and
  `loop.call_at` onto durable timers.
- Replay executor with command/event matching and nondeterminism detection.
- `@workflow` and `@step` decorators, deterministic `now()`, `random()`, `uuid4()`, and
  durable `sleep()`.
- In-memory store, embedded engine, workflow/step/timer workers and `Loomwork` facade.
- JSON payload converter with pydantic, dataclass and standard-library type support.
- SQLite store (`loomwork[sqlite]`) and PostgreSQL store (`loomwork[postgres]`) sharing one
  SQL implementation, with `SKIP LOCKED` claims, `LISTEN/NOTIFY` wake-ups and checksummed,
  dialect-aware migrations.
- `store_from_url()` for `memory://`, `sqlite:///path` and `postgresql://` URLs.
- Retention: `Store.delete_closed_before()` removes old closed runs.
- Task queues: `@step(queue=...)` routes steps to workers subscribed through
  `WorkerOptions(task_queues=...)`; `priority` orders claims within a queue.
- Heartbeats: `loomwork.heartbeat(details)` renews an attempt's lease, checkpoints
  progress that the next attempt reads with `loomwork.heartbeat_details()`, and delivers
  workflow-side cancellation to the running step. `heartbeat_timeout` detects silent
  attempts long before their overall timeout.
- Worker-local per-queue rate and concurrency limits (`QueueLimit`).
- Graceful shutdown hands in-flight step attempts back without consuming an attempt.
- Step loops wake exactly when a retry backoff ends instead of on the next poll.

- Typed channels (`loomwork.Channel`) for signals, validated on send; buffered until
  received; idempotent with message ids; `signal_with_start`.
- Queries (`@loomwork.query`) answered by replaying committed history, including for
  closed runs; updates (`@loomwork.update`) whose handlers run as workflow code and
  return results or errors to the caller.
- Child workflows via `loomwork.start_child()` or by calling a workflow from a workflow,
  with `terminate`, `request_cancel` and `abandon` parent-close policies.
- Workflows can signal other workflows (`Channel.send_to`, `signal_external`).
- `loomwork.Compensations` for saga-style rollbacks.
- A transactional outbox and relay for effects between runs (ADR-0004; migration 0003).

- Payload codec chain: `CompressionCodec` (zlib with a size threshold),
  `EncryptionCodec` (AES-256-GCM with authenticated metadata, key rotation through
  static, environment or file key rings, and an opt-in strict mode against downgrade),
  `PayloadSizeLimitCodec`, and `CodecChain` for layering; `loomwork[crypto]` extra.

- Cron expressions with seconds, names, `L`, `nL`, `n#k`, macros and IANA time zones,
  with defined behaviour across daylight-saving transitions.
- Schedules: recurring workflow starts on a cron expression or interval, with overlap
  policies (`skip`, `buffer_one`, `allow_all`, `cancel_other`, `terminate_other`),
  catch-up windows, jitter, pause-on-failure, manual trigger and backfill; scheduled runs
  see their nominal fire time in `loomwork.info().scheduled_time` (migration 0004).

- Determinism linter (`python -m loomwork.lint`): static analysis of workflow code for
  wall-clock time, unseeded randomness, I/O, threads, environment access, set iteration
  order, identity-based branching and global mutation, including violations reached
  through same-module helpers; suppression comments, `[tool.loomwork.lint]`
  configuration, and text, JSON and SARIF reports.

- Continue-as-new: `loomwork.continue_as_new()` closes a run and atomically starts its
  successor under the same workflow id (optionally a different workflow), keeping
  histories of long-lived workflows bounded. Handles, parents and schedules follow the
  chain; `history_length()` and `continue_as_new_suggested()` help decide when
  (migration 0005).

- Payload codecs are applied end to end: clients encode arguments, signals and updates;
  workers decode histories before replay and encode commands, step results and
  heartbeat details before they reach the store.

- Versioning: `loomwork.patched()` and `deprecate_patch()` for changing workflow logic
  safely while runs are in flight; every tick records a fingerprint of the workflow code
  that executed it (`TickCompleted.code_version`); `loomwork.versioning` replays stored
  or exported histories against new code and reports where they would diverge.

- Visibility queries: a small, safe query language (`status = 'failed' AND
  attr.customer_tier = 'gold' ORDER BY created_at DESC`) with a type checker, an
  in-memory evaluator and a SQL compiler proven equivalent by property tests.
- Search attributes: `loomwork.upsert_search_attributes()` from workflow code and
  `search_attributes=` when starting runs; typed, recorded in the history, carried over
  on continue-as-new, and queryable with `client.query_runs()` (migration 0006).

- Observability: `Telemetry` hooks reported by workers for ticks, step attempts, schedule
  evaluations, outbox relays and loop errors; a dependency-free metrics registry with
  Prometheus exposition (`MetricsTelemetry`), structured logging (`LoggingTelemetry`),
  and OpenTelemetry spans and metrics (`OpenTelemetryTelemetry`, `loomwork[otel]`), with
  step spans current while user code runs.

- Workflow timeouts and retries: `run_timeout` closes a run as `timed_out`,
  `execution_timeout` bounds the whole workflow id across retries and continue-as-new,
  and `retry_policy` starts a fresh attempt (with backoff) when a run fails. Handles,
  parents and schedules follow the chain of attempts.
- Benchmarks (`python -m benchmarks.bench`) for throughput, step fan-out, replay cost and
  tick latency on every store.
- Server mode (`loomwork.server`): a Starlette HTTP API in front of any store, with
  bearer tokens scoped to namespaces and permissions, per-namespace system workers, and a
  JSON API for starting, signalling, querying and cancelling workflows from any language.
- `HttpStore` (`loomwork.remote`) implements the full store contract over HTTP, so workers
  and clients run against a server without database credentials. Timestamps travel as
  offsets from the request time, so client and server clocks need not agree.
- The `loomwork` command: `workflow` (start, list, describe, history export, result,
  signal, query, update, cancel, terminate), `schedule`, `worker` (graceful shutdown on
  SIGTERM, queue limits, metrics port), `server`, `store` (migrate, info, prune),
  `compat`, `lint`, `cron` and `token`. JSON output everywhere, documented exit codes.
- Testkit (`loomwork.testing`): `WorkflowEnvironment` runs workflows in memory and skips
  idle time automatically, so a thirty-day sleep finishes in milliseconds without ever
  cutting a running step short; step mocks with call assertions; history assertions;
  golden-history recording and replay checks; a pytest plugin with a `loomwork_env`
  fixture.
- `Registry.replace_step()` swaps a registered step's implementation deliberately.
- Deployment kit (`deploy/`): a multi-stage container image running as an unprivileged
  user, a Compose stack, and Kubernetes manifests (server, workers, migration Job,
  retention CronJob, PodDisruptionBudgets, NetworkPolicies), all validated by tests.
- `loomwork token create --if-missing` for idempotent bootstrap.
- Operator web console at `/console` in server mode: runs with visibility queries, run
  detail with history timeline and pending steps, schedules, and signal, cancel,
  terminate, pause and trigger actions. Token login with signed session cookies, CSRF
  protection, role checks, a strict Content-Security-Policy and no JavaScript.
- History archives (`loomwork.archive`, `loomwork archive create|verify|list|show`):
  closed runs written to checksummed, gzip-compressed JSON Lines segments with a
  manifest, readable without decryption keys, and an archive-then-prune operation that
  deletes nothing until the archive is verified on disk.
- Replay debugger (`loomwork replay`): steps through a history tick by tick against
  workflow code and explains the first divergence; can stop at a tick and run queries.
- History graphs (`loomwork workflow graph`): steps, timers, children and waits as
  Mermaid, Graphviz or a terminal timeline.
- Workflow id policies: `IdReusePolicy` (allow, allow only after failure, reject) and
  `IdConflictPolicy` (fail, use the running run, terminate it), enforced atomically by
  every store.
- Reset: `handle.reset(to=ResetPoint...)` restarts a run from a tick boundary, typically
  just before the tick that failed, re-delivering signals and updates received since.
- Batch operations: `Client.batch_signal`, `batch_cancel` and `batch_terminate` over a
  visibility query, with bounded concurrency and a per-run outcome summary.
- History limits: a warning (metric and log) and a hard limit on events and bytes per run
  that fails the run cleanly with `HistoryLimitExceeded`.
- Class-based workflows: `@lw.workflow` on a class with `@lw.run`, `@lw.signal`,
  `@lw.query` and `@lw.update` methods; handler methods double as client references
  (`handle.signal(Cart.add, item)`). Same histories as function workflows.
- `lw.wait_condition(predicate, timeout=...)`, re-evaluated whenever a signal or update
  handler finishes.
- Large payload offloading: `OffloadCodec` moves payloads above a size threshold to a
  blob store (memory, filesystem, or S3-compatible storage with a built-in SigV4 signer)
  under content-hash keys verified on read, leaving a small reference in the history.
  It runs after compression and encryption, so blobs hold ciphertext.
  `collect_garbage` removes unreferenced blobs after a grace period.
- Headers on runs, steps and child workflows, and end-to-end trace propagation: a trace
  active when a run starts (or a `traceparent` header) becomes the parent of every step
  span, through children and continue-as-new, with span ids derived deterministically so
  replays record identical events. `loomwork.current_trace_context()` reads it.
- Interceptors: step-attempt interceptors on workers and outbound client interceptors,
  plus `TracingClientInterceptor` for OpenTelemetry.
- `CorrelationFilter` stamps workflow, run, step and trace ids on ordinary log records
  emitted from workflows and steps.
- CLI: `workflow reset`, `workflow batch signal|cancel|terminate` (with dry run and a
  confirmation guard) and id policies on `workflow start`. With `--server`, operator and
  reader tokens now work through the control API (`loomwork.remote.ControlClient`).
- Control API: schedule management, `whoami`, lookup by workflow id and visibility
  queries. Server config gains per-namespace `[server.search_attributes]`, used by the
  control API and the console.
- Console: reset from the run page and a batch page to preview, cancel or terminate runs
  matching a query.
- Seven example applications (order fulfilment saga, expense approval, bulk import,
  scheduled reports, task queues, encrypted payments, versioned workflow), each tested
  and lint-clean, and checked by CI.
- `MetricsServer`: a dependency-free `/metrics` and `/healthz` endpoint for worker
  processes.
- The server reloads its tokens file when it changes, so revoking a token no longer needs
  a restart.

### Changed

- Workers claim work with one dispatcher per kind of work feeding a bounded pool of
  tasks, instead of one polling loop per concurrency slot; idle workers now issue one
  claim per poll interval per kind of work.
- Claims, lease renewals and step completions use single conditional statements, and a
  tick's events are inserted with one statement (about 23% fewer statements per
  workflow); PostgreSQL notifications of a transaction are sent in one round trip.
- Callers waiting for results or update outcomes back off their fallback polling.

### Fixed

- SQLite transactions could be left open when the calling task was cancelled.
