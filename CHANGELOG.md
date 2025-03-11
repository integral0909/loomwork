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

### Fixed

- SQLite transactions could be left open when the calling task was cancelled.
