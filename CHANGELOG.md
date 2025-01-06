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
