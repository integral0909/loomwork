# ADR-0003: One SQL implementation for all relational stores

- Status: accepted
- Date: 2025-01-06

## Context

Loomwork supports SQLite (embedded, single node) and PostgreSQL (production, many
workers). The store contract (ADR-0001) has around twenty operations, each with subtle
state transitions: tick leasing, pending-tick bookkeeping, step cancellation, retention.
Two independent implementations would drift.

## Decision

`loomwork.store.sql.base.SQLStore` implements every store operation once, in portable
SQL with `?` placeholders. A backend supplies only:

- a transaction context yielding an object with `execute`, `fetchone`, `fetchall` and a
  deferred `notify`;
- timestamp conversion (`timestamptz` in PostgreSQL, integer microseconds in SQLite);
- its row-locking clauses (`FOR UPDATE`, `FOR UPDATE SKIP LOCKED`; empty for SQLite,
  whose `BEGIN IMMEDIATE` transactions are already serialised);
- detection of unique-constraint violations.

State transitions are expressed by loading an `ExecutionRecord`, mutating it with the
same logic as the in-memory store, and writing it back. The shared conformance suite
(`tests/store_conformance.py`) runs unchanged against every backend.

### Lock ordering

Any transaction that locks several rows locks the execution row first, then step-job
or timer rows. Firing timers therefore happens in one short transaction per timer
(execution row, then timer row) rather than one transaction that locks timers first.

### Notifications

Topics are collected during a transaction and published only after commit. PostgreSQL
also issues `pg_notify` inside the transaction, so other processes are woken exactly
when the change becomes visible.

### Migrations

Migrations are append-only, carry DDL for every dialect, and are checksummed. A
database whose recorded checksum differs, or whose schema is newer than the code, is
refused rather than silently used.

## Consequences

- New backends (for example MySQL) need roughly 150 lines of dialect code.
- Some operations issue more statements than hand-tuned SQL would; the hot paths
  (claiming ticks and steps, committing ticks) are single indexed queries per row.
