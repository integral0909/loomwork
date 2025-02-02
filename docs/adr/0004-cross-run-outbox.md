# ADR-0004: Cross-run effects go through a transactional outbox

- Status: accepted
- Date: 2025-02-02

## Context

Child workflows, parent-close policies and signals sent from one workflow to another
all make one run's transition change another run. Doing both changes in one database
transaction means locking two execution rows. A parent closing (locking itself, then
terminating its children) and a child closing (locking itself, then reporting to the
parent) take those locks in opposite orders and can deadlock under PostgreSQL.

## Decision

A transaction only ever locks **one** execution row. Effects on other runs are written
as rows of `lw_outbox` in the same transaction:

| Cause                                   | Outbox message                                  |
| --------------------------------------- | ----------------------------------------------- |
| child closes                            | `append_input` `ChildCompleted`/`ChildFailed` to the parent run |
| parent closes, child policy `terminate` | `terminate` the child run                       |
| parent closes, `request_cancel`         | `request_cancel` the child run                  |
| workflow calls `signal_external`        | `append_input` `SignalReceived` to the target's running execution |

A relay (one loop per worker, safe to run many) applies messages one per transaction:
lock the message (`SKIP LOCKED`), then lock its single target, apply, delete. Messages
for runs that are gone or closed are dropped.

Creating a child is the exception that needs no outbox: the child row is *new*, so the
parent's tick transaction inserts it directly, making "the parent decided to start a
child" and "the child exists" atomic. Child run ids are derived from the parent run id
and the child sequence number (`<parent>.c<n>`), so workflow code knows them
immediately and they are identical on replay.

## Consequences

- Cross-run effects are asynchronous but guaranteed: they are committed with their
  cause and applied at least once. Inputs are idempotent where it matters (signals carry
  message ids; update ids are deduplicated by the workflow instance).
- The outbox adds latency of one relay pass, normally milliseconds thanks to
  notifications.
- `Store.outbox_size()` is a useful health metric: a growing outbox means no relay runs.
