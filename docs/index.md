# Loomwork documentation

## Start here

- [Concepts](concepts.md): histories, ticks, replay, determinism and delivery guarantees

## Guides

- [Workflows and steps](guides/workflows.md): declaring, retries, timeouts, heartbeats,
  cancellation, continue-as-new
- [Messaging](guides/messaging.md): typed channels, queries and updates
- [Child workflows and sagas](guides/composition.md)
- [Schedules](guides/schedules.md): cron, intervals, overlap policies, backfill
- [Task queues](guides/task-queues.md): routing, priorities, rate and concurrency limits
- [Versioning](guides/versioning.md): safe changes, patches, compatibility checks
- [Testing](testing.md): the testkit, time skipping and golden histories

## Operations

- [Deployment](operations/deployment.md): embedded, dedicated workers, server mode,
  rolling deploys, migrations, retention
- [Configuration reference](operations/configuration.md)
- [Monitoring](operations/monitoring.md): metrics, tracing, logs, store health
- [Troubleshooting](operations/troubleshooting.md)
- [Server mode](server.md)

## Reference

- [Visibility queries](visibility-queries.md)
- [Payload encryption](payload-encryption.md)
- [Determinism lint rules](lint-rules.md)

## Design

Architecture decision records:

- [ADR-0001](adr/0001-architecture.md): overall architecture
- [ADR-0002](adr/0002-tick-delimited-history.md): tick-delimited history and replay
- [ADR-0003](adr/0003-shared-sql-store.md): one SQL implementation for all relational stores
- [ADR-0004](adr/0004-cross-run-outbox.md): cross-run effects through a transactional outbox
