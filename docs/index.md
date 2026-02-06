# Loomwork documentation

## Start here

- [Tutorial](tutorial.md): build a durable subscription workflow, test it, run it
- [Concepts](concepts.md): histories, ticks, replay, determinism and delivery guarantees

## Guides

- [Workflows and steps](guides/workflows.md): declaring, retries, timeouts, heartbeats,
  cancellation, continue-as-new
- [Messaging](guides/messaging.md): typed channels, queries and updates
- [Class-based workflows](guides/workflow-classes.md): state in attributes, handlers as
  methods, `wait_condition`
- [Child workflows and sagas](guides/composition.md)
- [Schedules](guides/schedules.md): cron, intervals, overlap policies, backfill
- [Task queues](guides/task-queues.md): routing, priorities, rate and concurrency limits
- [Interceptors and context propagation](guides/interceptors.md): step and client
  middleware, distributed tracing, structured logging correlation
- [Versioning](guides/versioning.md): safe changes, patches, compatibility checks
- [Testing](testing.md): the testkit, time skipping and golden histories
- [Simulation testing](simulation-testing.md): seeded fault injection, worker chaos and
  invariant checking across many workflows and workers
- [Debugging](debugging.md): the replay debugger and history graphs

## Operations

- [Deployment](operations/deployment.md): embedded, dedicated workers, server mode,
  rolling deploys, migrations, retention
- [Archiving](operations/archiving.md): writing closed runs to long-term storage and
  reading them back, crash-safely
- [Configuration reference](operations/configuration.md)
- [Monitoring](operations/monitoring.md): metrics, tracing, logs, store health
- [Performance](performance.md): sizing workers, choosing a store, replay cache sizing,
  payload size advice, backed by `benchmarks/`
- [Recovery and fleet operations](operations/recovery.md): resetting a run, batch
  signal/cancel/terminate, history size limits
- [Troubleshooting](operations/troubleshooting.md)
- [Server mode](server.md)
- [Operator web console](console.md)

## Reference

- [Command line](cli.md): the `loomwork` command
- [Visibility queries](visibility-queries.md)
- [Payload encryption](payload-encryption.md)
- [Large payloads](large-payloads.md): offloading big step results and arguments to a blob store
- [Determinism lint rules](lint-rules.md)

## Design

Architecture decision records:

- [ADR-0001](adr/0001-architecture.md): overall architecture
- [ADR-0002](adr/0002-tick-delimited-history.md): tick-delimited history and replay
- [ADR-0003](adr/0003-shared-sql-store.md): one SQL implementation for all relational stores
- [ADR-0004](adr/0004-cross-run-outbox.md): cross-run effects through a transactional outbox
- [ADR-0005](adr/0005-class-based-workflows.md): class-based workflows as a layer over
  function workflows
- [ADR-0006](adr/0006-worker-build-versioning.md): worker build versioning (pinned runs)

[Internals](internals.md) explains how the pieces fit, for people changing Loomwork.
