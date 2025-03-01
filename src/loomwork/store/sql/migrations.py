"""Versioned schema migrations for the SQL backends.

Each migration carries the DDL for every supported dialect. Migrations are applied in
order, each in its own transaction, and recorded in ``lw_schema_migrations``. They are
append-only: never edit a released migration, add a new one.

Table names share the ``lw_`` prefix. The prefix is fixed (not configurable) so that
operators and tooling can rely on it.
"""

from __future__ import annotations

import enum
import hashlib
from dataclasses import dataclass


class Dialect(enum.StrEnum):
    SQLITE = "sqlite"
    POSTGRES = "postgres"


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    sqlite: tuple[str, ...]
    postgres: tuple[str, ...]

    def statements(self, dialect: Dialect) -> tuple[str, ...]:
        return self.sqlite if dialect is Dialect.SQLITE else self.postgres

    def checksum(self, dialect: Dialect) -> str:
        digest = hashlib.sha256()
        for statement in self.statements(dialect):
            digest.update(" ".join(statement.split()).encode())
        return digest.hexdigest()[:16]


MIGRATIONS_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS lw_schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT    NOT NULL,
    checksum   TEXT    NOT NULL,
    applied_at INTEGER NOT NULL
)
"""

MIGRATIONS_TABLE_POSTGRES = """
CREATE TABLE IF NOT EXISTS lw_schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT        NOT NULL,
    checksum   TEXT        NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL
)
"""

# ---------------------------------------------------------------------------------------
# 0001: executions, events, step jobs, timers

_0001_SQLITE = (
    """
    CREATE TABLE lw_executions (
        seq               INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id            TEXT    NOT NULL UNIQUE,
        workflow_id       TEXT    NOT NULL,
        workflow_name     TEXT    NOT NULL,
        status            TEXT    NOT NULL,
        created_at        INTEGER NOT NULL,
        closed_at         INTEGER,
        last_event_id     INTEGER NOT NULL DEFAULT 0,
        last_input_id     INTEGER NOT NULL DEFAULT 0,
        consumed_through  INTEGER NOT NULL DEFAULT 0,
        tick_pending      INTEGER NOT NULL DEFAULT 0,
        tick_ready_at     INTEGER,
        tick_failures     INTEGER NOT NULL DEFAULT 0,
        last_tick_error   TEXT,
        lease_owner       TEXT,
        lease_expires_at  INTEGER,
        cancel_requested  INTEGER NOT NULL DEFAULT 0,
        result            TEXT,
        failure           TEXT,
        close_reason      TEXT    NOT NULL DEFAULT ''
    )
    """,
    "CREATE UNIQUE INDEX lw_executions_running_workflow_id ON lw_executions (workflow_id) "
    "WHERE status = 'running'",
    "CREATE INDEX lw_executions_workflow_id ON lw_executions (workflow_id, seq)",
    "CREATE INDEX lw_executions_name ON lw_executions (workflow_name, seq)",
    "CREATE INDEX lw_executions_ticks ON lw_executions (tick_ready_at) WHERE tick_pending = 1",
    """
    CREATE TABLE lw_events (
        run_id     TEXT    NOT NULL,
        event_id   INTEGER NOT NULL,
        kind       TEXT    NOT NULL,
        created_at INTEGER NOT NULL,
        body       TEXT    NOT NULL,
        PRIMARY KEY (run_id, event_id)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE lw_step_jobs (
        job_id           TEXT    PRIMARY KEY,
        run_id           TEXT    NOT NULL,
        workflow_id      TEXT    NOT NULL,
        workflow_name    TEXT    NOT NULL,
        seq              INTEGER NOT NULL,
        step_name        TEXT    NOT NULL,
        args             TEXT    NOT NULL,
        options          TEXT    NOT NULL,
        scheduled_at     INTEGER NOT NULL,
        ready_at         INTEGER NOT NULL,
        deadline         INTEGER,
        attempt          INTEGER NOT NULL DEFAULT 0,
        lease_owner      TEXT,
        lease_expires_at INTEGER,
        last_failure     TEXT,
        cancel_requested INTEGER NOT NULL DEFAULT 0,
        UNIQUE (run_id, seq)
    )
    """,
    "CREATE INDEX lw_step_jobs_ready ON lw_step_jobs (ready_at)",
    """
    CREATE TABLE lw_timers (
        run_id  TEXT    NOT NULL,
        seq     INTEGER NOT NULL,
        fire_at INTEGER NOT NULL,
        PRIMARY KEY (run_id, seq)
    ) WITHOUT ROWID
    """,
    "CREATE INDEX lw_timers_fire_at ON lw_timers (fire_at)",
)

_0001_POSTGRES = (
    """
    CREATE TABLE lw_executions (
        seq               BIGSERIAL   PRIMARY KEY,
        run_id            TEXT        NOT NULL UNIQUE,
        workflow_id       TEXT        NOT NULL,
        workflow_name     TEXT        NOT NULL,
        status            TEXT        NOT NULL,
        created_at        TIMESTAMPTZ NOT NULL,
        closed_at         TIMESTAMPTZ,
        last_event_id     INTEGER     NOT NULL DEFAULT 0,
        last_input_id     INTEGER     NOT NULL DEFAULT 0,
        consumed_through  INTEGER     NOT NULL DEFAULT 0,
        tick_pending      BOOLEAN     NOT NULL DEFAULT FALSE,
        tick_ready_at     TIMESTAMPTZ,
        tick_failures     INTEGER     NOT NULL DEFAULT 0,
        last_tick_error   TEXT,
        lease_owner       TEXT,
        lease_expires_at  TIMESTAMPTZ,
        cancel_requested  BOOLEAN     NOT NULL DEFAULT FALSE,
        result            JSONB,
        failure           JSONB,
        close_reason      TEXT        NOT NULL DEFAULT ''
    )
    """,
    "CREATE UNIQUE INDEX lw_executions_running_workflow_id ON lw_executions (workflow_id) "
    "WHERE status = 'running'",
    "CREATE INDEX lw_executions_workflow_id ON lw_executions (workflow_id, seq)",
    "CREATE INDEX lw_executions_name ON lw_executions (workflow_name, seq)",
    "CREATE INDEX lw_executions_ticks ON lw_executions (tick_ready_at) WHERE tick_pending",
    """
    CREATE TABLE lw_events (
        run_id     TEXT        NOT NULL,
        event_id   INTEGER     NOT NULL,
        kind       TEXT        NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        body       JSONB       NOT NULL,
        PRIMARY KEY (run_id, event_id)
    )
    """,
    """
    CREATE TABLE lw_step_jobs (
        job_id           TEXT        PRIMARY KEY,
        run_id           TEXT        NOT NULL,
        workflow_id      TEXT        NOT NULL,
        workflow_name    TEXT        NOT NULL,
        seq              INTEGER     NOT NULL,
        step_name        TEXT        NOT NULL,
        args             JSONB       NOT NULL,
        options          JSONB       NOT NULL,
        scheduled_at     TIMESTAMPTZ NOT NULL,
        ready_at         TIMESTAMPTZ NOT NULL,
        deadline         TIMESTAMPTZ,
        attempt          INTEGER     NOT NULL DEFAULT 0,
        lease_owner      TEXT,
        lease_expires_at TIMESTAMPTZ,
        last_failure     JSONB,
        cancel_requested BOOLEAN     NOT NULL DEFAULT FALSE,
        UNIQUE (run_id, seq)
    )
    """,
    "CREATE INDEX lw_step_jobs_ready ON lw_step_jobs (ready_at)",
    """
    CREATE TABLE lw_timers (
        run_id  TEXT        NOT NULL,
        seq     INTEGER     NOT NULL,
        fire_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (run_id, seq)
    )
    """,
    "CREATE INDEX lw_timers_fire_at ON lw_timers (fire_at)",
)

# ---------------------------------------------------------------------------------------
# 0002: task queues, priorities and heartbeats on step jobs

_0002_SQLITE = (
    "ALTER TABLE lw_step_jobs ADD COLUMN queue TEXT NOT NULL DEFAULT 'default'",
    "ALTER TABLE lw_step_jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE lw_step_jobs ADD COLUMN heartbeat_details TEXT",
    "ALTER TABLE lw_step_jobs ADD COLUMN last_heartbeat_at INTEGER",
    "DROP INDEX lw_step_jobs_ready",
    "CREATE INDEX lw_step_jobs_claim ON lw_step_jobs (queue, priority DESC, ready_at)",
)

_0002_POSTGRES = (
    "ALTER TABLE lw_step_jobs ADD COLUMN queue TEXT NOT NULL DEFAULT 'default'",
    "ALTER TABLE lw_step_jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE lw_step_jobs ADD COLUMN heartbeat_details JSONB",
    "ALTER TABLE lw_step_jobs ADD COLUMN last_heartbeat_at TIMESTAMPTZ",
    "DROP INDEX lw_step_jobs_ready",
    "CREATE INDEX lw_step_jobs_claim ON lw_step_jobs (queue, priority DESC, ready_at)",
)

# ---------------------------------------------------------------------------------------
# 0003: child workflows and the cross-run outbox

_0003_SQLITE = (
    "ALTER TABLE lw_executions ADD COLUMN parent_run_id TEXT",
    "ALTER TABLE lw_executions ADD COLUMN parent_seq INTEGER",
    "ALTER TABLE lw_executions ADD COLUMN parent_close_policy TEXT NOT NULL DEFAULT 'terminate'",
    "CREATE INDEX lw_executions_parent ON lw_executions (parent_run_id) "
    "WHERE parent_run_id IS NOT NULL",
    """
    CREATE TABLE lw_outbox (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        action             TEXT    NOT NULL,
        target_run_id      TEXT,
        target_workflow_id TEXT,
        event_kind         TEXT,
        event_body         TEXT,
        reason             TEXT    NOT NULL DEFAULT '',
        created_at         INTEGER NOT NULL
    )
    """,
)

_0003_POSTGRES = (
    "ALTER TABLE lw_executions ADD COLUMN parent_run_id TEXT",
    "ALTER TABLE lw_executions ADD COLUMN parent_seq INTEGER",
    "ALTER TABLE lw_executions ADD COLUMN parent_close_policy TEXT NOT NULL DEFAULT 'terminate'",
    "CREATE INDEX lw_executions_parent ON lw_executions (parent_run_id) "
    "WHERE parent_run_id IS NOT NULL",
    """
    CREATE TABLE lw_outbox (
        id                 BIGSERIAL   PRIMARY KEY,
        action             TEXT        NOT NULL,
        target_run_id      TEXT,
        target_workflow_id TEXT,
        event_kind         TEXT,
        event_body         JSONB,
        reason             TEXT        NOT NULL DEFAULT '',
        created_at         TIMESTAMPTZ NOT NULL
    )
    """,
)

# ---------------------------------------------------------------------------------------
# 0004: schedules

_0004_SQLITE = (
    """
    CREATE TABLE lw_schedules (
        schedule_id  TEXT    PRIMARY KEY,
        next_fire_at INTEGER,
        paused       INTEGER NOT NULL DEFAULT 0,
        has_buffer   INTEGER NOT NULL DEFAULT 0,
        version      INTEGER NOT NULL,
        body         TEXT    NOT NULL,
        updated_at   INTEGER NOT NULL
    )
    """,
    "CREATE INDEX lw_schedules_due ON lw_schedules (next_fire_at) WHERE paused = 0",
)

_0004_POSTGRES = (
    """
    CREATE TABLE lw_schedules (
        schedule_id  TEXT        PRIMARY KEY,
        next_fire_at TIMESTAMPTZ,
        paused       BOOLEAN     NOT NULL DEFAULT FALSE,
        has_buffer   BOOLEAN     NOT NULL DEFAULT FALSE,
        version      INTEGER     NOT NULL,
        body         JSONB       NOT NULL,
        updated_at   TIMESTAMPTZ NOT NULL
    )
    """,
    "CREATE INDEX lw_schedules_due ON lw_schedules (next_fire_at) WHERE NOT paused",
)

MIGRATIONS: tuple[Migration, ...] = (
    Migration(1, "initial schema", _0001_SQLITE, _0001_POSTGRES),
    Migration(2, "task queues and heartbeats", _0002_SQLITE, _0002_POSTGRES),
    Migration(3, "child workflows and outbox", _0003_SQLITE, _0003_POSTGRES),
    Migration(4, "schedules", _0004_SQLITE, _0004_POSTGRES),
)


class MigrationError(RuntimeError):
    """The database schema is newer than this code, or a migration was edited."""


def pending(applied: dict[int, str], dialect: Dialect) -> list[Migration]:
    """Migrations not yet applied, after validating the ones that were.

    ``applied`` maps version to recorded checksum.
    """
    known = {m.version: m for m in MIGRATIONS}
    newest_known = max(known)
    for version, checksum in applied.items():
        if version > newest_known:
            raise MigrationError(
                f"database schema version {version} is newer than this Loomwork release "
                f"(knows up to {newest_known}); upgrade Loomwork"
            )
        migration = known.get(version)
        if migration is not None and migration.checksum(dialect) != checksum:
            raise MigrationError(
                f"migration {version} ({migration.name}) differs from the one applied to "
                "this database"
            )
    return [m for m in MIGRATIONS if m.version not in applied]
