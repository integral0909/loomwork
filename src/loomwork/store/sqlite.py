"""Durable single-node store backed by SQLite.

Suited to embedded deployments: one application process (with any number of worker
coroutines) owning a database file. Several processes may share the file (WAL mode and a
busy timeout make that safe), but they only learn about each other's work by polling, so
latency is bounded by ``EngineConfig.poll_interval``.

Requires the ``sqlite`` extra (``pip install loomwork[sqlite]``).
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from loomwork.store.sql import codec
from loomwork.store.sql.base import SQLStore
from loomwork.store.sql.migrations import (
    MIGRATIONS_TABLE_SQLITE,
    Dialect,
    Migration,
    pending,
)

if TYPE_CHECKING:
    import aiosqlite

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICRO = timedelta(microseconds=1)


def _import_aiosqlite() -> Any:
    try:
        import aiosqlite
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "SQLiteStore requires the 'aiosqlite' package: pip install 'loomwork[sqlite]'"
        ) from exc
    return aiosqlite


class _SqliteTransaction:
    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = conn
        self.topics: list[str] = []

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> int:
        cursor = await self._conn.execute(sql, tuple(params))
        try:
            return cursor.rowcount
        finally:
            await cursor.close()

    async def fetchone(self, sql: str, params: Sequence[Any] = ()) -> codec.Row | None:
        async with self._conn.execute(sql, tuple(params)) as cursor:
            row = await cursor.fetchone()
        return None if row is None else dict(row)

    async def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[codec.Row]:
        async with self._conn.execute(sql, tuple(params)) as cursor:
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    def notify(self, topic: str) -> None:
        if topic not in self.topics:
            self.topics.append(topic)


class SQLiteStore(SQLStore):
    """A :class:`~loomwork.store.Store` in a SQLite database file.

    Args:
        path: database file, or ``":memory:"`` for a private in-memory database.
        busy_timeout: seconds to wait for another process's write lock.
    """

    dialect: ClassVar[Dialect] = Dialect.SQLITE

    def __init__(self, path: str | os.PathLike[str], *, busy_timeout: float = 5.0) -> None:
        super().__init__()
        self.path = str(path)
        self.busy_timeout = busy_timeout
        self._conn: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ lifecycle

    async def open(self) -> None:
        if self._conn is not None:
            return
        aiosqlite = _import_aiosqlite()
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(self.path, isolation_level=None)
        conn.row_factory = aiosqlite.Row
        await conn.execute(f"PRAGMA busy_timeout = {int(self.busy_timeout * 1000)}")
        await conn.execute("PRAGMA journal_mode = WAL")
        await conn.execute("PRAGMA synchronous = NORMAL")
        await conn.execute("PRAGMA foreign_keys = ON")
        self._conn = conn
        try:
            await self._migrate()
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            await conn.close()

    async def __aenter__(self) -> SQLiteStore:
        await self.open()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    def _connection(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("SQLiteStore is not open; call 'await store.open()' first")
        return self._conn

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[_SqliteTransaction]:
        conn = self._connection()
        async with self._lock:
            await self._control(conn, "BEGIN IMMEDIATE", rollback_on_cancel=True)
            tx = _SqliteTransaction(conn)
            try:
                yield tx
            except BaseException:
                await self._control(conn, "ROLLBACK")
                raise
            await self._control(conn, "COMMIT")
        for topic in tx.topics:
            self.notifier.publish(topic)

    @staticmethod
    async def _control(
        conn: aiosqlite.Connection, statement: str, *, rollback_on_cancel: bool = False
    ) -> None:
        """Run BEGIN/COMMIT/ROLLBACK so that cancelling the caller cannot interrupt it.

        aiosqlite executes statements on a background thread; if the awaiting task is
        cancelled, the statement still runs. Without shielding, a cancelled BEGIN would
        leave the shared connection inside a transaction nobody finishes.
        """
        future = asyncio.ensure_future(conn.execute(statement))
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError:
            await future
            if rollback_on_cancel:
                await conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------------ migrations

    async def _migrate(self) -> None:
        async with self.transaction() as tx:
            await tx.execute(MIGRATIONS_TABLE_SQLITE)
            rows = await tx.fetchall("SELECT version, checksum FROM lw_schema_migrations")
            todo = pending({int(r["version"]): r["checksum"] for r in rows}, self.dialect)
            for migration in todo:
                await self._apply(tx, migration)

    async def _apply(self, tx: _SqliteTransaction, migration: Migration) -> None:
        for statement in migration.statements(self.dialect):
            await tx.execute(statement)
        await tx.execute(
            "INSERT INTO lw_schema_migrations (version, name, checksum, applied_at) "
            "VALUES (?, ?, ?, ?)",
            (
                migration.version,
                migration.name,
                migration.checksum(self.dialect),
                self.ts_in(datetime.now(UTC)),
            ),
        )

    async def schema_version(self) -> int:
        async with self.transaction() as tx:
            row = await tx.fetchone("SELECT MAX(version) AS v FROM lw_schema_migrations")
        return int(row["v"]) if row and row["v"] is not None else 0

    # ------------------------------------------------------------------ dialect hooks

    def ts_in(self, value: datetime | None) -> int | None:
        if value is None:
            return None
        return (value - _EPOCH) // _MICRO

    def ts_out(self, value: Any) -> datetime | None:
        if value is None:
            return None
        return _EPOCH + timedelta(microseconds=int(value))

    def is_unique_violation(self, exc: BaseException) -> bool:
        import sqlite3

        return isinstance(exc, sqlite3.IntegrityError) and "UNIQUE" in str(exc)
