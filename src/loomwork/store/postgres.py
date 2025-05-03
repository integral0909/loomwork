"""Production store backed by PostgreSQL.

* Workers on any number of hosts coordinate through row leases; claims use
  ``FOR UPDATE SKIP LOCKED`` so concurrent workers never block on each other.
* Every state change issues ``NOTIFY loomwork, '<topic>'`` inside its transaction. A
  background ``LISTEN`` connection feeds those notifications into the local
  :class:`~loomwork.store.notify.Notifier`, so idle workers wake up within milliseconds
  instead of waiting for their poll interval.
* Migrations run under a transaction-scoped advisory lock, so many processes can start
  at once safely.

Requires the ``postgres`` extra (``pip install loomwork[postgres]``).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar

from loomwork.store.sql import codec
from loomwork.store.sql.base import SQLStore
from loomwork.store.sql.migrations import (
    MIGRATIONS_TABLE_POSTGRES,
    Dialect,
    Migration,
    pending,
)

if TYPE_CHECKING:
    import psycopg
    from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger("loomwork.store.postgres")

NOTIFY_CHANNEL = "loomwork"
# Arbitrary but fixed: identifies Loomwork's migration lock among advisory locks.
_MIGRATION_LOCK_KEY = 0x4C4F4F4D  # "LOOM"
_LISTEN_RETRY_SECONDS = 1.0


def _import_psycopg() -> tuple[Any, Any, Any]:
    try:
        import psycopg
        from psycopg.rows import dict_row
        from psycopg_pool import AsyncConnectionPool
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "PostgresStore requires 'psycopg' and 'psycopg-pool': "
            "pip install 'loomwork[postgres]'"
        ) from exc
    return psycopg, dict_row, AsyncConnectionPool


def _translate(sql: str) -> str:
    # Shared SQL uses '?' placeholders; psycopg expects '%s'. Literal percent signs
    # must be escaped first. None of the shared statements contain a literal '?'.
    return sql.replace("%", "%%").replace("?", "%s")


class _PgTransaction:
    def __init__(self, conn: psycopg.AsyncConnection[Any]) -> None:
        self._conn = conn
        self.topics: list[str] = []

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> int:
        cursor = await self._conn.execute(_translate(sql), tuple(params))
        return cursor.rowcount

    async def fetchone(self, sql: str, params: Sequence[Any] = ()) -> codec.Row | None:
        cursor = await self._conn.execute(_translate(sql), tuple(params))
        row: codec.Row | None = await cursor.fetchone()
        return row

    async def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[codec.Row]:
        cursor = await self._conn.execute(_translate(sql), tuple(params))
        return list(await cursor.fetchall())

    def notify(self, topic: str) -> None:
        if topic not in self.topics:
            self.topics.append(topic)


class PostgresStore(SQLStore):
    """A :class:`~loomwork.store.Store` in a PostgreSQL database.

    Args:
        dsn: libpq connection string, e.g. ``postgresql://user@host/db``. Keep
            credentials out of source code: read the DSN from the environment or use
            libpq's ``PGPASSFILE``.
        min_size, max_size: connection pool bounds.
        listen: run a ``LISTEN`` connection for low-latency wake-ups.
    """

    dialect: ClassVar[Dialect] = Dialect.POSTGRES
    FOR_UPDATE: ClassVar[str] = " FOR UPDATE"
    SKIP_LOCKED: ClassVar[str] = " FOR UPDATE SKIP LOCKED"

    def __init__(
        self,
        dsn: str,
        *,
        min_size: int = 1,
        max_size: int = 10,
        listen: bool = True,
    ) -> None:
        super().__init__()
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.listen = listen
        self._pool: AsyncConnectionPool | None = None
        self._listener: asyncio.Task[None] | None = None
        self._listening = asyncio.Event()

    # ------------------------------------------------------------------ lifecycle

    async def open(self) -> None:
        if self._pool is not None:
            return
        _, dict_row, pool_cls = _import_psycopg()
        pool = pool_cls(
            self.dsn,
            min_size=self.min_size,
            max_size=self.max_size,
            kwargs={"row_factory": dict_row, "autocommit": False},
            open=False,
        )
        await pool.open(wait=True)
        self._pool = pool
        try:
            await self._migrate()
        except BaseException:
            await self.close()
            raise
        if self.listen:
            self._listener = asyncio.ensure_future(self._listen_forever())
            self._listener.set_name("loomwork-pg-listen")

    async def close(self) -> None:
        listener, self._listener = self._listener, None
        if listener is not None:
            listener.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await listener
        pool, self._pool = self._pool, None
        if pool is not None:
            await pool.close()

    async def __aenter__(self) -> PostgresStore:
        await self.open()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    async def wait_listening(self, timeout: float = 5.0) -> None:
        """Wait until the LISTEN connection is established (useful in tests)."""
        async with asyncio.timeout(timeout):
            await self._listening.wait()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[_PgTransaction]:
        if self._pool is None:
            raise RuntimeError("PostgresStore is not open; call 'await store.open()' first")
        async with self._pool.connection() as conn, conn.transaction():
            tx = _PgTransaction(conn)
            yield tx
            if tx.topics:
                # One round trip for all notifications of the transaction.
                calls = ", ".join("pg_notify(%s, %s)" for _ in tx.topics)
                params = [value for topic in tx.topics for value in (NOTIFY_CHANNEL, topic)]
                await conn.execute(f"SELECT {calls}", params)
        # Local waiters learn immediately; remote ones through LISTEN.
        for topic in tx.topics:
            self.notifier.publish(topic)

    async def _listen_forever(self) -> None:
        psycopg, _, _ = _import_psycopg()
        while True:
            try:
                conn = await psycopg.AsyncConnection.connect(self.dsn, autocommit=True)
                async with conn:
                    await conn.execute(f"LISTEN {NOTIFY_CHANNEL}")
                    self._listening.set()
                    async for notification in conn.notifies():
                        self.notifier.publish(notification.payload)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._listening.clear()
                logger.warning("LISTEN connection lost; reconnecting", exc_info=True)
                await asyncio.sleep(_LISTEN_RETRY_SECONDS)

    # ------------------------------------------------------------------ migrations

    async def _migrate(self) -> None:
        async with self.transaction() as tx:
            await tx.execute("SELECT pg_advisory_xact_lock(?)", (_MIGRATION_LOCK_KEY,))
            await tx.execute(MIGRATIONS_TABLE_POSTGRES)
            rows = await tx.fetchall("SELECT version, checksum FROM lw_schema_migrations")
            todo = pending({int(r["version"]): r["checksum"] for r in rows}, self.dialect)
            for migration in todo:
                await self._apply(tx, migration)
                logger.info("applied migration %d: %s", migration.version, migration.name)

    async def _apply(self, tx: _PgTransaction, migration: Migration) -> None:
        for statement in migration.statements(self.dialect):
            await tx.execute(statement)
        await tx.execute(
            "INSERT INTO lw_schema_migrations (version, name, checksum, applied_at) "
            "VALUES (?, ?, ?, ?)",
            (
                migration.version,
                migration.name,
                migration.checksum(self.dialect),
                datetime.now(UTC),
            ),
        )

    async def schema_version(self) -> int:
        async with self.transaction() as tx:
            row = await tx.fetchone("SELECT MAX(version) AS v FROM lw_schema_migrations")
        return int(row["v"]) if row and row["v"] is not None else 0

    # ------------------------------------------------------------------ dialect hooks

    def ts_in(self, value: datetime | None) -> datetime | None:
        return value

    def ts_out(self, value: Any) -> datetime | None:
        if value is None:
            return None
        assert isinstance(value, datetime)
        return value.astimezone(UTC)

    def is_unique_violation(self, exc: BaseException) -> bool:
        psycopg, _, _ = _import_psycopg()
        return isinstance(exc, psycopg.errors.UniqueViolation)
