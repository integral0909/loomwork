from __future__ import annotations

from pathlib import Path

import pytest

from loomwork.store import MemoryStore, StoreURLError, redact_url, store_from_url
from loomwork.store.postgres import PostgresStore
from loomwork.store.sql.migrations import MIGRATIONS, Dialect, MigrationError, pending
from loomwork.store.sqlite import SQLiteStore


class TestStoreFromUrl:
    def test_memory(self) -> None:
        assert isinstance(store_from_url("memory://"), MemoryStore)
        with pytest.raises(StoreURLError):
            store_from_url("memory://?x=1")

    def test_sqlite_paths(self) -> None:
        relative = store_from_url("sqlite:///data/loom.db")
        assert isinstance(relative, SQLiteStore)
        assert relative.path == "data/loom.db"
        absolute = store_from_url("sqlite:////var/lib/loom.db?busy_timeout=2.5")
        assert isinstance(absolute, SQLiteStore)
        assert absolute.path == "/var/lib/loom.db"
        assert absolute.busy_timeout == 2.5
        in_memory = store_from_url("sqlite://:memory:")
        assert isinstance(in_memory, SQLiteStore)
        assert in_memory.path == ":memory:"

    @pytest.mark.parametrize(
        "url",
        [
            "sqlite://host/db",
            "sqlite:///",
            "sqlite:///x.db?busy_timeout=abc",
            "sqlite:///x.db?nope=1",
        ],
    )
    def test_sqlite_errors(self, url: str) -> None:
        with pytest.raises(StoreURLError):
            store_from_url(url)

    def test_postgres(self) -> None:
        store = store_from_url(
            "postgres://app:secret@db.internal:5433/loom?min_size=2&max_size=20&listen=off&sslmode=require"
        )
        assert isinstance(store, PostgresStore)
        assert (store.min_size, store.max_size, store.listen) == (2, 20, False)
        assert store.dsn == "postgresql://app:secret@db.internal:5433/loom?sslmode=require"

    @pytest.mark.parametrize(
        "url", ["postgresql://h/db?min_size=x", "postgresql://h/db?listen=maybe"]
    )
    def test_postgres_errors(self, url: str) -> None:
        with pytest.raises(StoreURLError):
            store_from_url(url)

    def test_unknown_scheme(self) -> None:
        with pytest.raises(StoreURLError, match="unsupported"):
            store_from_url("mysql://x/y")

    def test_redact(self) -> None:
        assert redact_url("postgresql://app:secret@h/db") == "postgresql://app:***@h/db"
        assert redact_url("sqlite:///x.db") == "sqlite:///x.db"


class TestMigrations:
    def test_versions_are_contiguous(self) -> None:
        assert [m.version for m in MIGRATIONS] == list(range(1, len(MIGRATIONS) + 1))

    def test_every_migration_has_both_dialects(self) -> None:
        for migration in MIGRATIONS:
            assert migration.statements(Dialect.SQLITE)
            assert migration.statements(Dialect.POSTGRES)

    def test_pending_on_empty_database(self) -> None:
        assert pending({}, Dialect.SQLITE) == list(MIGRATIONS)

    def test_nothing_pending_when_up_to_date(self) -> None:
        applied = {m.version: m.checksum(Dialect.SQLITE) for m in MIGRATIONS}
        assert pending(applied, Dialect.SQLITE) == []

    def test_edited_migration_is_detected(self) -> None:
        with pytest.raises(MigrationError, match="differs"):
            pending({1: "0000"}, Dialect.POSTGRES)

    def test_newer_database_is_rejected(self) -> None:
        with pytest.raises(MigrationError, match="newer than this Loomwork release"):
            pending({999: "x"}, Dialect.SQLITE)

    def test_checksum_ignores_whitespace(self) -> None:
        migration = MIGRATIONS[0]
        assert migration.checksum(Dialect.SQLITE) == migration.checksum(Dialect.SQLITE)
        assert len(migration.checksum(Dialect.POSTGRES)) == 16


class TestSQLiteLifecycle:
    async def test_reopen_is_idempotent_and_migrations_run_once(self, tmp_path: Path) -> None:
        path = tmp_path / "nested" / "dir" / "loom.db"
        async with SQLiteStore(path) as store:
            await store.open()
            assert await store.schema_version() == len(MIGRATIONS)
        async with SQLiteStore(path) as store:
            assert await store.schema_version() == len(MIGRATIONS)

    async def test_in_memory_database(self) -> None:
        async with SQLiteStore(":memory:") as store:
            assert await store.list_executions() == []

    async def test_use_before_open(self, tmp_path: Path) -> None:
        store = SQLiteStore(tmp_path / "x.db")
        with pytest.raises(RuntimeError, match="not open"):
            await store.list_executions()

    async def test_tampered_schema_is_rejected(self, tmp_path: Path) -> None:
        path = tmp_path / "loom.db"
        async with SQLiteStore(path) as store, store.transaction() as tx:
            await tx.execute("UPDATE lw_schema_migrations SET checksum = 'bad' WHERE version = 1")
        with pytest.raises(MigrationError):
            await SQLiteStore(path).open()
