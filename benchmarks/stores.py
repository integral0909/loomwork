"""Opening a fresh :class:`~loomwork.store.base.Store` for each backend a scenario runs
against.

Every trial gets an empty store: a new SQLite file, a truncated PostgreSQL database, a
fresh in-memory instance, a new in-process HTTP server backed by :class:`MemoryStore`.
Reusing one populated store across trials would let history accumulate and drift the
measurements as scenarios run.

PostgreSQL benchmarks always run against a dedicated ``loomwork_bench`` database in
whichever cluster ``--postgres-dsn`` names (its own database name, if it has one, is
ignored), created on first use if it does not exist yet. This is what makes it safe to
point ``--postgres-dsn`` at a shared test cluster's connection info, including
``LOOMWORK_TEST_POSTGRES_DSN`` itself: benchmarks never touch the database that names.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import urlsplit, urlunsplit

from benchmarks.harness import BenchContext
from loomwork.store.base import Store
from loomwork.store.memory import MemoryStore

DEFAULT_POSTGRES_DSN = "postgresql://loomwork@127.0.0.1:55432/loomwork_bench"
BENCH_DATABASE = "loomwork_bench"

_TABLES = (
    "lw_events",
    "lw_step_jobs",
    "lw_timers",
    "lw_outbox",
    "lw_schedules",
    "lw_search_attributes",
    "lw_build_compatibility",
    "lw_worker_presence",
    "lw_executions",
)


@asynccontextmanager
async def open_store(ctx: BenchContext) -> AsyncIterator[Store]:
    """An opened, empty store of ``ctx.store_kind``; closed on exit."""
    if ctx.store_kind == "memory":
        async with _open_memory() as store:
            yield store
    elif ctx.store_kind == "sqlite":
        async with _open_sqlite(ctx) as store:
            yield store
    elif ctx.store_kind == "postgres":
        async with _open_postgres(ctx) as store:
            yield store
    elif ctx.store_kind == "http":
        async with _open_http(ctx) as store:
            yield store
    else:
        raise ValueError(f"unknown store kind {ctx.store_kind!r}")


@asynccontextmanager
async def _open_memory() -> AsyncIterator[Store]:
    store: Store = MemoryStore()
    await store.open()
    try:
        yield store
    finally:
        await store.close()


@asynccontextmanager
async def _open_sqlite(ctx: BenchContext) -> AsyncIterator[Store]:
    from loomwork.store.sqlite import SQLiteStore

    path = ctx.work_dir / f"bench-{ctx.seed:016x}.db"
    store: Store = SQLiteStore(path)
    await store.open()
    try:
        yield store
    finally:
        await store.close()
        for suffix in ("", "-wal", "-shm"):
            candidate = path.with_name(path.name + suffix)
            candidate.unlink(missing_ok=True)


def _bench_dsn(dsn: str) -> str:
    """``dsn`` with its path replaced by :data:`BENCH_DATABASE`, whatever database it
    originally named -- benchmarks never open the database a caller-supplied DSN
    names, only the dedicated one in the same cluster."""
    parts = urlsplit(dsn)
    return urlunsplit((parts.scheme, parts.netloc, f"/{BENCH_DATABASE}", parts.query, ""))


def _admin_dsn(bench_dsn: str) -> str:
    """``bench_dsn`` pointed at the ``postgres`` maintenance database, used only to
    create :data:`BENCH_DATABASE` if it does not exist yet."""
    parts = urlsplit(bench_dsn)
    return urlunsplit((parts.scheme, parts.netloc, "/postgres", parts.query, ""))


async def _ensure_database(bench_dsn: str) -> None:
    import psycopg

    admin_dsn = _admin_dsn(bench_dsn)
    async with await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True) as conn:
        exists = await conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (BENCH_DATABASE,)
        )
        if await exists.fetchone() is None:
            # Fixed, constant name (BENCH_DATABASE): nothing here is external input.
            await conn.execute(f'CREATE DATABASE "{BENCH_DATABASE}"')


@asynccontextmanager
async def _open_postgres(ctx: BenchContext) -> AsyncIterator[Store]:
    from loomwork.store.postgres import PostgresStore

    dsn = _bench_dsn(ctx.postgres_dsn or DEFAULT_POSTGRES_DSN)
    await _ensure_database(dsn)
    store = PostgresStore(dsn, min_size=1, max_size=64)
    await store.open()
    try:
        async with store.transaction() as tx:
            # Fixed, constant table names: nothing here is assembled from external input.
            await tx.execute(f"TRUNCATE {', '.join(_TABLES)} RESTART IDENTITY")
        yield store
    finally:
        await store.close()


@asynccontextmanager
async def _open_http(_ctx: BenchContext) -> AsyncIterator[Store]:
    """An :class:`HttpStore` over an in-process server (no real socket) backed by a
    fresh :class:`MemoryStore`. Measures the server/serialization edge, not a
    particular backing store -- see ``benchmarks/README.md``."""
    import httpx

    from loomwork.remote.http_store import HttpStore
    from loomwork.server.app import create_app
    from loomwork.server.config import AuthConfig, ServerConfig

    config = ServerConfig(namespaces={"default": "memory://"}, auth=AuthConfig(disabled=True))
    app = create_app(config, run_system_workers=False)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        store: Store = HttpStore(
            "http://bench", None, "default", transport=transport, long_poll=0.0
        )
        await store.open()
        try:
            yield store
        finally:
            await store.close()
