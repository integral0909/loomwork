"""Helpers for tests that need a PostgreSQL server."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from loomwork.store.postgres import PostgresStore

DSN = os.environ.get("LOOMWORK_TEST_POSTGRES_DSN", "")

requires_postgres = pytest.mark.skipif(
    not DSN, reason="set LOOMWORK_TEST_POSTGRES_DSN to run PostgreSQL tests"
)

TABLES = ("lw_events", "lw_step_jobs", "lw_timers", "lw_outbox", "lw_schedules", "lw_executions")


@asynccontextmanager
async def fresh_postgres_store(**kwargs: object) -> AsyncIterator[PostgresStore]:
    """An open store whose tables have been emptied."""
    store = PostgresStore(DSN, **kwargs)  # type: ignore[arg-type]
    await store.open()
    try:
        async with store.transaction() as tx:
            await tx.execute(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY")
        yield store
    finally:
        await store.close()
