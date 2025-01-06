from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from loomwork.store.notify import TOPIC_TICKS
from loomwork.store.postgres import PostgresStore
from tests.helpers import T0
from tests.postgres_support import fresh_postgres_store, requires_postgres
from tests.store_conformance import StoreConformance, record, started

pytestmark = [pytest.mark.postgres, requires_postgres]


class TestPostgresStore(StoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[PostgresStore]:
        async with fresh_postgres_store() as store:
            yield store

    async def test_schema_version(self, store: PostgresStore) -> None:
        assert await store.schema_version() == 1

    async def test_notifications_cross_store_instances(self, store: PostgresStore) -> None:
        """A second store instance (another process in production) is woken via LISTEN."""
        async with fresh_postgres_store() as other:
            await other.wait_listening()
            token = other.notifier.token(TOPIC_TICKS)
            rec = record("remote-1")
            await store.create_execution(rec, started(rec), T0)
            assert await other.notifier.wait(TOPIC_TICKS, token, 5)

    async def test_concurrent_claims_never_double_lease(self, store: PostgresStore) -> None:
        for i in range(20):
            rec = record(f"r{i}")
            await store.create_execution(rec, started(rec), T0)
        from datetime import timedelta

        claims = await asyncio.gather(
            *(store.claim_tick(f"w{i}", T0, timedelta(seconds=30)) for i in range(30))
        )
        claimed = [c.run_id for c in claims if c is not None]
        assert len(claimed) == 20
        assert len(set(claimed)) == 20

    async def test_concurrent_starts_with_same_workflow_id(self, store: PostgresStore) -> None:
        from loomwork.errors import WorkflowAlreadyStartedError

        async def start(i: int) -> str | None:
            rec = record(f"dup-{i}", workflow_id="same")
            try:
                created, _ = await store.create_execution(rec, started(rec), T0)
            except WorkflowAlreadyStartedError:
                return None
            return created.run_id

        results = await asyncio.gather(*(start(i) for i in range(8)))
        assert len([r for r in results if r is not None]) == 1
