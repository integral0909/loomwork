from __future__ import annotations

import pytest

from loomwork.store import MemoryStore
from tests.store_conformance import StoreConformance


class TestMemoryStore(StoreConformance):
    @pytest.fixture
    def store(self) -> MemoryStore:
        return MemoryStore()

    async def test_counters(self, store: MemoryStore) -> None:
        await self.create(store)
        assert (store.run_count, store.job_count, store.timer_count) == (1, 0, 0)
