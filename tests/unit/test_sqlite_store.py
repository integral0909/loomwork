from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from loomwork.store.sqlite import SQLiteStore
from tests.store_conformance import StoreConformance


class TestSQLiteStore(StoreConformance):
    @pytest.fixture
    async def store(self, tmp_path: Path) -> AsyncIterator[SQLiteStore]:
        store = SQLiteStore(tmp_path / "loomwork.db")
        await store.open()
        yield store
        await store.close()
