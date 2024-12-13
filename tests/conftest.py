from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest

from loomwork import Loomwork, ManualClock, MemoryStore
from loomwork.sdk.definitions import StepDefinition, WorkflowDefinition
from loomwork.store.base import Store
from loomwork.worker import WorkerOptions
from tests.helpers import FAST_CONFIG, T0


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(T0)


@pytest.fixture
def memory_store() -> MemoryStore:
    return MemoryStore()


AppFactory = Callable[..., Awaitable[Loomwork]]


@pytest.fixture
async def make_app(clock: ManualClock) -> AsyncIterator[AppFactory]:
    """Build started ``Loomwork`` apps that are stopped at the end of the test."""
    apps: list[Loomwork] = []

    async def factory(
        workflows: list[WorkflowDefinition[..., Any]] | None = None,
        steps: list[StepDefinition[..., Any]] | None = None,
        *,
        store: Store | None = None,
        use_clock: bool = True,
        **kwargs: Any,
    ) -> Loomwork:
        kwargs.setdefault("config", FAST_CONFIG)
        kwargs.setdefault("worker_options", WorkerOptions(shutdown_grace=1.0))
        app = Loomwork(
            store=store,
            workflows=workflows or [],
            steps=steps or [],
            clock=clock if use_clock else None,
            **kwargs,
        )
        await app.start()
        apps.append(app)
        return app

    yield factory
    for app in apps:
        await app.stop(grace=1.0)
