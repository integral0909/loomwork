"""A running :class:`~loomwork.Loomwork` app over a freshly opened store, for
scenarios that exercise the full engine + worker stack rather than a store in
isolation.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

import loomwork as lw
from benchmarks.harness import BenchContext
from benchmarks.stores import open_store
from benchmarks.workloads import STEPS, WORKFLOWS
from loomwork.codec.converter import DataConverter
from loomwork.engine.config import EngineConfig
from loomwork.observability.telemetry import Telemetry
from loomwork.retry import RetryPolicy
from loomwork.worker.worker import WorkerOptions

#: A short poll interval and no useless retry backoff: nothing in the shared workload
#: functions fails, and idle-wait time would otherwise inflate latency measurements on
#: whichever path a store's notifier does not cover.
DEFAULT_CONFIG = EngineConfig(
    poll_interval=0.05,
    default_step_retry=RetryPolicy(initial_interval=0.01, max_attempts=3, jitter=0),
)


@asynccontextmanager
async def running_app(
    ctx: BenchContext,
    *,
    config: EngineConfig = DEFAULT_CONFIG,
    worker_options: WorkerOptions | None = None,
    telemetry: Telemetry | None = None,
    converter: DataConverter | None = None,
    workflows: Sequence[Any] = WORKFLOWS,
    steps: Sequence[Any] = STEPS,
) -> AsyncIterator[lw.Loomwork]:
    async with open_store(ctx) as store:
        app = lw.Loomwork(
            store=store,
            workflows=workflows,
            steps=steps,
            config=config,
            worker_options=worker_options or WorkerOptions(shutdown_grace=1.0),
            telemetry=telemetry,
            converter=converter,
        )
        async with app:
            yield app
