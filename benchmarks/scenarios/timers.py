"""Timer firing accuracy: how much later than its deadline a durable ``lw.sleep`` is
actually observed to resolve, alone and under load from many concurrent timers sharing
one worker's timer service loop.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence

from benchmarks.appctx import running_app
from benchmarks.harness import BenchContext, Case, Scenario, Trial
from benchmarks.workloads import sleepy_workflow
from loomwork.client.handle import WorkflowHandle
from loomwork.worker.worker import WorkerOptions


class TimerScenario(Scenario):
    name = "timer_accuracy"
    description = "Delay between a durable timer's deadline and its observed firing"
    metric = "latency"
    unit = "seconds"

    def cases(self, *, quick: bool) -> Sequence[Case]:
        if quick:
            return [
                Case("alone", {"count": 1, "delay": 0.1}, trials=3, warmup=0),
                Case("load=20", {"count": 20, "delay": 0.1}, trials=2, warmup=0),
            ]
        return [
            Case("alone", {"count": 1, "delay": 1.0}, trials=5, warmup=1),
            Case("load=50", {"count": 50, "delay": 1.0}, trials=3, warmup=1),
        ]

    async def run_trial(self, ctx: BenchContext, case: Case) -> Trial:
        count = case.params["count"]
        delay = case.params["delay"]
        options = WorkerOptions(workflow_concurrency=max(count, 4), shutdown_grace=1.0)
        async with running_app(ctx, worker_options=options) as app:
            start = time.perf_counter()
            handles = [await app.start_workflow(sleepy_workflow, delay) for _ in range(count)]

            async def elapsed_for(handle: WorkflowHandle[None]) -> float:
                await handle.result(timeout=60)
                return time.perf_counter() - start

            elapsed_times = await asyncio.gather(*(elapsed_for(h) for h in handles))
        return Trial(samples=[max(e - delay, 0.0) for e in elapsed_times])
