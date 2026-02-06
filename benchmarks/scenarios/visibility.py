"""Visibility query performance vs the number of runs already in the store -- the cost
:meth:`~loomwork.client.client.Client.query_runs` pays as a deployment accumulates
history (see ``docs/visibility-queries.md``).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence

from benchmarks.appctx import running_app
from benchmarks.harness import BenchContext, Case, Scenario, Trial
from benchmarks.workloads import make_payload, noop_workflow
from loomwork.worker.worker import WorkerOptions

_QUERIES_PER_TRIAL = 10


class VisibilityScenario(Scenario):
    name = "visibility"
    description = "query_runs latency vs number of completed runs in the store"
    metric = "latency"
    unit = "seconds"

    def cases(self, *, quick: bool) -> Sequence[Case]:
        trials, warmup = (2, 0) if quick else (4, 1)
        counts = (50, 300) if quick else (100, 1000, 5000)
        return [Case(f"runs={n}", {"runs": n}, trials=trials, warmup=warmup) for n in counts]

    async def run_trial(self, ctx: BenchContext, case: Case) -> Trial:
        n = case.params["runs"]
        payload = make_payload(32)
        options = WorkerOptions(workflow_concurrency=16, shutdown_grace=1.0)
        async with running_app(ctx, worker_options=options) as app:
            handles = [await app.start_workflow(noop_workflow, payload) for _ in range(n)]
            await asyncio.gather(*(h.result(timeout=180) for h in handles))
            samples = []
            for _ in range(_QUERIES_PER_TRIAL):
                start = time.perf_counter()
                await app.client.query_runs("status = 'completed'", limit=100)
                samples.append(time.perf_counter() - start)
        return Trial(samples=samples, extra={"runs": float(n)})
