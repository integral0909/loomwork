"""End-to-end throughput: completed workflows per second, started and awaited in bulk
against one worker, for a one-step workflow and a step-heavy one.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence

from benchmarks.appctx import running_app
from benchmarks.harness import BenchContext, Case, Scenario, Trial
from benchmarks.workloads import chain_workflow, make_payload, simple_workflow
from loomwork.worker.worker import WorkerOptions

_PAYLOAD_BYTES = 64


class ThroughputScenario(Scenario):
    name = "throughput"
    description = "Workflows completed per second, started and awaited in bulk"
    metric = "throughput"
    unit = "workflows/s"

    def cases(self, *, quick: bool) -> Sequence[Case]:
        trials, warmup = (2, 0) if quick else (5, 1)
        simple_runs = 40 if quick else 400
        heavy_runs = 15 if quick else 100
        return [
            Case(
                "simple",
                {"runs": simple_runs, "steps": 1, "payload_bytes": _PAYLOAD_BYTES},
                trials=trials,
                warmup=warmup,
            ),
            Case(
                "step_heavy",
                {"runs": heavy_runs, "steps": 20, "payload_bytes": _PAYLOAD_BYTES},
                trials=trials,
                warmup=warmup,
            ),
        ]

    async def run_trial(self, ctx: BenchContext, case: Case) -> Trial:
        runs = case.params["runs"]
        steps = case.params["steps"]
        payload = make_payload(case.params["payload_bytes"])
        options = WorkerOptions(workflow_concurrency=8, step_concurrency=32, shutdown_grace=1.0)
        async with running_app(ctx, worker_options=options) as app:
            start = time.perf_counter()
            if case.label == "simple":
                handles = [await app.start_workflow(simple_workflow, payload) for _ in range(runs)]
            else:
                handles = [
                    await app.start_workflow(chain_workflow, steps, payload) for _ in range(runs)
                ]
            await asyncio.gather(*(h.result(timeout=120) for h in handles))
            elapsed = time.perf_counter() - start
        return Trial(operations=runs, seconds=elapsed)
