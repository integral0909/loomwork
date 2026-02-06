"""Continue-as-new chains: throughput through a workflow that hands off to a fresh run
every few steps, keeping its own history bounded (see ``examples/bulk_import`` for the
pattern this measures the cost of).
"""

from __future__ import annotations

import time
from collections.abc import Sequence

from benchmarks.appctx import running_app
from benchmarks.harness import BenchContext, Case, Scenario, Trial
from benchmarks.workloads import continuation_workflow, make_payload

_STEPS_PER_RUN = 5


class ContinuationScenario(Scenario):
    name = "continuation"
    description = "Throughput through a chain of continue-as-new generations"
    metric = "throughput"
    unit = "steps/s"

    def cases(self, *, quick: bool) -> Sequence[Case]:
        trials, warmup = (2, 0) if quick else (5, 1)
        generations = 6 if quick else 40
        return [
            Case(
                f"generations={generations}",
                {"generations": generations, "steps_per_run": _STEPS_PER_RUN},
                trials=trials,
                warmup=warmup,
            )
        ]

    async def run_trial(self, ctx: BenchContext, case: Case) -> Trial:
        generations = case.params["generations"]
        steps_per_run = case.params["steps_per_run"]
        payload = make_payload(64)
        async with running_app(ctx) as app:
            start = time.perf_counter()
            handle = await app.start_workflow(
                continuation_workflow, steps_per_run, generations, payload
            )
            await handle.result(timeout=180)
            elapsed = time.perf_counter() - start
        return Trial(
            operations=generations * steps_per_run,
            seconds=elapsed,
            extra={"generations_per_second": generations / elapsed},
        )
