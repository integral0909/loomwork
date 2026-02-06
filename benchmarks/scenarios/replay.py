"""Cold replay cost: the time :func:`~loomwork.runtime.replay_history` takes to rebuild
a workflow instance from scratch, as a function of history length. This is what every
tick pays without the replay cache (see ``tick_latency``) and what
``loomwork.continue_as_new()`` keeps bounded for long-lived workflows.
"""

from __future__ import annotations

import time
from collections.abc import Sequence

from benchmarks.appctx import running_app
from benchmarks.harness import BenchContext, Case, Scenario, Trial
from benchmarks.workloads import chain_workflow, make_payload
from loomwork.codec.converter import default_converter
from loomwork.history.events import WorkflowStarted
from loomwork.runtime import WorkflowInstance, replay_history


class ReplayScenario(Scenario):
    name = "replay"
    description = "Cold replay cost (no cache) as a function of history length"
    metric = "latency"
    unit = "seconds"

    def cases(self, *, quick: bool) -> Sequence[Case]:
        trials, warmup = (2, 0) if quick else (5, 1)
        lengths = (10, 50) if quick else (10, 100, 500, 1000)
        return [Case(f"steps={n}", {"steps": n}, trials=trials, warmup=warmup) for n in lengths]

    async def run_trial(self, ctx: BenchContext, case: Case) -> Trial:
        steps = case.params["steps"]
        payload = make_payload(64)
        async with running_app(ctx) as app:
            handle = await app.start_workflow(chain_workflow, steps, payload)
            await handle.result(timeout=180)
            history = await handle.history()

        def factory(started: WorkflowStarted) -> WorkflowInstance:
            return WorkflowInstance(chain_workflow, started, converter=default_converter)

        start = time.perf_counter()
        replayer = replay_history(history, factory)
        elapsed = time.perf_counter() - start
        replayer.close()
        return Trial(samples=[elapsed], extra={"events": float(len(history))})
