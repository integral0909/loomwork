"""Step dispatch latency: the delay between a step job becoming ready and a worker
starting its attempt (``AttemptInfo.queue_delay``), under a fan-out that makes many
jobs ready in the same instant so the step dispatcher's queue actually builds up.
"""

from __future__ import annotations

from collections.abc import Sequence

from benchmarks.appctx import running_app
from benchmarks.harness import BenchContext, Case, Scenario, Trial
from benchmarks.telemetry import RecordingTelemetry
from benchmarks.workloads import fanout_workflow, make_payload
from loomwork.worker.worker import WorkerOptions


class DispatchLatencyScenario(Scenario):
    name = "dispatch_latency"
    description = "Step dispatch latency (ready to attempt start) under fan-out"
    metric = "latency"
    unit = "seconds"

    def cases(self, *, quick: bool) -> Sequence[Case]:
        trials, warmup = (2, 0) if quick else (5, 1)
        widths = (20,) if quick else (20, 200)
        concurrencies = (4, 16) if quick else (4, 16, 64)
        return [
            Case(
                f"width={width},concurrency={concurrency}",
                {"width": width, "concurrency": concurrency},
                trials=trials,
                warmup=warmup,
            )
            for width in widths
            for concurrency in concurrencies
        ]

    async def run_trial(self, ctx: BenchContext, case: Case) -> Trial:
        width = case.params["width"]
        concurrency = case.params["concurrency"]
        payload = make_payload(64)
        telemetry = RecordingTelemetry()
        options = WorkerOptions(
            workflow_concurrency=4, step_concurrency=concurrency, shutdown_grace=1.0
        )
        async with running_app(ctx, worker_options=options, telemetry=telemetry) as app:
            await app.execute(fanout_workflow, width, payload, timeout=120)
        return Trial(samples=list(telemetry.queue_delays))
