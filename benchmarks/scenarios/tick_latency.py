"""Per-tick latency on a long-running workflow, with and without the worker's replay
cache. Without it, every tick replays the whole history seen so far; with it, only the
events since the last tick are fed to the still-live instance -- see ``docs/internals.md``
("Wake-ups"/"The runtime") and ``benchmarks/README.md`` for how to read the difference.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from benchmarks.appctx import DEFAULT_CONFIG, running_app
from benchmarks.harness import BenchContext, Case, Scenario, Trial
from benchmarks.telemetry import RecordingTelemetry
from benchmarks.workloads import chain_workflow, make_payload


class TickLatencyScenario(Scenario):
    name = "tick_latency"
    description = "Per-tick latency for a long chain, replay cache on vs off"
    metric = "latency"
    unit = "seconds"

    def cases(self, *, quick: bool) -> Sequence[Case]:
        trials, warmup = (2, 0) if quick else (4, 1)
        steps = 40 if quick else 300
        return [
            Case("cache=on", {"steps": steps, "cache_size": 1000}, trials=trials, warmup=warmup),
            Case("cache=off", {"steps": steps, "cache_size": 0}, trials=trials, warmup=warmup),
        ]

    async def run_trial(self, ctx: BenchContext, case: Case) -> Trial:
        steps = case.params["steps"]
        config = replace(DEFAULT_CONFIG, replay_cache_size=case.params["cache_size"])
        telemetry = RecordingTelemetry()
        payload = make_payload(64)
        async with running_app(ctx, config=config, telemetry=telemetry) as app:
            await app.execute(chain_workflow, steps, payload, timeout=180)
        return Trial(samples=list(telemetry.tick_durations))
