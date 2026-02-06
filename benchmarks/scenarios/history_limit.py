"""History size limit effects: per-tick cost as a run's history grows toward the
configured limit, and the cost of the tick that finally exceeds it.

That last tick is checked "before a tick runs" (``docs/internals.md``): it fails the run
with :class:`~loomwork.HistoryLimitExceeded` from the record alone, with no replay. Its
duration, reported as ``extra.last_tick_seconds``, should stay flat regardless of history
length while the pooled sample's ``p95``/``p99`` (ordinary ticks against a growing
history) grow with it.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

import loomwork as lw
from benchmarks.appctx import DEFAULT_CONFIG, running_app
from benchmarks.harness import BenchContext, Case, Scenario, Trial
from benchmarks.telemetry import RecordingTelemetry
from benchmarks.workloads import chain_workflow, make_payload


class HistoryLimitScenario(Scenario):
    name = "history_limit"
    description = "Tick cost near the configured history limit, and the failing tick's own cost"
    metric = "latency"
    unit = "seconds"

    def cases(self, *, quick: bool) -> Sequence[Case]:
        trials, warmup = (2, 0) if quick else (4, 1)
        limit_events = 30 if quick else 150
        return [
            Case(
                f"limit_events={limit_events}",
                {"limit_events": limit_events, "steps": limit_events},
                trials=trials,
                warmup=warmup,
            )
        ]

    async def run_trial(self, ctx: BenchContext, case: Case) -> Trial:
        limit_events = case.params["limit_events"]
        steps = case.params["steps"]
        config = replace(
            DEFAULT_CONFIG,
            history_warning_events=max(limit_events // 2, 1),
            history_limit_events=limit_events,
        )
        telemetry = RecordingTelemetry()
        payload = make_payload(64)
        hit_limit = 0.0
        async with running_app(ctx, config=config, telemetry=telemetry) as app:
            handle = await app.start_workflow(chain_workflow, steps, payload)
            try:
                await handle.result(timeout=180)
            except lw.WorkflowFailedError:
                hit_limit = 1.0
        samples = telemetry.tick_durations
        return Trial(
            samples=samples,
            extra={
                "last_tick_seconds": samples[-1] if samples else 0.0,
                "history_limit_hit": hit_limit,
            },
        )
