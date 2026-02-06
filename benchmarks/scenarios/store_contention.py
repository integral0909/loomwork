"""Store contention: throughput draining a batch of pre-seeded ticks or step jobs with
N concurrent claimers sharing one store.

This is where ``FOR UPDATE SKIP LOCKED`` earns its keep (see ``docs/internals.md``,
"Stores"): on PostgreSQL and SQLite, concurrent claimers should keep the store busy
without serializing on one lock, so throughput should scale (sublinearly, but
noticeably) as claimers increase. :class:`~loomwork.store.memory.MemoryStore` claims
under one process-wide lock by design, so it is the baseline "no real concurrency"
comparison, not a regression when it does not scale the same way.

Runs are seeded through a client-only (``run_worker=False``) :class:`~loomwork.Loomwork`
app so no worker has touched them yet; draining is then done by bare
:class:`~loomwork.worker.worker.Worker` instances, one slot each, so ``claimers`` is
exactly the number of concurrent ``claim_tick``/``claim_step`` callers.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence

import loomwork as lw
from benchmarks.appctx import DEFAULT_CONFIG
from benchmarks.harness import BenchContext, Case, Scenario, Trial
from benchmarks.stores import open_store
from benchmarks.workloads import STEPS, WORKFLOWS, fanout_workflow, make_payload, noop_workflow
from loomwork.clock import Clock, SystemClock
from loomwork.codec.converter import default_converter
from loomwork.store.base import Store
from loomwork.worker.worker import Worker, WorkerOptions


def _claimers(
    store: Store,
    registry: lw.Registry,
    clock: Clock,
    *,
    count: int,
    id_prefix: str,
    workflow_concurrency: int,
    step_concurrency: int,
) -> list[Worker]:
    return [
        Worker(
            store,
            registry,
            clock=clock,
            converter=default_converter,
            config=DEFAULT_CONFIG,
            options=WorkerOptions(
                workflow_concurrency=workflow_concurrency,
                step_concurrency=step_concurrency,
                run_timers=False,
                run_outbox=False,
                run_schedules=False,
                shutdown_grace=1.0,
            ),
            worker_id=f"{id_prefix}-{i}",
        )
        for i in range(count)
    ]


class StoreContentionScenario(Scenario):
    name = "store_contention"
    description = "Throughput draining pre-seeded ticks/steps with N concurrent claimers"
    metric = "throughput"
    unit = "ops/s"

    def cases(self, *, quick: bool) -> Sequence[Case]:
        trials, warmup = (2, 0) if quick else (4, 1)
        claimers = (1, 4) if quick else (1, 4, 16, 64)
        tick_runs = 20 if quick else 150
        step_width = 20 if quick else 200
        cases = [
            Case(
                f"claim_tick,claimers={c}",
                {"kind": "tick", "claimers": c, "runs": tick_runs},
                trials=trials,
                warmup=warmup,
            )
            for c in claimers
        ]
        cases += [
            Case(
                f"claim_step,claimers={c}",
                {"kind": "step", "claimers": c, "width": step_width},
                trials=trials,
                warmup=warmup,
            )
            for c in claimers
        ]
        return cases

    async def run_trial(self, ctx: BenchContext, case: Case) -> Trial:
        if case.params["kind"] == "tick":
            return await self._claim_tick_trial(ctx, case)
        return await self._claim_step_trial(ctx, case)

    async def _claim_tick_trial(self, ctx: BenchContext, case: Case) -> Trial:
        runs = case.params["runs"]
        n_claimers = case.params["claimers"]
        payload = make_payload(64)
        clock = SystemClock()
        async with open_store(ctx) as store:
            app = lw.Loomwork(store=store, workflows=WORKFLOWS, steps=STEPS, run_worker=False)
            async with app:
                handles = [
                    await app.start_workflow(noop_workflow, payload, id=f"tick-{ctx.seed:x}-{i}")
                    for i in range(runs)
                ]
            claimers = _claimers(
                store,
                app.registry,
                clock,
                count=n_claimers,
                id_prefix="tick-claimer",
                workflow_concurrency=1,
                step_concurrency=0,
            )
            start = time.perf_counter()
            await asyncio.gather(*(w.start() for w in claimers))
            try:
                await asyncio.gather(*(h.result(timeout=60) for h in handles))
            finally:
                await asyncio.gather(*(w.stop(grace=0) for w in claimers))
            elapsed = time.perf_counter() - start
        return Trial(operations=runs, seconds=elapsed)

    async def _claim_step_trial(self, ctx: BenchContext, case: Case) -> Trial:
        width = case.params["width"]
        n_claimers = case.params["claimers"]
        payload = make_payload(64)
        clock = SystemClock()
        async with open_store(ctx) as store:
            app = lw.Loomwork(store=store, workflows=WORKFLOWS, steps=STEPS, run_worker=False)
            async with app:
                handle = await app.start_workflow(
                    fanout_workflow, width, payload, id=f"step-{ctx.seed:x}"
                )
            workers = _claimers(
                store,
                app.registry,
                clock,
                count=1,
                id_prefix="step-ticker",
                workflow_concurrency=1,
                step_concurrency=0,
            ) + _claimers(
                store,
                app.registry,
                clock,
                count=n_claimers,
                id_prefix="step-claimer",
                workflow_concurrency=0,
                step_concurrency=1,
            )
            start = time.perf_counter()
            await asyncio.gather(*(w.start() for w in workers))
            try:
                await handle.result(timeout=60)
            finally:
                await asyncio.gather(*(w.stop(grace=0) for w in workers))
            elapsed = time.perf_counter() - start
        return Trial(operations=width, seconds=elapsed)
