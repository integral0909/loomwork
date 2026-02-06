"""Signal and update round-trip latency: wall time from a client call to its effect
being visible.

``update`` is the precise measurement: :meth:`~loomwork.client.handle.WorkflowHandle.
update` already waits on the engine's own resolution of that specific update, with no
polling on our side. ``signal`` has no such return path -- delivery is fire-and-forget --
so it is measured by polling a query the workflow updates after each signal; the poll
interval below is deliberately much finer than the queue delays being measured, but it
is still an upper bound on precision, not an exact reading (see ``benchmarks/README.md``).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence

from benchmarks.appctx import running_app
from benchmarks.harness import BenchContext, Case, Scenario, Trial
from benchmarks.workloads import ping_channel, ping_pong_workflow, signal_sink_workflow
from loomwork.client.handle import WorkflowHandle
from loomwork.errors import NotFoundError

_POLL_SECONDS = 0.001


async def _current_count(handle: WorkflowHandle[int]) -> int:
    """The workflow's ``count`` query, or -1 before its first tick has run (so the
    query handler even exists to answer)."""
    try:
        return int(await handle.query("count", result_type=int))
    except NotFoundError:
        return -1


class MessagingScenario(Scenario):
    name = "messaging"
    description = "Round-trip latency of updates (exact) and signals (poll-observed)"
    metric = "latency"
    unit = "seconds"

    def cases(self, *, quick: bool) -> Sequence[Case]:
        trials, warmup = (2, 0) if quick else (4, 1)
        rounds = 15 if quick else 50
        return [
            Case("update", {"rounds": rounds}, trials=trials, warmup=warmup),
            Case("signal", {"rounds": rounds}, trials=trials, warmup=warmup),
        ]

    async def run_trial(self, ctx: BenchContext, case: Case) -> Trial:
        rounds = case.params["rounds"]
        if case.label == "update":
            return await self._update_trial(ctx, rounds)
        return await self._signal_trial(ctx, rounds)

    async def _update_trial(self, ctx: BenchContext, rounds: int) -> Trial:
        async with running_app(ctx) as app:
            handle = await app.start_workflow(ping_pong_workflow, rounds)
            samples = []
            for i in range(rounds):
                start = time.perf_counter()
                await handle.update("ping", i, timeout=30)
                samples.append(time.perf_counter() - start)
            await handle.result(timeout=30)
        return Trial(samples=samples)

    async def _signal_trial(self, ctx: BenchContext, rounds: int) -> Trial:
        async with running_app(ctx) as app:
            handle = await app.start_workflow(signal_sink_workflow, rounds)
            samples = []
            for i in range(rounds):
                start = time.perf_counter()
                await handle.signal(ping_channel, i)
                # Polling a remote query is the measurement itself here, not something
                # an in-process Event could replace: the count only changes on the
                # server's side of the round trip we are timing.
                while await _current_count(handle) <= i:  # noqa: ASYNC110
                    await asyncio.sleep(_POLL_SECONDS)
                samples.append(time.perf_counter() - start)
            await handle.result(timeout=30)
        return Trial(samples=samples)
