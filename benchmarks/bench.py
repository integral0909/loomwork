"""Loomwork benchmarks.

Run with::

    uv run python -m benchmarks.bench                 # all benchmarks, memory store
    uv run python -m benchmarks.bench --store sqlite  # durable single node
    uv run python -m benchmarks.bench --store postgres --dsn postgresql://...
    uv run python -m benchmarks.bench --only replay --json results.json

Each benchmark prints one line per configuration and can emit JSON for tracking results
over time. Numbers depend heavily on hardware and the store; compare runs on the same
machine only.

Benchmarks
    throughput   workflows per second for a short workflow (two steps)
    fanout       step attempts per second with concurrency 1..64
    replay       cost of replaying a history, by history length (cold cache)
    tick         tick latency with and without the replay cache
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import tempfile
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

import loomwork as lw
from loomwork.codec import default_converter
from loomwork.engine.config import EngineConfig
from loomwork.retry import RetryPolicy
from loomwork.runtime import WorkflowInstance, replay_history
from loomwork.store.base import Store
from loomwork.worker import WorkerOptions

CONFIG = EngineConfig(
    poll_interval=0.05,
    default_step_retry=RetryPolicy(initial_interval=0.01, max_attempts=3, jitter=0),
)


# --------------------------------------------------------------------------- workloads


@lw.step(name="bench.echo")
async def echo(value: int) -> int:
    return value


@lw.workflow(name="bench.pair")
async def pair(value: int) -> int:
    first = await echo(value)
    return await echo(first + 1)


@lw.workflow(name="bench.fanout")
async def fanout(width: int) -> int:
    results = await asyncio.gather(*(echo(i) for i in range(width)))
    return sum(results)


@lw.workflow(name="bench.chain")
async def chain(length: int) -> int:
    total = 0
    for i in range(length):
        total += await echo(i)
    return total


WORKFLOWS: list[lw.WorkflowDefinition[..., Any]] = [pair, fanout, chain]
STEPS: list[lw.StepDefinition[..., Any]] = [echo]


# --------------------------------------------------------------------------- harness


@dataclass(slots=True)
class Result:
    benchmark: str
    parameters: dict[str, Any]
    seconds: float
    operations: int
    extra: dict[str, float] = field(default_factory=dict)

    @property
    def per_second(self) -> float:
        return self.operations / self.seconds if self.seconds else float("inf")

    def line(self) -> str:
        params = ", ".join(f"{k}={v}" for k, v in self.parameters.items())
        extra = "".join(f"  {k}={v:.3f}" for k, v in self.extra.items())
        return (
            f"{self.benchmark:<11} {params:<32} {self.operations:>7} ops "
            f"in {self.seconds:7.3f}s  -> {self.per_second:10.1f}/s{extra}"
        )


StoreFactory = Callable[[], Awaitable[Store]]


def store_factory(kind: str, dsn: str | None) -> StoreFactory:
    async def make() -> Store:
        if kind == "memory":
            return lw.MemoryStore()
        if kind == "sqlite":
            from loomwork.store.sqlite import SQLiteStore

            directory = Path(tempfile.mkdtemp(prefix="loomwork-bench-"))
            store: Store = SQLiteStore(directory / "bench.db")
        elif kind == "postgres":
            if not dsn:
                raise SystemExit("--dsn is required for the postgres store")
            from loomwork.store.postgres import PostgresStore

            # Enough connections for the worker's slots, its service loops and the
            # benchmark's concurrent result waiters.
            store = PostgresStore(dsn, min_size=4, max_size=60)
        else:
            raise SystemExit(f"unknown store {kind!r}")
        await store.open()
        if kind == "postgres":
            async with store.transaction() as tx:  # type: ignore[attr-defined]
                await tx.execute(
                    "TRUNCATE lw_events, lw_step_jobs, lw_timers, lw_outbox, lw_schedules, "
                    "lw_search_attributes, lw_executions RESTART IDENTITY"
                )
        return store

    return make


@asynccontextmanager
async def running_app(
    make_store: StoreFactory, *, config: EngineConfig = CONFIG, steps: int = 32, ticks: int = 8
) -> AsyncIterator[lw.Loomwork]:
    store = await make_store()
    app = lw.Loomwork(
        store=store,
        workflows=WORKFLOWS,
        steps=STEPS,
        config=config,
        worker_options=WorkerOptions(
            workflow_concurrency=ticks, step_concurrency=steps, shutdown_grace=1.0
        ),
    )
    try:
        async with app:
            yield app
    finally:
        await store.close()


# --------------------------------------------------------------------------- benchmarks


async def bench_throughput(make_store: StoreFactory, count: int) -> list[Result]:
    async with running_app(make_store) as app:
        start = time.perf_counter()
        handles = [await app.start_workflow(pair, i) for i in range(count)]
        await asyncio.gather(*(h.result(timeout=120) for h in handles))
        elapsed = time.perf_counter() - start
    return [Result("throughput", {"workflows": count}, elapsed, count)]


async def bench_fanout(make_store: StoreFactory, width: int) -> list[Result]:
    results = []
    for concurrency in (1, 4, 16, 64):
        async with running_app(make_store, steps=concurrency) as app:
            start = time.perf_counter()
            await app.execute(fanout, width, timeout=300)
            elapsed = time.perf_counter() - start
        results.append(
            Result("fanout", {"steps": width, "concurrency": concurrency}, elapsed, width)
        )
    return results


async def bench_replay(make_store: StoreFactory) -> list[Result]:
    results = []
    for length in (10, 100, 500, 1000):
        async with running_app(make_store) as app:
            handle = await app.start_workflow(chain, length)
            await handle.result(timeout=300)
            history = await handle.history()

        def factory(started: Any) -> WorkflowInstance:
            return WorkflowInstance(chain, started, converter=default_converter)

        timings = []
        for _ in range(5):
            start = time.perf_counter()
            replayer = replay_history(history, factory)
            timings.append(time.perf_counter() - start)
            replayer.close()
        median = statistics.median(timings)
        results.append(
            Result(
                "replay",
                {"steps": length, "events": len(history)},
                median,
                len(history),
                {"ms_per_replay": median * 1000},
            )
        )
    return results


async def bench_tick(make_store: StoreFactory, length: int) -> list[Result]:
    results = []
    for cache_size in (1000, 0):
        config = replace(CONFIG, replay_cache_size=cache_size)
        async with running_app(make_store, config=config) as app:
            start = time.perf_counter()
            await app.execute(chain, length, timeout=300)
            elapsed = time.perf_counter() - start
            ticks = app.worker.ticks.ticks_completed
        results.append(
            Result(
                "tick",
                {"steps": length, "cache": "on" if cache_size else "off"},
                elapsed,
                ticks,
                {"ms_per_tick": elapsed / max(ticks, 1) * 1000},
            )
        )
    return results


Benchmark = Callable[[StoreFactory, argparse.Namespace], Awaitable[list[Result]]]

BENCHMARKS: dict[str, Benchmark] = {
    "throughput": lambda f, a: bench_throughput(f, a.workflows),
    "fanout": lambda f, a: bench_fanout(f, a.width),
    "replay": lambda f, _args: bench_replay(f),
    "tick": lambda f, a: bench_tick(f, a.chain),
}


async def main_async(args: argparse.Namespace) -> list[Result]:
    make_store = store_factory(args.store, args.dsn)
    selected = args.only or list(BENCHMARKS)
    results: list[Result] = []
    for name in selected:
        for result in await BENCHMARKS[name](make_store, args):
            print(result.line(), flush=True)
            results.append(result)
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--store", choices=["memory", "sqlite", "postgres"], default="memory")
    parser.add_argument("--dsn", help="PostgreSQL DSN for --store postgres")
    parser.add_argument("--only", action="append", choices=sorted(BENCHMARKS))
    parser.add_argument("--workflows", type=int, default=500, help="runs for 'throughput'")
    parser.add_argument("--width", type=int, default=500, help="steps for 'fanout'")
    parser.add_argument("--chain", type=int, default=200, help="steps for 'tick'")
    parser.add_argument("--json", type=Path, help="write results to this file")
    args = parser.parse_args(argv)
    print(f"loomwork {lw.__version__} benchmarks on the {args.store} store", flush=True)
    results = asyncio.run(main_async(args))
    if args.json:
        payload = [{**asdict(r), "per_second": r.per_second, "store": args.store} for r in results]
        args.json.write_text(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
