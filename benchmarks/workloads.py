"""Shared workflow and step definitions used across scenarios.

Kept in one place so every scenario measures the same handful of workloads rather than
each defining subtly different ones -- a difference between two scenarios should come
from what they measure, not from an incidental difference in the workflow code.
"""

from __future__ import annotations

import asyncio
import random
from typing import Any

import loomwork as lw

#: Repeats to a compressible payload of any requested size (text-like, as most real
#: workflow arguments are); see ``compressible=False`` in :func:`make_payload` for the
#: incompressible case codec benchmarks compare it against.
_TEXT_UNIT = b"loomwork-benchmark-payload-0123456789 "


def make_payload(
    size: int, *, compressible: bool = True, rng: random.Random | None = None
) -> bytes:
    """``size`` bytes of workload data: repeating text if ``compressible``, otherwise
    uniformly random bytes (representative of already-compressed media)."""
    if size <= 0:
        return b""
    if compressible:
        repeats = size // len(_TEXT_UNIT) + 1
        return (_TEXT_UNIT * repeats)[:size]
    source = rng or random.Random(size)
    return bytes(source.getrandbits(8) for _ in range(size))


@lw.step(name="bench.echo")
async def echo_step(payload: bytes) -> bytes:
    return payload


@lw.workflow(name="bench.noop")
async def noop_workflow(payload: bytes) -> int:
    """Closes in its first tick without scheduling any step -- isolates tick-claim
    contention and visibility-query benchmarks from step dispatch entirely."""
    return len(payload)


@lw.workflow(name="bench.simple")
async def simple_workflow(payload: bytes) -> int:
    """One step: the smallest possible unit of throughput."""
    return len(await echo_step(payload))


@lw.workflow(name="bench.chain")
async def chain_workflow(steps: int, payload: bytes) -> int:
    """``steps`` sequential steps, each awaited before the next is scheduled -- history
    grows by roughly three events per step (schedule, completion, tick)."""
    total = 0
    for _ in range(steps):
        total += len(await echo_step(payload))
    return total


@lw.workflow(name="bench.fanout")
async def fanout_workflow(width: int, payload: bytes) -> int:
    """``width`` steps scheduled in the same tick and awaited together."""
    results = await asyncio.gather(*(echo_step(payload) for _ in range(width)))
    return sum(len(r) for r in results)


@lw.workflow(name="bench.sleepy")
async def sleepy_workflow(delay_seconds: float) -> None:
    """A single durable timer: measures the worker's own scheduling delay, not step or
    store cost."""
    await lw.sleep(delay_seconds)


ping_channel: lw.Channel[int] = lw.Channel("bench.ping", int)


@lw.workflow(name="bench.signal_sink")
async def signal_sink_workflow(rounds: int) -> int:
    """Counts signals received on :data:`ping_channel`, exposing the count as a query
    so a caller can observe delivery without waiting for the run to close."""
    count = 0

    @lw.query("count")
    def current_count() -> int:
        return count

    while count < rounds:
        await ping_channel.receive()
        count += 1
    return count


@lw.workflow(name="bench.ping_pong")
async def ping_pong_workflow(rounds: int) -> int:
    """Answers ``rounds`` updates by echoing their argument back -- a clean round trip
    with no polling on the caller's side, since :meth:`~loomwork.client.handle.
    WorkflowHandle.update` already waits on the engine's own resolution."""
    count = 0
    done = asyncio.Event()

    @lw.update("ping")
    async def ping(value: int) -> int:
        nonlocal count
        count += 1
        if count >= rounds:
            done.set()
        return value

    await done.wait()
    return count


@lw.workflow(name="bench.continuation")
async def continuation_workflow(steps_per_run: int, remaining_runs: int, payload: bytes) -> int:
    """``steps_per_run`` steps, then either hands off to a fresh run via
    ``continue_as_new`` or returns -- a chain of ``remaining_runs`` generations."""
    total = 0
    for _ in range(steps_per_run):
        total += len(await echo_step(payload))
    if remaining_runs > 1:
        lw.continue_as_new(steps_per_run, remaining_runs - 1, payload)
    return total


#: Registered together with every scenario's :class:`~loomwork.Loomwork` app so any of
#: these workflows can run regardless of which scenario is asking.
WORKFLOWS: list[lw.WorkflowDefinition[..., Any]] = [
    noop_workflow,
    simple_workflow,
    chain_workflow,
    fanout_workflow,
    sleepy_workflow,
    signal_sink_workflow,
    ping_pong_workflow,
    continuation_workflow,
]
STEPS: list[lw.StepDefinition[..., Any]] = [echo_step]
