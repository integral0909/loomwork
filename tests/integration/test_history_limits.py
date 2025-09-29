"""EngineConfig.history_warning_events/_bytes and history_limit_events/_bytes: the
telemetry hook and the clean, deterministic failure once a run's history grows past the
configured limit."""

from __future__ import annotations

from dataclasses import replace

import pytest

import loomwork as lw
from loomwork import ExecutionStatus, ManualClock, MemoryStore
from loomwork.observability.telemetry import NoopTelemetry
from tests.helpers import FAST_CONFIG

pytestmark = pytest.mark.integration


@lw.step(name="hl.tick")
async def tick_step(i: int) -> int:
    return i


@lw.workflow(name="hl.grower")
async def grower(rounds: int) -> int:
    total = 0
    for i in range(rounds):
        total += await tick_step(i)
    return total


class RecordingTelemetry(NoopTelemetry):
    def __init__(self) -> None:
        self.warnings: list[tuple[str, str, int, str]] = []

    def history_size_warning(self, run_id: str, workflow_name: str, size: int, unit: str) -> None:
        self.warnings.append((run_id, workflow_name, size, unit))


async def test_history_warning_fires_once_when_crossing_the_threshold(
    clock: ManualClock,
) -> None:
    telemetry = RecordingTelemetry()
    store = MemoryStore()
    config = replace(FAST_CONFIG, history_warning_events=10, history_limit_events=1000)
    app = lw.Loomwork(
        store=store,
        workflows=[grower],
        steps=[tick_step],
        clock=clock,
        config=config,
        telemetry=telemetry,
    )
    async with app:
        handle = await app.start_workflow(grower, 5, id="grower-1")
        assert await handle.result(timeout=5) == sum(range(5))

    assert len(telemetry.warnings) == 1
    run_id, name, size, unit = telemetry.warnings[0]
    assert run_id == handle.run_id
    assert name == "hl.grower"
    assert size >= 10
    assert unit == "events"


async def test_history_limit_fails_the_run_cleanly(clock: ManualClock) -> None:
    store = MemoryStore()
    config = replace(FAST_CONFIG, history_warning_events=5, history_limit_events=8)
    app = lw.Loomwork(
        store=store, workflows=[grower], steps=[tick_step], clock=clock, config=config
    )
    async with app:
        handle = await app.start_workflow(grower, 20, id="grower-2")
        with pytest.raises(lw.WorkflowFailedError) as exc_info:
            await handle.result(timeout=5)
        assert exc_info.value.failure.type == "HistoryLimitExceeded"
        assert "continue_as_new" in exc_info.value.failure.message

    assert handle.run_id is not None
    record = await store.get_execution(handle.run_id)
    assert record.status is ExecutionStatus.FAILED
    assert record.last_event_id >= 8
    history = await store.load_history(handle.run_id)
    # The tick that discovered the limit committed cleanly: a TickCompleted plus the
    # WorkflowFailed decision it produced, nothing half-written.
    assert history[-1].kind == "workflow_failed"
    assert history[-2].kind == "tick_completed"


async def test_history_limit_is_deterministic_across_workers(clock: ManualClock) -> None:
    """The same run, claimed by two different worker ids in sequence (as if the first
    crashed), fails with the identical outcome either way: the check depends only on
    already-committed store state, not on which worker or how many times it retries."""
    store = MemoryStore()
    config = replace(FAST_CONFIG, history_warning_events=5, history_limit_events=6)
    app = lw.Loomwork(
        store=store, workflows=[grower], steps=[tick_step], clock=clock, config=config
    )
    async with app:
        handle = await app.start_workflow(grower, 20, id="grower-3")
        with pytest.raises(lw.WorkflowFailedError):
            await handle.result(timeout=5)
    assert handle.run_id is not None
    first_failure = (await store.get_execution(handle.run_id)).failure
    assert first_failure is not None and first_failure.type == "HistoryLimitExceeded"


async def test_history_limit_bytes(clock: ManualClock) -> None:
    store = MemoryStore()
    config = replace(
        FAST_CONFIG,
        history_warning_events=100_000,
        history_limit_events=100_000,
        history_warning_bytes=200,
        history_limit_bytes=2_000,
    )
    app = lw.Loomwork(
        store=store, workflows=[grower], steps=[tick_step], clock=clock, config=config
    )
    async with app:
        handle = await app.start_workflow(grower, 50, id="grower-bytes")
        with pytest.raises(lw.WorkflowFailedError) as exc_info:
            await handle.result(timeout=5)
        assert exc_info.value.failure.type == "HistoryLimitExceeded"
    assert handle.run_id is not None
    record = await store.get_execution(handle.run_id)
    assert record.history_bytes >= 2_000
