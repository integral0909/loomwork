"""Tests for ``examples.versioned_workflow``."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from examples.support import advance_until
from examples.versioned_workflow.steps import send_email, send_sms
from examples.versioned_workflow.workflows import (
    onboard_v1,
    onboard_v2,
    onboard_v2_unsafe,
    onboard_v3,
)
from loomwork.history import history_to_json
from loomwork.sdk.definitions import Registry
from loomwork.versioning import check_compatibility, check_history, check_history_file

if TYPE_CHECKING:
    from loomwork import Loomwork, ManualClock
    from tests.conftest import AppFactory

STEP_DEFS = [send_email, send_sms]


async def _wait_until_sleeping(clock: ManualClock, app: Loomwork, expected_timers: int) -> None:
    store = app.store

    def sleeping() -> bool:
        return bool(store.timer_count == expected_timers)  # type: ignore[attr-defined]

    await advance_until(clock, sleeping, step=timedelta(0))


async def test_safe_v2_is_compatible_with_a_v1_history_in_flight(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app([onboard_v1], STEP_DEFS)
    await app.start_workflow(onboard_v1, "user-1", id="onboard-user-1")
    await _wait_until_sleeping(clock, app, 1)

    report = await check_compatibility(app.store, Registry([onboard_v2]))
    assert report.ok
    assert report.summary() == {"versioned_workflow.onboard": (1, 0)}


async def test_unsafe_v2_diverges_from_the_recorded_email_decision(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app([onboard_v1], STEP_DEFS)
    await app.start_workflow(onboard_v1, "user-1", id="onboard-user-1")
    await _wait_until_sleeping(clock, app, 1)

    report = await check_compatibility(app.store, Registry([onboard_v2_unsafe]))
    assert not report.ok
    (failure,) = report.failures
    assert failure.nondeterministic
    assert failure.diverged_at_event is not None
    assert "send_email" in (failure.error or "")
    assert "send_sms" in (failure.error or "")


async def test_deploying_the_guarded_v2_preserves_old_decisions_and_makes_new_ones(
    make_app: AppFactory, clock: ManualClock
) -> None:
    v1_app = await make_app([onboard_v1], STEP_DEFS)
    await v1_app.start_workflow(onboard_v1, "user-1", id="onboard-user-1")
    await _wait_until_sleeping(clock, v1_app, 1)
    await v1_app.stop()

    v2_app = await make_app([onboard_v2], STEP_DEFS, store=v1_app.store)
    old_handle = v2_app.get_handle(workflow_id="onboard-user-1", result_type=str)
    new_handle = await v2_app.start_workflow(onboard_v2, "user-2", id="onboard-user-2")
    await _wait_until_sleeping(clock, v2_app, 2)
    await clock.advance(timedelta(hours=1, seconds=1))

    assert await old_handle.result(timeout=5) == "email:user-1"
    assert await new_handle.result(timeout=5) == "sms:user-2"

    final_report = await check_compatibility(v1_app.store, Registry([onboard_v2]))
    assert final_report.summary() == {"versioned_workflow.onboard": (2, 0)}


async def test_deprecated_patch_still_replays_a_history_that_recorded_it(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app([onboard_v2], STEP_DEFS)
    handle = await app.start_workflow(onboard_v2, "user-9", id="onboard-user-9")
    await _wait_until_sleeping(clock, app, 1)
    await clock.advance(timedelta(hours=1, seconds=1))
    assert await handle.result(timeout=5) == "sms:user-9"

    history = await app.store.load_history(handle.run_id or "")
    result = check_history(history, Registry([onboard_v3]))
    assert result.ok


async def test_golden_history_file_checks_v3_without_touching_the_store(
    make_app: AppFactory, clock: ManualClock, tmp_path: Path
) -> None:
    """The CI pattern from ``loomwork.versioning``: export a run's history once, then
    check future code changes against the exported file, no store access required."""
    app = await make_app([onboard_v2], STEP_DEFS)
    handle = await app.start_workflow(onboard_v2, "user-10", id="onboard-user-10")
    await _wait_until_sleeping(clock, app, 1)
    await clock.advance(timedelta(hours=1, seconds=1))
    await handle.result(timeout=5)

    golden = tmp_path / "onboard-user-10.json"
    history = await app.store.load_history(handle.run_id or "")
    golden.write_text(history_to_json(history))

    assert check_history_file(golden, Registry([onboard_v3])).ok
    # The unsafe variant still fails, whether checked from the store or from the file.
    assert not check_history_file(golden, Registry([onboard_v2_unsafe])).ok
