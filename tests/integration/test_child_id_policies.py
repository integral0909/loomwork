"""Id reuse/conflict policies and the versioning policy override for child workflows,
end to end with real workers (see docs/guides/composition.md)."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

import loomwork as lw
from loomwork import ChildWorkflowError, ExecutionStatus, ManualClock
from loomwork.options import IdConflictPolicy, IdReusePolicy, VersioningPolicy
from loomwork.worker import WorkerOptions
from tests.conftest import AppFactory
from tests.helpers import FAST_CONFIG, eventually

pytestmark = pytest.mark.integration


@lw.step(name="cip.touch")
async def touch(label: str) -> str:
    return label


@lw.workflow(name="cip.sleeper")
async def sleeper(label: str) -> str:
    await lw.sleep(timedelta(days=30))
    return await touch(label)


@lw.workflow(name="cip.quick")
async def quick(label: str) -> str:
    return await touch(label)


@lw.workflow(name="cip.reject_parent")
async def reject_parent(target_id: str) -> str:
    try:
        await lw.start_child(
            quick, "second", id=target_id, id_reuse_policy=IdReusePolicy.REJECT_DUPLICATE
        )
    except ChildWorkflowError as exc:
        return exc.type
    return "started"


@lw.workflow(name="cip.use_existing_parent")
async def use_existing_parent(target_id: str) -> str:
    handle = lw.start_child(
        sleeper, "attached", id=target_id, id_conflict_policy=IdConflictPolicy.USE_EXISTING
    )
    return await handle


@lw.workflow(name="cip.terminate_existing_parent")
async def terminate_existing_parent(target_id: str) -> str:
    handle = lw.start_child(
        quick,
        "replacement",
        id=target_id,
        id_conflict_policy=IdConflictPolicy.TERMINATE_EXISTING,
    )
    return await handle


@lw.workflow(name="cip.versioned_child")
async def versioned_child() -> str:
    return "child-done"


@lw.workflow(name="cip.versioning_parent")
async def versioning_parent() -> tuple[str, str]:
    handle = lw.start_child(versioned_child, versioning_policy=VersioningPolicy.PINNED)
    result = await handle
    return result, handle.run_id


WORKFLOWS = [
    sleeper,
    quick,
    reject_parent,
    use_existing_parent,
    terminate_existing_parent,
    versioned_child,
    versioning_parent,
]
STEPS = [touch]


async def test_reject_duplicate_child_surfaces_deterministic_failure(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app(WORKFLOWS, STEPS)
    # "second" is a plain run that already closed under the target workflow id; the
    # child's own REJECT_DUPLICATE must then refuse to start a duplicate of it.
    first = await app.start_workflow(quick, "first", id="cip-target-reject")
    assert await first.result(timeout=5) == "first"
    outcome = await app.execute(reject_parent, "cip-target-reject", timeout=5)
    assert outcome == "ChildWorkflowIdReuseRejected"


async def test_use_existing_child_delivers_the_existing_runs_result(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app(WORKFLOWS, STEPS)
    # The "existing" run is started independently of the attaching parent -- exactly
    # the case the id conflict policy is meant to cover.
    existing = await app.start_workflow(sleeper, "already-running", id="cip-target-use")
    parent = await app.start_workflow(use_existing_parent, "cip-target-use")
    await eventually(lambda: app.store.timer_count == 1)  # type: ignore[attr-defined]
    await clock.advance(timedelta(days=31))
    assert await existing.result(timeout=5) == "already-running"
    assert await parent.result(timeout=5) == "already-running"


async def test_terminate_existing_child_replaces_and_returns_new_result(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app(WORKFLOWS, STEPS)
    existing = await app.start_workflow(sleeper, "stale", id="cip-target-term")
    outcome = await app.execute(terminate_existing_parent, "cip-target-term", timeout=5)
    assert outcome == "replacement"
    old_record = await existing.describe()
    assert old_record.status is ExecutionStatus.TERMINATED
    assert old_record.close_reason == "superseded by child id conflict policy"


async def test_child_versioning_policy_override_is_independent_of_parent(
    make_app: AppFactory, clock: ManualClock
) -> None:
    # The parent stays AUTO_UPGRADE (engine default); the child explicitly pins.
    config = replace(FAST_CONFIG, default_versioning_policy=VersioningPolicy.AUTO_UPGRADE)
    app = await make_app(
        WORKFLOWS, STEPS, config=config, worker_options=WorkerOptions(build_id="build-a")
    )
    handle = await app.start_workflow(versioning_parent, id="cip-versioning-1")
    result, child_run_id = await handle.result(timeout=5)
    assert result == "child-done"
    parent_record = await handle.describe()
    assert parent_record.versioning_policy is VersioningPolicy.AUTO_UPGRADE
    assert parent_record.pinned_build_id is None
    child_record = await app.get_handle(run_id=child_run_id).describe()
    assert child_record.versioning_policy is VersioningPolicy.PINNED
    assert child_record.pinned_build_id == "build-a"


async def test_child_id_policies_replay_identically_after_cache_eviction(
    make_app: AppFactory, clock: ManualClock
) -> None:
    """With no replay cache, every tick re-derives its commands from scratch: if the
    conflict-resolution decisions (or the versioning override) were not perfectly
    deterministic, this would raise NondeterminismError instead of completing."""
    config = replace(FAST_CONFIG, replay_cache_size=0)
    app = await make_app(WORKFLOWS, STEPS, config=config)
    existing = await app.start_workflow(sleeper, "no-cache", id="cip-target-nocache")
    parent = await app.start_workflow(use_existing_parent, "cip-target-nocache")
    await eventually(lambda: app.store.timer_count == 1)  # type: ignore[attr-defined]
    await clock.advance(timedelta(days=31))
    assert await existing.result(timeout=5) == "no-cache"
    assert await parent.result(timeout=5) == "no-cache"
    assert app.worker.ticks.cache.hits == 0

    versioning_app = await make_app(
        WORKFLOWS, STEPS, config=config, worker_options=WorkerOptions(build_id="build-b")
    )
    handle = await versioning_app.start_workflow(versioning_parent, id="cip-versioning-nocache")
    result, _ = await handle.result(timeout=5)
    assert result == "child-done"
