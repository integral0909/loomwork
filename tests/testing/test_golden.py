"""Golden histories: ``record_golden`` / ``assert_replays`` / ``replay_check``."""

from __future__ import annotations

from pathlib import Path

import pytest

import loomwork as lw
from loomwork.testing import WorkflowEnvironment, assert_replays, record_golden, replay_check

pytestmark = pytest.mark.integration


@lw.step(name="golden.double")
async def double(n: int) -> int:
    return n * 2


@lw.workflow(name="golden.adder")
async def adder(n: int) -> int:
    a = await double(n)
    b = await double(n)
    return a + b


async def _recorded_handle(env: WorkflowEnvironment):
    handle = await env.start_workflow(adder, 7)
    await handle.result(timeout=5)
    return handle


async def test_record_and_assert_replays_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "adder.json"
    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await _recorded_handle(env)
        written = await record_golden(handle, path)

    assert written == path
    assert path.exists()
    assert_replays(path, adder)  # no live environment needed at all


async def test_assert_replays_catches_an_incompatible_change(tmp_path: Path) -> None:
    path = tmp_path / "adder.json"
    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await _recorded_handle(env)
        await record_golden(handle, path)

    @lw.workflow(name="golden.adder")
    async def rewritten(n: int) -> int:
        # Drops the second call: a run recorded by the old code cannot replay this.
        return await double(n)

    with pytest.raises(AssertionError, match="no longer replays"):
        assert_replays(path, rewritten)


async def test_assert_replays_creates_parent_directories(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "dir" / "adder.json"
    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await _recorded_handle(env)
        await record_golden(handle, path)

    assert path.exists()
    assert_replays(path, adder)


async def test_replay_check_works_directly_from_a_handle_without_a_file() -> None:
    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await _recorded_handle(env)
        await replay_check(handle, adder)  # ok: same code

        @lw.workflow(name="golden.adder")
        async def rewritten(n: int) -> int:
            await double(n)
            await double(n)
            await double(n)  # a third call the recorded history never made
            return 0

        with pytest.raises(AssertionError, match="would not replay"):
            await replay_check(handle, rewritten)


async def test_environment_convenience_methods_use_its_own_registry(tmp_path: Path) -> None:
    path = tmp_path / "adder.json"
    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await _recorded_handle(env)
        await env.record_golden(handle, path)
        env.assert_replays(path)  # defaults to every workflow env was built with
        await env.replay_check(handle)
