"""Fingerprints, patches and the compatibility checker."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

import loomwork as lw
from loomwork import ManualClock, MemoryStore
from loomwork.history import PatchRecorded, TickCompleted, history_to_json
from loomwork.sdk.definitions import Registry, WorkflowDefinition
from loomwork.versioning import (
    check_compatibility,
    check_history,
    check_history_file,
    code_fingerprint,
)
from loomwork.worker import WorkerOptions
from tests.helpers import FAST_CONFIG, eventually

# --------------------------------------------------------------------------- fingerprints


def f_original(x: int) -> int:
    """Docstring."""
    y = x + 1  # comment
    return y * 2


def f_cosmetic(x: int) -> int:
    # Only formatting, comments, annotations and docstring differ.
    y: int = x + 1
    return y * 2


def f_changed(x: int) -> int:
    y = x + 2
    return y * 2


class TestFingerprint:
    def test_ignores_cosmetics(self) -> None:
        assert code_fingerprint(f_original) == code_fingerprint(f_cosmetic)

    def test_detects_logic_changes(self) -> None:
        assert code_fingerprint(f_original) != code_fingerprint(f_changed)

    def test_is_short_hex(self) -> None:
        value = code_fingerprint(f_original)
        assert len(value) == 12
        int(value, 16)

    def test_falls_back_to_bytecode(self) -> None:
        namespace: dict[str, Any] = {}
        exec("def dynamic(x):\n    return x + 1\n", namespace)
        assert len(code_fingerprint(namespace["dynamic"])) == 12

    def test_definition_caches(self) -> None:
        @lw.workflow(name="fp.demo")
        async def demo() -> None:
            return None

        assert demo.fingerprint == demo.fingerprint == code_fingerprint(demo.fn)


# --------------------------------------------------------------------------- patches


@lw.step(name="ver.email")
async def send_email(to: str) -> str:
    return f"email:{to}"


@lw.step(name="ver.sms")
async def send_sms(to: str) -> str:
    return f"sms:{to}"


async def _v1(to: str) -> list[str]:
    await lw.sleep(timedelta(hours=1))
    return [await send_email(to)]


async def _v2(to: str) -> list[str]:
    await lw.sleep(timedelta(hours=1))
    if lw.patched("sms-receipts"):
        return [await send_sms(to)]
    return [await send_email(to)]


async def _v2_unsafe(to: str) -> list[str]:
    await lw.sleep(timedelta(hours=1))
    return [await send_sms(to)]


async def _v3(to: str) -> list[str]:
    await lw.sleep(timedelta(hours=1))
    lw.deprecate_patch("sms-receipts")
    return [await send_sms(to)]


def version(fn: Callable[..., Coroutine[Any, Any, Any]]) -> WorkflowDefinition[..., Any]:
    return lw.workflow(name="ver.notify")(fn)


def app(
    clock: ManualClock, store: MemoryStore, definition: WorkflowDefinition[..., Any]
) -> lw.Loomwork:
    return lw.Loomwork(
        store=store,
        workflows=[definition],
        steps=[send_email, send_sms],
        clock=clock,
        config=FAST_CONFIG,
        worker_options=WorkerOptions(shutdown_grace=0.2),
    )


async def _run_until_sleeping(
    clock: ManualClock, store: MemoryStore, definition: Any, to: str
) -> str:
    async with app(clock, store, version(definition)) as first:
        handle = await first.start_workflow("ver.notify", to)
        await eventually(lambda: store.timer_count >= 1)
        run_id = handle.run_id
    assert run_id is not None
    return run_id


async def _v1_notify_first(to: str) -> list[str]:
    sent = await send_email(to)
    await lw.sleep(timedelta(hours=1))
    return [sent]


async def _v2_notify_first(to: str) -> list[str]:
    sent = await send_sms(to) if lw.patched("sms-receipts") else await send_email(to)
    await lw.sleep(timedelta(hours=1))
    return [sent]


async def test_patch_replays_decisions_made_before_it(clock: ManualClock) -> None:
    store = MemoryStore()
    # The old run already sent its email before the upgrade.
    old_run = await _run_until_sleeping(clock, store, _v1_notify_first, "old")
    async with app(clock, store, version(_v2_notify_first)) as upgraded:
        new_handle = await upgraded.start_workflow("ver.notify", "new")
        await eventually(lambda: store.timer_count == 2)
        await clock.advance(timedelta(hours=1))
        assert await upgraded.get_handle(run_id=old_run).result(timeout=5) == ["email:old"]
        assert await new_handle.result(timeout=5) == ["sms:new"]
        assert not any(isinstance(e, PatchRecorded) for e in await store.load_history(old_run))
        assert any(isinstance(e, PatchRecorded) for e in await new_handle.history())


async def test_patch_applies_to_runs_reaching_it_after_deploy(clock: ManualClock) -> None:
    store = MemoryStore()
    # This run was sleeping *before* the patched call when the new code was deployed.
    old_run = await _run_until_sleeping(clock, store, _v1, "old")
    async with app(clock, store, version(_v2)) as upgraded:
        new_handle = await upgraded.start_workflow("ver.notify", "new")
        await eventually(lambda: store.timer_count == 2)
        await clock.advance(timedelta(hours=1))
        assert await upgraded.get_handle(run_id=old_run).result(timeout=5) == ["sms:old"]
        assert await new_handle.result(timeout=5) == ["sms:new"]
        # Both runs made the decision under the new code, so both recorded the marker.
        assert any(isinstance(e, PatchRecorded) for e in await new_handle.history())
        assert any(isinstance(e, PatchRecorded) for e in await store.load_history(old_run))


async def test_deprecated_patch_replays_patched_histories(clock: ManualClock) -> None:
    store = MemoryStore()
    patched_run = await _run_until_sleeping(clock, store, _v2, "a")
    history = await store.load_history(patched_run)
    registry_v3 = Registry([version(_v3)], [send_email, send_sms])
    assert check_history(history, registry_v3).ok
    async with app(clock, store, version(_v3)) as v3:
        await clock.advance(timedelta(hours=1))
        assert await v3.get_handle(run_id=patched_run).result(timeout=5) == ["sms:a"]


async def test_code_version_is_recorded_per_tick(clock: ManualClock) -> None:
    store = MemoryStore()
    definition = version(_v1)
    run_id = await _run_until_sleeping(clock, store, _v1, "x")
    ticks = [e for e in await store.load_history(run_id) if isinstance(e, TickCompleted)]
    assert ticks
    assert {t.code_version for t in ticks} == {definition.fingerprint}


# --------------------------------------------------------------------------- compatibility


async def test_compatibility_checker(clock: ManualClock, tmp_path: Path) -> None:
    store = MemoryStore()
    await _run_until_sleeping(clock, store, _v1, "running")
    # A completed run: its history already contains the email step.
    async with app(clock, store, version(_v1)) as v1:
        done = await v1.start_workflow("ver.notify", "done")
        await eventually(lambda: store.timer_count == 2)
        await clock.advance(timedelta(hours=1))
        await done.result(timeout=5)
        await v1.get_handle(run_id=(await store.list_executions())[-1].run_id).result(timeout=5)

    safe = await check_compatibility(store, Registry([version(_v2)]))
    assert safe.ok
    assert safe.summary() == {"ver.notify": (2, 0)}

    # Replacing the email with an SMS without a patch breaks every history that has
    # already scheduled the email; histories that stopped earlier still replay.
    await _run_until_sleeping(clock, store, _v1, "still-sleeping")
    unsafe = await check_compatibility(store, Registry([version(_v2_unsafe)]))
    assert not unsafe.ok
    assert unsafe.summary() == {"ver.notify": (1, 2)}
    failure = unsafe.failures[0]
    assert failure.nondeterministic
    assert failure.diverged_at_event is not None
    assert failure.code_versions == (version(_v1).fingerprint,)
    text = unsafe.format_text()
    assert "[FAIL] ver.notify: 1 compatible, 2 incompatible" in text
    assert "ver.email" in text

    skipped = await check_compatibility(store, Registry([]))
    assert skipped.checks == []
    assert skipped.unknown_workflows["ver.notify"] == 3
    assert "[skip]" in skipped.format_text()

    # Golden history files.
    run = next(r for r in await store.list_executions() if r.is_closed)
    path = tmp_path / "golden.json"
    path.write_text(history_to_json(await store.load_history(run.run_id)))
    assert check_history_file(path, Registry([version(_v2)])).ok
    assert not check_history_file(path, Registry([version(_v2_unsafe)])).ok


def test_empty_history() -> None:
    result = check_history([], Registry([]))
    assert not result.ok
    assert "does not start" in (result.error or "")


async def test_patched_outside_workflow() -> None:
    with pytest.raises(lw.NotInWorkflowError):
        lw.patched("x")
    await asyncio.sleep(0)
