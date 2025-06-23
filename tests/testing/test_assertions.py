"""``env.history()``, ``assert_history()`` and the fluent ``HistoryAssertion`` API."""

from __future__ import annotations

import pytest

import loomwork as lw
from loomwork import ApplicationError
from loomwork.testing import HistoryView, WorkflowEnvironment, assert_history

pytestmark = pytest.mark.integration


@lw.step(name="assert.double")
async def double(n: int) -> int:
    return n * 2


@lw.workflow(name="assert.adder")
async def adder(n: int) -> int:
    a = await double(n)
    b = await double(n)
    return a + b


@lw.workflow(name="assert.boom")
async def boom() -> None:
    raise ApplicationError("nope", type="Nope")


async def test_history_view_pretty_and_kinds() -> None:
    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await env.start_workflow(adder, 3)
        await handle.result(timeout=5)
        view = await env.history(handle)

    assert isinstance(view, HistoryView)
    assert "workflow_started" in view.kinds()
    assert "workflow_completed" in view.kinds()
    assert len(view.of_kind("step_scheduled")) == 2
    rendered = view.pretty()
    assert "workflow_started" in rendered
    assert "step=" in rendered  # decision events describe the step they schedule


async def test_contains_step_counts_call_sites() -> None:
    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await env.start_workflow(adder, 5)
        await handle.result(timeout=5)
        assertion = await env.assert_history(handle)

    assertion.contains_step("assert.double", times=2)
    with pytest.raises(AssertionError):
        assertion.contains_step("assert.double", times=1)
    with pytest.raises(AssertionError):
        assertion.contains_step("assert.nonexistent")


async def test_completed_with_checks_the_decoded_result() -> None:
    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await env.start_workflow(adder, 5)
        await handle.result(timeout=5)
        assertion = await env.assert_history(handle)

    assertion.completed_with(20)
    with pytest.raises(AssertionError):
        assertion.completed_with(21)


async def test_failed_with_checks_the_failure_type() -> None:
    async with WorkflowEnvironment(workflows=[boom]) as env:
        handle = await env.start_workflow(boom)
        with pytest.raises(lw.WorkflowFailedError):
            await handle.result(timeout=5)
        assertion = await env.assert_history(handle)

    assertion.failed_with(ApplicationError)
    with pytest.raises(AssertionError):
        assertion.completed_with(None)


async def test_no_nondeterminism_passes_for_unchanged_code() -> None:
    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await env.start_workflow(adder, 2)
        await handle.result(timeout=5)
        assertion = await env.assert_history(handle)

    assertion.no_nondeterminism()  # uses the environment's own registry
    assertion.no_nondeterminism(adder)  # or an explicit list


async def test_no_nondeterminism_catches_a_behavioural_change() -> None:
    @lw.workflow(name="assert.adder")
    async def rewritten_adder(n: int) -> int:
        # Same name, but only calls the step once: replaying old history diverges.
        a = await double(n)
        return a + a

    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await env.start_workflow(adder, 4)
        await handle.result(timeout=5)
        assertion = await env.assert_history(handle)

    with pytest.raises(AssertionError, match="diverges"):
        assertion.no_nondeterminism(rewritten_adder)


async def test_standalone_assert_history_accepts_a_history_view() -> None:
    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await env.start_workflow(adder, 6)
        await handle.result(timeout=5)
        view = await env.history(handle)

    assertion = await assert_history(view)
    assertion.contains_step("assert.double", times=2).completed_with(24)


async def test_standalone_assert_history_accepts_a_plain_event_list() -> None:
    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await env.start_workflow(adder, 1)
        await handle.result(timeout=5)
        events = list(await env.history(handle))

    assertion = await assert_history(events)
    assertion.completed_with(4)


async def test_step_completed_counts_successful_completions() -> None:
    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await env.start_workflow(adder, 9)
        await handle.result(timeout=5)
        assertion = await env.assert_history(handle)

    assertion.step_completed("assert.double", times=2)
    with pytest.raises(AssertionError, match="never completed"):
        assertion.step_completed("assert.nonexistent")
    with pytest.raises(AssertionError):
        assertion.step_completed("assert.double", times=1)


async def test_no_nondeterminism_without_a_registry_or_workflows_raises() -> None:
    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await env.start_workflow(adder, 1)
        await handle.result(timeout=5)
        events = list(await env.history(handle))

    # The standalone function builds no registry unless one is passed in.
    bare = await assert_history(events)
    with pytest.raises(AssertionError, match="needs workflow definitions"):
        bare.no_nondeterminism()


async def test_history_view_len_and_indexing() -> None:
    async with WorkflowEnvironment(workflows=[adder], steps=[double]) as env:
        handle = await env.start_workflow(adder, 1)
        await handle.result(timeout=5)
        view = await env.history(handle)

    assert len(view) == len(view.events)
    assert view[0] is view.events[0]
    assert view[0].kind == "workflow_started"


async def test_canceled_assertion() -> None:
    @lw.workflow(name="assert.cancelable")
    async def cancelable() -> None:
        await lw.sleep(60 * 60 * 24)

    async with WorkflowEnvironment(workflows=[cancelable]) as env:
        handle = await env.start_workflow(cancelable)
        await handle.cancel("no longer needed")
        with pytest.raises(lw.WorkflowCanceledError):
            await handle.result(timeout=5)
        assertion = await env.assert_history(handle)

    assertion.canceled()
    with pytest.raises(AssertionError):
        assertion.completed_with(None)
