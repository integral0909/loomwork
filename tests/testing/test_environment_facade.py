"""The ``WorkflowEnvironment`` convenience methods that mirror ``WorkflowHandle`` and
``Client`` for callers who only have a workflow id, not a handle in hand."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pydantic
import pytest

import loomwork as lw
from loomwork import ExecutionStatus
from loomwork.testing import WorkflowEnvironment

pytestmark = pytest.mark.integration

approvals = lw.Channel("approvals", str)


class Result(pydantic.BaseModel):
    value: int


@lw.workflow(name="facade.approval")
async def approval_workflow() -> str:
    stage = "waiting"

    @lw.query("stage")
    def get_stage() -> str:
        return stage

    @lw.update("set_stage")
    async def set_stage(value: str) -> str:
        nonlocal stage
        stage = value
        return stage

    async with asyncio.timeout(timedelta(days=1).total_seconds()):
        decision = await approvals.receive()
    stage = "done"
    return decision


@lw.workflow(name="facade.long_wait")
async def long_wait() -> str:
    await lw.sleep(timedelta(days=1))
    return "woke up"


@lw.workflow(name="facade.typed_result")
async def typed_result(n: int) -> Result:
    return Result(value=n * 2)


# These tests interact with a run through several separate calls before letting it
# reach its natural (durable-timeout) conclusion. Auto time-skipping is disabled for
# them: it would be free to jump straight past that timeout the moment nothing else is
# happening, which is exactly correct behaviour but would race the very thing each test
# means to exercise (signalling, querying, cancelling, ... a run that is still open).


async def test_signal_shortcut_delivers_to_a_running_workflow() -> None:
    async with WorkflowEnvironment(workflows=[approval_workflow], auto_time_skipping=False) as env:
        handle = await env.start_workflow(approval_workflow, id="wf-1")
        await env.signal("wf-1", approvals, "approved")
        result = await handle.result(timeout=5)

    assert result == "approved"


async def test_query_shortcut_reads_workflow_state() -> None:
    async with WorkflowEnvironment(workflows=[approval_workflow], auto_time_skipping=False) as env:
        await env.start_workflow(approval_workflow, id="wf-2")
        # The query handler is registered by the workflow's first tick; give the worker
        # a moment to commit it before asking (a query only replays committed history).
        stage = await _query_until_registered(env, "wf-2")
        assert stage == "waiting"
        await env.signal("wf-2", approvals, "ok")


async def _query_until_registered(env: WorkflowEnvironment, workflow_id: str) -> str:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 2.0
    while True:
        try:
            result: str = await env.query(workflow_id, "stage")
            return result
        except lw.NotFoundError:
            if loop.time() > deadline:
                raise
            await asyncio.sleep(0.005)


async def test_update_shortcut_runs_a_handler_and_returns_its_result() -> None:
    async with WorkflowEnvironment(workflows=[approval_workflow], auto_time_skipping=False) as env:
        await env.start_workflow(approval_workflow, id="wf-3")
        result = await env.update("wf-3", "set_stage", "reviewing")
        assert result == "reviewing"
        await env.signal("wf-3", approvals, "ok")


async def test_cancel_shortcut() -> None:
    async with WorkflowEnvironment(workflows=[long_wait], auto_time_skipping=False) as env:
        await env.start_workflow(long_wait, id="wf-4")
        assert await env.cancel("wf-4", "no longer needed")
        with pytest.raises(lw.WorkflowCanceledError):
            await env.get_handle("wf-4").result(timeout=5)


async def test_terminate_shortcut() -> None:
    async with WorkflowEnvironment(workflows=[long_wait], auto_time_skipping=False) as env:
        await env.start_workflow(long_wait, id="wf-5")
        assert await env.terminate("wf-5", "operator action")
        with pytest.raises(lw.WorkflowTerminatedError):
            await env.get_handle("wf-5").result(timeout=5)


async def test_list_executions_shortcut_filters_by_status() -> None:
    async with WorkflowEnvironment(workflows=[long_wait], auto_time_skipping=False) as env:
        await env.start_workflow(long_wait, id="wf-6")
        await env.start_workflow(long_wait, id="wf-7")
        await env.terminate("wf-6", "done")

        running = await env.list_executions(workflow=long_wait, status=ExecutionStatus.RUNNING)
        terminated = await env.list_executions(
            workflow=long_wait, status=ExecutionStatus.TERMINATED
        )

    assert {r.workflow_id for r in running} == {"wf-7"}
    assert {r.workflow_id for r in terminated} == {"wf-6"}


async def test_get_handle_with_a_result_type_decodes_typed_results() -> None:
    async with WorkflowEnvironment(workflows=[typed_result]) as env:
        await env.start_workflow(typed_result, 21, id="wf-8")
        handle = env.get_handle("wf-8", result_type=Result)
        result = await handle.result(timeout=5)

    assert isinstance(result, Result)
    assert result.value == 42


async def test_register_adds_definitions_after_construction() -> None:
    async with WorkflowEnvironment() as env:
        env.register(long_wait)
        handle = await env.start_workflow(long_wait)
        assert await handle.result(timeout=5) == "woke up"


def test_repr_shows_the_current_simulated_time() -> None:
    env = WorkflowEnvironment()
    assert repr(env) == f"WorkflowEnvironment(now={env.now().isoformat()!r})"
