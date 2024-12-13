"""The engine's control-plane API: starting, inspecting and stopping runs.

The :class:`Engine` does not execute workflow code; workers do (see
:mod:`loomwork.worker`). It is deliberately independent of any workflow registry so the
same API can back an HTTP server that never imports user code.
"""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import Sequence

from loomwork.clock import Clock, SystemClock
from loomwork.codec.payload import Payload
from loomwork.engine.config import EngineConfig
from loomwork.errors import NotFoundError
from loomwork.history.events import Event, WorkflowStarted
from loomwork.ids import new_id
from loomwork.store.base import Store
from loomwork.store.notify import closed_topic
from loomwork.store.records import ExecutionRecord, ExecutionStatus, StepJob

_MAX_ID_LENGTH = 256


class Engine:
    """Starts and controls workflow runs stored in a :class:`Store`."""

    def __init__(
        self,
        store: Store,
        *,
        clock: Clock | None = None,
        config: EngineConfig | None = None,
    ) -> None:
        self.store = store
        self.clock = clock or SystemClock()
        self.config = config or EngineConfig()

    # ------------------------------------------------------------------ starting

    async def start_workflow(
        self,
        workflow_name: str,
        args: Sequence[Payload] = (),
        *,
        workflow_id: str | None = None,
    ) -> ExecutionRecord:
        """Create a new run of ``workflow_name``.

        ``workflow_id`` identifies the business entity the workflow is about (an order
        id, for example). Only one run per workflow id can be running at a time.

        Raises:
            WorkflowAlreadyStartedError: a run with this workflow id is running.
        """
        run_id = new_id()
        workflow_id = workflow_id or run_id
        if not workflow_id or len(workflow_id) > _MAX_ID_LENGTH:
            raise ValueError(f"workflow_id must be 1..{_MAX_ID_LENGTH} characters")
        now = self.clock.now()
        started = WorkflowStarted(
            workflow_name=workflow_name,
            workflow_id=workflow_id,
            run_id=run_id,
            args=list(args),
            random_seed=secrets.randbits(63),
        )
        record = ExecutionRecord(
            run_id=run_id,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            status=ExecutionStatus.RUNNING,
            created_at=now,
        )
        created, _ = await self.store.create_execution(record, started, now)
        return created

    # ------------------------------------------------------------------ inspection

    async def describe(self, run_id: str) -> ExecutionRecord:
        return await self.store.get_execution(run_id)

    async def resolve(
        self, *, workflow_id: str | None = None, run_id: str | None = None
    ) -> ExecutionRecord:
        """Find a run by run id, or the latest run of a workflow id."""
        if run_id is not None:
            record = await self.store.get_execution(run_id)
            if workflow_id is not None and record.workflow_id != workflow_id:
                raise NotFoundError(f"run {run_id} does not belong to workflow id {workflow_id!r}")
            return record
        if workflow_id is not None:
            return await self.store.find_execution(workflow_id)
        raise ValueError("either workflow_id or run_id is required")

    async def history(self, run_id: str) -> list[Event]:
        return await self.store.load_history(run_id)

    async def pending_steps(self, run_id: str) -> list[StepJob]:
        return await self.store.get_step_jobs(run_id)

    async def list_executions(
        self,
        *,
        workflow_name: str | None = None,
        status: ExecutionStatus | None = None,
        limit: int = 100,
    ) -> list[ExecutionRecord]:
        return await self.store.list_executions(
            workflow_name=workflow_name, status=status, limit=limit
        )

    # ------------------------------------------------------------------ control

    async def request_cancel(self, run_id: str, reason: str = "") -> bool:
        """Ask the workflow to cancel. Its code sees ``asyncio.CancelledError`` and may
        clean up (for example run compensating steps) before finishing.

        Returns ``False`` if the run had already closed.
        """
        return await self.store.request_cancel(run_id, reason, self.clock.now())

    async def terminate(self, run_id: str, reason: str = "") -> bool:
        """Close the run immediately, without running any more workflow code."""
        return await self.store.terminate(run_id, reason, self.clock.now())

    # ------------------------------------------------------------------ waiting

    async def wait_closed(self, run_id: str, timeout: float | None = None) -> ExecutionRecord:
        """Wait until the run closes and return its final record.

        Raises:
            TimeoutError: the run did not close within ``timeout`` seconds.
        """
        topic = closed_topic(run_id)
        notifier = self.store.notifier
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        try:
            while True:
                token = notifier.token(topic)
                record = await self.store.get_execution(run_id)
                if record.is_closed:
                    return record
                wait_for = self.config.poll_interval
                if deadline is not None:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise TimeoutError(f"run {run_id} did not close within {timeout}s")
                    wait_for = min(wait_for, remaining)
                await notifier.wait(topic, token, wait_for)
        finally:
            notifier.discard(topic)
