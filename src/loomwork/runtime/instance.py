"""A single workflow run executing on a deterministic event loop.

:class:`WorkflowInstance` owns the loop, the per-kind sequence counters, and the futures
and timer handles that are waiting for input events. It knows nothing about histories or
storage: :class:`~loomwork.runtime.replayer.Replayer` decides which inputs to deliver in
which activation and checks the commands it gets back.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import random
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from loomwork.codec.converter import DataConverter, return_hint
from loomwork.errors import NondeterminismError, WorkflowTickError
from loomwork.failure import FailureInfo
from loomwork.history.events import (
    CancelRequested,
    Event,
    StepCompleted,
    StepFailed,
    TimerFired,
    WorkflowStarted,
)
from loomwork.info import WorkflowInfo
from loomwork.runtime.commands import (
    CancelTimer,
    CancelWorkflow,
    Command,
    CompleteWorkflow,
    FailWorkflow,
    RequestCancelStep,
    ScheduleStep,
    StartTimer,
)
from loomwork.runtime.loop import DeterministicEventLoop

if TYPE_CHECKING:
    from loomwork.options import StepOptions
    from loomwork.sdk.definitions import StepDefinition, WorkflowDefinition


@dataclass(slots=True)
class _PendingStep:
    name: str
    future: asyncio.Future[Any]
    result_hint: Any


class WorkflowInstance:
    """Runs one workflow function deterministically, one activation at a time."""

    def __init__(
        self,
        definition: WorkflowDefinition[Any, Any],
        started: WorkflowStarted,
        *,
        converter: DataConverter,
    ) -> None:
        self.definition = definition
        self.converter = converter
        self.info = WorkflowInfo(
            workflow_id=started.workflow_id,
            run_id=started.run_id,
            workflow_name=started.workflow_name,
            attempt=started.attempt,
            started_at=started.timestamp,
        )
        self.random = random.Random(started.random_seed)
        self.loop = DeterministicEventLoop(self, start_time=started.timestamp.timestamp())
        self.loop.owner = self
        self.now: datetime = started.timestamp
        self.replaying = False
        self.closed = False
        self.cancel_requested = False
        self.activations = 0

        self._commands: list[Command] = []
        self._step_seq = 0
        self._timer_seq = 0
        self._steps: dict[int, _PendingStep] = {}
        self._timers: dict[int, asyncio.TimerHandle] = {}
        self._timer_seq_by_handle: dict[int, int] = {}
        self._main: asyncio.Task[Any] | None = None

    # ------------------------------------------------------------------ activation

    def activate(
        self, time: datetime, inputs: Sequence[Event], *, replaying: bool
    ) -> list[Command]:
        """Deliver ``inputs`` at workflow time ``time`` and run until idle.

        Returns the commands produced during this activation.

        Raises:
            WorkflowTickError: the activation could not be completed (a determinism
                violation, an unexpected exception escaping the workflow, ...).
        """
        if self.closed:
            raise WorkflowTickError(f"run {self.info.run_id} has already produced its result")
        self.replaying = replaying
        self.now = max(time, self.now)
        self.loop.set_time(self.now.timestamp())
        for event in inputs:
            self._deliver(event)
        self.loop.run_until_idle()
        self._check_main()
        self.activations += 1
        commands, self._commands = self._commands, []
        return commands

    def _deliver(self, event: Event) -> None:
        match event:
            case WorkflowStarted():
                self._start(event)
            case StepCompleted(seq=seq):
                pending = self._resolve_step(seq)
                if pending is not None and not pending.future.done():
                    try:
                        value = self.converter.decode(event.result, pending.result_hint)
                    except Exception as exc:
                        pending.future.set_exception(exc)
                    else:
                        pending.future.set_result(value)
            case StepFailed(seq=seq):
                pending = self._resolve_step(seq)
                if pending is not None and not pending.future.done():
                    pending.future.set_exception(event.failure.to_exception())
            case TimerFired(seq=seq):
                handle = self._timers.pop(seq, None)
                if handle is None:
                    if seq > self._timer_seq:
                        raise NondeterminismError(f"timer {seq} fired but was never started")
                    return  # cancelled before it fired
                self._timer_seq_by_handle.pop(id(handle), None)
                self.loop.fire_timer(handle)
            case CancelRequested():
                if not self.cancel_requested:
                    self.cancel_requested = True
                    if self._main is not None and not self._main.done():
                        self._main.cancel(event.reason or None)
            case _:
                raise WorkflowTickError(f"cannot deliver {type(event).__name__} to workflow code")

    def _start(self, event: WorkflowStarted) -> None:
        if self._main is not None:
            raise NondeterminismError("workflow started twice")
        args = self.converter.decode_args(self.definition.fn, event.args)
        self._main = self.loop.create_task(self.definition.fn(*args), name="workflow-main")

    def _resolve_step(self, seq: int) -> _PendingStep | None:
        pending = self._steps.pop(seq, None)
        if pending is None and seq > self._step_seq:
            raise NondeterminismError(f"result delivered for step {seq}, which was never scheduled")
        return pending

    def _check_main(self) -> None:
        main = self._main
        if main is None or not main.done() or self.closed:
            return
        command: Command
        if main.cancelled():
            command = CancelWorkflow(reason="canceled")
        else:
            exc = main.exception()
            if exc is None:
                command = CompleteWorkflow(result=self.converter.encode(main.result()))
            elif self.definition.is_failure(exc):
                command = FailWorkflow(failure=FailureInfo.from_exception(exc))
            else:
                raise WorkflowTickError(
                    f"workflow {self.info.workflow_name!r} raised {type(exc).__name__}: {exc}"
                ) from exc
        self._emit(command)
        self.closed = True

    # ------------------------------------------------------------------ commands

    def _emit(self, command: Command) -> None:
        if not self.closed:
            self._commands.append(command)

    def schedule_step(
        self,
        definition: StepDefinition[Any, Any],
        args: Sequence[Any],
        options: StepOptions,
    ) -> asyncio.Future[Any]:
        payloads = self.converter.encode_args(args)
        self._step_seq += 1
        seq = self._step_seq
        future: asyncio.Future[Any] = self.loop.create_future()
        self._steps[seq] = _PendingStep(definition.name, future, return_hint(definition.fn))
        future.add_done_callback(functools.partial(self._on_step_future_done, seq))
        self._emit(ScheduleStep(seq=seq, step_name=definition.name, args=payloads, options=options))
        return future

    def _on_step_future_done(self, seq: int, future: asyncio.Future[Any]) -> None:
        # Cancelled by workflow code (directly or via gather/timeout) before a result
        # arrived: tell the engine it may stop the step.
        if future.cancelled() and self._steps.pop(seq, None) is not None:
            self._emit(RequestCancelStep(seq=seq))

    # TimerBridge ---------------------------------------------------------

    def start_timer(self, handle: asyncio.TimerHandle) -> None:
        self._timer_seq += 1
        seq = self._timer_seq
        self._timers[seq] = handle
        self._timer_seq_by_handle[id(handle)] = seq
        self._emit(StartTimer(seq=seq, fire_at=datetime.fromtimestamp(handle.when(), UTC)))

    def cancel_timer(self, handle: asyncio.TimerHandle) -> None:
        seq = self._timer_seq_by_handle.pop(id(handle), None)
        if seq is None:
            return
        self._timers.pop(seq, None)
        self._emit(CancelTimer(seq=seq))

    # ------------------------------------------------------------------ deterministic helpers

    def uuid4(self) -> uuid.UUID:
        return uuid.UUID(int=self.random.getrandbits(128), version=4)

    # ------------------------------------------------------------------ introspection

    @property
    def pending_step_seqs(self) -> list[int]:
        return sorted(self._steps)

    @property
    def pending_timer_seqs(self) -> list[int]:
        return sorted(self._timers)

    @property
    def main_task(self) -> asyncio.Task[Any] | None:
        return self._main

    def _finished_tasks_with_errors(self) -> list[asyncio.Task[Any]]:
        main = self._main
        if main is not None and main.done() and not main.cancelled():
            return [main]
        return []

    def close(self) -> None:
        """Release the instance (for example when it is evicted from a worker cache).

        Suspended workflow coroutines are closed while this loop is installed as the
        running loop and with command emission disabled, so ``finally`` blocks in
        workflow code cannot schedule work or run steps inline.
        """
        if self.loop.is_closed():
            return
        self.closed = True
        previous = asyncio.events._get_running_loop()
        asyncio.events._set_running_loop(self.loop)
        try:
            for task in self._finished_tasks_with_errors():
                task.exception()  # mark as retrieved so asyncio does not log it
            for task in asyncio.all_tasks(self.loop):
                task._log_destroy_pending = False  # type: ignore[attr-defined]
                coro = task.get_coro()
                close = getattr(coro, "close", None)
                if close is not None:
                    # RuntimeError: the coroutine awaited inside its finally block;
                    # nothing more can be done for it.
                    with contextlib.suppress(RuntimeError):
                        close()
        finally:
            asyncio.events._set_running_loop(previous)
            self._commands.clear()
            self.loop.close()
