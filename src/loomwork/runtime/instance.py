"""A single workflow run executing on a deterministic event loop.

:class:`WorkflowInstance` owns the loop, the per-kind sequence counters, and the futures
and timer handles that are waiting for input events. It knows nothing about histories or
storage: :class:`~loomwork.runtime.replayer.Replayer` decides which inputs to deliver in
which activation and checks the commands it gets back.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import functools
import inspect
import random
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from loomwork.codec.converter import DataConverter, return_hint
from loomwork.codec.payload import Payload
from loomwork.errors import (
    ApplicationError,
    ContinueAsNew,
    NondeterminismError,
    NotFoundError,
    WorkflowTickError,
)
from loomwork.failure import FailureInfo
from loomwork.history.events import (
    CancelRequested,
    ChildCompleted,
    ChildFailed,
    Event,
    SignalReceived,
    StepCompleted,
    StepFailed,
    TimerFired,
    UpdateRequested,
    WorkflowStarted,
)
from loomwork.info import WorkflowInfo
from loomwork.runtime.commands import (
    CancelTimer,
    CancelWorkflow,
    Command,
    CompleteUpdate,
    CompleteWorkflow,
    ContinueAsNewCommand,
    FailWorkflow,
    RecordPatch,
    RequestCancelStep,
    ScheduleChild,
    ScheduleStep,
    SignalExternal,
    StartTimer,
)
from loomwork.runtime.loop import DeterministicEventLoop

if TYPE_CHECKING:
    from loomwork.options import StepOptions
    from loomwork.sdk.definitions import StepDefinition, WorkflowDefinition


_NO_HINT: Any = inspect.Parameter.empty


@dataclass(slots=True)
class _PendingStep:
    name: str
    future: asyncio.Future[Any]
    result_hint: Any


@dataclass(slots=True)
class _ChannelState:
    """Messages received on one channel but not consumed, and receivers waiting."""

    buffer: collections.deque[SignalReceived] = field(default_factory=collections.deque)
    waiters: collections.deque[tuple[asyncio.Future[Any], Any]] = field(
        default_factory=collections.deque
    )


class QueryError(ApplicationError):
    """A query could not be answered (unknown name, handler error, side effects)."""


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
            parent_workflow_id=started.parent_workflow_id or None,
            parent_run_id=started.parent_run_id or None,
            schedule_id=started.schedule_id or None,
            scheduled_time=started.scheduled_time,
            continued_from_run_id=started.continued_from or None,
        )
        #: Approximate history size: the id of the newest input the current activation
        #: consumed (identical on replay). Workflows use it to decide when to continue
        #: as new.
        self.history_length = 0
        self.continue_as_new_suggested_at = 10_000
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
        self._channels: dict[str, _ChannelState] = {}
        self._seen_message_ids: set[str] = set()
        self._query_handlers: dict[str, Callable[..., Any]] = {}
        self._update_handlers: dict[str, Callable[..., Any]] = {}
        self._buffered_updates: dict[str, list[UpdateRequested]] = {}
        self._handled_updates: set[str] = set()
        self._running_updates: dict[str, asyncio.Task[Any]] = {}
        self._child_seq = 0
        self._children: dict[int, _PendingStep] = {}
        self._external_seq = 0
        #: Patch ids recorded anywhere in the history fed so far (set by the replayer).
        self.recorded_patches: set[str] = set()
        self._patch_results: dict[str, bool] = {}

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
            case SignalReceived():
                self._deliver_signal(event)
            case UpdateRequested():
                self._deliver_update(event)
            case ChildCompleted(seq=seq):
                child = self._resolve_child(seq)
                if child is not None and not child.future.done():
                    try:
                        value = self.converter.decode(event.result, child.result_hint)
                    except Exception as exc:
                        child.future.set_exception(exc)
                    else:
                        child.future.set_result(value)
            case ChildFailed(seq=seq):
                child = self._resolve_child(seq)
                if child is not None and not child.future.done():
                    child.future.set_exception(event.failure.to_exception())
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

    def _continue_as_new(self, request: ContinueAsNew) -> Command:
        name = request.workflow_name or self.info.workflow_name
        return ContinueAsNewCommand(
            workflow_name=name, args=self.converter.encode_args(list(request.args))
        )

    def _resolve_child(self, seq: int) -> _PendingStep | None:
        child = self._children.pop(seq, None)
        if child is None and seq > self._child_seq:
            raise NondeterminismError(f"result delivered for child {seq}, which was never started")
        return child

    def _check_main(self) -> None:
        main = self._main
        if main is None or not main.done() or self.closed:
            return
        self._fail_outstanding_updates()
        command: Command
        if main.cancelled():
            command = CancelWorkflow(reason="canceled")
        else:
            exc = main.exception()
            if exc is None:
                command = CompleteWorkflow(result=self.converter.encode(main.result()))
            elif isinstance(exc, ContinueAsNew):
                command = self._continue_as_new(exc)
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

    # ------------------------------------------------------------------ versioning

    def patched(self, patch_id: str) -> bool:
        """See :func:`loomwork.patched`."""
        cached = self._patch_results.get(patch_id)
        if cached is not None:
            return cached
        if self.replaying and patch_id not in self.recorded_patches:
            # History written before the patch existed: keep taking the old branch.
            result = False
        else:
            result = True
            self._emit(RecordPatch(patch_id=patch_id))
        self._patch_results[patch_id] = result
        return result

    def deprecate_patch(self, patch_id: str) -> None:
        """See :func:`loomwork.deprecate_patch`."""
        if patch_id in self._patch_results:
            return
        self._patch_results[patch_id] = True
        if patch_id in self.recorded_patches:
            # Consume the marker recorded by the (now removed) patched() call.
            self._emit(RecordPatch(patch_id=patch_id))

    # ------------------------------------------------------------------ signals

    def _channel(self, name: str) -> _ChannelState:
        state = self._channels.get(name)
        if state is None:
            state = self._channels[name] = _ChannelState()
        return state

    def _deliver_signal(self, event: SignalReceived) -> None:
        if event.message_id:
            if event.message_id in self._seen_message_ids:
                return  # duplicate delivery of the same message
            self._seen_message_ids.add(event.message_id)
        state = self._channel(event.channel)
        while state.waiters:
            future, hint = state.waiters.popleft()
            if future.done():
                continue  # the receiver gave up (timeout or cancellation)
            try:
                future.set_result(self.converter.decode(event.payload, hint))
            except Exception as exc:
                future.set_exception(exc)
            return
        state.buffer.append(event)

    def receive(self, channel: str, hint: Any) -> asyncio.Future[Any]:
        """A future for the next message on ``channel`` (buffered messages first)."""
        future: asyncio.Future[Any] = self.loop.create_future()
        state = self._channel(channel)
        if state.buffer:
            event = state.buffer.popleft()
            try:
                future.set_result(self.converter.decode(event.payload, hint))
            except Exception as exc:
                future.set_exception(exc)
        else:
            state.waiters.append((future, hint))
        return future

    def pending_messages(self, channel: str) -> int:
        return len(self._channel(channel).buffer)

    def signal_external(
        self, workflow_id: str, channel: str, value: Any, message_id: str | None
    ) -> None:
        payload = self.converter.encode(value)
        self._external_seq += 1
        seq = self._external_seq
        self._emit(
            SignalExternal(
                seq=seq,
                workflow_id=workflow_id,
                channel=channel,
                payload=payload,
                message_id=message_id or f"{self.info.run_id}:signal:{seq}",
            )
        )

    # ------------------------------------------------------------------ queries

    def set_query_handler(self, name: str, handler: Callable[..., Any]) -> None:
        if inspect.iscoroutinefunction(handler):
            raise TypeError(f"query handler {name!r} must be a plain function, not async")
        self._query_handlers[name] = handler

    @property
    def query_names(self) -> list[str]:
        return sorted(self._query_handlers)

    def run_query(self, name: str, args: Sequence[Payload]) -> Payload:
        """Call a query handler against the current state and encode its answer.

        Handlers run with this loop installed (so ``loomwork.now()`` and friends work)
        but must not schedule anything: queries never change a run.
        """
        handler = self._query_handlers.get(name)
        if handler is None:
            known = ", ".join(self.query_names) or "none"
            raise NotFoundError(f"workflow has no query {name!r} (known: {known})")
        values = self.converter.decode_args(handler, args)
        previous = asyncio.events._get_running_loop()
        asyncio.events._set_running_loop(self.loop)
        replaying, self.replaying = self.replaying, True
        before = len(self._commands)
        try:
            result = handler(*values)
        except Exception as exc:
            raise QueryError(f"query {name!r} failed: {exc}", type="QueryFailed") from exc
        finally:
            self.replaying = replaying
            asyncio.events._set_running_loop(previous)
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise QueryError(f"query {name!r} returned an awaitable", type="QueryFailed")
        if len(self._commands) != before:
            del self._commands[before:]
            raise QueryError(
                f"query {name!r} tried to schedule work; queries must be read-only",
                type="QueryFailed",
            )
        return self.converter.encode(result)

    # ------------------------------------------------------------------ updates

    def set_update_handler(self, name: str, handler: Callable[..., Any]) -> None:
        self._update_handlers[name] = handler
        for event in self._buffered_updates.pop(name, []):
            self._start_update(event, handler)

    def _deliver_update(self, event: UpdateRequested) -> None:
        if event.update_id in self._handled_updates:
            return  # the same request delivered twice
        self._handled_updates.add(event.update_id)
        handler = self._update_handlers.get(event.name)
        if handler is None:
            # The handler may be registered later in this activation (for example when
            # the update arrives together with WorkflowStarted).
            self._buffered_updates.setdefault(event.name, []).append(event)
            return
        self._start_update(event, handler)

    def _start_update(self, event: UpdateRequested, handler: Callable[..., Any]) -> None:
        task = self.loop.create_task(
            self._run_update(event, handler), name=f"update-{event.update_id}"
        )
        self._running_updates[event.update_id] = task

    async def _run_update(self, event: UpdateRequested, handler: Callable[..., Any]) -> None:
        try:
            values = self.converter.decode_args(handler, event.args)
            result = handler(*values)
            if inspect.isawaitable(result):
                result = await result
            payload = self.converter.encode(result)
        except asyncio.CancelledError:
            self._emit(
                CompleteUpdate(
                    update_id=event.update_id,
                    failure=FailureInfo(
                        message="update was cancelled", type="Cancelled", kind="canceled"
                    ),
                )
            )
            raise
        except Exception as exc:
            self._emit(
                CompleteUpdate(update_id=event.update_id, failure=FailureInfo.from_exception(exc))
            )
        else:
            self._emit(CompleteUpdate(update_id=event.update_id, result=payload))
        finally:
            self._running_updates.pop(event.update_id, None)

    def _fail_outstanding_updates(self) -> None:
        """The run is closing: answer every update that will never be handled."""
        for events in self._buffered_updates.values():
            for event in events:
                self._emit(
                    CompleteUpdate(
                        update_id=event.update_id,
                        failure=FailureInfo(
                            message=f"workflow closed without handling update {event.name!r}",
                            type="UpdateNotHandled",
                        ),
                    )
                )
        self._buffered_updates.clear()
        for update_id in list(self._running_updates):
            self._emit(
                CompleteUpdate(
                    update_id=update_id,
                    failure=FailureInfo(
                        message="workflow closed while the update was running",
                        type="UpdateNotHandled",
                    ),
                )
            )
        self._running_updates.clear()

    # ------------------------------------------------------------------ children

    def start_child(
        self,
        definition: WorkflowDefinition[Any, Any],
        args: Sequence[Any],
        *,
        workflow_id: str | None,
        parent_close_policy: str,
    ) -> tuple[asyncio.Future[Any], str, str]:
        payloads = self.converter.encode_args(args)
        self._child_seq += 1
        seq = self._child_seq
        run_id = f"{self.info.run_id}.c{seq}"
        child_workflow_id = workflow_id or f"{self.info.workflow_id}/{definition.name}/{seq}"
        future: asyncio.Future[Any] = self.loop.create_future()
        self._children[seq] = _PendingStep(definition.name, future, return_hint(definition.fn))
        self._emit(
            ScheduleChild(
                seq=seq,
                workflow_name=definition.name,
                workflow_id=child_workflow_id,
                run_id=run_id,
                args=payloads,
                parent_close_policy=parent_close_policy,
            )
        )
        return future, child_workflow_id, run_id

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
