"""Communicating with running workflows, and composing workflows.

**Channels** are typed mailboxes. Declare one next to the workflow that reads it::

    approvals = lw.Channel("approvals", Approval)

    @lw.workflow
    async def expense_report(report: Report) -> str:
        decision = await approvals.receive()      # durable wait
        ...

    await handle.signal(approvals, Approval(approved=True, by="kim"))

Messages are validated against the channel's type when sent and decoded into it when
received. Messages that arrive before the workflow asks for them are buffered.

**Queries** read workflow state without changing it; **updates** change it and return a
result to the caller. Both are registered from inside the workflow body, so replay
re-registers them::

    @lw.workflow
    async def cart() -> None:
        items: list[str] = []

        @lw.query("items")
        def list_items() -> list[str]:
            return items

        @lw.update("add")
        async def add(item: str) -> int:
            await reserve_stock(item)   # updates may run steps
            items.append(item)
            return len(items)
        ...

**Child workflows** are started with :func:`start_child`, or simply by calling another
workflow from workflow code (which starts it and waits for its result).
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Generator, Sequence
from typing import Any, Generic, TypeVar, cast, overload

from pydantic import TypeAdapter, ValidationError

from loomwork.errors import PayloadError
from loomwork.options import IdConflictPolicy, IdReusePolicy, ParentClosePolicy, VersioningPolicy
from loomwork.runtime.context import current_instance, current_instance_or_none
from loomwork.sdk.conditions import notifying
from loomwork.sdk.definitions import (
    WorkflowDefinition,
    WorkflowLike,
    as_definition,
    bind_positional,
)
from loomwork.sdk.handlers import CHANNEL_ATTRIBUTE, is_method, mark_query, mark_update

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.:\-]{0,99}$")


def _validate_name(name: str, what: str) -> str:
    if not _NAME_RE.match(name):
        raise ValueError(f"invalid {what} name {name!r}: use letters, digits and _ . : - (max 100)")
    return name


class ChannelEmpty(Exception):
    """:meth:`Channel.receive_nowait` found no buffered message."""


# --------------------------------------------------------------------------- channels


class Channel(Generic[T]):
    """A typed, named mailbox of a workflow."""

    @overload
    def __init__(self: Channel[Any], name: str) -> None: ...

    @overload
    def __init__(self, name: str, type_: type[T]) -> None: ...

    def __init__(self, name: str, type_: Any = Any) -> None:
        self.name = _validate_name(name, "channel")
        self.type = type_
        self._adapter: TypeAdapter[Any] | None = None

    def __repr__(self) -> str:
        type_name = getattr(self.type, "__name__", repr(self.type))
        return f"Channel({self.name!r}, {type_name})"

    def validate(self, value: Any) -> T:
        """Check ``value`` against the channel type (used when sending)."""
        if self.type is Any:
            return value  # type: ignore[no-any-return]
        if self._adapter is None:
            self._adapter = TypeAdapter(self.type)
        try:
            validated: T = self._adapter.validate_python(value)
        except ValidationError as exc:
            raise PayloadError(f"message does not match channel {self.name!r}: {exc}") from exc
        return validated

    # Workflow side --------------------------------------------------------

    def receive(self) -> Awaitable[T]:
        """Wait for the next message. *Workflow-only.*

        Combine with ``asyncio.timeout`` for a durable deadline::

            async with asyncio.timeout(3600):
                decision = await approvals.receive()
        """
        return current_instance().receive(self.name, self.type)

    def receive_nowait(self) -> T:
        """The next buffered message, or :class:`ChannelEmpty`. *Workflow-only.*"""
        instance = current_instance()
        if not instance.pending_messages(self.name):
            raise ChannelEmpty(self.name)
        future = instance.receive(self.name, self.type)
        result: T = future.result()
        return result

    @property
    def pending(self) -> int:
        """Messages received but not yet consumed. *Workflow-only.*"""
        return current_instance().pending_messages(self.name)

    async def stream(self) -> AsyncIterator[T]:
        """Receive messages forever (use ``break`` or a timeout to stop). *Workflow-only.*"""
        while True:
            yield await self.receive()

    def send_to(self, workflow_id: str, value: T, *, message_id: str | None = None) -> None:
        """Send a message to another workflow's channel. *Workflow-only.*

        Delivery is asynchronous and exactly-once from the recording's point of view:
        the send is recorded in this workflow's history and delivered by the engine.
        """
        signal_external(workflow_id, self, value, message_id=message_id)


def resolve_signal(reference: Channel[Any] | str | Callable[..., Any]) -> Channel[Any] | str:
    """A channel given as a :class:`Channel`, a name, or a signal method of a workflow class
    (``Cart.add``)."""
    if isinstance(reference, Channel | str):
        return reference
    channel = getattr(reference, CHANNEL_ATTRIBUTE, None)
    if isinstance(channel, Channel):
        return channel
    raise TypeError(
        f"{reference!r} is not a channel or a signal handler method of a @workflow class"
    )


def signal_external(
    workflow_id: str,
    channel: Channel[Any] | str | Callable[..., Any],
    value: Any,
    *,
    message_id: str | None = None,
) -> None:
    """Send ``value`` to ``channel`` of the running workflow ``workflow_id``. *Workflow-only.*

    ``channel`` may also be a signal method of a workflow class (``Cart.add``).
    """
    instance = current_instance()
    channel = resolve_signal(channel)
    if isinstance(channel, Channel):
        value = channel.validate(value)
        name = channel.name
    else:
        name = _validate_name(channel, "channel")
    instance.signal_external(workflow_id, name, value, message_id)


# --------------------------------------------------------------------------- queries / updates


@overload
def query(name: F, /) -> F: ...


@overload
def query(name: str | None = None, /) -> Callable[[F], F]: ...


def query(name: str | F | None = None, /) -> F | Callable[[F], F]:
    """Register a query handler for the running workflow. *Workflow-only.*

    Use as ``@lw.query`` or ``@lw.query("name")`` on a nested function, or on a method
    of a class-based workflow (see :mod:`loomwork.sdk.classes`). Handlers must be plain
    (not async) functions that read state; anything that would schedule work is rejected.
    """

    def register(fn: F, handler_name: str | None) -> F:
        if current_instance_or_none() is None and is_method(fn):
            return mark_query(fn, handler_name)
        current_instance().set_query_handler(
            _validate_name(handler_name or fn.__name__, "query"), fn
        )
        return fn

    if callable(name):
        return register(cast(F, name), None)
    return lambda fn: register(fn, name)


@overload
def update(name: F, /) -> F: ...


@overload
def update(name: str | None = None, /) -> Callable[[F], F]: ...


def update(name: str | F | None = None, /) -> F | Callable[[F], F]:
    """Register an update handler for the running workflow. *Workflow-only.*

    The handler runs as workflow code (it may be async, run steps and sleep). Its return
    value, or the error it raises, is recorded and delivered to the caller; an error
    does not fail the workflow. Also usable on a method of a class-based workflow.
    """

    def register(fn: F, handler_name: str | None) -> F:
        if current_instance_or_none() is None and is_method(fn):
            return mark_update(fn, handler_name)
        current_instance().set_update_handler(
            _validate_name(handler_name or fn.__name__, "update"), notifying(fn)
        )
        return fn

    if callable(name):
        return register(cast(F, name), None)
    return lambda fn: register(fn, name)


# --------------------------------------------------------------------------- children


class ChildHandle(Generic[T]):
    """A child run started by :func:`start_child`. Await it for the child's result."""

    def __init__(self, future: asyncio.Future[T], workflow_id: str, run_id: str) -> None:
        self._future = future
        self.workflow_id = workflow_id
        self.run_id = run_id

    def __await__(self) -> Generator[Any, None, T]:
        return self._future.__await__()

    async def result(self) -> T:
        return await self._future

    def signal(self, channel: Channel[Any] | str | Callable[..., Any], value: Any) -> None:
        signal_external(self.workflow_id, channel, value)

    def __repr__(self) -> str:
        return f"ChildHandle(workflow_id={self.workflow_id!r}, run_id={self.run_id!r})"


@overload
def start_child(
    workflow: WorkflowDefinition[..., T],
    *args: Any,
    id: str | None = None,
    parent_close_policy: ParentClosePolicy | str = ParentClosePolicy.TERMINATE,
    id_reuse_policy: IdReusePolicy | str = IdReusePolicy.ALLOW_DUPLICATE,
    id_conflict_policy: IdConflictPolicy | str = IdConflictPolicy.FAIL,
    versioning_policy: VersioningPolicy | str | None = None,
) -> ChildHandle[T]: ...


@overload
def start_child(
    workflow: type,
    *args: Any,
    id: str | None = None,
    parent_close_policy: ParentClosePolicy | str = ParentClosePolicy.TERMINATE,
    id_reuse_policy: IdReusePolicy | str = IdReusePolicy.ALLOW_DUPLICATE,
    id_conflict_policy: IdConflictPolicy | str = IdConflictPolicy.FAIL,
    versioning_policy: VersioningPolicy | str | None = None,
) -> ChildHandle[Any]: ...


def start_child(
    workflow: WorkflowLike,
    *args: Any,
    id: str | None = None,
    parent_close_policy: ParentClosePolicy | str = ParentClosePolicy.TERMINATE,
    id_reuse_policy: IdReusePolicy | str = IdReusePolicy.ALLOW_DUPLICATE,
    id_conflict_policy: IdConflictPolicy | str = IdConflictPolicy.FAIL,
    versioning_policy: VersioningPolicy | str | None = None,
) -> ChildHandle[Any]:
    """Start ``workflow`` as a child of the running workflow. *Workflow-only.*

    The child is created atomically with the parent's next tick. Its workflow id
    defaults to ``<parent workflow id>/<child name>/<n>``; its run id is derived from the
    parent's run id, so both are known immediately and stable across replays.

    ``parent_close_policy`` decides what happens to the child if the parent closes
    first: ``terminate`` (default), ``request_cancel`` or ``abandon``.

    ``id_reuse_policy`` and ``id_conflict_policy`` govern the child's workflow id exactly
    as the same-named arguments do for :meth:`~loomwork.engine.engine.Engine.
    start_workflow` (see :class:`~loomwork.options.IdReusePolicy` and
    :class:`~loomwork.options.IdConflictPolicy`), with one difference: since a child
    cannot raise into the caller the way starting a top-level run can, every rejection
    (``FAIL`` on a running conflict, ``REJECT_DUPLICATE``/``ALLOW_DUPLICATE_FAILED_ONLY``
    on reuse) surfaces the same way an ordinary child failure does -- ``ChildHandle``
    raises :class:`~loomwork.errors.ChildWorkflowError` when awaited. ``USE_EXISTING``
    attaches this call to whatever run currently has the target workflow id (which need
    not be one of this workflow's own children, or a child at all) and delivers its
    eventual result here, without creating a new run or affecting the existing run's own
    parent-close policy; ``TERMINATE_EXISTING`` terminates it and starts the child in its
    place, atomically. See ``docs/guides/composition.md``.

    ``versioning_policy`` overrides :attr:`~loomwork.engine.config.EngineConfig.
    workflow_versioning_policy`/``default_versioning_policy`` for this child only, exactly
    as the same argument does for ``start_workflow``; unset, the child resolves its
    policy fresh from engine configuration, independent of the parent's own policy (see
    ``docs/adr/0006-worker-build-versioning.md``).
    """
    policy = ParentClosePolicy(parent_close_policy)
    reuse = IdReusePolicy(id_reuse_policy)
    conflict = IdConflictPolicy(id_conflict_policy)
    version = None if versioning_policy is None else VersioningPolicy(versioning_policy)
    instance = current_instance()
    definition = as_definition(workflow)
    positional = bind_positional(definition.fn, args, {})
    future, workflow_id, run_id = instance.start_child(
        definition,
        positional,
        workflow_id=id,
        parent_close_policy=policy.value,
        id_reuse_policy=reuse.value,
        id_conflict_policy=conflict.value,
        versioning_policy=version.value if version is not None else "",
    )
    return ChildHandle(future, workflow_id, run_id)


# --------------------------------------------------------------------------- sagas


class Compensations:
    """A stack of undo actions for the saga pattern.

    Register a compensation after each step that succeeds; if a later step fails, run
    them in reverse order::

        undo = lw.Compensations()
        try:
            charge_id = await charge(order)
            undo.add(refund, charge_id)
            await reserve(order)
            undo.add(release, order.id)
            await ship(order)
        except Exception:
            await undo.run()
            raise

    Compensations are ordinary step (or async) calls, so they are durable and retried
    like any other step. Failures of individual compensations are collected rather than
    stopping the rollback; :meth:`run` raises an ``ExceptionGroup`` afterwards if any
    compensation failed.
    """

    def __init__(self, *, parallel: bool = False) -> None:
        self._actions: list[tuple[Callable[..., Awaitable[Any]], Sequence[Any]]] = []
        self.parallel = parallel

    def add(self, action: Callable[..., Awaitable[Any]], *args: Any) -> None:
        self._actions.append((action, args))

    def __len__(self) -> int:
        return len(self._actions)

    async def run(self) -> None:
        actions = list(reversed(self._actions))
        self._actions.clear()
        errors: list[Exception] = []
        if self.parallel:
            results = await asyncio.gather(
                *(action(*args) for action, args in actions), return_exceptions=True
            )
            errors = [r for r in results if isinstance(r, Exception)]
        else:
            for action, args in actions:
                try:
                    await action(*args)
                except Exception as exc:
                    errors.append(exc)
        if errors:
            raise ExceptionGroup("compensation failed", errors)
