"""Class-based workflows: state in attributes, handlers as methods.

A workflow with a few signals, queries and updates reads more naturally as a class than
as one function with nested handlers::

    @lw.workflow(name="shop.cart")
    class Cart:
        def __init__(self, customer_id: str) -> None:
            self.items: list[str] = []
            self.checked_out = False

        @lw.run
        async def run(self, customer_id: str) -> list[str]:
            await lw.wait_condition(lambda: self.checked_out)
            return self.items

        @lw.signal("add")
        def add(self, item: str) -> None:
            self.items.append(item)

        @lw.query
        def count(self) -> int:
            return len(self.items)

        @lw.update
        async def checkout(self) -> int:
            self.checked_out = True
            return len(self.items)

This module adds no new runtime concepts. The decorated class becomes a
:class:`WorkflowClass`, an ordinary :class:`~loomwork.sdk.definitions.WorkflowDefinition`
whose entry function creates the instance, registers the query and update methods with
:func:`~loomwork.query` and :func:`~loomwork.update`, and feeds every signal method from
its :class:`~loomwork.Channel` in a workflow task. Histories are therefore identical to
those of the equivalent function-style workflow.

Semantics worth knowing:

* The workflow's arguments go to the ``@run`` method, and also to ``__init__`` when it
  accepts parameters, so handlers that fire before ``run`` starts see initialised state.
* Messages of one signal are handled one at a time, in arrival order; an ``async``
  handler finishes before the next message of that signal is handled.
* :func:`wait_condition` re-evaluates its predicate whenever a signal or update handler
  finishes, so a condition over state those handlers change never needs polling.
* The decorator returns the class itself (so type checkers and IDEs keep seeing an
  ordinary class) with its :class:`WorkflowClass` definition attached; every API that
  takes a workflow accepts the class. On the client, the handler methods name their
  handlers: ``handle.signal(Cart.add, "book")`` sends to the channel (validated against
  the handler's parameter type), ``handle.query(Cart.count)`` and
  ``handle.update(Cart.checkout)`` name the query and the update.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import inspect
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, get_type_hints

from loomwork.errors import RegistrationError
from loomwork.sdk.conditions import conditions_of_current, notify_conditions, wait_condition
from loomwork.sdk.definitions import WorkflowDefinition
from loomwork.sdk.handlers import (
    CHANNEL_ATTRIBUTE,
    DEFINITION_ATTRIBUTE,
    HANDLER_ATTRIBUTE,
    run,
    signal,
)
from loomwork.sdk.messaging import Channel
from loomwork.sdk.messaging import query as register_query
from loomwork.sdk.messaging import update as register_update

T = TypeVar("T")
C = TypeVar("C", bound=type)

_RUN = "run"
_SIGNAL = "signal"
_QUERY = "query"
_UPDATE = "update"

# --------------------------------------------------------------------------- classes


@dataclass(frozen=True, slots=True)
class HandlerSpec:
    """One handler method of a workflow class."""

    kind: str
    name: str
    attribute: str
    function: Callable[..., Any]


def _signal_type(cls: type, attribute: str, fn: Callable[..., Any]) -> Any:
    params = list(inspect.signature(fn).parameters.values())[1:]
    if len(params) != 1 or params[0].kind not in (
        params[0].POSITIONAL_ONLY,
        params[0].POSITIONAL_OR_KEYWORD,
    ):
        raise RegistrationError(
            f"signal handler {cls.__qualname__}.{attribute} must take exactly one message "
            "argument besides self"
        )
    try:
        hints = get_type_hints(fn)
    except (NameError, TypeError):
        hints = {}
    return hints.get(params[0].name, Any)


def collect_handlers(cls: type) -> tuple[HandlerSpec, list[HandlerSpec]]:
    """The ``@run`` method and the other handlers of ``cls``, in definition order.

    Raises:
        RegistrationError: no or several ``@run`` methods, or two handlers of one kind
            with the same name.
    """
    runs: list[HandlerSpec] = []
    handlers: list[HandlerSpec] = []
    seen: dict[tuple[str, str], str] = {}
    for klass in reversed(cls.__mro__[:-1]):
        for attribute, value in vars(klass).items():
            marker = getattr(value, HANDLER_ATTRIBUTE, None) if inspect.isfunction(value) else None
            if marker is None:
                continue
            kind, name = marker
            if kind == _RUN:
                runs = [r for r in runs if r.attribute != attribute]
                runs.append(HandlerSpec(kind, "", attribute, value))
                continue
            key = (kind, name)
            if key in seen and seen[key] != attribute:
                raise RegistrationError(
                    f"{cls.__qualname__}: {kind} {name!r} is handled by both "
                    f"{seen[key]!r} and {attribute!r}"
                )
            seen[key] = attribute
            handlers = [h for h in handlers if h.attribute != attribute]
            handlers.append(HandlerSpec(kind, name, attribute, value))
    if len(runs) != 1:
        found = ", ".join(r.attribute for r in runs) or "none"
        raise RegistrationError(
            f"workflow class {cls.__qualname__} needs exactly one @run method (found: {found})"
        )
    return runs[0], handlers


def _init_takes_arguments(cls: type) -> bool:
    try:
        params = inspect.signature(cls).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD) for p in params)


class WorkflowClass(WorkflowDefinition[..., T], Generic[T]):
    """The definition of a workflow written as a class; see the module documentation.

    ``@workflow`` attaches it to the class; :func:`~loomwork.sdk.definitions.as_definition`
    finds it again.
    """

    def __init__(self, cls: type, *, name: str | None, fail_on: tuple[type[BaseException], ...]):
        run_spec, handlers = collect_handlers(cls)
        self.cls = cls
        self.handlers: tuple[HandlerSpec, ...] = tuple(handlers)
        self.channels: dict[str, Channel[Any]] = {
            h.attribute: Channel(h.name, _signal_type(cls, h.attribute, h.function))
            for h in handlers
            if h.kind == _SIGNAL
        }
        for handler in handlers:
            if handler.kind == _SIGNAL:
                # Lets ``handle.signal(Cart.add, value)`` find the typed channel.
                setattr(handler.function, CHANNEL_ATTRIBUTE, self.channels[handler.attribute])
        entry = self._entry_function(cls, run_spec)
        super().__init__(entry, name=name or cls.__name__, fail_on=fail_on)
        self.__doc__ = cls.__doc__

    def _entry_function(
        self, cls: type, run_spec: HandlerSpec
    ) -> Callable[..., Coroutine[Any, Any, T]]:
        pass_to_init = _init_takes_arguments(cls)
        definition = self

        async def entry(*args: Any) -> T:
            obj = cls(*args) if pass_to_init else cls()
            pumps = definition._install_handlers(obj)
            try:
                result: T = await getattr(obj, run_spec.attribute)(*args)
            finally:
                for pump in pumps:
                    pump.cancel()
            return result

        functools.update_wrapper(entry, run_spec.function)
        # The entry function is not itself a handler method.
        entry.__dict__.pop(HANDLER_ATTRIBUTE, None)
        signature = inspect.signature(run_spec.function)
        entry.__signature__ = signature.replace(  # type: ignore[attr-defined]
            parameters=list(signature.parameters.values())[1:]
        )
        entry.__qualname__ = f"{cls.__qualname__}.{run_spec.attribute}"
        return entry

    def _install_handlers(self, obj: Any) -> list[asyncio.Task[None]]:
        pumps: list[asyncio.Task[None]] = []
        for handler in self.handlers:
            bound = getattr(obj, handler.attribute)
            if handler.kind == _QUERY:
                register_query(handler.name)(bound)
            elif handler.kind == _UPDATE:
                register_update(handler.name)(bound)
            else:
                channel = self.channels[handler.attribute]
                pumps.append(asyncio.ensure_future(_pump(channel, bound)))
        return pumps

    @property
    def fingerprint(self) -> str:
        """Covers every method of the class, not only ``@run``: a change to any handler
        changes what the workflow does."""
        if self._fingerprint is None:
            from loomwork.versioning.fingerprint import FINGERPRINT_LENGTH, code_fingerprint

            digest = hashlib.sha256()
            for attribute, value in sorted(_class_functions(self.cls).items()):
                digest.update(attribute.encode())
                marker = getattr(value, HANDLER_ATTRIBUTE, None)
                digest.update(repr(marker).encode())
                digest.update(code_fingerprint(value).encode())
            self._fingerprint = digest.hexdigest()[:FINGERPRINT_LENGTH]
        return self._fingerprint

    def __repr__(self) -> str:
        return f"<WorkflowClass {self.name!r} ({self.cls.__qualname__})>"


def _class_functions(cls: type) -> dict[str, Callable[..., Any]]:
    functions: dict[str, Callable[..., Any]] = {}
    for klass in reversed(cls.__mro__[:-1]):
        for attribute, value in vars(klass).items():
            target = value.__func__ if isinstance(value, staticmethod | classmethod) else value
            if inspect.isfunction(target):
                functions[attribute] = target
    return functions


async def _pump(channel: Channel[Any], handler: Callable[[Any], Any]) -> None:
    conditions = conditions_of_current()
    while True:
        message = await channel.receive()
        try:
            outcome = handler(message)
            if inspect.isawaitable(outcome):
                await outcome
        finally:
            conditions.notify()


def workflow_class(
    cls: C, *, name: str | None = None, fail_on: tuple[type[BaseException], ...] = ()
) -> C:
    """Attach a :class:`WorkflowClass` definition to ``cls`` and return ``cls``
    (``@lw.workflow`` on a class calls this)."""
    is_class = inspect.isclass(cls)
    if not is_class:
        raise RegistrationError(f"{cls!r} is not a class")
    setattr(cls, DEFINITION_ATTRIBUTE, WorkflowClass(cls, name=name, fail_on=fail_on))
    return cls


__all__ = [
    "HANDLER_ATTRIBUTE",
    "WorkflowClass",
    "notify_conditions",
    "run",
    "signal",
    "wait_condition",
    "workflow_class",
]
