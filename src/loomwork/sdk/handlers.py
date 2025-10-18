"""Marking methods as handlers of a class-based workflow.

Kept apart from :mod:`loomwork.sdk.classes` so that :func:`loomwork.query` and
:func:`loomwork.update` (in :mod:`loomwork.sdk.messaging`) can mark methods without an
import cycle.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable
from typing import Any, TypeVar, overload

from loomwork.errors import RegistrationError

F = TypeVar("F", bound=Callable[..., Any])

#: The same rule as channel, query and update names everywhere else.
_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.:\-]{0,99}$")

#: Attribute set on methods by the handler decorators: ``(kind, name)``.
HANDLER_ATTRIBUTE = "__loomwork_handler__"
#: Attribute set on signal methods once their class is a workflow: the typed channel.
CHANNEL_ATTRIBUTE = "__loomwork_channel__"
#: Attribute set on a workflow class: its :class:`~loomwork.sdk.classes.WorkflowClass`.
DEFINITION_ATTRIBUTE = "__loomwork_workflow__"

_RUN = "run"
_SIGNAL = "signal"
_QUERY = "query"
_UPDATE = "update"


def _mark(fn: F, kind: str, name: str | None) -> F:
    if not callable(fn):
        raise RegistrationError(f"@{kind} must decorate a method, got {fn!r}")
    existing = getattr(fn, HANDLER_ATTRIBUTE, None)
    if existing is not None:
        raise RegistrationError(
            f"{fn.__qualname__} is already a {existing[0]} handler; a method can be only one"
        )
    handler_name = None if kind == _RUN else validate_handler_name(name or fn.__name__, kind)
    setattr(fn, HANDLER_ATTRIBUTE, (kind, handler_name))
    return fn


def is_method(fn: Callable[..., Any]) -> bool:
    """Whether ``fn`` looks like an instance method being defined in a class body."""
    try:
        params = list(inspect.signature(fn).parameters)
    except (TypeError, ValueError):
        return False
    return bool(params) and params[0] == "self" and "." in getattr(fn, "__qualname__", "")


def mark_query(fn: F, name: str | None) -> F:
    return _mark(fn, _QUERY, name)


def mark_update(fn: F, name: str | None) -> F:
    return _mark(fn, _UPDATE, name)


def run(fn: F) -> F:
    """Mark the entry point of a class-based workflow (exactly one per class)."""
    if not inspect.iscoroutinefunction(fn):
        raise RegistrationError(f"@run method {fn.__qualname__} must be 'async def'")
    return _mark(fn, _RUN, None)


@overload
def signal(name: F, /) -> F: ...


@overload
def signal(name: str | None = None, /) -> Callable[[F], F]: ...


def signal(name: str | F | None = None, /) -> F | Callable[[F], F]:
    """Mark a method of a class-based workflow as the handler of a signal channel.

    The method takes one argument, the message; its annotation is the channel type, and
    messages are validated against it when sent. Use as ``@lw.signal`` (the channel is
    named after the method) or ``@lw.signal("name")``.
    """
    if callable(name):
        return _mark(name, _SIGNAL, None)
    label = name

    def decorate(fn: F) -> F:
        return _mark(fn, _SIGNAL, label)

    return decorate


def validate_handler_name(name: str, what: str) -> str:
    if not _NAME_RE.match(name):
        raise ValueError(f"invalid {what} name {name!r}: use letters, digits and _ . : - (max 100)")
    return name


def handler_name(reference: str | Callable[..., Any], kind: str) -> str:
    """The name of a query or update given as a string or as a handler method
    (``Cart.count``)."""
    if isinstance(reference, str):
        return reference
    marker = getattr(reference, HANDLER_ATTRIBUTE, None)
    if marker is None or marker[0] != kind:
        raise TypeError(f"{reference!r} is not a {kind} handler method")
    name: str = marker[1]
    return name
