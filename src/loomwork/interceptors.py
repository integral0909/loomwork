"""Typed middleware for step attempts and outbound client calls.

Two extension points, both optional and additive:

``StepInterceptor``
    Wraps one step attempt on the worker side: see the step's identity, arguments and
    propagated headers, run code before and after it, and observe (without suppressing)
    exceptions. Useful for injecting an auth context, tagging metrics, or opening a
    logging scope. Composed like :class:`~loomwork.observability.telemetry.
    CompositeTelemetry`: each interceptor is a context manager entered around the
    attempt, outermost first.

``ClientInterceptor``
    Wraps one outbound call (``start``, ``signal_with_start``, ``signal``, ``update``)
    made through :class:`~loomwork.client.client.Client` or a
    :class:`~loomwork.client.handle.WorkflowHandle`. It sees a mutable ``headers`` dict
    it may add to before the call is sent. For ``start`` and ``signal_with_start`` those
    headers become the new run's ``WorkflowStarted.headers`` and are what makes a caller's
    trace the parent of the workflow's steps (see
    :mod:`loomwork.observability.propagation`); a ``signal`` or ``update`` against an
    already-running workflow has no event that carries headers into its history (see
    ``docs/guides/interceptors.md``), so there they are visible only to this call's own
    interceptors, for example to close a client-side span around it.

There is deliberately no general **workflow-side** inbound interceptor. Workflow code
must produce the same commands on every replay; a hook that ran arbitrary application
code around a workflow activation would be an easy way to break that without realising
it (checking a feature flag from the environment, or timing out sooner on one replay
than another, would both pass code review while corrupting a run's history the first
time the interceptor's decision differs from what it decided originally). Read
propagated context from workflow code with ``loomwork.info().headers`` or
:func:`loomwork.current_trace_context` instead, which are pure functions
of the recorded history and therefore replay-safe by construction.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from loomwork.info import StepInfo

# --------------------------------------------------------------------------- steps


@dataclass(frozen=True, slots=True)
class StepAttemptContext:
    """What a :class:`StepInterceptor` can see about the attempt it wraps."""

    info: StepInfo
    args: tuple[Any, ...]
    headers: Mapping[str, str]


class StepInterceptor(Protocol):
    def intercept_step(self, ctx: StepAttemptContext) -> contextlib.AbstractContextManager[None]:
        """Wrap one step attempt.

        Entered just before the step function runs and exited just after it returns or
        raises. Implement with ``@contextlib.contextmanager``; code before ``yield``
        runs first, code after (in a ``finally``, or in an ``except`` that re-raises)
        runs last. Do not swallow the exception: returning a truthy value from
        ``__exit__`` breaks retry and failure reporting.
        """
        ...


def wrap_step(
    interceptors: Sequence[StepInterceptor], ctx: StepAttemptContext
) -> contextlib.AbstractContextManager[None]:
    """Compose ``interceptors`` around one attempt, outermost first."""

    @contextlib.contextmanager
    def _wrapped() -> Iterator[None]:
        with contextlib.ExitStack() as stack:
            for interceptor in interceptors:
                stack.enter_context(interceptor.intercept_step(ctx))
            yield

    return _wrapped()


# --------------------------------------------------------------------------- client


CallKind = Literal["start", "signal_with_start", "signal", "update"]


@dataclass(slots=True)
class ClientCallContext:
    """One outbound call. ``headers`` is mutable; interceptors add to it before the
    call is sent (see the module docstring for what happens to it per ``kind``)."""

    kind: CallKind
    workflow_name: str | None
    workflow_id: str | None
    headers: dict[str, str]


class ClientInterceptor(Protocol):
    def intercept_call(self, ctx: ClientCallContext) -> contextlib.AbstractContextManager[None]:
        """Wrap one outbound call. Same shape as :meth:`StepInterceptor.intercept_step`:
        mutate ``ctx.headers`` before ``yield``, observe the outcome after it."""
        ...


def wrap_call(
    interceptors: Sequence[ClientInterceptor], ctx: ClientCallContext
) -> contextlib.AbstractContextManager[None]:
    """Compose ``interceptors`` around one outbound call, outermost first."""

    @contextlib.contextmanager
    def _wrapped() -> Iterator[None]:
        with contextlib.ExitStack() as stack:
            for interceptor in interceptors:
                stack.enter_context(interceptor.intercept_call(ctx))
            yield

    return _wrapped()


__all__ = [
    "CallKind",
    "ClientCallContext",
    "ClientInterceptor",
    "StepAttemptContext",
    "StepInterceptor",
    "wrap_call",
    "wrap_step",
]
