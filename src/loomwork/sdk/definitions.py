"""Workflow and step definitions.

.. code-block:: python

    @step(retry=RetryPolicy(max_attempts=3), timeout=30)
    async def charge_card(order_id: str, amount_cents: int) -> str: ...

    @workflow(name="orders.process")
    async def process_order(order: Order) -> Receipt:
        charge_id = await charge_card(order.id, order.total_cents)
        ...

Calling a step *inside* a workflow schedules it durably and returns an awaitable for its
recorded result. Calling it anywhere else simply runs the function, so steps remain
ordinary, unit-testable coroutines.

Names are part of the recorded history. They default to the function's ``__name__`` and
should be set explicitly for anything long-lived, so that renaming a Python function does
not break running workflows.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import re
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Sequence
from datetime import timedelta
from typing import Any, Generic, ParamSpec, TypeVar, overload

from loomwork.errors import FailureError, NotFoundError, RegistrationError
from loomwork.options import StepOptions, as_timedelta
from loomwork.retry import RetryPolicy
from loomwork.runtime.context import current_instance_or_none

P = ParamSpec("P")
R = TypeVar("R")

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.:\-/]{0,199}$")


def _validate_name(name: str, what: str) -> str:
    if not _NAME_RE.match(name):
        raise RegistrationError(
            f"invalid {what} name {name!r}: use letters, digits and _ . : - / "
            "(max 200 characters, not starting with a digit)"
        )
    return name


def bind_positional(
    fn: Callable[..., Any], args: Sequence[Any], kwargs: dict[str, Any]
) -> list[Any]:
    """Normalise a call to positional arguments, filling in defaults.

    Arguments are recorded positionally in the history. Keyword-only parameters and
    ``**kwargs`` cannot be represented and are rejected when the call is made.
    """
    signature = inspect.signature(fn)
    bound = signature.bind(*args, **kwargs)
    bound.apply_defaults()
    if bound.kwargs:
        raise TypeError(
            f"{getattr(fn, '__qualname__', fn)}: keyword-only parameters and **kwargs are not "
            f"supported for durable calls (got {sorted(bound.kwargs)})"
        )
    return list(bound.args)


def _check_signature(fn: Callable[..., Any], what: str) -> None:
    for param in inspect.signature(fn).parameters.values():
        if param.kind in (param.KEYWORD_ONLY, param.VAR_KEYWORD):
            raise RegistrationError(
                f"{what} {fn.__qualname__}: parameter {param.name!r} is keyword-only; durable "
                "calls record arguments positionally"
            )


# --------------------------------------------------------------------------- workflows


class WorkflowDefinition(Generic[P, R]):
    """A registered workflow function."""

    def __init__(
        self,
        fn: Callable[P, Coroutine[Any, Any, R]],
        *,
        name: str | None = None,
        fail_on: Iterable[type[BaseException]] = (),
    ) -> None:
        if not inspect.iscoroutinefunction(fn):
            raise RegistrationError(f"workflow {fn!r} must be an 'async def' function")
        _check_signature(fn, "workflow")
        self.fn = fn
        self.name = _validate_name(name or fn.__name__, "workflow")
        self.fail_on: tuple[type[BaseException], ...] = tuple(fail_on)
        functools.update_wrapper(self, fn)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
        return self.fn(*args, **kwargs)

    def bind(self, args: Sequence[Any], kwargs: dict[str, Any] | None = None) -> list[Any]:
        return bind_positional(self.fn, args, kwargs or {})

    def is_failure(self, exc: BaseException) -> bool:
        """Whether ``exc`` escaping the workflow fails the run (instead of the tick)."""
        return isinstance(exc, (FailureError, TimeoutError, *self.fail_on))

    def __repr__(self) -> str:
        return f"<WorkflowDefinition {self.name!r}>"


@overload
def workflow(fn: Callable[P, Coroutine[Any, Any, R]], /) -> WorkflowDefinition[P, R]: ...


@overload
def workflow(
    *,
    name: str | None = None,
    fail_on: Iterable[type[BaseException]] = (),
) -> Callable[[Callable[P, Coroutine[Any, Any, R]]], WorkflowDefinition[P, R]]: ...


def workflow(
    fn: Callable[P, Coroutine[Any, Any, R]] | None = None,
    /,
    *,
    name: str | None = None,
    fail_on: Iterable[type[BaseException]] = (),
) -> (
    WorkflowDefinition[P, R]
    | Callable[[Callable[P, Coroutine[Any, Any, R]]], WorkflowDefinition[P, R]]
):
    """Declare a workflow.

    Args:
        name: stable name recorded in histories; defaults to the function name.
        fail_on: extra exception types that fail the run when they escape the workflow.
            :class:`~loomwork.ApplicationError`, :class:`~loomwork.StepError` and
            :class:`TimeoutError` always do. Any other exception fails only the current
            tick, which is retried; this lets a fixed deploy resume the run.
    """

    def decorate(f: Callable[P, Coroutine[Any, Any, R]]) -> WorkflowDefinition[P, R]:
        return WorkflowDefinition(f, name=name, fail_on=fail_on)

    if fn is not None:
        return decorate(fn)
    return decorate


# --------------------------------------------------------------------------- steps


class StepDefinition(Generic[P, R]):
    """A registered step function (``async def`` or plain ``def``).

    Synchronous steps run in a worker thread so they can use blocking libraries.
    """

    def __init__(
        self,
        fn: Callable[P, Awaitable[R]] | Callable[P, R],
        *,
        name: str | None = None,
        options: StepOptions | None = None,
    ) -> None:
        if not callable(fn):
            raise RegistrationError(f"step {fn!r} is not callable")
        _check_signature(fn, "step")
        self.fn = fn
        self.name = _validate_name(name or fn.__name__, "step")
        self.options = options or StepOptions()
        self.is_async = inspect.iscoroutinefunction(fn)
        functools.update_wrapper(self, fn)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Awaitable[R]:
        instance = current_instance_or_none()
        positional = bind_positional(self.fn, args, kwargs)
        if instance is None:
            return self.invoke(positional)
        future: Awaitable[R] = instance.schedule_step(self, positional, self.options)
        return future

    async def invoke(self, args: Sequence[Any]) -> R:
        """Run the step function directly (used by step workers and outside workflows)."""
        if self.is_async:
            return await self.fn(*args)  # type: ignore[no-any-return, misc]
        return await asyncio.to_thread(self.fn, *args)  # type: ignore[arg-type]

    def with_options(
        self,
        *,
        retry: RetryPolicy | None = None,
        timeout: timedelta | float | None = None,
        total_timeout: timedelta | float | None = None,
    ) -> StepDefinition[P, R]:
        """A copy of this step with some options overridden for one call site."""
        override = StepOptions(
            retry=retry, timeout=as_timedelta(timeout), total_timeout=as_timedelta(total_timeout)
        )
        return StepDefinition(self.fn, name=self.name, options=self.options.merged(override))

    def __repr__(self) -> str:
        return f"<StepDefinition {self.name!r}>"


@overload
def step(fn: Callable[P, Coroutine[Any, Any, R]], /) -> StepDefinition[P, R]: ...


@overload
def step(fn: Callable[P, R], /) -> StepDefinition[P, R]: ...


@overload
def step(
    *,
    name: str | None = None,
    retry: RetryPolicy | None = None,
    timeout: timedelta | float | None = None,
    total_timeout: timedelta | float | None = None,
) -> _StepDecorator: ...


def step(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    name: str | None = None,
    retry: RetryPolicy | None = None,
    timeout: timedelta | float | None = None,
    total_timeout: timedelta | float | None = None,
) -> StepDefinition[Any, Any] | _StepDecorator:
    """Declare a step.

    Args:
        name: stable name recorded in histories; defaults to the function name.
        retry: retry policy; defaults to the engine's default policy.
        timeout: maximum duration of one attempt (seconds or ``timedelta``).
        total_timeout: maximum time from scheduling to final outcome, across retries.
    """
    options = StepOptions(
        retry=retry, timeout=as_timedelta(timeout), total_timeout=as_timedelta(total_timeout)
    )
    decorator = _StepDecorator(name, options)
    if fn is not None:
        return decorator(fn)
    return decorator


class _StepDecorator:
    def __init__(self, name: str | None, options: StepOptions) -> None:
        self._name = name
        self._options = options

    @overload
    def __call__(self, fn: Callable[P, Coroutine[Any, Any, R]], /) -> StepDefinition[P, R]: ...

    @overload
    def __call__(self, fn: Callable[P, R], /) -> StepDefinition[P, R]: ...

    def __call__(self, fn: Callable[..., Any], /) -> StepDefinition[Any, Any]:
        return StepDefinition(fn, name=self._name, options=self._options)


# --------------------------------------------------------------------------- registry


class Registry:
    """Maps names to workflow and step definitions for one worker or engine."""

    def __init__(
        self,
        workflows: Iterable[WorkflowDefinition[Any, Any]] = (),
        steps: Iterable[StepDefinition[Any, Any]] = (),
    ) -> None:
        self._workflows: dict[str, WorkflowDefinition[Any, Any]] = {}
        self._steps: dict[str, StepDefinition[Any, Any]] = {}
        for wf in workflows:
            self.add_workflow(wf)
        for st in steps:
            self.add_step(st)

    def add_workflow(self, definition: WorkflowDefinition[Any, Any]) -> None:
        if not isinstance(definition, WorkflowDefinition):
            raise RegistrationError(
                f"{definition!r} is not a workflow; decorate it with @loomwork.workflow"
            )
        existing = self._workflows.get(definition.name)
        if existing is not None and existing.fn is not definition.fn:
            raise RegistrationError(f"workflow name {definition.name!r} is registered twice")
        self._workflows[definition.name] = definition

    def add_step(self, definition: StepDefinition[Any, Any]) -> None:
        if not isinstance(definition, StepDefinition):
            raise RegistrationError(
                f"{definition!r} is not a step; decorate it with @loomwork.step"
            )
        existing = self._steps.get(definition.name)
        if existing is not None and existing.fn is not definition.fn:
            raise RegistrationError(f"step name {definition.name!r} is registered twice")
        self._steps[definition.name] = definition

    def workflow(self, name: str) -> WorkflowDefinition[Any, Any]:
        try:
            return self._workflows[name]
        except KeyError:
            raise NotFoundError(f"workflow {name!r} is not registered") from None

    def step(self, name: str) -> StepDefinition[Any, Any]:
        try:
            return self._steps[name]
        except KeyError:
            raise NotFoundError(f"step {name!r} is not registered") from None

    def has_workflow(self, name: str) -> bool:
        return name in self._workflows

    def has_step(self, name: str) -> bool:
        return name in self._steps

    @property
    def workflow_names(self) -> frozenset[str]:
        return frozenset(self._workflows)

    @property
    def step_names(self) -> frozenset[str]:
        return frozenset(self._steps)
