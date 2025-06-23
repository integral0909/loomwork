"""Replacing step implementations inside a :class:`~loomwork.testing.WorkflowEnvironment`.

Mocking never mutates the :class:`~loomwork.sdk.definitions.StepDefinition` objects a
test passes in: it installs a replacement under the same name in that environment's own
:class:`~loomwork.sdk.definitions.Registry`, so the change is invisible outside the
environment and the original object stays usable elsewhere (production code, other
tests running concurrently). Unmocked steps keep running their real implementation.
"""

from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass
from typing import Any

from loomwork.sdk.definitions import Registry, StepDefinition

UNSET: Any = object()
"""Sentinel distinguishing "no return value configured" from an explicit ``None``."""


@dataclass(slots=True)
class StepCall:
    """One recorded invocation of a mocked step, with its decoded positional arguments."""

    args: tuple[Any, ...]


class StepMock:
    """Handle returned by :meth:`~loomwork.testing.WorkflowEnvironment.mock_step`.

    Configure the effect with :meth:`configure` (done once by ``mock_step`` itself);
    inspect what happened through :attr:`calls`, :attr:`call_count` and the
    ``assert_*`` helpers below.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.calls: list[StepCall] = []
        self._return_value: Any = UNSET
        self._side_effect: Any = None
        self._side_effects: list[Any] | None = None

    def configure(self, *, return_value: Any = UNSET, side_effect: Any = None) -> None:
        """Set what the mock does. At most one of the two may be given.

        ``side_effect`` may be:

        * an exception instance or class, raised on every call (retries apply exactly
          as they would to a real step raising it);
        * a callable, invoked with the step's positional arguments; its return value
          (awaited first if it is a coroutine) becomes the step's result;
        * a list mixing any of the above, one outcome per call in order -- useful for
          "fails twice, then succeeds" retry tests.
        """
        if return_value is not UNSET and side_effect is not None:
            raise ValueError("pass at most one of return_value and side_effect")
        self._return_value = return_value
        if isinstance(side_effect, list | tuple):
            self._side_effects = list(side_effect)
            self._side_effect = None
        else:
            self._side_effects = None
            self._side_effect = side_effect

    @property
    def call_count(self) -> int:
        return len(self.calls)

    async def _invoke(self, args: tuple[Any, ...]) -> Any:
        self.calls.append(StepCall(args=args))
        if self._side_effects is not None:
            index = self.call_count - 1
            if index >= len(self._side_effects):
                raise AssertionError(
                    f"mocked step {self.name!r} was called {self.call_count} time(s) but its "
                    f"side_effect list only has {len(self._side_effects)} outcome(s)"
                )
            return await self._resolve(self._side_effects[index], args)
        if self._side_effect is not None:
            return await self._resolve(self._side_effect, args)
        if self._return_value is not UNSET:
            return self._return_value
        return None

    @staticmethod
    async def _resolve(effect: Any, args: tuple[Any, ...]) -> Any:
        if isinstance(effect, BaseException):
            raise effect
        if isinstance(effect, type) and issubclass(effect, BaseException):
            raise effect()
        if callable(effect):
            result = effect(*args)
            if inspect.isawaitable(result):
                result = await result
            return result
        return effect

    # ------------------------------------------------------------------ assertions

    def assert_called(self) -> None:
        if not self.calls:
            raise AssertionError(f"step {self.name!r} was never called")

    def assert_not_called(self) -> None:
        if self.calls:
            raise AssertionError(f"step {self.name!r} was called {self.call_count} time(s)")

    def assert_called_once(self) -> None:
        if self.call_count != 1:
            raise AssertionError(
                f"step {self.name!r} was called {self.call_count} time(s), expected exactly once"
            )

    def assert_called_with(self, *args: Any) -> None:
        """Check the arguments of the *last* call."""
        self.assert_called()
        actual = self.calls[-1].args
        if actual != args:
            raise AssertionError(
                f"step {self.name!r} was last called with {actual!r}, expected {args!r}"
            )

    def assert_any_call(self, *args: Any) -> None:
        """Check that *some* call used these arguments."""
        if not any(call.args == args for call in self.calls):
            seen = [call.args for call in self.calls]
            raise AssertionError(
                f"step {self.name!r} was never called with {args!r} (saw {seen!r})"
            )


def install_step_mock(
    registry: Registry,
    step: StepDefinition[Any, Any] | str,
    *,
    return_value: Any = UNSET,
    side_effect: Any = None,
) -> StepMock:
    """Replace ``step``'s implementation in ``registry`` with a recording mock.

    The replacement keeps the original function's signature (via :func:`functools.wraps`)
    so that argument decoding -- which is driven by the step's type hints -- is unaffected,
    and keeps its retry policy, timeout and queue, so a mocked step still behaves like the
    real one from the engine's point of view.
    """
    name = step.name if isinstance(step, StepDefinition) else step
    original = registry.step(name)
    mock = StepMock(name)
    mock.configure(return_value=return_value, side_effect=side_effect)

    async def _mocked(*args: Any) -> Any:
        return await mock._invoke(args)

    # functools.update_wrapper (what the @wraps decorator calls) rather than the
    # decorator itself, so the async def above keeps its own plain, easily-typed
    # signature; only its introspectable metadata (__wrapped__, __name__, ...) is
    # copied from the original function.
    functools.update_wrapper(_mocked, original.fn)
    replacement: StepDefinition[Any, Any] = StepDefinition(
        _mocked, name=name, options=original.options
    )
    install_step(registry, replacement)
    return mock


def install_step(registry: Registry, definition: StepDefinition[Any, Any]) -> None:
    """Install ``definition`` in place of the step registered under its name.

    Not part of the public :mod:`loomwork.testing` surface
    (:class:`~loomwork.testing.WorkflowEnvironment` is the intended entry point); exposed
    at module level so :meth:`WorkflowEnvironment.unmock_step` can put the original
    implementation back.
    """
    registry.replace_step(definition)
