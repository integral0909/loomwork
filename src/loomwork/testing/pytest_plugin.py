"""Optional pytest integration for :mod:`loomwork.testing`.

Loomwork ships no ``pytest11`` entry point, so installing it never changes what a
project's test suite collects. Opt in from your own ``conftest.py``::

    pytest_plugins = ["loomwork.testing.pytest_plugin"]

which adds the ``loomwork_env`` fixture (a factory for
:class:`~loomwork.testing.WorkflowEnvironment` instances that are closed automatically
at the end of the test) and the ``loomwork_start_time`` marker (its default
``start_time``).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime
from typing import Any

import pytest

from loomwork.testing.environment import WorkflowEnvironment

EnvironmentFactory = Callable[..., Awaitable[WorkflowEnvironment]]


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "loomwork_start_time(dt): the ManualClock start time for environments built by "
        "the loomwork_env() fixture in this test (default: the environment's own "
        "default, midnight UTC on 2024-01-01)",
    )


def _marker_start_time(request: pytest.FixtureRequest) -> datetime | None:
    marker = request.node.get_closest_marker("loomwork_start_time")
    if marker is None or not marker.args:
        return None
    start = marker.args[0]
    if not isinstance(start, datetime):
        raise TypeError("@pytest.mark.loomwork_start_time expects a single timezone-aware datetime")
    return start


@pytest.fixture
async def loomwork_env(request: pytest.FixtureRequest) -> AsyncIterator[EnvironmentFactory]:
    """A factory for :class:`~loomwork.testing.WorkflowEnvironment` instances.

    Every environment the factory builds is entered immediately and exited (its worker
    stopped, its auto time-skipping task cancelled) when the test ends, in reverse
    order of creation -- most tests only ever build one::

        async def test_charges_the_card(loomwork_env):
            env = await loomwork_env(workflows=[process_order], steps=[charge_card])
            env.mock_step(charge_card, return_value="ch_123")
            receipt = await env.execute(process_order, an_order)
            assert receipt.charge_id == "ch_123"

    ``@pytest.mark.loomwork_start_time(dt)`` sets the default ``start_time`` for every
    environment built in that test; pass ``start_time=`` to one call to override it.
    """
    default_start = _marker_start_time(request)
    environments: list[WorkflowEnvironment] = []

    async def factory(*, start_time: datetime | None = None, **kwargs: Any) -> WorkflowEnvironment:
        env = WorkflowEnvironment(start_time=start_time or default_start, **kwargs)
        await env.__aenter__()
        environments.append(env)
        return env

    yield factory

    for env in reversed(environments):
        await env.__aexit__(None, None, None)


__all__ = ["loomwork_env", "pytest_configure"]
