"""The optional pytest fixtures in ``loomwork.testing.pytest_plugin``."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

import loomwork as lw
from loomwork.testing.pytest_plugin import EnvironmentFactory, _marker_start_time


@lw.step(name="plugin.shout")
async def shout(value: str) -> str:
    return value.upper()


@lw.workflow(name="plugin.echo")
async def echo(value: str) -> str:
    return value


@lw.workflow(name="plugin.greet")
async def greet(value: str) -> str:
    return await shout(value)


async def test_factory_builds_a_working_environment(loomwork_env: EnvironmentFactory) -> None:
    env = await loomwork_env(workflows=[echo])
    assert await env.execute(echo, "hi") == "hi"


async def test_factory_can_build_more_than_one_environment(
    loomwork_env: EnvironmentFactory,
) -> None:
    first = await loomwork_env(workflows=[echo])
    second = await loomwork_env(workflows=[echo])
    assert first is not second
    assert await first.execute(echo, "a") == "a"
    assert await second.execute(echo, "b") == "b"


@pytest.mark.loomwork_start_time(datetime(2030, 6, 1, tzinfo=UTC))
async def test_marker_sets_the_default_start_time(loomwork_env: EnvironmentFactory) -> None:
    env = await loomwork_env(workflows=[echo])
    assert env.now() == datetime(2030, 6, 1, tzinfo=UTC)


async def test_start_time_argument_overrides_the_marker(loomwork_env: EnvironmentFactory) -> None:
    env = await loomwork_env(workflows=[echo], start_time=datetime(2031, 1, 1, tzinfo=UTC))
    assert env.now() == datetime(2031, 1, 1, tzinfo=UTC)


async def test_environment_from_the_fixture_supports_mocking(
    loomwork_env: EnvironmentFactory,
) -> None:
    env = await loomwork_env(workflows=[greet], steps=[shout])
    env.mock_step(shout, return_value="MOCKED")
    assert await env.execute(greet, "hi") == "MOCKED"


class _FakeMarker:
    def __init__(self, *args: Any) -> None:
        self.args = args


class _FakeNode:
    def __init__(self, marker: _FakeMarker | None) -> None:
        self._marker = marker

    def get_closest_marker(self, name: str) -> _FakeMarker | None:
        assert name == "loomwork_start_time"
        return self._marker


class _FakeRequest:
    def __init__(self, marker: _FakeMarker | None) -> None:
        self.node = _FakeNode(marker)


def test_marker_start_time_returns_none_without_the_marker() -> None:
    assert _marker_start_time(_FakeRequest(None)) is None  # type: ignore[arg-type]


def test_marker_start_time_rejects_a_non_datetime_argument() -> None:
    with pytest.raises(TypeError, match="timezone-aware datetime"):
        _marker_start_time(_FakeRequest(_FakeMarker("not-a-datetime")))  # type: ignore[arg-type]
