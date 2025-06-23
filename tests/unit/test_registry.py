"""Registering and replacing workflow and step definitions."""

from __future__ import annotations

import pytest

import loomwork as lw
from loomwork.errors import NotFoundError, RegistrationError
from loomwork.sdk.definitions import Registry


@lw.step(name="reg.fetch")
async def fetch(key: str) -> str:
    return key


@lw.step(name="reg.fetch")
async def fetch_fake(key: str) -> str:
    return "fake"


@lw.step(name="reg.other")
async def other() -> None: ...


@lw.workflow(name="reg.flow")
async def flow() -> None: ...


def test_names() -> None:
    registry = Registry([flow], [fetch, other])
    assert registry.workflow_names == {"reg.flow"}
    assert registry.step_names == {"reg.fetch", "reg.other"}
    assert registry.has_workflow("reg.flow")
    assert not registry.has_step("reg.flow")


def test_adding_the_same_definition_twice_is_harmless() -> None:
    registry = Registry([flow, flow], [fetch, fetch])
    assert registry.step("reg.fetch") is fetch


def test_two_definitions_with_one_name_are_rejected() -> None:
    with pytest.raises(RegistrationError, match="registered twice"):
        Registry(steps=[fetch, fetch_fake])


def test_wrong_kinds_are_rejected() -> None:
    registry = Registry()
    with pytest.raises(RegistrationError, match="not a workflow"):
        registry.add_workflow(fetch)  # type: ignore[arg-type]
    with pytest.raises(RegistrationError, match="not a step"):
        registry.add_step(flow)  # type: ignore[arg-type]


def test_unknown_names() -> None:
    registry = Registry()
    with pytest.raises(NotFoundError):
        registry.workflow("nope")
    with pytest.raises(NotFoundError):
        registry.step("nope")


def test_replace_step_swaps_and_returns_previous() -> None:
    registry = Registry(steps=[fetch])
    assert registry.replace_step(fetch_fake) is fetch
    assert registry.step("reg.fetch") is fetch_fake
    assert registry.replace_step(fetch) is fetch_fake


def test_replace_step_requires_an_existing_step() -> None:
    registry = Registry()
    with pytest.raises(NotFoundError):
        registry.replace_step(fetch)
    registry.add_step(fetch)
    with pytest.raises(RegistrationError):
        registry.replace_step(flow)  # type: ignore[arg-type]
