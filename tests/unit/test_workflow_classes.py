"""Defining class-based workflows: validation, naming, fingerprints and client-side names."""

from __future__ import annotations

from typing import Any

import pytest

import loomwork as lw
from loomwork.errors import NotInWorkflowError, RegistrationError
from loomwork.sdk.classes import HANDLER_ATTRIBUTE, WorkflowClass, collect_handlers
from loomwork.sdk.definitions import Registry, as_definition
from loomwork.sdk.handlers import handler_name
from loomwork.sdk.messaging import resolve_signal


def make(**members: Any) -> type:
    namespace: dict[str, Any] = {"__qualname__": "Made", **members}
    return type("Made", (), namespace)


def fresh_run() -> Any:
    """A new, unmarked ``@run`` candidate (a function can be marked only once)."""

    async def run(self: Any, value: int) -> int:
        return value

    run.__qualname__ = "Made.run"
    return run


def test_decorated_class_is_a_workflow_definition() -> None:
    @lw.workflow
    class Plain:
        @lw.run
        async def run(self, value: int) -> int:
            return value

    # The class stays a class; its definition is attached.
    assert isinstance(Plain, type)
    definition = as_definition(Plain)
    assert isinstance(definition, WorkflowClass)
    assert definition.name == "Plain"
    assert definition.bind([3]) == [3]
    assert Registry([Plain]).workflow("Plain") is definition


def test_explicit_name_and_fail_on() -> None:
    @lw.workflow(name="orders.v2", fail_on=[KeyError])
    class Orders:
        @lw.run
        async def run(self) -> None: ...

    assert as_definition(Orders).name == "orders.v2"
    assert as_definition(Orders).is_failure(KeyError("x"))


def test_run_signature_drops_self_and_keeps_defaults() -> None:
    @lw.workflow
    class WithDefaults:
        @lw.run
        async def run(self, a: int, b: str = "x") -> str:
            return f"{a}{b}"

    assert as_definition(WithDefaults).bind([1]) == [1, "x"]
    with pytest.raises(TypeError):
        as_definition(WithDefaults).bind([])


def test_exactly_one_run_method() -> None:
    with pytest.raises(RegistrationError, match="exactly one @run"):
        lw.workflow(make())
    with pytest.raises(RegistrationError, match="found: a, b"):
        lw.workflow(make(a=lw.run(fresh_run()), b=lw.run(fresh_run())))


def test_run_must_be_async() -> None:
    def sync_run(self: Any) -> None: ...

    with pytest.raises(RegistrationError, match="async def"):
        lw.run(sync_run)


def test_a_method_can_have_only_one_role() -> None:
    def handler(self: Any, value: int) -> None: ...

    lw.signal(handler)
    with pytest.raises(RegistrationError, match="already a signal handler"):
        lw.query(handler)


def test_signal_handlers_take_one_message() -> None:
    def two(self: Any, a: int, b: int) -> None: ...

    with pytest.raises(RegistrationError, match="exactly one message"):
        lw.workflow(make(run=lw.run(fresh_run()), two=lw.signal(two)))


def test_duplicate_handler_names_are_rejected() -> None:
    def first(self: Any, v: int) -> None: ...

    def second(self: Any, v: int) -> None: ...

    with pytest.raises(RegistrationError, match="'same' is handled by both"):
        lw.workflow(
            make(run=lw.run(fresh_run()), a=lw.signal("same")(first), b=lw.signal("same")(second))
        )


def test_invalid_handler_names() -> None:
    def handler(self: Any) -> None: ...

    with pytest.raises(ValueError, match="invalid query name"):
        lw.query("has space")(handler)


def test_query_outside_a_workflow_on_a_plain_function_still_fails() -> None:
    def not_a_method() -> int:
        return 1

    with pytest.raises(NotInWorkflowError):
        lw.query(not_a_method)


def test_client_side_names() -> None:
    @lw.workflow
    class Named:
        LIMIT = 3

        @lw.run
        async def run(self) -> None: ...

        @lw.signal("approve")
        def approve_it(self, by: str) -> None: ...

        @lw.query("state")
        def get_state(self) -> str:
            return ""

        @lw.update
        def rename(self, name: str) -> None: ...

    channel = resolve_signal(Named.approve_it)
    assert isinstance(channel, lw.Channel)
    assert channel.name == "approve"
    assert channel.type is str
    assert handler_name(Named.get_state, "query") == "state"
    assert handler_name(Named.rename, "update") == "rename"
    assert handler_name("anything", "query") == "anything"
    with pytest.raises(TypeError, match="not a query handler"):
        handler_name(Named.rename, "query")
    with pytest.raises(TypeError, match="not a channel"):
        resolve_signal(Named.get_state)


def test_undecorated_and_subclassed_classes_are_not_workflows() -> None:
    @lw.workflow
    class Base:
        @lw.run
        async def run(self) -> None: ...

    class Sub(Base):
        pass

    with pytest.raises(RegistrationError, match="decorate it"):
        as_definition(Sub)
    with pytest.raises(RegistrationError, match="decorate it"):
        Registry([Sub])


def test_inherited_handlers_and_overrides() -> None:
    class Base:
        @lw.run
        async def run(self) -> str:
            return "base"

        @lw.query
        def status(self) -> str:
            return "base"

    @lw.workflow
    class Child(Base):
        @lw.query
        def status(self) -> str:
            return "child"

        @lw.signal
        def poke(self, _: int) -> None: ...

    run_spec, handlers = collect_handlers(Child)
    assert run_spec.function is Base.run
    by_name = {h.name: h for h in handlers}
    assert by_name["status"].function is vars(Child)["status"]
    assert set(by_name) == {"status", "poke"}


def test_fingerprint_covers_handlers_not_just_run() -> None:
    def build(bonus: int) -> WorkflowClass[Any]:
        class Versioned:
            total = 0

            @lw.run
            async def run(self) -> int:
                return self.total

            @lw.signal
            def add(self, value: int) -> None:
                self.total += value + bonus

        definition = as_definition(lw.workflow(Versioned))
        assert isinstance(definition, WorkflowClass)
        return definition

    first, same = build(0), build(0)
    assert first.fingerprint == same.fingerprint
    assert len(first.fingerprint) == 12

    class Other:
        total = 0

        @lw.run
        async def run(self) -> int:
            return self.total

        @lw.signal
        def add(self, value: int) -> None:
            self.total -= value

    assert as_definition(lw.workflow(Other)).fingerprint != first.fingerprint


def test_marker_records_kind_and_name() -> None:
    def handler(self: Any) -> int:
        return 0

    lw.update("do-it")(handler)
    assert getattr(handler, HANDLER_ATTRIBUTE) == ("update", "do-it")
