"""The rule registry itself."""

from __future__ import annotations

import pytest

from loomwork.lint.models import Severity
from loomwork.lint.registry import DEFAULT_REGISTRY, Rule, RuleRegistry


class TestRuleRegistry:
    def test_register_and_lookup(self) -> None:
        registry = RuleRegistry()
        rule = registry.register(
            Rule(code="LW900", name="test", severity=Severity.ERROR, message="m", hint="h")
        )
        assert registry["LW900"] is rule
        assert registry.get("LW900") is rule
        assert "LW900" in registry
        assert registry.codes() == frozenset({"LW900"})

    def test_get_of_unknown_code_returns_none(self) -> None:
        registry = RuleRegistry()
        assert registry.get("LW999") is None

    def test_double_registration_is_rejected(self) -> None:
        registry = RuleRegistry()
        registry.register(
            Rule(code="LW900", name="a", severity=Severity.ERROR, message="m", hint="h")
        )
        with pytest.raises(ValueError, match="LW900"):
            registry.register(
                Rule(code="LW900", name="b", severity=Severity.WARNING, message="m2", hint="h2")
            )

    def test_all_returns_rules_sorted_by_code(self) -> None:
        registry = RuleRegistry()
        registry.register(
            Rule(code="LW902", name="b", severity=Severity.ERROR, message="m", hint="h")
        )
        registry.register(
            Rule(code="LW901", name="a", severity=Severity.ERROR, message="m", hint="h")
        )
        assert [rule.code for rule in registry.all()] == ["LW901", "LW902"]

    def test_rule_format_fills_in_the_message_template(self) -> None:
        rule = Rule(
            code="LW900",
            name="test",
            severity=Severity.ERROR,
            message="{name} is bad",
            hint="fix it",
        )
        assert rule.format(name="time.time") == "time.time is bad"


class TestDefaultRegistry:
    def test_every_built_in_rule_is_registered(self) -> None:
        expected = {
            "LW000",
            "LW101",
            "LW102",
            "LW103",
            "LW104",
            "LW105",
            "LW106",
            "LW107",
            "LW108",
            "LW109",
            "LW110",
            "LW111",
            "LW112",
            "LW113",
            "LW199",
        }
        assert expected <= DEFAULT_REGISTRY.codes()

    def test_every_rule_has_a_non_empty_message_and_hint(self) -> None:
        for rule in DEFAULT_REGISTRY.all():
            assert rule.message
            assert rule.hint
            assert rule.name
