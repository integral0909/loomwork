"""The rule registry.

Every rule the linter can report is registered exactly once, in one place, as a
:class:`Rule`. Rule modules under :mod:`loomwork.lint.rules` register their rules at
import time; the analyzer and the reporters only ever look rules up by code. Adding a
new rule therefore never touches the analyzer: define the :class:`Rule`, register it,
and teach the checker (or the forbidden-call table) to emit it.
"""

from __future__ import annotations

from dataclasses import dataclass

from loomwork.lint.models import Severity


@dataclass(slots=True, frozen=True)
class Rule:
    """Static metadata about one rule.

    ``message`` may contain a ``{name}`` placeholder, filled in with the specific
    construct that triggered the finding (a dotted call name, a variable name, ...).
    """

    code: str
    name: str
    severity: Severity
    message: str
    hint: str

    def format(self, **kwargs: object) -> str:
        return self.message.format(**kwargs)


class RuleRegistry:
    """A collection of rules, keyed by their stable code."""

    __slots__ = ("_rules",)

    def __init__(self) -> None:
        self._rules: dict[str, Rule] = {}

    def register(self, rule: Rule) -> Rule:
        if rule.code in self._rules:
            raise ValueError(f"rule {rule.code!r} is already registered")
        self._rules[rule.code] = rule
        return rule

    def __contains__(self, code: object) -> bool:
        return code in self._rules

    def __getitem__(self, code: str) -> Rule:
        return self._rules[code]

    def get(self, code: str) -> Rule | None:
        return self._rules.get(code)

    def all(self) -> tuple[Rule, ...]:
        return tuple(sorted(self._rules.values(), key=lambda rule: rule.code))

    def codes(self) -> frozenset[str]:
        return frozenset(self._rules)


#: The registry every built-in rule module registers into. Tests that need a scratch
#: registry (for example to check that double-registration is rejected) should build
#: their own ``RuleRegistry()`` rather than mutate this one.
DEFAULT_REGISTRY = RuleRegistry()
