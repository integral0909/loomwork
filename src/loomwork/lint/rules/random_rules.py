"""LW102: unseeded randomness."""

from __future__ import annotations

from loomwork.lint.models import Severity
from loomwork.lint.registry import DEFAULT_REGISTRY, Rule
from loomwork.lint.rules.tables import ForbiddenCall, MatchKind

RULE_NONDETERMINISTIC_RANDOMNESS = DEFAULT_REGISTRY.register(
    Rule(
        code="LW102",
        name="nondeterministic-randomness",
        severity=Severity.ERROR,
        message="{name}() produces a different value on replay",
        hint="use loomwork.random() for a per-run seeded Random, or loomwork.uuid4() for a "
        "replay-stable UUID",
    )
)

FORBIDDEN_CALLS = (
    ForbiddenCall("random", RULE_NONDETERMINISTIC_RANDOMNESS.code, MatchKind.PREFIX),
    ForbiddenCall("secrets", RULE_NONDETERMINISTIC_RANDOMNESS.code, MatchKind.PREFIX),
    ForbiddenCall("uuid.uuid1", RULE_NONDETERMINISTIC_RANDOMNESS.code),
    ForbiddenCall("uuid.uuid4", RULE_NONDETERMINISTIC_RANDOMNESS.code),
    ForbiddenCall("os.urandom", RULE_NONDETERMINISTIC_RANDOMNESS.code),
    # uuid3()/uuid5() are pure functions of (namespace, name) and are deliberately not
    # flagged: they are deterministic.
)
