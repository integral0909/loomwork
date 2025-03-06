"""LW109: threads, process pools and executors.

The deterministic event loop (see ``loomwork.runtime.loop.DeterministicEventLoop``)
already refuses ``run_in_executor`` at runtime, but catching it statically lets a deploy
fail fast instead of a running workflow crashing mid-tick. ``run_in_executor`` is matched
by attribute name alone (see :mod:`loomwork.lint.checker`) because its receiver is
almost always an opaque ``asyncio.AbstractEventLoop`` this linter cannot resolve.
"""

from __future__ import annotations

from loomwork.lint.models import Severity
from loomwork.lint.registry import DEFAULT_REGISTRY, Rule
from loomwork.lint.rules.tables import ForbiddenCall, MatchKind

RULE_BLOCKING_CONCURRENCY = DEFAULT_REGISTRY.register(
    Rule(
        code="LW109",
        name="blocking-concurrency",
        severity=Severity.ERROR,
        message="{name}() escapes the deterministic event loop onto a real thread or process",
        hint="await a step instead; steps already run off the workflow loop",
    )
)

FORBIDDEN_CALLS = (
    ForbiddenCall("threading", RULE_BLOCKING_CONCURRENCY.code, MatchKind.PREFIX),
    ForbiddenCall("concurrent.futures", RULE_BLOCKING_CONCURRENCY.code, MatchKind.PREFIX),
    ForbiddenCall("multiprocessing", RULE_BLOCKING_CONCURRENCY.code, MatchKind.PREFIX),
    ForbiddenCall("asyncio.to_thread", RULE_BLOCKING_CONCURRENCY.code),
)

#: Attribute names that are flagged regardless of their receiver, because the receiver
#: (an ``asyncio`` event loop, almost always) cannot be resolved statically but the name
#: is distinctive enough that false positives are very unlikely.
OPAQUE_RECEIVER_METHODS: frozenset[str] = frozenset({"run_in_executor"})
