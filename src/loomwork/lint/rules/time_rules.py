"""LW101: reading the wall clock instead of workflow time."""

from __future__ import annotations

from loomwork.lint.models import Severity
from loomwork.lint.registry import DEFAULT_REGISTRY, Rule
from loomwork.lint.rules.tables import ForbiddenCall

RULE_WALL_CLOCK_TIME = DEFAULT_REGISTRY.register(
    Rule(
        code="LW101",
        name="wall-clock-time",
        severity=Severity.ERROR,
        message="{name}() reads the wall clock, which returns a different value on replay",
        hint="use loomwork.now() for the current workflow time, or loomwork.sleep() / "
        "loomwork.sleep_until() for a durable delay",
    )
)

FORBIDDEN_CALLS = (
    ForbiddenCall("time.time", RULE_WALL_CLOCK_TIME.code),
    ForbiddenCall("time.monotonic", RULE_WALL_CLOCK_TIME.code),
    ForbiddenCall("time.monotonic_ns", RULE_WALL_CLOCK_TIME.code),
    ForbiddenCall("time.perf_counter", RULE_WALL_CLOCK_TIME.code),
    ForbiddenCall("time.perf_counter_ns", RULE_WALL_CLOCK_TIME.code),
    ForbiddenCall("time.time_ns", RULE_WALL_CLOCK_TIME.code),
    ForbiddenCall("time.sleep", RULE_WALL_CLOCK_TIME.code),
    ForbiddenCall("datetime.datetime.now", RULE_WALL_CLOCK_TIME.code),
    ForbiddenCall("datetime.datetime.utcnow", RULE_WALL_CLOCK_TIME.code),
    ForbiddenCall("datetime.datetime.today", RULE_WALL_CLOCK_TIME.code),
    ForbiddenCall("datetime.date.today", RULE_WALL_CLOCK_TIME.code),
    # ``asyncio.get_event_loop().time()`` is deliberately absent: it returns the
    # deterministic DeterministicEventLoop clock inside a workflow, so it is fine.
)
