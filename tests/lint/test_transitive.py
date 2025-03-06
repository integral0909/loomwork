"""Transitive detection: a workflow calling a plain local helper that violates a rule."""

from __future__ import annotations

from loomwork.lint.analyzer import MAX_CALL_DEPTH
from tests.lint.helpers import codes_of, lint


class TestTransitiveCalls:
    def test_violation_in_a_directly_called_helper_is_reported_with_a_call_path(self) -> None:
        findings = lint(
            """
            import time
            import loomwork as lw

            def compute_total() -> float:
                return time.time()

            @lw.workflow
            async def process() -> None:
                compute_total()
            """
        )
        assert codes_of(findings) == ["LW101"]
        assert findings[0].call_path == ("process",)
        assert findings[0].location.line == 6  # inside compute_total, not the call site

    def test_violation_two_hops_away_records_the_full_path(self) -> None:
        findings = lint(
            """
            import time
            import loomwork as lw

            def inner() -> float:
                return time.time()

            def outer() -> float:
                return inner()

            @lw.workflow
            async def process() -> None:
                outer()
            """
        )
        assert codes_of(findings) == ["LW101"]
        assert findings[0].call_path == ("process", "outer")

    def test_step_bodies_are_not_recursed_into(self) -> None:
        findings = lint(
            """
            import time
            import loomwork as lw

            @lw.step
            async def fetch() -> float:
                return time.time()

            @lw.workflow
            async def process() -> None:
                await fetch()
            """
        )
        assert findings == []

    def test_another_workflows_body_is_not_recursed_into(self) -> None:
        # ``child`` is analyzed as its own entry point; recursing into it from ``parent``
        # too would report the same violation twice.
        findings = lint(
            """
            import time
            import loomwork as lw

            @lw.workflow
            async def child() -> float:
                return time.time()

            @lw.workflow
            async def parent() -> None:
                pass
            """
        )
        assert codes_of(findings) == ["LW101"]
        assert findings[0].call_path == ()

    def test_direct_recursion_does_not_hang_or_duplicate(self) -> None:
        findings = lint(
            """
            import time
            import loomwork as lw

            def recurse(n: int) -> float:
                if n <= 0:
                    return time.time()
                return recurse(n - 1)

            @lw.workflow
            async def process() -> None:
                recurse(3)
            """
        )
        assert codes_of(findings) == ["LW101"]
        assert findings[0].call_path == ("process",)

    def test_mutual_recursion_does_not_hang(self) -> None:
        findings = lint(
            """
            import time
            import loomwork as lw

            def ping(n: int) -> float:
                if n <= 0:
                    return time.time()
                return pong(n - 1)

            def pong(n: int) -> float:
                return ping(n - 1)

            @lw.workflow
            async def process() -> None:
                ping(5)
            """
        )
        assert codes_of(findings) == ["LW101"]

    def test_clean_helper_produces_no_findings(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            def add(a: int, b: int) -> int:
                return a + b

            @lw.workflow
            async def process() -> None:
                add(1, 2)
            """
        )
        assert findings == []

    def test_call_depth_beyond_the_limit_is_not_followed(self) -> None:
        # Build a straight-line chain one hop longer than MAX_CALL_DEPTH allows; the
        # violation at the very end must not be reported.
        chain_length = MAX_CALL_DEPTH + 2
        helpers = []
        for i in range(chain_length):
            if i == 0:
                helpers.append("def helper_0() -> float:\n    return time.time()")
            else:
                helpers.append(f"def helper_{i}() -> float:\n    return helper_{i - 1}()")
        source = (
            "import time\nimport loomwork as lw\n\n"
            + "\n\n".join(helpers)
            + f"\n\n@lw.workflow\nasync def process() -> None:\n    helper_{chain_length - 1}()\n"
        )
        findings = lint(source)
        assert findings == []
