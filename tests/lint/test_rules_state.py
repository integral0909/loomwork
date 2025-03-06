"""LW113: mutation of module-global state."""

from __future__ import annotations

from tests.lint.helpers import codes_of, lint


class TestGlobalMutation:
    def test_global_assignment_is_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            _cache = None

            @lw.workflow
            async def process() -> None:
                global _cache
                _cache = 1
            """
        )
        assert codes_of(findings) == ["LW113"]
        assert "_cache" in findings[0].message

    def test_global_augmented_assignment_is_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            _counter = 0

            @lw.workflow
            async def process() -> None:
                global _counter
                _counter += 1
            """
        )
        assert codes_of(findings) == ["LW113"]

    def test_global_tuple_unpacking_assignment_is_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            _a = 0
            _b = 0

            @lw.workflow
            async def process() -> None:
                global _a, _b
                _a, _b = 1, 2
            """
        )
        assert codes_of(findings) == ["LW113", "LW113"]

    def test_global_statement_without_assignment_is_not_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            _cache = None

            @lw.workflow
            async def process() -> None:
                global _cache
                str(_cache)
            """
        )
        assert findings == []

    def test_plain_local_assignment_is_not_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                total = 0
                total += 1
            """
        )
        assert findings == []
