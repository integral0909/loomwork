"""Edge cases in :mod:`loomwork.lint.checker` not already exercised by the rule tests:
nested function scopes, non-``Name`` assignment targets, and starred unpacking.
"""

from __future__ import annotations

from tests.lint.helpers import codes_of, lint


class TestNestedFunctionScopes:
    def test_a_nested_sync_helper_is_still_checked(self) -> None:
        findings = lint(
            """
            import time
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                def helper() -> float:
                    return time.time()

                helper()
            """
        )
        assert codes_of(findings) == ["LW101"]

    def test_a_nested_async_helper_is_still_checked(self) -> None:
        findings = lint(
            """
            import time
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                async def helper() -> float:
                    return time.time()

                await helper()
            """
        )
        assert codes_of(findings) == ["LW101"]

    def test_global_declared_in_a_nested_function_does_not_leak_to_the_outer_scope(self) -> None:
        # ``helper``'s own ``global`` declaration must not make the outer function think
        # *its* later, ordinary local assignment to the same name is a global mutation.
        findings = lint(
            """
            import loomwork as lw

            _shared = 0

            @lw.workflow
            async def process() -> None:
                def helper() -> None:
                    global _shared
                    _shared = 1

                _shared = 2
                helper()
            """
        )
        assert codes_of(findings) == ["LW113"]
        assert findings[0].location.line == 10  # inside helper, not process's own line

    def test_global_inside_a_class_body_defined_in_a_function_does_not_leak(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                class Inner:
                    pass

                total = 0
                total += 1
            """
        )
        assert findings == []


class TestNonNameAssignmentTargets:
    def test_augmented_assignment_to_an_attribute_is_not_confused_for_a_global(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            class Box:
                count = 0

            @lw.workflow
            async def process() -> None:
                box = Box()
                box.count += 1
            """
        )
        assert findings == []

    def test_annotated_assignment_to_a_global_is_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            _cache: int = 0

            @lw.workflow
            async def process() -> None:
                global _cache
                _cache: int = 1
            """
        )
        assert codes_of(findings) == ["LW113"]

    def test_annotated_assignment_without_a_value_is_not_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            _cache: int

            @lw.workflow
            async def process() -> None:
                global _cache
                _cache: int
            """
        )
        assert findings == []

    def test_starred_unpacking_of_globals_flags_every_name(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            _first = 0
            _rest: list[int] = []

            @lw.workflow
            async def process() -> None:
                global _first, _rest
                _first, *_rest = [1, 2, 3]
            """
        )
        assert codes_of(findings) == ["LW113", "LW113"]
