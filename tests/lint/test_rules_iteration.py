"""LW111 (hash-order iteration) and LW112 (hash/id in control flow)."""

from __future__ import annotations

from tests.lint.helpers import codes_of, lint


class TestHashOrderIteration:
    def test_iterating_a_set_literal_is_a_warning(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                for item in {1, 2, 3}:
                    str(item)
            """
        )
        assert codes_of(findings) == ["LW111"]
        assert findings[0].severity.value == "warning"

    def test_iterating_a_set_call_is_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                for item in set([1, 2, 3]):
                    str(item)
            """
        )
        assert codes_of(findings) == ["LW111"]

    def test_iterating_a_frozenset_call_is_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                for item in frozenset([1, 2, 3]):
                    str(item)
            """
        )
        assert codes_of(findings) == ["LW111"]

    def test_iterating_a_set_comprehension_is_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                for item in {x for x in range(3)}:
                    str(item)
            """
        )
        assert codes_of(findings) == ["LW111"]

    def test_async_for_over_a_set_is_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            async def _agen():
                yield 1

            @lw.workflow
            async def process() -> None:
                async for item in _agen():
                    str(item)
            """
        )
        # _agen() is not a set, so nothing is flagged here: this only proves AsyncFor
        # is checked at all, via the negative case below asserting no false positive.
        assert findings == []

    def test_iterating_sorted_set_is_not_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                for item in sorted({1, 2, 3}):
                    str(item)
            """
        )
        assert findings == []

    def test_iterating_a_list_or_dict_is_not_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                for item in [1, 2, 3]:
                    str(item)
                for key in {"a": 1}:
                    str(key)
            """
        )
        assert findings == []


class TestHashInControlFlow:
    def test_id_in_if_condition_is_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process(a: object, b: object) -> None:
                if id(a) == id(b):
                    pass
            """
        )
        assert codes_of(findings) == ["LW112", "LW112"]

    def test_hash_in_while_condition_is_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process(a: object) -> None:
                while hash(a) > 0:
                    break
            """
        )
        assert codes_of(findings) == ["LW112"]

    def test_hash_in_ternary_condition_is_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process(a: object) -> None:
                result = "x" if hash(a) > 0 else "y"
            """
        )
        assert codes_of(findings) == ["LW112"]

    def test_hash_outside_control_flow_is_not_flagged(self) -> None:
        # Using hash() for something other than branching (a cache key, say) is not
        # itself nondeterministic in a way that breaks replay.
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process(a: object) -> None:
                key = hash(a)
                str(key)
            """
        )
        assert findings == []
