"""Inline suppression comments."""

from __future__ import annotations

from loomwork.lint.suppressions import Suppressions, _record_comment, collect_suppressions
from tests.lint.helpers import codes_of, lint


class TestSuppressionParsing:
    def test_bare_ignore_suppresses_the_line(self) -> None:
        suppressions = collect_suppressions("x = 1  # loomwork: ignore\n")
        assert suppressions.suppresses("LW101", 1)
        assert suppressions.suppresses("LW999", 1)
        assert not suppressions.ignore_file

    def test_ignore_with_one_code_only_suppresses_that_code(self) -> None:
        suppressions = collect_suppressions("x = 1  # loomwork: ignore[LW101]\n")
        assert suppressions.suppresses("LW101", 1)
        assert not suppressions.suppresses("LW102", 1)

    def test_ignore_with_multiple_codes(self) -> None:
        suppressions = collect_suppressions("x = 1  # loomwork: ignore[LW101, LW102]\n")
        assert suppressions.suppresses("LW101", 1)
        assert suppressions.suppresses("LW102", 1)
        assert not suppressions.suppresses("LW103", 1)

    def test_ignore_file_directive_is_file_wide(self) -> None:
        suppressions = collect_suppressions("# loomwork: ignore-file\nx = 1\n")
        assert suppressions.ignore_file
        assert suppressions.suppresses("LW101", 2)

    def test_comment_inside_a_string_literal_is_not_a_directive(self) -> None:
        suppressions = collect_suppressions('x = "# loomwork: ignore"\n')
        assert not suppressions.ignore_file
        assert not suppressions.suppresses("LW101", 1)

    def test_unrelated_comment_is_ignored(self) -> None:
        suppressions = collect_suppressions("x = 1  # just a note\n")
        assert not suppressions.suppresses("LW101", 1)

    def test_unterminated_construct_does_not_raise(self) -> None:
        # tokenize.TokenError on malformed source; the caller's ast.parse() reports the
        # real syntax error, so suppressions just quietly finds nothing.
        suppressions = collect_suppressions("x = (\n")
        assert suppressions == Suppressions()

    def test_a_bare_ignore_already_recorded_is_not_narrowed_by_a_later_call(self) -> None:
        # Defends the line-1 "already silenced" fast path in _record_comment directly:
        # once a line is marked "ignore everything", a second directive for the same
        # line must not replace it with a narrower, code-specific set.
        result = Suppressions()
        _record_comment(result, "# loomwork: ignore", 1)
        _record_comment(result, "# loomwork: ignore[LW101]", 1)
        assert result.lines[1] is None
        assert result.suppresses("LW999", 1)


class TestSuppressionInLinting:
    def test_suppressed_line_produces_no_finding(self) -> None:
        findings = lint(
            """
            import time
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                time.time()  # loomwork: ignore[LW101]
            """
        )
        assert findings == []

    def test_suppressing_a_different_code_does_not_help(self) -> None:
        findings = lint(
            """
            import time
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                time.time()  # loomwork: ignore[LW999]
            """
        )
        assert codes_of(findings) == ["LW101"]

    def test_ignore_file_suppresses_every_finding_in_the_file(self) -> None:
        findings = lint(
            """
            # loomwork: ignore-file
            import time
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                time.time()
            """
        )
        assert findings == []
