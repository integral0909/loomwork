"""LW110: environment variable access."""

from __future__ import annotations

from tests.lint.helpers import codes_of, lint


class TestEnvironmentAccess:
    def test_os_getenv_is_a_warning(self) -> None:
        findings = lint(
            """
            import os
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                os.getenv("FEATURE_FLAG")
            """
        )
        assert codes_of(findings) == ["LW110"]
        assert findings[0].severity.value == "warning"

    def test_os_environ_get_is_flagged_once(self) -> None:
        findings = lint(
            """
            import os
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                os.environ.get("FEATURE_FLAG")
            """
        )
        assert codes_of(findings) == ["LW110"]

    def test_os_environ_subscript_is_flagged(self) -> None:
        findings = lint(
            """
            import os
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                value = os.environ["FEATURE_FLAG"]
            """
        )
        assert codes_of(findings) == ["LW110"]

    def test_os_environ_passed_around_is_flagged(self) -> None:
        findings = lint(
            """
            import os
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                for key in os.environ:
                    str(key)
            """
        )
        assert codes_of(findings) == ["LW110"]

    def test_os_environ_items_is_flagged_once(self) -> None:
        findings = lint(
            """
            import os
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                list(os.environ.items())
            """
        )
        assert codes_of(findings) == ["LW110"]

    def test_from_import_environ_subscript_is_flagged(self) -> None:
        findings = lint(
            """
            from os import environ
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                value = environ["FEATURE_FLAG"]
            """
        )
        assert codes_of(findings) == ["LW110"]

    def test_os_path_join_is_not_environment_access(self) -> None:
        findings = lint(
            """
            import os
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                os.path.join("a", "b")
            """
        )
        assert findings == []
