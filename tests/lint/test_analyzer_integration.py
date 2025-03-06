"""End-to-end tests over realistic, multi-file packages written to ``tmp_path``.

These exercise :func:`loomwork.lint.analyzer.lint_paths` the way ``python -m
loomwork.lint`` actually uses it: real files on disk, a real ``pyproject.toml``, a
package that spans several modules, and directory-wide discovery - as opposed to the
single-source-string tests everywhere else in this suite.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

from loomwork.lint.analyzer import lint_paths
from loomwork.lint.config import LintConfig
from loomwork.lint.models import Finding


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content))


class TestRealisticPackage:
    def test_scans_every_module_and_reports_across_files(self, tmp_path: Path) -> None:
        pkg = tmp_path / "myapp"
        _write(
            pkg / "__init__.py",
            "",
        )
        _write(
            pkg / "orders.py",
            """\
            import time

            import loomwork as lw


            def _price_total(items: list[float]) -> float:
                return sum(items)


            @lw.workflow(name="orders.process")
            async def process_order(items: list[float]) -> float:
                await lw.sleep(1)
                return _price_total(items)


            @lw.step
            async def charge_card(amount: float) -> str:
                # Steps may do real work; this is not a workflow body.
                time.sleep(0)
                return "ok"
            """,
        )
        _write(
            pkg / "reminders.py",
            """\
            import time

            import loomwork as lw


            @lw.workflow
            async def send_reminder() -> None:
                sent_at = time.time()
                print(f"sent at {sent_at}")
            """,
        )
        _write(
            pkg / "clean.py",
            """\
            def add(a: int, b: int) -> int:
                return a + b
            """,
        )

        result = lint_paths([tmp_path])

        assert result.files_scanned == 4
        codes = sorted(f.code for f in result.findings)
        assert codes == ["LW101", "LW104"]
        files = {f.location.file for f in result.findings}
        assert files == {str(pkg / "reminders.py")}

    def test_findings_are_sorted_deterministically_across_files(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "b_module.py",
            "import time\nimport loomwork as lw\n\n\n"
            "@lw.workflow\nasync def b() -> None:\n    time.time()\n",
        )
        _write(
            tmp_path / "a_module.py",
            "import time\nimport loomwork as lw\n\n\n"
            "@lw.workflow\nasync def a() -> None:\n    time.time()\n",
        )

        result = lint_paths([tmp_path])
        files_in_order = [f.location.file for f in result.findings]
        assert files_in_order == sorted(files_in_order)

    def test_a_syntax_error_in_one_file_does_not_stop_the_scan(self, tmp_path: Path) -> None:
        _write(tmp_path / "broken.py", "def f(:\n    pass\n")
        _write(
            tmp_path / "ok.py",
            "import time\nimport loomwork as lw\n\n\n"
            "@lw.workflow\nasync def process() -> None:\n    time.time()\n",
        )

        result = lint_paths([tmp_path])
        codes = sorted(f.code for f in result.findings)
        assert codes == ["LW000", "LW101"]

    def test_exclude_glob_from_config_skips_a_subpackage(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "generated" / "wf.py",
            "import time\nimport loomwork as lw\n\n\n"
            "@lw.workflow\nasync def process() -> None:\n    time.time()\n",
        )
        _write(
            tmp_path / "app" / "wf.py",
            "import time\nimport loomwork as lw\n\n\n"
            "@lw.workflow\nasync def process() -> None:\n    time.time()\n",
        )

        config = LintConfig.from_mapping({"exclude": ["**/generated/**"]})
        result = lint_paths([tmp_path], config)
        files = {f.location.file for f in result.findings}
        assert files == {str(tmp_path / "app" / "wf.py")}

    def test_ignore_file_directive_excludes_a_module_from_a_package_scan(
        self, tmp_path: Path
    ) -> None:
        _write(
            tmp_path / "legacy.py",
            "# loomwork: ignore-file\n"
            "import time\nimport loomwork as lw\n\n\n"
            "@lw.workflow\nasync def process() -> None:\n    time.time()\n",
        )
        result = lint_paths([tmp_path])
        assert result.findings == ()
        assert result.files_scanned == 1

    def test_pyproject_toml_next_to_the_package_is_picked_up_explicitly(
        self, tmp_path: Path
    ) -> None:
        _write(
            tmp_path / "wf.py",
            "import time\nimport loomwork as lw\n\n\n"
            "@lw.workflow\nasync def process() -> None:\n    time.time()\n    print('x')\n",
        )
        _write(
            tmp_path / "pyproject.toml",
            "[tool.loomwork.lint]\nignore = ['LW104']\n",
        )
        config = LintConfig.from_pyproject(tmp_path / "pyproject.toml")
        result = lint_paths([tmp_path], config)
        assert [f.code for f in result.findings] == ["LW101"]


class TestFindingIsHashableForCallers:
    def test_findings_support_equality(self, tmp_path: Path) -> None:
        # LintResult / Finding are frozen dataclasses; callers often deduplicate or diff
        # results between runs, which requires equality (and, for sets, hashability of
        # the immutable fields) to work as expected.
        _write(
            tmp_path / "wf.py",
            "import time\nimport loomwork as lw\n\n\n"
            "@lw.workflow\nasync def process() -> None:\n    time.time()\n",
        )
        first = lint_paths([tmp_path]).findings
        second = lint_paths([tmp_path]).findings
        assert first == second
        assert isinstance(first[0], Finding)
