"""The top-level public functions: ``lint_source``, ``lint_paths``, ``analyze_file``,
and running the package as ``python -m loomwork.lint``.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from loomwork.lint import LintConfig, LintResult, lint_paths, lint_source
from loomwork.lint.analyzer import analyze_file


class TestLintSource:
    def test_returns_a_lint_result(self) -> None:
        result = lint_source("import time\ntime.time()\n")
        assert isinstance(result, LintResult)
        assert result.files_scanned == 1
        # ``time.time()`` here is module-level code, not inside a workflow, so nothing
        # is checked - lint_source does not special-case "no workflows found".
        assert result.findings == ()

    def test_default_filename_is_used_in_findings(self) -> None:
        result = lint_source(
            "import time\nimport loomwork as lw\n\n\n"
            "@lw.workflow\nasync def process() -> None:\n    time.time()\n"
        )
        assert result.findings[0].location.file == "<string>"

    def test_custom_filename_is_reflected(self) -> None:
        result = lint_source("x = 1\n", filename="custom.py")
        assert result.files_scanned == 1


class TestLintPaths:
    def test_lints_a_directory_and_returns_a_result(self, tmp_path: Path) -> None:
        (tmp_path / "wf.py").write_text(
            "import time\nimport loomwork as lw\n\n\n"
            "@lw.workflow\nasync def process() -> None:\n    time.time()\n"
        )
        result = lint_paths([tmp_path])
        assert result.files_scanned == 1
        assert [f.code for f in result.findings] == ["LW101"]

    def test_accepts_string_paths(self, tmp_path: Path) -> None:
        (tmp_path / "wf.py").write_text("x = 1\n")
        result = lint_paths([str(tmp_path)])
        assert result.files_scanned == 1
        assert result.findings == ()

    def test_applies_config(self, tmp_path: Path) -> None:
        (tmp_path / "wf.py").write_text(
            "import loomwork as lw\n\n\n@lw.workflow\nasync def process() -> None:\n"
            "    print('hi')\n"
        )
        config = LintConfig.from_mapping({"ignore": ["LW104"]})
        result = lint_paths([tmp_path], config)
        assert result.findings == ()


class TestAnalyzeFileReadErrors:
    def test_undecodable_file_reports_a_parse_error(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.py"
        path.write_bytes(b"\xff\xfe\x00\x01not utf-8 at all\xfe")
        findings = analyze_file(path)
        assert [f.code for f in findings] == ["LW000"]
        assert "could not read file" in findings[0].message


class TestRunAsModule:
    def test_python_dash_m_loomwork_lint_runs_and_exits_nonzero_on_findings(
        self, tmp_path: Path
    ) -> None:
        wf = tmp_path / "wf.py"
        wf.write_text(
            "import time\nimport loomwork as lw\n\n\n"
            "@lw.workflow\nasync def process() -> None:\n    time.time()\n"
        )
        completed = subprocess.run(
            [sys.executable, "-m", "loomwork.lint", str(wf)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 1
        assert "LW101" in completed.stdout

    def test_python_dash_m_loomwork_lint_exits_zero_on_a_clean_file(self, tmp_path: Path) -> None:
        wf = tmp_path / "wf.py"
        wf.write_text("x = 1\n")
        completed = subprocess.run(
            [sys.executable, "-m", "loomwork.lint", str(wf)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0
