"""Workflow discovery: decorator forms, import aliases, and file/directory discovery."""

from __future__ import annotations

from pathlib import Path

import pytest

from loomwork.lint.discovery import iter_python_files
from loomwork.lint.errors import LintUsageError
from tests.lint.helpers import codes_of, lint


class TestDecoratorDiscovery:
    def test_import_loomwork_as_lw_at_workflow(self) -> None:
        findings = lint(
            """
            import time
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                time.time()
            """
        )
        assert codes_of(findings) == ["LW101"]

    def test_from_loomwork_import_workflow(self) -> None:
        findings = lint(
            """
            import time
            from loomwork import workflow

            @workflow
            async def process() -> None:
                time.time()
            """
        )
        assert codes_of(findings) == ["LW101"]

    def test_from_loomwork_import_workflow_as_alias(self) -> None:
        findings = lint(
            """
            import time
            from loomwork import workflow as wf

            @wf
            async def process() -> None:
                time.time()
            """
        )
        assert codes_of(findings) == ["LW101"]

    def test_decorator_called_with_arguments(self) -> None:
        findings = lint(
            """
            import time
            import loomwork as lw

            @lw.workflow(name="orders.process")
            async def process() -> None:
                time.time()
            """
        )
        assert codes_of(findings) == ["LW101"]

    def test_import_loomwork_sdk_at_workflow(self) -> None:
        findings = lint(
            """
            import time
            import loomwork.sdk as sdk

            @sdk.workflow
            async def process() -> None:
                time.time()
            """
        )
        assert codes_of(findings) == ["LW101"]

    def test_plain_import_loomwork_dotted_workflow(self) -> None:
        findings = lint(
            """
            import time
            import loomwork

            @loomwork.workflow
            async def process() -> None:
                time.time()
            """
        )
        assert codes_of(findings) == ["LW101"]

    def test_undecorated_function_is_not_checked(self) -> None:
        findings = lint(
            """
            import time

            async def process() -> None:
                time.time()
            """
        )
        assert findings == []

    def test_unrelated_decorator_is_not_a_workflow(self) -> None:
        findings = lint(
            """
            import time
            import functools

            @functools.wraps(print)
            async def process() -> None:
                time.time()
            """
        )
        assert findings == []


class TestFileDiscovery:
    def test_finds_py_files_in_a_directory_sorted(self, tmp_path: Path) -> None:
        (tmp_path / "b.py").write_text("x = 1\n")
        (tmp_path / "a.py").write_text("y = 2\n")
        (tmp_path / "notes.txt").write_text("hello\n")
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "c.py").write_text("z = 3\n")

        found = list(iter_python_files([tmp_path]))
        assert found == sorted(found)
        assert {f.name for f in found} == {"a.py", "b.py", "c.py"}

    def test_a_single_file_argument_is_linted_directly(self, tmp_path: Path) -> None:
        target = tmp_path / "only.py"
        target.write_text("x = 1\n")
        assert list(iter_python_files([target])) == [target]

    def test_missing_path_is_a_usage_error(self, tmp_path: Path) -> None:
        with pytest.raises(LintUsageError, match="does not exist"):
            list(iter_python_files([tmp_path / "missing.py"]))

    def test_exclude_glob_skips_matching_files(self, tmp_path: Path) -> None:
        (tmp_path / "keep.py").write_text("x = 1\n")
        generated = tmp_path / "generated"
        generated.mkdir()
        (generated / "skip.py").write_text("x = 1\n")

        found = list(iter_python_files([tmp_path], exclude=["**/generated/**"]))
        assert {f.name for f in found} == {"keep.py"}

    def test_deduplicates_overlapping_paths(self, tmp_path: Path) -> None:
        (tmp_path / "a.py").write_text("x = 1\n")
        found = list(iter_python_files([tmp_path, tmp_path / "a.py"]))
        assert len(found) == 1
