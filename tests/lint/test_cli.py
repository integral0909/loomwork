"""``python -m loomwork.lint`` / :func:`loomwork.lint.cli.main`."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from loomwork.lint.cli import main

_CLEAN_SOURCE = """\
import loomwork as lw


@lw.workflow
async def process() -> None:
    await lw.sleep(1)
"""

_DIRTY_SOURCE = """\
import time
import loomwork as lw


@lw.workflow
async def process() -> None:
    time.time()
"""


class TestExitCodes:
    def test_clean_file_exits_zero(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf = tmp_path / "wf.py"
        wf.write_text(_CLEAN_SOURCE)
        assert main([str(wf)]) == 0
        out = capsys.readouterr().out
        assert "0 finding(s)" in out

    def test_dirty_file_exits_one(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        wf = tmp_path / "wf.py"
        wf.write_text(_DIRTY_SOURCE)
        assert main([str(wf)]) == 1
        out = capsys.readouterr().out
        assert "LW101" in out

    def test_missing_path_exits_two(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        assert main([str(tmp_path / "missing.py")]) == 2
        err = capsys.readouterr().err
        assert "does not exist" in err

    def test_no_arguments_exits_two(self, capsys: pytest.CaptureFixture[str]) -> None:
        assert main([]) == 2
        err = capsys.readouterr().err
        assert err  # argparse's own usage error

    def test_unknown_select_code_exits_two(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf = tmp_path / "wf.py"
        wf.write_text(_CLEAN_SOURCE)
        assert main([str(wf), "--select", "LW999"]) == 2
        err = capsys.readouterr().err
        assert "LW999" in err

    def test_invalid_config_exits_two(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf = tmp_path / "wf.py"
        wf.write_text(_CLEAN_SOURCE)
        config = tmp_path / "pyproject.toml"
        config.write_text("[tool.loomwork.lint]\nselect = ['LW999']\n")
        assert main([str(wf), "--config", str(config)]) == 2
        err = capsys.readouterr().err
        assert "LW999" in err


class TestFormats:
    def test_json_format(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        wf = tmp_path / "wf.py"
        wf.write_text(_DIRTY_SOURCE)
        assert main([str(wf), "--format", "json"]) == 1
        payload = json.loads(capsys.readouterr().out)
        assert payload["findings"][0]["code"] == "LW101"

    def test_sarif_format(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        wf = tmp_path / "wf.py"
        wf.write_text(_DIRTY_SOURCE)
        assert main([str(wf), "--format", "sarif"]) == 1
        document = json.loads(capsys.readouterr().out)
        assert document["version"] == "2.1.0"

    def test_output_file(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        wf = tmp_path / "wf.py"
        wf.write_text(_DIRTY_SOURCE)
        report = tmp_path / "report.txt"
        assert main([str(wf), "--output", str(report)]) == 1
        assert "LW101" in report.read_text()
        assert capsys.readouterr().out == ""


class TestSelectAndIgnoreFlags:
    def test_select_flag_restricts_output(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf = tmp_path / "wf.py"
        wf.write_text(
            "import time\nimport loomwork as lw\n\n\n"
            "@lw.workflow\nasync def process() -> None:\n"
            "    time.time()\n"
            "    print('hi')\n"
        )
        assert main([str(wf), "--select", "LW104"]) == 1
        out = capsys.readouterr().out
        assert "LW104" in out
        assert "LW101" not in out

    def test_ignore_flag_suppresses_a_code(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf = tmp_path / "wf.py"
        wf.write_text(_DIRTY_SOURCE)
        assert main([str(wf), "--ignore", "LW101"]) == 0
        out = capsys.readouterr().out
        assert "0 finding(s)" in out


class TestConfigDiscovery:
    def test_default_config_path_is_pyproject_toml_in_cwd(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        wf = tmp_path / "wf.py"
        wf.write_text(_DIRTY_SOURCE)
        (tmp_path / "pyproject.toml").write_text("[tool.loomwork.lint]\nignore = ['LW101']\n")
        monkeypatch.chdir(tmp_path)
        assert main([str(wf)]) == 0
