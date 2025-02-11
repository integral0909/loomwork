"""Tests for the ``loomwork.codec.keys`` operator helper and CLI."""

from __future__ import annotations

import base64
import subprocess
import sys

import pytest

from loomwork.codec.keyring import KEY_BYTES, StaticKeyRing
from loomwork.codec.keys import generate_key, main


class TestGenerateKey:
    def test_generates_correct_length_material(self) -> None:
        key = generate_key()
        assert len(base64.b64decode(key)) == KEY_BYTES

    def test_generates_valid_base64(self) -> None:
        key = generate_key()
        # round-trips through base64 without raising
        base64.b64decode(key, validate=True)

    def test_successive_keys_differ(self) -> None:
        assert generate_key() != generate_key()

    def test_generated_key_works_with_static_key_ring(self) -> None:
        key = generate_key()
        ring = StaticKeyRing({"k1": key}, current="k1")
        assert len(ring.current_key().material) == KEY_BYTES


class TestCliMain:
    def test_generate_prints_a_usable_key(self, capsys: pytest.CaptureFixture[str]) -> None:
        exit_code = main(["generate"])
        assert exit_code == 0
        output = capsys.readouterr().out.strip()
        assert len(base64.b64decode(output)) == KEY_BYTES

    def test_unknown_argument_prints_usage_and_fails(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        exit_code = main(["bogus"])
        assert exit_code != 0
        assert "usage" in capsys.readouterr().err

    def test_no_arguments_prints_usage_and_fails(self, capsys: pytest.CaptureFixture[str]) -> None:
        exit_code = main([])
        assert exit_code != 0
        assert "usage" in capsys.readouterr().err


@pytest.mark.slow
def test_module_runs_as_a_script() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "loomwork.codec.keys", "generate"],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert len(base64.b64decode(result.stdout.strip())) == KEY_BYTES
