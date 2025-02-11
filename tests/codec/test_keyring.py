"""Tests for :class:`Key` and the :class:`KeyRing` implementations."""

from __future__ import annotations

import base64
import json
import os
import stat
from pathlib import Path

import pytest

from loomwork.codec.errors import UnknownKeyError
from loomwork.codec.keyring import (
    ENV_CURRENT_KEY,
    ENV_KEY_PREFIX,
    EnvironmentKeyRing,
    FileKeyRing,
    InsecureKeyFilePermissionsWarning,
    Key,
    KeyRing,
    StaticKeyRing,
)

KEY_A = b"a" * 32
KEY_B = b"b" * 32
B64_A = base64.b64encode(KEY_A).decode()
B64_B = base64.b64encode(KEY_B).decode()


class TestKey:
    def test_rejects_wrong_length_material(self) -> None:
        with pytest.raises(ValueError, match="32 bytes"):
            Key("k1", b"too-short")

    def test_repr_redacts_material(self) -> None:
        key = Key("k1", KEY_A)
        text = repr(key)
        assert "k1" in text
        assert "redacted" in text
        assert KEY_A.hex() not in text
        assert base64.b64encode(KEY_A).decode() not in text

    def test_str_also_redacts_material(self) -> None:
        # object.__str__ falls back to __repr__ when __str__ is not defined.
        assert base64.b64encode(KEY_A).decode() not in str(Key("k1", KEY_A))

    def test_equality_by_value(self) -> None:
        assert Key("k1", KEY_A) == Key("k1", KEY_A)
        assert Key("k1", KEY_A) != Key("k1", KEY_B)


class TestStaticKeyRing:
    def test_current_key_and_lookup(self) -> None:
        ring = StaticKeyRing({"a": KEY_A, "b": B64_B}, current="a")
        assert ring.current_key_id == "a"
        assert ring.current_key() == Key("a", KEY_A)
        assert ring.get_key("b") == Key("b", KEY_B)

    def test_accepts_base64_or_raw_bytes(self) -> None:
        ring = StaticKeyRing({"a": B64_A}, current="a")
        assert ring.current_key().material == KEY_A

    def test_unknown_key_raises(self) -> None:
        ring = StaticKeyRing({"a": KEY_A}, current="a")
        with pytest.raises(UnknownKeyError) as exc_info:
            ring.get_key("missing")
        assert exc_info.value.key_id == "missing"

    def test_current_must_be_among_keys(self) -> None:
        with pytest.raises(ValueError, match="not among the provided keys"):
            StaticKeyRing({"a": KEY_A}, current="b")

    def test_invalid_base64_material_raises(self) -> None:
        with pytest.raises(ValueError, match="not valid base64"):
            StaticKeyRing({"a": "not-base64!!"}, current="a")

    def test_satisfies_keyring_protocol(self) -> None:
        assert isinstance(StaticKeyRing({"a": KEY_A}, current="a"), KeyRing)

    # ------------------------------------------------------------------- rotation

    def test_rotation_makes_new_key_current_but_keeps_old_key_decodable(self) -> None:
        original = StaticKeyRing({"a": KEY_A}, current="a")
        rotated = original.rotated("b", KEY_B)

        assert rotated.current_key_id == "b"
        assert rotated.current_key().material == KEY_B
        assert rotated.get_key("a").material == KEY_A  # old key still resolvable

    def test_rotation_does_not_mutate_the_original_ring(self) -> None:
        original = StaticKeyRing({"a": KEY_A}, current="a")
        original.rotated("b", KEY_B)
        assert original.current_key_id == "a"
        with pytest.raises(UnknownKeyError):
            original.get_key("b")

    def test_repeated_rotation_keeps_every_previous_key(self) -> None:
        ring = StaticKeyRing({"a": KEY_A}, current="a")
        ring = ring.rotated("b", KEY_B)
        ring = ring.rotated("c", base64.b64encode(b"c" * 32).decode())
        assert ring.current_key_id == "c"
        assert ring.get_key("a").material == KEY_A
        assert ring.get_key("b").material == KEY_B


class TestEnvironmentKeyRing:
    def test_reads_keys_and_current_from_mapping(self) -> None:
        env = {f"{ENV_KEY_PREFIX}A": B64_A, f"{ENV_KEY_PREFIX}B": B64_B, ENV_CURRENT_KEY: "A"}
        ring = EnvironmentKeyRing(env=env)
        assert ring.current_key_id == "A"
        assert ring.current_key().material == KEY_A
        assert ring.get_key("B").material == KEY_B

    def test_reads_from_os_environ_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(f"{ENV_KEY_PREFIX}X", B64_A)
        monkeypatch.setenv(ENV_CURRENT_KEY, "X")
        ring = EnvironmentKeyRing()
        assert ring.current_key().material == KEY_A

    def test_missing_keys_raises(self) -> None:
        with pytest.raises(ValueError, match="LOOMWORK_KEY_"):
            EnvironmentKeyRing(env={ENV_CURRENT_KEY: "A"})

    def test_missing_current_raises(self) -> None:
        with pytest.raises(ValueError, match=ENV_CURRENT_KEY):
            EnvironmentKeyRing(env={f"{ENV_KEY_PREFIX}A": B64_A})

    def test_current_naming_unknown_key_raises(self) -> None:
        with pytest.raises(ValueError, match="not among the provided keys"):
            EnvironmentKeyRing(env={f"{ENV_KEY_PREFIX}A": B64_A, ENV_CURRENT_KEY: "B"})

    def test_unrelated_environment_variables_are_ignored(self) -> None:
        env = {
            f"{ENV_KEY_PREFIX}A": B64_A,
            ENV_CURRENT_KEY: "A",
            "PATH": "/usr/bin",
            "LOOMWORK_KEY_": "not-a-real-key",  # exact prefix, no id: must be ignored
        }
        ring = EnvironmentKeyRing(env=env)
        assert ring.current_key_id == "A"
        with pytest.raises(UnknownKeyError):
            ring.get_key("")


class TestFileKeyRing:
    def _write(self, path: Path, current: str, keys: dict[str, str]) -> Path:
        path.write_text(json.dumps({"current": current, "keys": keys}))
        path.chmod(0o600)
        return path

    def test_loads_keys_from_json_file(self, tmp_path: Path) -> None:
        path = self._write(tmp_path / "keys.json", "a", {"a": B64_A, "b": B64_B})
        ring = FileKeyRing(path)
        assert ring.current_key_id == "a"
        assert ring.current_key().material == KEY_A
        assert ring.get_key("b").material == KEY_B
        assert ring.path == path

    def test_accepts_str_path(self, tmp_path: Path) -> None:
        path = self._write(tmp_path / "keys.json", "a", {"a": B64_A})
        ring = FileKeyRing(str(path))
        assert ring.current_key().material == KEY_A

    def test_missing_file_raises_value_error(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="cannot read key file"):
            FileKeyRing(tmp_path / "does-not-exist.json")

    def test_invalid_json_raises_value_error(self, tmp_path: Path) -> None:
        path = tmp_path / "keys.json"
        path.write_text("{not json")
        path.chmod(0o600)
        with pytest.raises(ValueError, match="not valid JSON"):
            FileKeyRing(path)

    def test_missing_fields_raise_value_error(self, tmp_path: Path) -> None:
        path = tmp_path / "keys.json"
        path.write_text(json.dumps({"keys": {"a": B64_A}}))  # no "current"
        path.chmod(0o600)
        with pytest.raises(ValueError, match="'keys' and 'current'"):
            FileKeyRing(path)

    @pytest.mark.skipif(os.name == "nt", reason="POSIX file permissions only")
    def test_warns_when_group_or_other_can_read_the_file(self, tmp_path: Path) -> None:
        path = self._write(tmp_path / "keys.json", "a", {"a": B64_A})
        path.chmod(0o644)
        with pytest.warns(InsecureKeyFilePermissionsWarning, match="chmod 600"):
            FileKeyRing(path)

    @pytest.mark.skipif(os.name == "nt", reason="POSIX file permissions only")
    def test_no_warning_for_owner_only_permissions(self, tmp_path: Path) -> None:
        path = self._write(tmp_path / "keys.json", "a", {"a": B64_A})
        path.chmod(0o600)
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error")  # any warning fails the test
            FileKeyRing(path)

    @pytest.mark.skipif(os.name == "nt", reason="POSIX file permissions only")
    def test_world_writable_also_warns(self, tmp_path: Path) -> None:
        path = self._write(tmp_path / "keys.json", "a", {"a": B64_A})
        path.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IWOTH)
        with pytest.warns(InsecureKeyFilePermissionsWarning):
            FileKeyRing(path)
