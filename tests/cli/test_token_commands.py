"""``loomwork token ...``."""

from __future__ import annotations

import json
import stat
from pathlib import Path

from loomwork.server.auth import TokenFile
from tests.cli.conftest import Cli


def test_create_prints_token_once_and_stores_only_a_hash(cli: Cli, tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    result = cli.ok(
        "token", "create", "ci-worker", "--role", "worker", "--namespace", "prod", "-f", str(path)
    )
    token = result.out.strip()
    assert token.startswith("lwk_ci-worker_")
    assert "not shown again" in result.err

    stored = path.read_text()
    secret = token.split("_", 2)[2]
    assert secret not in stored
    assert stat.S_IMODE(path.stat().st_mode) == 0o600

    record = TokenFile.load(path).authenticate(token)
    assert record.namespaces == ("prod",)


def test_create_defaults_and_multiple_roles(cli: Cli, tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    result = cli.ok(
        "token",
        "create",
        "ops",
        "--role",
        "operator",
        "--role",
        "reader",
        "-f",
        str(path),
        "--json",
    )
    data = json.loads(result.out)
    assert data["record"]["roles"] == ["operator", "reader"]
    assert data["record"]["namespaces"] == ["default"]
    assert data["token"].startswith("lwk_ops_")


def test_create_to_output_file(cli: Cli, tmp_path: Path) -> None:
    path, output = tmp_path / "tokens.json", tmp_path / "secret"
    result = cli.ok("token", "create", "w", "--role", "worker", "-f", str(path), "-o", str(output))
    assert result.out == ""
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    TokenFile.load(path).authenticate(output.read_text().strip())


def test_existing_id_needs_replace(cli: Cli, tmp_path: Path) -> None:
    path = str(tmp_path / "tokens.json")
    first = cli.ok("token", "create", "w", "--role", "worker", "-f", path).out.strip()
    again = cli("token", "create", "w", "--role", "worker", "-f", path)
    assert again.code == 2
    assert "--replace" in again.err

    rotated = cli.ok(
        "token", "create", "w", "--role", "worker", "-f", path, "--replace"
    ).out.strip()
    assert rotated != first
    assert cli("token", "verify", first, "-f", path).code == 1
    assert cli.ok("token", "verify", rotated, "-f", path).out.startswith("valid: w")


def test_invalid_id_is_rejected(cli: Cli, tmp_path: Path) -> None:
    result = cli("token", "create", "has_underscore", "--role", "worker", "-f", str(tmp_path / "t"))
    assert result.code == 2
    assert "invalid token id" in result.err


def test_list_never_shows_hashes(cli: Cli, tmp_path: Path) -> None:
    path = str(tmp_path / "tokens.json")
    cli.ok("token", "create", "b", "--role", "reader", "-f", path, "--description", "dashboards")
    cli.ok("token", "create", "a", "--role", "admin", "--namespace", "*", "-f", path)
    listed = cli.ok("token", "list", "-f", path)
    assert [line.split()[0] for line in listed.lines[1:]] == ["a", "b"]
    assert "dashboards" in listed.out

    as_json = json.loads(cli.ok("token", "ls", "-f", path, "--json").out)
    assert set(as_json[0]) == {"id", "roles", "namespaces", "description"}


def test_revoke(cli: Cli, tmp_path: Path) -> None:
    path = str(tmp_path / "tokens.json")
    token = cli.ok("token", "create", "gone", "--role", "worker", "-f", path).out.strip()
    cli.ok("token", "create", "kept", "--role", "worker", "-f", path)
    revoked = cli.ok("token", "revoke", "gone", "-f", path)
    assert "revoked gone" in revoked.err
    assert cli("token", "verify", token, "-f", path).code == 1
    assert [r.split()[0] for r in cli.ok("token", "list", "-f", path).lines[1:]] == ["kept"]

    missing = cli("token", "revoke", "gone", "-f", path)
    assert missing.code == 2
    assert "no such token: gone" in missing.err


def test_verify_reads_stdin_by_default(cli: Cli, tmp_path: Path) -> None:
    path = str(tmp_path / "tokens.json")
    token = cli.ok("token", "create", "s", "--role", "worker", "-f", path).out.strip()
    result = cli.ok("token", "verify", "-f", path, "--json", stdin=token + "\n")
    assert json.loads(result.out) == {"id": "s", "roles": ["worker"]}

    wrong = cli("token", "verify", "-f", path, stdin="lwk_s_wrong\n")
    assert wrong.code == 1
    assert "does not match" in wrong.err


def test_missing_and_corrupt_token_files(cli: Cli, tmp_path: Path) -> None:
    assert cli("token", "list", "-f", str(tmp_path / "absent.json")).code == 2
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text('{"not": "a list"}')
    result = cli("token", "list", "-f", str(corrupt))
    assert result.code == 2
    assert "not a valid token file" in result.err


def test_tokens_file_from_environment(cli: Cli, tmp_path: Path, monkeypatch) -> None:
    # The default is read when the parser is built, i.e. per invocation.
    path = tmp_path / "env-tokens.json"
    monkeypatch.setenv("LOOMWORK_TOKENS_FILE", str(path))
    cli.ok("token", "create", "e", "--role", "worker")
    assert path.exists()


def test_if_missing_is_idempotent(cli: Cli, tmp_path: Path) -> None:
    path, output = str(tmp_path / "tokens.json"), tmp_path / "worker.token"
    args = ("token", "create", "w", "--role", "worker", "-f", path, "-o", str(output))
    cli.ok(*args, "--if-missing")
    first = output.read_text()
    again = cli.ok(*args, "--if-missing")
    assert "already exists" in again.err
    assert output.read_text() == first
    assert cli("token", "create", "w", "--role", "worker", "--replace", "--if-missing").code == 2
