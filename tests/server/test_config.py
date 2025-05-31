"""``ServerConfig`` loading: TOML parsing, environment overrides, validation."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from loomwork.server.config import ServerConfig, load_config


def write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "server.toml"
    path.write_text(text)
    return path


def test_load_minimal_config(tmp_path: Path) -> None:
    path = write(
        tmp_path,
        """
        [server]
        host = "0.0.0.0"
        port = 9000

        [server.namespaces]
        default = "memory://"

        [server.auth]
        disabled = true
        """,
    )
    config = load_config(path)
    assert config.host == "0.0.0.0"
    assert config.port == 9000
    assert config.namespaces == {"default": "memory://"}
    assert config.auth.disabled is True
    # Defaults still apply where the file is silent.
    assert config.limits.long_poll_max_seconds == 20.0
    assert config.cors.enabled is False


def test_config_without_top_level_server_table(tmp_path: Path) -> None:
    """A short file whose whole document *is* the server table also works."""
    path = write(
        tmp_path,
        """
        host = "127.0.0.1"
        [namespaces]
        default = "memory://"
        [auth]
        disabled = true
        """,
    )
    config = load_config(path)
    assert config.host == "127.0.0.1"
    assert config.namespaces == {"default": "memory://"}


def test_env_overrides_scalar_fields(tmp_path: Path) -> None:
    path = write(
        tmp_path,
        """
        [server]
        host = "127.0.0.1"
        port = 8080
        [server.namespaces]
        default = "memory://"
        [server.auth]
        disabled = true
        """,
    )
    config = load_config(
        path,
        env={
            "LOOMWORK_SERVER__HOST": "0.0.0.0",
            "LOOMWORK_SERVER__PORT": "9999",
            "LOOMWORK_SERVER__LIMITS__LONG_POLL_MAX_SECONDS": "5",
            "LOOMWORK_SERVER__CORS__ENABLED": "true",
        },
    )
    assert config.host == "0.0.0.0"
    assert config.port == 9999
    assert config.limits.long_poll_max_seconds == 5.0
    assert config.cors.enabled is True


def test_env_overrides_nested_namespace(tmp_path: Path) -> None:
    path = write(
        tmp_path,
        """
        [server]
        [server.namespaces]
        default = "memory://"
        [server.auth]
        disabled = true
        """,
    )
    config = load_config(
        path, env={"LOOMWORK_SERVER__NAMESPACES__BILLING": "sqlite:///./billing.db"}
    )
    assert config.namespaces == {"default": "memory://", "billing": "sqlite:///./billing.db"}


def test_env_override_unrelated_variables_are_ignored(tmp_path: Path) -> None:
    path = write(
        tmp_path,
        """
        [server]
        [server.namespaces]
        default = "memory://"
        [server.auth]
        disabled = true
        """,
    )
    config = load_config(path, env={"HOME": "/root", "PATH": "/bin", "LOOMWORK_OTHER": "x"})
    assert config.host == "127.0.0.1"  # untouched default


def test_auth_requires_tokens_file_unless_disabled() -> None:
    with pytest.raises(ValidationError):
        ServerConfig.model_validate({"namespaces": {"default": "memory://"}, "auth": {}})


def test_invalid_namespace_name_rejected() -> None:
    with pytest.raises(ValidationError):
        ServerConfig.model_validate(
            {
                "namespaces": {"bad/name": "memory://"},
                "auth": {"disabled": True},
            }
        )


def test_extra_fields_rejected(tmp_path: Path) -> None:
    path = write(
        tmp_path,
        """
        [server]
        unknown_field = "x"
        [server.namespaces]
        default = "memory://"
        [server.auth]
        disabled = true
        """,
    )
    with pytest.raises(ValidationError):
        load_config(path)


def test_limits_validate_positive() -> None:
    with pytest.raises(ValidationError):
        ServerConfig.model_validate(
            {
                "namespaces": {"default": "memory://"},
                "auth": {"disabled": True},
                "limits": {"long_poll_max_seconds": -1},
            }
        )
