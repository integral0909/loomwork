"""Unit tests for the CLI's parsing, output, loading and connection helpers."""

from __future__ import annotations

import argparse
import io
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from loomwork.cli.connection import ConnectionOptions, parse_attribute_types
from loomwork.cli.errors import UsageError
from loomwork.cli.loader import load_app
from loomwork.cli.output import Console, format_cell, render_fields, render_table, to_jsonable
from loomwork.cli.parsing import (
    format_duration,
    parse_arguments,
    parse_duration,
    parse_key_values,
    parse_timestamp,
    read_at_file,
)
from loomwork.codec import default_converter
from loomwork.visibility.schema import AttributeType

# --------------------------------------------------------------------------- durations


@pytest.mark.parametrize(
    ("text", "seconds"),
    [
        ("90", 90),
        ("1.5", 1.5),
        ("250ms", 0.25),
        ("30s", 30),
        ("5m", 300),
        ("2h", 7200),
        ("7d", 604800),
        ("1w", 604800),
        ("1h30m", 5400),
        ("1d2h3m4s", 93784),
        (" 2H ", 7200),
    ],
)
def test_parse_duration(text: str, seconds: float) -> None:
    assert parse_duration(text) == timedelta(seconds=seconds)


@pytest.mark.parametrize("text", ["", "abc", "5x", "m5", "1h 30m", "-5", "5m-", "1..5s"])
def test_parse_duration_rejects(text: str) -> None:
    with pytest.raises(UsageError):
        parse_duration(text)


@pytest.mark.parametrize(
    ("seconds", "text"),
    [(0, "0s"), (0.25, "250ms"), (59, "59s"), (90, "1m30s"), (5400, "1h30m"), (93784, "1d2h3m4s")],
)
def test_format_duration(seconds: float, text: str) -> None:
    assert format_duration(timedelta(seconds=seconds)) == text


def test_format_duration_none_and_negative() -> None:
    assert format_duration(None) == "-"
    assert format_duration(-90) == "-1m30s"


@given(st.integers(min_value=1, max_value=10_000_000))
def test_duration_round_trip(seconds: int) -> None:
    assert parse_duration(format_duration(timedelta(seconds=seconds))) == timedelta(seconds=seconds)


# --------------------------------------------------------------------------- timestamps

NOW = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("now", NOW),
        ("-2h", NOW - timedelta(hours=2)),
        ("+30m", NOW + timedelta(minutes=30)),
        ("2024-05-01T10:00:00Z", datetime(2024, 5, 1, 10, tzinfo=UTC)),
        ("2024-05-01T10:00:00", datetime(2024, 5, 1, 10, tzinfo=UTC)),
        (
            "2024-05-01T12:00:00+02:00",
            datetime(2024, 5, 1, 10, tzinfo=UTC),
        ),
        ("2024-05-01", datetime(2024, 5, 1, tzinfo=UTC)),
    ],
)
def test_parse_timestamp(text: str, expected: datetime) -> None:
    assert parse_timestamp(text, now=NOW) == expected


def test_parse_timestamp_rejects_garbage() -> None:
    with pytest.raises(UsageError, match="ISO 8601"):
        parse_timestamp("yesterday", now=NOW)


# --------------------------------------------------------------------------- arguments


def test_parse_arguments_json_and_raw(tmp_path: Path) -> None:
    (tmp_path / "a.json").write_text('{"k": [1, 2]}')
    assert parse_arguments(["1", '"x"', "null", f"@{tmp_path / 'a.json'}"]) == [
        1,
        "x",
        None,
        {"k": [1, 2]},
    ]
    assert parse_arguments(["1", "two"], raw_strings=True) == ["1", "two"]
    assert parse_arguments(["@-"], stdin_text="[true]") == [[True]]


def test_parse_arguments_errors(tmp_path: Path) -> None:
    with pytest.raises(UsageError, match="argument 2"):
        parse_arguments(["1", "{bad"])
    with pytest.raises(UsageError, match="file not found"):
        parse_arguments([f"@{tmp_path / 'missing.json'}"])
    with pytest.raises(UsageError, match="stdin"):
        read_at_file("@-", None)


def test_parse_key_values() -> None:
    assert parse_key_values(["tier=gold", "n=3", "ok=true", "list=[1]", "eq=a=b"]) == {
        "tier": "gold",
        "n": 3,
        "ok": True,
        "list": [1],
        "eq": "a=b",
    }
    with pytest.raises(UsageError, match="key=value"):
        parse_key_values(["novalue"])
    with pytest.raises(UsageError):
        parse_key_values(["=x"])


# --------------------------------------------------------------------------- output


class Color(Enum):
    RED = "red"


@dataclass
class Point:
    x: int
    when: datetime


def test_to_jsonable_handles_domain_types() -> None:
    payload = default_converter.encode("v")
    value = {
        "enum": Color.RED,
        "delta": timedelta(minutes=1),
        "point": Point(1, NOW),
        "payload": payload,
        "items": (1, 2),
        "set": frozenset({"a"}),
    }
    data = to_jsonable(value)
    assert data["enum"] == "red"
    assert data["delta"] == 60
    assert data["point"] == {"x": 1, "when": NOW.isoformat()}
    assert data["payload"] == payload.to_dict()
    assert data["items"] == [1, 2]
    assert data["set"] == ["a"]
    json.dumps(data)


def test_format_cell() -> None:
    assert format_cell(None) == "-"
    assert format_cell(True) == "yes"
    assert format_cell(NOW.replace(microsecond=123)) == "2024-06-01T12:00:00Z"
    assert format_cell(timedelta(hours=2)) == "2h"
    assert format_cell(1.50) == "1.5"
    assert format_cell({"b": 1, "a": [1]}) == '{"a":[1],"b":1}'
    assert format_cell("line\nbreak") == "line break"
    long = format_cell("x" * 100)
    assert len(long) == 60
    assert long.endswith("…")
    assert format_cell("x" * 100, limit=None) == "x" * 100


def test_render_table_aligns_columns() -> None:
    text = render_table(["name", "n"], [["alpha", 1], ["b", 22]])
    assert text.splitlines() == ["NAME   N", "alpha  1", "b      22"]


def test_render_table_full_last_column() -> None:
    text = render_table(["id", "details"], [[1, "d" * 80]], full_last=True)
    assert text.splitlines()[1].endswith("d" * 80)


def test_render_fields() -> None:
    assert render_fields([("a", 1), ("longer", None)]) == "a:      1\nlonger: -"
    assert render_fields([]) == ""
    multi = render_fields([("k", "one\ntwo")])
    assert multi == "k:\n   one\n   two"


def test_console_table_json_mode_prefers_records() -> None:
    out = io.StringIO()
    console = Console(out, io.StringIO(), json_output=True)
    console.table(["a"], [[1]], records=[{"full": True}])
    assert json.loads(out.getvalue()) == [{"full": True}]
    out.truncate(0)
    out.seek(0)
    console.table(["a", "b"], [[1, 2]])
    assert json.loads(out.getvalue()) == [{"a": 1, "b": 2}]


def test_console_empty_table_goes_to_stderr() -> None:
    out, err = io.StringIO(), io.StringIO()
    Console(out, err).table(["a"], [], empty="nothing here")
    assert out.getvalue() == ""
    assert err.getvalue() == "nothing here\n"


def test_console_read_stdin_ignores_terminals() -> None:
    class Tty(io.StringIO):
        def isatty(self) -> bool:
            return True

    assert Console(stdin=Tty("x")).read_stdin() is None
    assert Console(stdin=io.StringIO("x")).read_stdin() == "x"


# --------------------------------------------------------------------------- loader


def test_load_module_collects_definitions() -> None:
    spec = load_app("tests.cli.sample_app")
    assert spec.workflow_names == ["cli.approval", "cli.fail", "cli.greet", "cli.sleeper"]
    assert spec.step_names == ["cli.shout"]


@pytest.mark.parametrize("attribute", ["WORKFLOWS", "build"])
def test_load_attribute_list_or_callable(attribute: str) -> None:
    spec = load_app(f"tests.cli.sample_app:{attribute}")
    assert "cli.greet" in spec.workflow_names


def test_load_loomwork_app_uses_its_converter(tmp_path: Path) -> None:
    (tmp_path / "embedded.py").write_text(
        "import loomwork as lw\n"
        "from loomwork.codec import DataConverter\n"
        "@lw.workflow(name='e.flow')\nasync def flow() -> None: ...\n"
        "converter = DataConverter()\n"
        "app = lw.Loomwork(workflows=[flow], converter=converter)\n"
    )
    spec = load_app(f"{tmp_path / 'embedded.py'}:app")
    assert spec.workflow_names == ["e.flow"]
    module = __import__("sys").modules["_loomwork_app_embedded"]
    assert spec.converter is module.converter


def test_load_registry(tmp_path: Path) -> None:
    (tmp_path / "reg.py").write_text(
        "import loomwork as lw\nfrom loomwork.sdk.definitions import Registry\n"
        "@lw.workflow(name='r.flow')\nasync def flow() -> None: ...\n"
        "registry = Registry([flow])\n"
    )
    assert load_app(f"{tmp_path / 'reg.py'}:registry").workflow_names == ["r.flow"]


def test_load_errors(tmp_path: Path) -> None:
    with pytest.raises(UsageError, match="cannot import module"):
        load_app("no_such_module_xyz")
    with pytest.raises(UsageError, match="no attribute"):
        load_app("tests.cli.sample_app:missing")
    with pytest.raises(UsageError, match="file not found"):
        load_app(str(tmp_path / "absent.py"))
    (tmp_path / "empty.py").write_text("x = 1\n")
    with pytest.raises(UsageError, match="defines no workflows"):
        load_app(str(tmp_path / "empty.py"))
    with pytest.raises(UsageError, match="expected a Loomwork app"):
        load_app(f"{tmp_path / 'empty.py'}:x")


def test_import_errors_inside_the_app_are_not_hidden(tmp_path: Path) -> None:
    (tmp_path / "broken.py").write_text("import definitely_not_installed_abc\n")
    with pytest.raises(ModuleNotFoundError):
        load_app(str(tmp_path / "broken.py"))


# --------------------------------------------------------------------------- connection


def namespace(**values: object) -> argparse.Namespace:
    defaults: dict[str, object] = {
        "store": None,
        "server": None,
        "token_file": None,
        "namespace": None,
        "timeout": None,
        "attr_type": None,
    }
    return argparse.Namespace(**{**defaults, **values})


def test_options_from_environment() -> None:
    options = ConnectionOptions.from_args(
        namespace(),
        {
            "LOOMWORK_SERVER_URL": "https://lw.example",
            "LOOMWORK_TOKEN": "lwk_a_b",
            "LOOMWORK_NAMESPACE": "prod",
            "LOOMWORK_SEARCH_ATTRIBUTES": "tier=string, n=int",
        },
    )
    assert options.server_url == "https://lw.example"
    assert options.token == "lwk_a_b"
    assert options.namespace == "prod"
    assert options.search_attributes.type_of("n") is AttributeType.INT
    assert options.target == "https://lw.example (namespace prod)"


def test_flags_win_over_environment() -> None:
    env = {"LOOMWORK_SERVER_URL": "https://lw.example", "LOOMWORK_STORE_URL": "sqlite:///x.db"}
    by_flag = ConnectionOptions.from_args(namespace(store="sqlite:///y.db"), env)
    assert by_flag.store_url == "sqlite:///y.db"
    assert by_flag.server_url is None
    with pytest.raises(UsageError, match="both"):
        ConnectionOptions.from_args(namespace(), env)
    with pytest.raises(UsageError, match="not both"):
        ConnectionOptions.from_args(namespace(store="a", server="b"), {})


def test_token_file(tmp_path: Path) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("lwk_x_secret\n")
    options = ConnectionOptions.from_args(
        namespace(server="http://s", token_file=str(token_file)), {"LOOMWORK_TOKEN": "ignored"}
    )
    assert options.token == "lwk_x_secret"
    assert "secret" not in options.target

    (tmp_path / "empty").write_text("\n")
    with pytest.raises(UsageError, match="empty"):
        ConnectionOptions.from_args(namespace(token_file=str(tmp_path / "empty")), {})
    with pytest.raises(UsageError, match="not found"):
        ConnectionOptions.from_args(namespace(token_file=str(tmp_path / "nope")), {})


def test_store_target_redacts_password() -> None:
    options = ConnectionOptions(store_url="postgresql://lw:hunter2@db/loomwork")
    assert "hunter2" not in options.target


def test_parse_attribute_types() -> None:
    schema = parse_attribute_types(["tier=STRING", "tags=keyword_list"])
    assert schema.type_of("tier") is AttributeType.STRING
    assert schema.type_of("tags") is AttributeType.KEYWORD_LIST
    with pytest.raises(UsageError, match="unknown search attribute type"):
        parse_attribute_types(["x=decimal"])
    with pytest.raises(UsageError, match="NAME=TYPE"):
        parse_attribute_types(["x"])
    with pytest.raises(UsageError, match="invalid search attribute name"):
        parse_attribute_types(["bad name=string"])


def test_load_finds_workflow_classes(tmp_path: Path) -> None:
    (tmp_path / "classy.py").write_text(
        "import loomwork as lw\n"
        "@lw.workflow(name='c.flow')\n"
        "class Flow:\n"
        "    @lw.run\n"
        "    async def run(self) -> None: ...\n"
        "class Undecorated:\n"
        "    pass\n"
    )
    assert load_app(str(tmp_path / "classy.py")).workflow_names == ["c.flow"]
    assert load_app(f"{tmp_path / 'classy.py'}:Flow").workflow_names == ["c.flow"]
