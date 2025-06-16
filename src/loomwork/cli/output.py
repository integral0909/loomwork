"""Rendering command results as aligned text tables, key/value blocks or JSON."""

from __future__ import annotations

import dataclasses
import enum
import json
import sys
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any, TextIO

from loomwork.cli.parsing import format_duration
from loomwork.codec.payload import Payload

#: Longest cell rendered in a table before it is shortened with an ellipsis.
MAX_CELL = 60


def to_jsonable(value: Any) -> Any:
    """Convert records, enums, datetimes and payloads into plain JSON values."""
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, enum.Enum):
        return to_jsonable(value.value)
    if isinstance(value, Payload):
        return value.to_dict()
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return to_jsonable(to_dict())
    if not isinstance(value, type) and dataclasses.is_dataclass(value):
        return {f.name: to_jsonable(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, Iterable):
        return [to_jsonable(v) for v in value]
    return str(value)


def format_time(value: datetime | None) -> str:
    """A compact UTC-or-local timestamp for tables: seconds precision, no microseconds."""
    if value is None:
        return "-"
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def format_cell(value: Any, *, limit: int | None = MAX_CELL) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return format_time(value)
    if isinstance(value, timedelta):
        return format_duration(value)
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, enum.Enum):
        return str(value.value)
    if isinstance(value, float):
        return f"{value:g}"
    if isinstance(value, dict | list | tuple):
        text = json.dumps(to_jsonable(value), sort_keys=True, separators=(",", ":"))
    else:
        text = str(value)
    text = text.replace("\n", " ")
    if limit is not None and len(text) > limit:
        text = text[: limit - 1] + "…"
    return text


def render_table(
    headers: Sequence[str], rows: Iterable[Sequence[Any]], *, full_last: bool = False
) -> str:
    """Left-aligned columns separated by two spaces, headers in upper case.

    Long cells are shortened with an ellipsis, except in the last column when
    ``full_last`` is set (for free-text columns such as event details).
    """
    last = len(headers) - 1
    cells = [
        [
            format_cell(v, limit=None if full_last and i == last else MAX_CELL)
            for i, v in enumerate(row)
        ]
        for row in rows
    ]
    widths = [len(h) for h in headers]
    for row in cells:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def line(values: Sequence[str]) -> str:
        return "  ".join(v.ljust(w) for v, w in zip(values, widths, strict=True)).rstrip()

    return "\n".join([line([h.upper() for h in headers]), *(line(row) for row in cells)])


def render_fields(fields: Sequence[tuple[str, Any]]) -> str:
    """``key: value`` lines with the values aligned."""
    if not fields:
        return ""
    width = max(len(key) for key, _ in fields) + 1
    lines = []
    for key, value in fields:
        text = value if isinstance(value, str) and "\n" in value else format_cell(value)
        if isinstance(value, str) and "\n" in value:
            indented = "\n".join(" " * (width + 1) + line for line in text.splitlines())
            lines.append(f"{key + ':':<{width}}\n{indented}")
        else:
            lines.append(f"{key + ':':<{width}} {text}")
    return "\n".join(lines)


class Console:
    """Where a command writes. Tests pass string buffers; the CLI passes the process
    streams."""

    def __init__(
        self,
        out: TextIO | None = None,
        err: TextIO | None = None,
        *,
        json_output: bool = False,
        stdin: TextIO | None = None,
    ) -> None:
        self.out = out or sys.stdout
        self.err = err or sys.stderr
        self.stdin = stdin or sys.stdin
        self.json_output = json_output

    def print(self, text: str = "") -> None:
        self.out.write(text + "\n")

    def note(self, text: str) -> None:
        """Human-oriented remarks go to stderr so stdout stays machine readable."""
        self.err.write(text + "\n")

    def error(self, text: str) -> None:
        self.err.write(f"error: {text}\n")

    def json(self, value: Any) -> None:
        self.out.write(json.dumps(to_jsonable(value), indent=2, sort_keys=True) + "\n")

    def table(
        self,
        headers: Sequence[str],
        rows: Iterable[Sequence[Any]],
        *,
        records: Sequence[Any] | None = None,
        empty: str = "no results",
        full_last: bool = False,
    ) -> None:
        """Print ``rows`` as a table, or ``records`` (the full objects) in JSON mode."""
        materialized = list(rows)
        if self.json_output:
            self.json(
                records
                if records is not None
                else [dict(zip(headers, r, strict=True)) for r in materialized]
            )
            return
        if not materialized:
            self.note(empty)
            return
        self.print(render_table(headers, materialized, full_last=full_last))

    def fields(self, fields: Sequence[tuple[str, Any]], *, record: Any = None) -> None:
        if self.json_output:
            self.json(record if record is not None else dict(fields))
            return
        self.print(render_fields(fields))

    def read_stdin(self) -> str | None:
        """Everything on stdin, or ``None`` when stdin is an interactive terminal."""
        isatty = getattr(self.stdin, "isatty", None)
        if callable(isatty) and isatty():
            return None
        return self.stdin.read()
