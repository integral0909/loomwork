"""Search attribute values: typing, recording in histories and storing as rows.

Workflows set search attributes with :func:`loomwork.upsert_search_attributes`; clients can
set initial ones when starting a run. Values are typed by their Python type:

=====================  ================================
Python value           :class:`AttributeType`
=====================  ================================
``str``                ``STRING``
``bool``               ``BOOL``
``int``                ``INT``
``float``              ``FLOAT``
aware ``datetime``     ``DATETIME``
``list[str]``          ``KEYWORD_LIST``
``None``               removes the attribute
=====================  ================================

In histories the values travel as JSON objects tagged with their type
(``{"type": "datetime", "value": "2024-05-01T00:00:00+00:00"}``), so they decode to the
same Python values on every replay.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from loomwork.visibility.columns import VALUE_COLUMNS, VALUE_TYPES
from loomwork.visibility.schema import _NAME_RE, AttributeType, SchemaError, SearchAttributes

MAX_ATTRIBUTES = 64
MAX_STRING_LENGTH = 2048
MAX_KEYWORDS = 100


def infer_type(name: str, value: Any) -> AttributeType:
    """The attribute type of ``value`` (see the table in the module docstring)."""
    if isinstance(value, bool):
        return AttributeType.BOOL
    if isinstance(value, int):
        return AttributeType.INT
    if isinstance(value, float):
        return AttributeType.FLOAT
    if isinstance(value, str):
        return AttributeType.STRING
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise SchemaError(f"search attribute {name!r}: datetimes must be timezone-aware")
        return AttributeType.DATETIME
    if isinstance(value, list | tuple) and all(isinstance(v, str) for v in value):
        return AttributeType.KEYWORD_LIST
    raise SchemaError(
        f"search attribute {name!r}: unsupported value {value!r} "
        "(use str, int, float, bool, datetime or a list of str)"
    )


def _check(name: str, value: Any, attr_type: AttributeType) -> None:
    if not _NAME_RE.match(name):
        raise SchemaError(f"invalid search attribute name {name!r}: must match {_NAME_RE.pattern}")
    if attr_type is AttributeType.STRING and len(value) > MAX_STRING_LENGTH:
        raise SchemaError(
            f"search attribute {name!r} is longer than {MAX_STRING_LENGTH} characters"
        )
    if attr_type is AttributeType.KEYWORD_LIST:
        if len(value) > MAX_KEYWORDS:
            raise SchemaError(f"search attribute {name!r} has more than {MAX_KEYWORDS} keywords")
        if any(len(v) > MAX_STRING_LENGTH for v in value):
            raise SchemaError(f"a keyword of search attribute {name!r} is too long")


def encode_attributes(
    values: Mapping[str, Any], schema: SearchAttributes | None = None
) -> dict[str, Any]:
    """Validate ``values`` and turn them into their JSON form for a history event."""
    if len(values) > MAX_ATTRIBUTES:
        raise SchemaError(f"at most {MAX_ATTRIBUTES} search attributes can be set at once")
    encoded: dict[str, Any] = {}
    for name, value in values.items():
        if value is None:
            if not _NAME_RE.match(name):
                raise SchemaError(f"invalid search attribute name {name!r}")
            encoded[name] = {"type": None}
            continue
        attr_type = infer_type(name, value)
        if schema is not None and name in schema:
            schema.validate_value(name, list(value) if isinstance(value, tuple) else value)
            attr_type = schema.types[name]
        _check(name, value, attr_type)
        if attr_type is AttributeType.DATETIME:
            json_value: Any = value.isoformat()
        elif attr_type is AttributeType.KEYWORD_LIST:
            json_value = list(value)
        elif attr_type is AttributeType.FLOAT:
            json_value = float(value)
        else:
            json_value = value
        encoded[name] = {"type": attr_type.value, "value": json_value}
    return encoded


def decode_attributes(encoded: Mapping[str, Any]) -> dict[str, Any]:
    """Reverse :func:`encode_attributes`; removed attributes decode to ``None``."""
    values: dict[str, Any] = {}
    for name, item in encoded.items():
        type_name = item.get("type")
        if type_name is None:
            values[name] = None
        elif type_name == AttributeType.DATETIME.value:
            values[name] = datetime.fromisoformat(item["value"])
        elif type_name == AttributeType.KEYWORD_LIST.value:
            values[name] = list(item["value"])
        else:
            values[name] = item["value"]
    return values


def merge(current: Mapping[str, Any], update: Mapping[str, Any]) -> dict[str, Any]:
    """Apply an encoded update (with removals) to encoded current attributes."""
    merged = dict(current)
    for name, item in update.items():
        if item.get("type") is None:
            merged.pop(name, None)
        else:
            merged[name] = item
    return merged


def attribute_rows(item: Mapping[str, Any]) -> list[tuple[str, str, Any]]:
    """``(value_type, column, value)`` rows for one encoded attribute (``lw_search_attributes``).

    Datetimes are returned as aware :class:`datetime` objects; the store converts them to
    its column representation. Empty keyword lists produce no rows.
    """
    type_name = item.get("type")
    if type_name is None:
        return []
    column = VALUE_COLUMNS[type_name]
    value_type = VALUE_TYPES[type_name]
    value = item["value"]
    if type_name == AttributeType.KEYWORD_LIST.value:
        return [(value_type, column, element) for element in dict.fromkeys(value)]
    if type_name == AttributeType.DATETIME.value:
        return [(value_type, column, datetime.fromisoformat(value))]
    return [(value_type, column, value)]
