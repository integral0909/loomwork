"""Encoding search attribute values for histories and rows."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from loomwork.visibility import AttributeType, SchemaError, SearchAttributes
from loomwork.visibility.attributes import (
    MAX_ATTRIBUTES,
    attribute_rows,
    decode_attributes,
    encode_attributes,
    infer_type,
    merge,
)

T = datetime(2024, 5, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("x", AttributeType.STRING),
        (True, AttributeType.BOOL),
        (3, AttributeType.INT),
        (1.5, AttributeType.FLOAT),
        (T, AttributeType.DATETIME),
        (["a", "b"], AttributeType.KEYWORD_LIST),
        (("a",), AttributeType.KEYWORD_LIST),
    ],
)
def test_infer_type(value: object, expected: AttributeType) -> None:
    assert infer_type("x", value) is expected


@pytest.mark.parametrize("value", [datetime(2024, 1, 1), {"a": 1}, [1, 2], object()])
def test_unsupported_values(value: object) -> None:
    with pytest.raises(SchemaError):
        infer_type("x", value)


def test_roundtrip() -> None:
    values = {"s": "x", "b": False, "i": 7, "f": 2.5, "d": T, "k": ["a", "b"], "gone": None}
    assert decode_attributes(encode_attributes(values)) == values


def test_schema_validation_and_coercion() -> None:
    schema = SearchAttributes.define(score=AttributeType.FLOAT, name=AttributeType.STRING)
    assert encode_attributes({"score": 3}, schema)["score"] == {"type": "float", "value": 3.0}
    with pytest.raises(SchemaError):
        encode_attributes({"name": 5}, schema)


def test_limits_and_names() -> None:
    with pytest.raises(SchemaError, match="invalid search attribute name"):
        encode_attributes({"Bad-Name": 1})
    with pytest.raises(SchemaError, match="at most"):
        encode_attributes({f"a{i}": i for i in range(MAX_ATTRIBUTES + 1)})
    with pytest.raises(SchemaError, match="longer than"):
        encode_attributes({"s": "x" * 5000})
    with pytest.raises(SchemaError, match="keywords"):
        encode_attributes({"k": [str(i) for i in range(500)]})


def test_merge_and_rows() -> None:
    current = encode_attributes({"a": 1, "b": "x"})
    merged = merge(current, encode_attributes({"a": None, "c": ["p", "p", "q"]}))
    assert decode_attributes(merged) == {"b": "x", "c": ["p", "p", "q"]}
    assert attribute_rows(merged["c"]) == [
        ("keyword", "str_value", "p"),
        ("keyword", "str_value", "q"),
    ]
    assert attribute_rows(encode_attributes({"d": T})["d"]) == [("datetime", "time_value", T)]
    assert attribute_rows({"type": None}) == []
