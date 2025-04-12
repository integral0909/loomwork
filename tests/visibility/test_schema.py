"""Tests for :mod:`loomwork.visibility.schema`."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from loomwork.store.records import ExecutionStatus
from loomwork.visibility.schema import (
    BUILTIN_FIELDS,
    STATUS_VALUES,
    AttributeType,
    SchemaError,
    SearchAttributes,
)


def test_define_builds_a_schema() -> None:
    schema = SearchAttributes.define(customer_tier=AttributeType.STRING)
    assert schema.type_of("customer_tier") is AttributeType.STRING
    assert "customer_tier" in schema
    assert schema.type_of("nope") is None
    assert schema.names() == ("customer_tier",)


@pytest.mark.parametrize("name", ["Customer", "1abc", "_abc", "a-b", "a" * 64, "", "attr.name"])
def test_invalid_attribute_names_are_rejected(name: str) -> None:
    with pytest.raises(SchemaError):
        SearchAttributes.define(**{name: AttributeType.STRING})


def test_valid_attribute_name_boundary() -> None:
    # exactly 63 characters, the documented maximum
    name = "a" * 63
    schema = SearchAttributes.define(**{name: AttributeType.STRING})
    assert name in schema


def test_builtin_fields_cover_execution_record() -> None:
    expected = {
        "run_id",
        "workflow_id",
        "workflow_name",
        "status",
        "created_at",
        "closed_at",
        "tick_failures",
        "cancel_requested",
        "close_reason",
        "last_tick_error",
    }
    assert set(BUILTIN_FIELDS) == expected


def test_status_values_match_execution_status() -> None:
    # Mirrors ExecutionStatus exactly, whatever members it currently has, rather than a
    # hardcoded list that would drift if the store subsystem adds a new status.
    assert set(STATUS_VALUES) == {s.value for s in ExecutionStatus}
    assert {"running", "completed", "failed", "canceled", "terminated"} <= set(STATUS_VALUES)


class TestValidateValue:
    def test_string(self) -> None:
        schema = SearchAttributes.define(tier=AttributeType.STRING)
        schema.validate_value("tier", "gold")
        with pytest.raises(SchemaError):
            schema.validate_value("tier", 1)

    def test_int_rejects_bool(self) -> None:
        schema = SearchAttributes.define(retries=AttributeType.INT)
        schema.validate_value("retries", 3)
        with pytest.raises(SchemaError):
            schema.validate_value("retries", True)

    def test_float_accepts_int(self) -> None:
        schema = SearchAttributes.define(score=AttributeType.FLOAT)
        schema.validate_value("score", 3)
        schema.validate_value("score", 3.5)

    def test_bool(self) -> None:
        schema = SearchAttributes.define(vip=AttributeType.BOOL)
        schema.validate_value("vip", True)
        with pytest.raises(SchemaError):
            schema.validate_value("vip", 1)

    def test_datetime(self) -> None:
        schema = SearchAttributes.define(seen=AttributeType.DATETIME)
        schema.validate_value("seen", datetime(2024, 1, 1, tzinfo=UTC))
        with pytest.raises(SchemaError):
            schema.validate_value("seen", "2024-01-01")

    def test_keyword_list(self) -> None:
        schema = SearchAttributes.define(tags=AttributeType.KEYWORD_LIST)
        schema.validate_value("tags", ["a", "b"])
        with pytest.raises(SchemaError):
            schema.validate_value("tags", "a")
        with pytest.raises(SchemaError):
            schema.validate_value("tags", [1, 2])

    def test_none_is_always_allowed(self) -> None:
        schema = SearchAttributes.define(tier=AttributeType.STRING)
        schema.validate_value("tier", None)

    def test_unknown_attribute(self) -> None:
        schema = SearchAttributes.define(tier=AttributeType.STRING)
        with pytest.raises(SchemaError):
            schema.validate_value("nope", "x")
