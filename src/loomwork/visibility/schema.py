"""Search attribute schemas and the built-in fields every query can reference.

Search attributes are user-defined key/value pairs attached to a workflow run (think
``customer_tier = 'gold'``). They are typed up front, in a :class:`SearchAttributes`
schema, so the query language can reject typos and type mismatches before touching a
database. Built-in fields (``status``, ``created_at``, ...) map directly onto
:class:`~loomwork.store.records.ExecutionRecord` and need no schema entry.
"""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass
from typing import Any

from loomwork.store.records import ExecutionStatus

#: Attribute names: lowercase, starting with a letter, ASCII word characters only. This
#: matches typical column/identifier naming and keeps names safe to use unquoted in the
#: proposed ``lw_search_attributes`` schema and in error messages.
_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class AttributeType(enum.Enum):
    """The value types a search attribute may hold."""

    STRING = "string"
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"
    DATETIME = "datetime"
    KEYWORD_LIST = "keyword_list"


#: Python types accepted at runtime for the attribute types that :meth:`SearchAttributes
#: .validate_value` checks generically. ``bool``, ``datetime`` and keyword lists are
#: checked separately above, since ``bool`` is an ``int`` subclass in Python and the other
#: two are not plain scalar types.
_PYTHON_TYPES: dict[AttributeType, tuple[type, ...]] = {
    AttributeType.STRING: (str,),
    AttributeType.INT: (int,),
    AttributeType.FLOAT: (float, int),
}


class SchemaError(ValueError):
    """A :class:`SearchAttributes` schema or attribute name/value is invalid."""


@dataclass(frozen=True, slots=True)
class SearchAttributes:
    """An immutable name -> :class:`AttributeType` schema for a set of search attributes.

    Construct with :meth:`define`; the mapping form is validated once, at construction,
    so every other component can trust it.
    """

    types: dict[str, AttributeType]

    @classmethod
    def define(cls, **attributes: AttributeType) -> SearchAttributes:
        return cls.from_mapping(attributes)

    @classmethod
    def from_mapping(cls, attributes: dict[str, AttributeType]) -> SearchAttributes:
        for name in attributes:
            if not _NAME_RE.match(name):
                raise SchemaError(
                    f"invalid search attribute name {name!r}: must match {_NAME_RE.pattern}"
                )
        return cls(dict(attributes))

    def __contains__(self, name: str) -> bool:
        return name in self.types

    def type_of(self, name: str) -> AttributeType | None:
        return self.types.get(name)

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self.types))

    def validate_value(self, name: str, value: Any) -> None:
        """Raise :class:`SchemaError` if ``value`` does not fit attribute ``name``'s type.

        Used by callers that build the ``attrs`` mapping passed to the evaluator or that
        write search attribute rows, so bad data is caught at the source rather than as a
        silent mismatch in a query result.
        """
        from datetime import datetime  # local import: keep the schema module dependency-light

        attr_type = self.types.get(name)
        if attr_type is None:
            raise SchemaError(f"unknown search attribute {name!r}")
        if value is None:
            return
        if attr_type is AttributeType.DATETIME:
            if not isinstance(value, datetime):
                raise SchemaError(f"search attribute {name!r} expects a datetime, got {value!r}")
            return
        if attr_type is AttributeType.KEYWORD_LIST:
            if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                raise SchemaError(
                    f"search attribute {name!r} expects a list of strings, got {value!r}"
                )
            return
        if attr_type is AttributeType.BOOL:
            if not isinstance(value, bool):
                raise SchemaError(f"search attribute {name!r} expects a bool, got {value!r}")
            return
        expected = _PYTHON_TYPES[attr_type]
        # bool is an int subclass; reject it explicitly for numeric attributes.
        if isinstance(value, bool) or not isinstance(value, expected):
            raise SchemaError(f"search attribute {name!r} expects {attr_type.value}, got {value!r}")


@dataclass(frozen=True, slots=True)
class BuiltinField:
    """One field of :class:`~loomwork.store.records.ExecutionRecord` exposed to queries."""

    name: str
    type: AttributeType
    nullable: bool
    column: str
    is_status: bool = False


#: Every field a query may reference without the ``attr.`` prefix, in the order they are
#: documented. Column names match ``lw_executions`` exactly (see
#: ``loomwork.store.sql.migrations``), so the SQL compiler can use them unmodified.
BUILTIN_FIELDS: dict[str, BuiltinField] = {
    f.name: f
    for f in (
        BuiltinField("run_id", AttributeType.STRING, nullable=False, column="run_id"),
        BuiltinField("workflow_id", AttributeType.STRING, nullable=False, column="workflow_id"),
        BuiltinField("workflow_name", AttributeType.STRING, nullable=False, column="workflow_name"),
        BuiltinField(
            "status", AttributeType.STRING, nullable=False, column="status", is_status=True
        ),
        BuiltinField("created_at", AttributeType.DATETIME, nullable=False, column="created_at"),
        BuiltinField("closed_at", AttributeType.DATETIME, nullable=True, column="closed_at"),
        BuiltinField("tick_failures", AttributeType.INT, nullable=False, column="tick_failures"),
        BuiltinField(
            "cancel_requested", AttributeType.BOOL, nullable=False, column="cancel_requested"
        ),
        BuiltinField("close_reason", AttributeType.STRING, nullable=False, column="close_reason"),
        BuiltinField(
            "last_tick_error", AttributeType.STRING, nullable=True, column="last_tick_error"
        ),
    )
}

#: Valid literal values for the ``status`` field, for error messages and validation.
STATUS_VALUES: tuple[str, ...] = tuple(s.value for s in ExecutionStatus)


__all__ = [
    "BUILTIN_FIELDS",
    "STATUS_VALUES",
    "AttributeType",
    "BuiltinField",
    "SchemaError",
    "SearchAttributes",
]
