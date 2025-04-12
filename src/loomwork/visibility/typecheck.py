"""Type checking and time resolution for a parsed query.

:func:`check_query` is the single entry point both the evaluator and the SQL compiler call
before doing anything else. It validates a parsed :class:`~loomwork.visibility.nodes.Query`
against a :class:`~loomwork.visibility.schema.SearchAttributes` schema and returns an
equivalent query with every ``now()`` / interval expression folded into a concrete
:class:`~loomwork.visibility.nodes.TimestampLiteral`, using the ``now`` passed in. Folding
time expressions here, rather than at evaluation time, is what makes a compiled query
deterministic: the same :class:`Query` object always means the same thing, however many
times (or how much later) it is run.

What is checked:

* every field referenced exists (a built-in field, or ``attr.<name>`` present in the schema);
* operand types match the field's type (``LIKE`` only on strings, ``CONTAINS`` only on
  keyword lists, ``BETWEEN`` only on orderable types, interval arithmetic only on timestamps);
* ``status`` literals are valid :class:`~loomwork.store.records.ExecutionStatus` values;
* ``ORDER BY`` fields are built-in (not search attributes) and orderable (not keyword lists
  or booleans).
"""

from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import TypeVar

from loomwork.visibility.errors import QueryTypeError
from loomwork.visibility.nodes import (
    And,
    Between,
    BoolExpr,
    BoolLiteral,
    Compare,
    Contains,
    Field,
    FloatLiteral,
    InList,
    IntervalLiteral,
    IntLiteral,
    IsNull,
    Like,
    Literal,
    Not,
    NowLiteral,
    NullLiteral,
    Or,
    OrderTerm,
    Query,
    ResolvedLiteral,
    StringLiteral,
    TimeArith,
    TimestampLiteral,
)
from loomwork.visibility.schema import (
    BUILTIN_FIELDS,
    STATUS_VALUES,
    AttributeType,
    SearchAttributes,
)

#: Field types comparable with ``<``, ``<=``, ``>``, ``>=`` and usable in ``BETWEEN``.
_ORDERABLE = {AttributeType.STRING, AttributeType.INT, AttributeType.FLOAT, AttributeType.DATETIME}

_LiteralT = TypeVar("_LiteralT", bound=Literal)


@dataclasses.dataclass(frozen=True, slots=True)
class ResolvedField:
    """A field together with the type/nullability information looked up for it."""

    field: Field
    type: AttributeType
    nullable: bool


def resolve_field(fld: Field, schema: SearchAttributes, source: str) -> ResolvedField:
    """Look up ``fld``'s type, raising :class:`QueryTypeError` if it does not exist."""
    if fld.is_attr:
        attr_type = schema.type_of(fld.name)
        if attr_type is None:
            known = ", ".join(schema.names())
            suffix = (
                f"; defined attributes: {known}" if known else "; no search attributes are defined"
            )
            raise QueryTypeError(
                f"unknown search attribute 'attr.{fld.name}'{suffix}", source, fld.position
            )
        return ResolvedField(fld, attr_type, nullable=True)
    spec = BUILTIN_FIELDS.get(fld.name)
    if spec is None:
        known = ", ".join(sorted(BUILTIN_FIELDS))
        raise QueryTypeError(
            f"unknown field {fld.name!r}; known fields: {known}", source, fld.position
        )
    return ResolvedField(fld, spec.type, spec.nullable)


def literal_type(lit: ResolvedLiteral) -> AttributeType | None:
    """The type of an already-resolved literal, or ``None`` for ``NULL``."""
    if isinstance(lit, StringLiteral):
        return AttributeType.STRING
    if isinstance(lit, IntLiteral):
        return AttributeType.INT
    if isinstance(lit, FloatLiteral):
        return AttributeType.FLOAT
    if isinstance(lit, BoolLiteral):
        return AttributeType.BOOL
    if isinstance(lit, TimestampLiteral):
        return AttributeType.DATETIME
    if isinstance(lit, NullLiteral):
        return None
    raise AssertionError(f"unresolved literal reached literal_type: {lit!r}")  # pragma: no cover


class _Checker:
    def __init__(self, schema: SearchAttributes, now: datetime, source: str) -> None:
        self.schema = schema
        self.now = now
        self.source = source

    # ------------------------------------------------------------------ time folding

    def resolve_literal(self, lit: _LiteralT) -> ResolvedLiteral:
        if isinstance(lit, NowLiteral):
            return TimestampLiteral(self.now, lit.position)
        if isinstance(lit, TimeArith):
            base = self.resolve_literal(lit.left)
            if not isinstance(base, TimestampLiteral):
                raise QueryTypeError(
                    "interval arithmetic requires a timestamp on the left",
                    self.source,
                    lit.position,
                )
            delta = lit.interval.delta
            value = base.value + delta if lit.op == "+" else base.value - delta
            return TimestampLiteral(value, lit.position)
        if isinstance(lit, IntervalLiteral):
            raise QueryTypeError(
                "an interval cannot be used by itself; combine it with now() or a timestamp, "
                "e.g. now() - interval '1 day'",
                self.source,
                lit.position,
            )
        assert isinstance(
            lit,
            StringLiteral
            | IntLiteral
            | FloatLiteral
            | BoolLiteral
            | NullLiteral
            | TimestampLiteral,
        )
        return lit

    # ------------------------------------------------------------------ value compatibility

    def check_value(self, resolved: ResolvedField, lit: ResolvedLiteral) -> None:
        if isinstance(lit, NullLiteral):
            return
        lit_type = literal_type(lit)
        compatible = lit_type is resolved.type or (
            resolved.type is AttributeType.FLOAT and lit_type is AttributeType.INT
        )
        if not compatible:
            got = lit_type.value if lit_type is not None else "null"
            raise QueryTypeError(
                f"{resolved.field} is {resolved.type.value} but the value is {got}",
                self.source,
                lit.position,
            )
        spec = BUILTIN_FIELDS.get(resolved.field.name)
        if (
            not resolved.field.is_attr
            and spec is not None
            and spec.is_status
            and isinstance(lit, StringLiteral)
            and lit.value not in STATUS_VALUES
        ):
            valid = ", ".join(STATUS_VALUES)
            raise QueryTypeError(
                f"unknown status {lit.value!r}; expected one of {valid}", self.source, lit.position
            )

    # ------------------------------------------------------------------ boolean expressions

    def check_bool(self, node: BoolExpr) -> BoolExpr:
        if isinstance(node, Compare):
            resolved = resolve_field(node.field, self.schema, self.source)
            value = self.resolve_literal(node.value)
            self.check_value(resolved, value)
            return dataclasses.replace(node, value=value)
        if isinstance(node, InList):
            resolved = resolve_field(node.field, self.schema, self.source)
            values = tuple(self.resolve_literal(v) for v in node.values)
            for value in values:
                self.check_value(resolved, value)
            return dataclasses.replace(node, values=values)
        if isinstance(node, Like):
            resolved = resolve_field(node.field, self.schema, self.source)
            if resolved.type is not AttributeType.STRING:
                raise QueryTypeError(
                    f"LIKE requires a string field, but {node.field} is {resolved.type.value}",
                    self.source,
                    node.position,
                )
            return node
        if isinstance(node, IsNull):
            resolved = resolve_field(node.field, self.schema, self.source)
            if not resolved.nullable:
                raise QueryTypeError(f"{node.field} is never null", self.source, node.position)
            return node
        if isinstance(node, Between):
            resolved = resolve_field(node.field, self.schema, self.source)
            if resolved.type not in _ORDERABLE:
                raise QueryTypeError(
                    f"BETWEEN is not supported on {node.field} ({resolved.type.value})",
                    self.source,
                    node.position,
                )
            low = self.resolve_literal(node.low)
            high = self.resolve_literal(node.high)
            self.check_value(resolved, low)
            self.check_value(resolved, high)
            return dataclasses.replace(node, low=low, high=high)
        if isinstance(node, Contains):
            resolved = resolve_field(node.field, self.schema, self.source)
            if resolved.type is not AttributeType.KEYWORD_LIST:
                raise QueryTypeError(
                    f"CONTAINS requires a keyword-list attribute, but {node.field} is "
                    f"{resolved.type.value}",
                    self.source,
                    node.position,
                )
            return node
        if isinstance(node, And):
            return dataclasses.replace(
                node, left=self.check_bool(node.left), right=self.check_bool(node.right)
            )
        if isinstance(node, Or):
            return dataclasses.replace(
                node, left=self.check_bool(node.left), right=self.check_bool(node.right)
            )
        if isinstance(node, Not):
            return dataclasses.replace(node, operand=self.check_bool(node.operand))
        raise AssertionError(f"unhandled boolean node: {node!r}")  # pragma: no cover

    # ------------------------------------------------------------------ ORDER BY

    def check_order_term(self, term: OrderTerm) -> OrderTerm:
        resolved = resolve_field(term.field, self.schema, self.source)
        if resolved.field.is_attr:
            # Both the evaluator and the SQL compiler could sort by a search attribute (the
            # SQL side already knows how, via the same scalar subquery comparisons use), but
            # restricting ORDER BY to built-in fields keeps the two implementations trivially
            # in sync and avoids ambiguity about where a run with the attribute unset sorts
            # relative to one from a schema that no longer defines it. Sort application code
            # can always fetch by a built-in field (e.g. ``created_at``) as a stable proxy.
            raise QueryTypeError(
                f"cannot ORDER BY {term.field}: only built-in fields can be used in ORDER BY, "
                "not search attributes",
                self.source,
                term.field.position,
            )
        if resolved.type not in _ORDERABLE:
            raise QueryTypeError(
                f"cannot ORDER BY {term.field} ({resolved.type.value})",
                self.source,
                term.field.position,
            )
        return term


def check_query(query: Query, schema: SearchAttributes, now: datetime, source: str = "") -> Query:
    """Validate ``query`` against ``schema`` and fold time expressions using ``now``.

    ``source`` is the original query text, used only to render error messages; pass it
    along whenever it is available so :meth:`QueryTypeError.format` can show the offending
    line, but an empty string is safe (the error is still raised with correct positions).
    """
    checker = _Checker(schema, now, source)
    where = checker.check_bool(query.where) if query.where is not None else None
    order_by = tuple(checker.check_order_term(t) for t in query.order_by)
    return Query(where, order_by, query.limit)


__all__ = ["ResolvedField", "check_query", "literal_type", "resolve_field"]
