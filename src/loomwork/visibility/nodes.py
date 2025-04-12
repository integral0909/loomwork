"""Immutable AST for the visibility query language.

Named ``nodes`` rather than ``ast`` to avoid any confusion with the standard library
module of that name. Every node is a frozen, slotted dataclass so a parsed query is
hashable, cheap to copy with :func:`dataclasses.replace`, and safe to share between the
evaluator and the SQL compiler. Each node carries the
:class:`~loomwork.visibility.lexer.Position` of the token
that introduced it, for type-checker error messages; positions are excluded from equality
and hashing (via ``compare=False``) so tests can compare ASTs structurally without also
pinning down exact source offsets.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as _field
from datetime import datetime, timedelta

from loomwork.visibility.lexer import Position

# --------------------------------------------------------------------------- fields


@dataclass(frozen=True, slots=True)
class Field:
    """A reference to a built-in field or, when ``is_attr``, a ``attr.<name>`` attribute."""

    name: str
    is_attr: bool
    position: Position = _field(compare=False)

    def __str__(self) -> str:
        return f"attr.{self.name}" if self.is_attr else self.name


# --------------------------------------------------------------------------- literals


@dataclass(frozen=True, slots=True)
class StringLiteral:
    value: str
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class IntLiteral:
    value: int
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class FloatLiteral:
    value: float
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class BoolLiteral:
    value: bool
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class NullLiteral:
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class TimestampLiteral:
    value: datetime
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class NowLiteral:
    """``now()``. Folded into a :class:`TimestampLiteral` by the type checker."""

    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class IntervalLiteral:
    """``interval '1 day'``, pre-parsed into a :class:`~datetime.timedelta`."""

    delta: timedelta
    raw: str
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class TimeArith:
    """``<time expression> (+ | -) <interval>``. Folded away by the type checker."""

    left: NowLiteral | TimestampLiteral | TimeArith
    op: str  # "+" or "-"
    interval: IntervalLiteral
    position: Position = _field(compare=False)


#: Anything that can appear where a scalar value is expected, before time folding.
Literal = (
    StringLiteral
    | IntLiteral
    | FloatLiteral
    | BoolLiteral
    | NullLiteral
    | TimestampLiteral
    | NowLiteral
    | IntervalLiteral
    | TimeArith
)

#: What a literal can fold down to: a plain, immediately usable value.
ResolvedLiteral = (
    StringLiteral | IntLiteral | FloatLiteral | BoolLiteral | NullLiteral | TimestampLiteral
)

# --------------------------------------------------------------------------- predicates


@dataclass(frozen=True, slots=True)
class Compare:
    field: Field
    op: str  # one of "=", "!=", "<", "<=", ">", ">="
    value: Literal
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class InList:
    field: Field
    values: tuple[Literal, ...]
    negated: bool
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class Like:
    field: Field
    pattern: StringLiteral
    negated: bool
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class IsNull:
    field: Field
    negated: bool
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class Between:
    field: Field
    low: Literal
    high: Literal
    negated: bool
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class Contains:
    field: Field
    value: StringLiteral
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class And:
    left: BoolExpr
    right: BoolExpr
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class Or:
    left: BoolExpr
    right: BoolExpr
    position: Position = _field(compare=False)


@dataclass(frozen=True, slots=True)
class Not:
    operand: BoolExpr
    position: Position = _field(compare=False)


#: Any node that evaluates to a (possibly unknown) boolean.
BoolExpr = Compare | InList | Like | IsNull | Between | Contains | And | Or | Not

# --------------------------------------------------------------------------- ORDER BY / LIMIT


@dataclass(frozen=True, slots=True)
class OrderTerm:
    field: Field
    descending: bool


@dataclass(frozen=True, slots=True)
class Query:
    """A fully parsed query: an optional filter, ordering and a row limit."""

    where: BoolExpr | None
    order_by: tuple[OrderTerm, ...]
    limit: int | None


__all__ = [
    "And",
    "Between",
    "BoolExpr",
    "BoolLiteral",
    "Compare",
    "Contains",
    "Field",
    "FloatLiteral",
    "InList",
    "IntLiteral",
    "IntervalLiteral",
    "IsNull",
    "Like",
    "Literal",
    "NowLiteral",
    "NullLiteral",
    "Not",
    "Or",
    "OrderTerm",
    "Query",
    "ResolvedLiteral",
    "StringLiteral",
    "TimeArith",
    "TimestampLiteral",
]
