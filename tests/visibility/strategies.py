"""Hypothesis strategies for generating checked queries and matching fixtures.

Expressions are built directly as AST nodes (not as query text) against the fixed test
schema in ``tests/visibility/support.py``, so every generated tree is valid by construction:
fields exist, literal types match, and operators are only used where the type checker
allows them. That keeps the property test focused on what it actually needs to verify --
that the evaluator and the SQL compiler agree -- rather than also re-deriving the grammar.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from hypothesis import strategies as st

from loomwork.store.records import ExecutionStatus
from loomwork.visibility.nodes import (
    And,
    Between,
    BoolExpr,
    Compare,
    Contains,
    Field,
    InList,
    IsNull,
    Like,
    Literal,
    Not,
    NullLiteral,
    Or,
    OrderTerm,
    StringLiteral,
)
from loomwork.visibility.nodes import BoolLiteral as _BoolLit
from loomwork.visibility.nodes import FloatLiteral as _FloatLit
from loomwork.visibility.nodes import IntLiteral as _IntLit
from loomwork.visibility.nodes import TimestampLiteral as _TsLit
from loomwork.visibility.schema import AttributeType
from tests.visibility.support import NOW, POS, make_record

STRING_POOL = ["gold", "silver", "bronze", "alpha", "beta", ""]
INT_POOL = list(range(-3, 8))
FLOAT_POOL = [-1.5, 0.0, 1.5, 2.5, 10.0]
BOOL_POOL = [True, False]
HOUR_OFFSETS = [-72, -24, -2, 0, 2, 24, 72]
KEYWORD_POOL = ["urgent", "billing", "vip", "beta"]
STATUS_POOL = [s.value for s in ExecutionStatus]
WORKFLOW_NAME_POOL = ["orders.process", "billing.charge"]
_ALL_STATUSES: list[ExecutionStatus] = list(ExecutionStatus)

# (field name, is_attr, type, nullable)
_STRING_FIELDS = [
    ("workflow_name", False, AttributeType.STRING, False),
    ("close_reason", False, AttributeType.STRING, False),
    ("last_tick_error", False, AttributeType.STRING, True),
    ("customer_tier", True, AttributeType.STRING, True),
]
_STATUS_FIELD = ("status", False, AttributeType.STRING, False)
_NUMERIC_FIELDS = [
    ("tick_failures", False, AttributeType.INT, False),
    ("retries", True, AttributeType.INT, True),
    ("score", True, AttributeType.FLOAT, True),
]
_BOOL_FIELDS = [
    ("cancel_requested", False, AttributeType.BOOL, False),
    ("is_vip", True, AttributeType.BOOL, True),
]
_DATETIME_FIELDS = [
    ("created_at", False, AttributeType.DATETIME, False),
    ("closed_at", False, AttributeType.DATETIME, True),
    ("activated_at", True, AttributeType.DATETIME, True),
]
_KEYWORD_LIST_FIELDS = [("tags", True, AttributeType.KEYWORD_LIST, True)]

ALL_FIELDS = [
    *_STRING_FIELDS,
    _STATUS_FIELD,
    *_NUMERIC_FIELDS,
    *_BOOL_FIELDS,
    *_DATETIME_FIELDS,
    *_KEYWORD_LIST_FIELDS,
]

#: Built-in fields ORDER BY may use (search attributes are rejected by the type checker).
ORDERABLE_BUILTIN_FIELDS = [
    "run_id",
    "workflow_name",
    "status",
    "close_reason",
    "tick_failures",
    "created_at",
    "closed_at",
]


def _timestamp(offset_hours: int) -> _TsLit:
    return _TsLit(NOW + timedelta(hours=offset_hours), POS)


def _literal_strategy(name: str, attr_type: AttributeType) -> st.SearchStrategy[Literal]:
    if attr_type is AttributeType.STRING:
        pool = STATUS_POOL if name == "status" else STRING_POOL
        return st.sampled_from(pool).map(lambda v: StringLiteral(v, POS))
    if attr_type is AttributeType.INT:
        return st.sampled_from(INT_POOL).map(lambda v: _IntLit(v, POS))
    if attr_type is AttributeType.FLOAT:
        return st.sampled_from(FLOAT_POOL).map(lambda v: _FloatLit(v, POS))
    if attr_type is AttributeType.BOOL:
        return st.sampled_from(BOOL_POOL).map(lambda v: _BoolLit(v, POS))
    if attr_type is AttributeType.DATETIME:
        return st.sampled_from(HOUR_OFFSETS).map(_timestamp)
    raise AssertionError(attr_type)  # pragma: no cover


def _like_pattern(base: str) -> list[str]:
    head = base[:1] or "_"
    tail = base[-1:] or "_"
    return [base, f"{head}%", f"%{tail}", f"{head}_" if base else "_", f"%{base}%"]


@st.composite
def _leaf_expr(draw: st.DrawFn) -> BoolExpr:
    name, is_attr, attr_type, nullable = draw(st.sampled_from(ALL_FIELDS))
    fld = Field(name, is_attr, POS)

    if attr_type is AttributeType.KEYWORD_LIST:
        kind = draw(st.sampled_from(["contains", "is_null", "is_not_null"]))
        if kind == "contains":
            keyword = draw(st.sampled_from(KEYWORD_POOL))
            return Contains(fld, StringLiteral(keyword, POS), POS)
        return IsNull(fld, kind == "is_not_null", POS)

    lit = _literal_strategy(name, attr_type)
    ops = ["=", "!="]
    if attr_type in (AttributeType.INT, AttributeType.FLOAT, AttributeType.DATETIME):
        ops += ["<", "<=", ">", ">=", "between", "in"]
    if attr_type is AttributeType.STRING:
        ops += ["like", "in", "between"]
    if nullable:
        ops += ["is_null", "is_not_null"]
    kind = draw(st.sampled_from(ops))

    if kind in ("=", "!="):
        compare_value = draw(st.one_of(lit, st.just(NullLiteral(POS))))
        return Compare(fld, kind, compare_value, POS)
    if kind in ("<", "<=", ">", ">="):
        return Compare(fld, kind, draw(lit), POS)
    if kind == "is_null":
        return IsNull(fld, False, POS)
    if kind == "is_not_null":
        return IsNull(fld, True, POS)
    if kind == "like":
        base = draw(st.sampled_from(STRING_POOL))
        pattern = draw(st.sampled_from(_like_pattern(base)))
        return Like(fld, StringLiteral(pattern, POS), draw(st.booleans()), POS)
    if kind == "in":
        values = draw(st.lists(lit, min_size=1, max_size=4))
        return InList(fld, tuple(values), draw(st.booleans()), POS)
    if kind == "between":
        a, b = draw(lit), draw(lit)
        a_val: Any = getattr(a, "value", None)
        b_val: Any = getattr(b, "value", None)
        lo, hi = (a, b) if a_val <= b_val else (b, a)
        return Between(fld, lo, hi, draw(st.booleans()), POS)
    raise AssertionError(kind)  # pragma: no cover


@st.composite
def bool_exprs(draw: st.DrawFn, max_depth: int = 3) -> BoolExpr:
    def build(depth: int) -> BoolExpr:
        if depth >= max_depth or draw(st.booleans()):
            return draw(_leaf_expr())
        kind = draw(st.sampled_from(("and", "or", "not")))
        if kind == "not":
            return Not(build(depth + 1), POS)
        left, right = build(depth + 1), build(depth + 1)
        return And(left, right, POS) if kind == "and" else Or(left, right, POS)

    return build(0)


@st.composite
def order_terms(draw: st.DrawFn) -> tuple[OrderTerm, ...]:
    names = draw(
        st.lists(st.sampled_from(ORDERABLE_BUILTIN_FIELDS), min_size=1, max_size=3, unique=True)
    )
    return tuple(OrderTerm(Field(name, False, POS), draw(st.booleans())) for name in names)


@st.composite
def record_batches(draw: st.DrawFn, min_size: int = 1, max_size: int = 6):
    """A list of (record, attrs) pairs with unique run ids, for seeding both backends."""
    count = draw(st.integers(min_value=min_size, max_value=max_size))
    batch = []
    for i in range(count):
        status = draw(st.sampled_from(_ALL_STATUSES))
        created_at = NOW + timedelta(hours=draw(st.sampled_from(HOUR_OFFSETS)))
        closed_at = draw(
            st.one_of(
                st.none(), st.sampled_from(HOUR_OFFSETS).map(lambda h: NOW + timedelta(hours=h))
            )
        )
        rec = make_record(
            run_id=f"run-{i}",
            workflow_name=draw(st.sampled_from(WORKFLOW_NAME_POOL)),
            status=status,
            created_at=created_at,
            closed_at=closed_at,
            tick_failures=draw(st.sampled_from(INT_POOL)),
            cancel_requested=draw(st.booleans()),
            close_reason=draw(st.sampled_from(STRING_POOL)),
            last_tick_error=draw(st.one_of(st.none(), st.sampled_from(STRING_POOL))),
        )
        attrs: dict[str, object] = {}
        if draw(st.booleans()):
            attrs["customer_tier"] = draw(st.sampled_from(STRING_POOL))
        if draw(st.booleans()):
            attrs["retries"] = draw(st.sampled_from(INT_POOL))
        if draw(st.booleans()):
            attrs["score"] = draw(st.sampled_from(FLOAT_POOL))
        if draw(st.booleans()):
            attrs["is_vip"] = draw(st.booleans())
        if draw(st.booleans()):
            attrs["activated_at"] = NOW + timedelta(hours=draw(st.sampled_from(HOUR_OFFSETS)))
        if draw(st.booleans()):
            # min_size=1: an explicitly empty keyword list is indistinguishable, in the
            # proposed one-row-per-element schema, from the attribute never having been set
            # at all (both store zero rows) -- see loomwork/visibility/ddl.py.
            attrs["tags"] = draw(
                st.lists(st.sampled_from(KEYWORD_POOL), min_size=1, max_size=3, unique=True)
            )
        batch.append((rec, attrs))
    return batch


__all__ = ["bool_exprs", "order_terms", "record_batches"]
