"""In-memory evaluation of a checked query against :class:`ExecutionRecord` rows.

``compile_predicate`` is the in-process counterpart to the SQL compiler in
:mod:`loomwork.visibility.sql`: both start from the same checked, time-resolved
:class:`~loomwork.visibility.nodes.Query` and must agree on every result (that agreement is
what the property test in ``tests/visibility`` checks).

NULL semantics
    Comparisons follow standard SQL three-valued logic. A field is NULL when the record's
    column is ``None`` (``closed_at``, ``last_tick_error``) or a search attribute was never
    set. Every comparison operator (``=``, ``<``, ``LIKE``, ``IN``, ``BETWEEN``, ``CONTAINS``)
    against a NULL operand evaluates to *unknown*, not ``False``. ``IS [NOT] NULL`` is the
    only operator that is never unknown. ``AND``/``OR``/``NOT`` propagate unknown using
    Kleene's K3 logic (``UNKNOWN AND FALSE = FALSE``, ``UNKNOWN OR TRUE = TRUE``, etc). A
    top-level ``WHERE`` keeps only rows where the whole expression is ``True`` -- exactly
    like SQL, unknown rows are excluded just as false ones are.

    We represent the three truth values as ``True``, ``False`` and ``None`` (for unknown)
    throughout this module, matching Python's own falsy/None distinction.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import datetime
from typing import Any

from loomwork.store.records import ExecutionRecord
from loomwork.visibility.nodes import (
    And,
    Between,
    BoolExpr,
    Compare,
    Contains,
    Field,
    InList,
    IntervalLiteral,
    IsNull,
    Like,
    Literal,
    Not,
    NowLiteral,
    Or,
    Query,
    TimeArith,
)
from loomwork.visibility.schema import BUILTIN_FIELDS, SearchAttributes
from loomwork.visibility.typecheck import check_query

#: SQL NULL semantics represented as Python's ``True`` / ``False`` / ``None`` (unknown).
Trilean = bool | None

#: ``compile_predicate``'s result: given a record and its search attributes, is it a match?
Predicate = Callable[[ExecutionRecord, Mapping[str, Any]], bool]

_BUILTIN_GETTERS: dict[str, Callable[[ExecutionRecord], Any]] = {
    "run_id": lambda r: r.run_id,
    "workflow_id": lambda r: r.workflow_id,
    "workflow_name": lambda r: r.workflow_name,
    "status": lambda r: r.status.value,
    "created_at": lambda r: r.created_at,
    "closed_at": lambda r: r.closed_at,
    "tick_failures": lambda r: r.tick_failures,
    "cancel_requested": lambda r: r.cancel_requested,
    "close_reason": lambda r: r.close_reason,
    "last_tick_error": lambda r: r.last_tick_error,
}
assert set(_BUILTIN_GETTERS) == set(BUILTIN_FIELDS)


def _field_value(fld: Field, record: ExecutionRecord, attrs: Mapping[str, Any]) -> Any:
    if fld.is_attr:
        return attrs.get(fld.name)
    return _BUILTIN_GETTERS[fld.name](record)


def _and3(a: Trilean, b: Trilean) -> Trilean:
    if a is False or b is False:
        return False
    if a is None or b is None:
        return None
    return True


def _or3(a: Trilean, b: Trilean) -> Trilean:
    if a is True or b is True:
        return True
    if a is None or b is None:
        return None
    return False


def _not3(a: Trilean) -> Trilean:
    return None if a is None else not a


def _compare(op: str, left: Any, right: Any) -> bool:
    if op == "=":
        return bool(left == right)
    if op == "!=":
        return bool(left != right)
    if op == "<":
        return bool(left < right)
    if op == "<=":
        return bool(left <= right)
    if op == ">":
        return bool(left > right)
    if op == ">=":
        return bool(left >= right)
    raise AssertionError(f"unknown comparison operator {op!r}")  # pragma: no cover


def _literal_value(lit: Literal) -> Any:
    # NullLiteral has no ``value`` attribute; every other resolved literal does. Now()/interval
    # nodes must already be folded away by check_query before a query reaches the evaluator.
    if isinstance(lit, NowLiteral | TimeArith | IntervalLiteral):
        raise AssertionError(  # pragma: no cover - guarded by check_query
            f"unresolved literal {lit!r} reached the evaluator; call check_query first"
        )
    return getattr(lit, "value", None)


def _like_to_regex(pattern: str) -> re.Pattern[str]:
    """Translate a SQL ``LIKE`` pattern (``%``, ``_``, ``\\`` escape) into a regex."""
    out: list[str] = []
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "\\" and i + 1 < len(pattern):
            out.append(re.escape(pattern[i + 1]))
            i += 2
            continue
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        else:
            out.append(re.escape(ch))
        i += 1
    return re.compile("^" + "".join(out) + "$", re.DOTALL)


def _eval(node: BoolExpr, record: ExecutionRecord, attrs: Mapping[str, Any]) -> Trilean:
    if isinstance(node, Compare):
        value = _field_value(node.field, record, attrs)
        target = _literal_value(node.value)
        if value is None or target is None:
            return None
        return _compare(node.op, value, target)
    if isinstance(node, InList):
        value = _field_value(node.field, record, attrs)
        if value is None:
            return None
        targets = [_literal_value(v) for v in node.values]
        if any(t is None for t in targets):
            # NULL among the candidates never matches but can turn a false into unknown,
            # exactly like SQL's ``x IN (1, NULL)``.
            found = value in [t for t in targets if t is not None]
            result: Trilean = True if found else None
        else:
            result = value in targets
        return _not3(result) if node.negated else result
    if isinstance(node, Like):
        value = _field_value(node.field, record, attrs)
        if value is None:
            return None
        matched = bool(_like_to_regex(node.pattern.value).match(value))
        return _not3(matched) if node.negated else matched
    if isinstance(node, IsNull):
        value = _field_value(node.field, record, attrs)
        is_null = value is None
        return _not3(is_null) if node.negated else is_null
    if isinstance(node, Between):
        value = _field_value(node.field, record, attrs)
        low = _literal_value(node.low)
        high = _literal_value(node.high)
        if value is None or low is None or high is None:
            return None
        result = bool(low <= value <= high)
        return _not3(result) if node.negated else result
    if isinstance(node, Contains):
        value = _field_value(node.field, record, attrs)
        if value is None:
            return None
        return node.value.value in value
    if isinstance(node, And):
        return _and3(_eval(node.left, record, attrs), _eval(node.right, record, attrs))
    if isinstance(node, Or):
        return _or3(_eval(node.left, record, attrs), _eval(node.right, record, attrs))
    if isinstance(node, Not):
        return _not3(_eval(node.operand, record, attrs))
    raise AssertionError(f"unhandled boolean node: {node!r}")  # pragma: no cover


def compile_predicate(query: Query, schema: SearchAttributes, now: datetime) -> Predicate:
    """Check ``query`` against ``schema`` and return a callable matcher.

    ``now`` resolves ``now()`` and interval arithmetic once, at compile time: the returned
    predicate is a pure function of its arguments and gives the same answer no matter when
    it is called, which is what makes a compiled query safe to reuse across a batch of
    records or to cache.
    """
    checked = check_query(query, schema, now)
    where = checked.where
    if where is None:
        return lambda record, attrs: True  # noqa: ARG005 - fixed predicate signature

    def predicate(record: ExecutionRecord, attrs: Mapping[str, Any]) -> bool:
        return _eval(where, record, attrs) is True

    return predicate


class _SortKey:
    """Comparable wrapper implementing NULLS-LAST ordering in either direction.

    Used as the ``key=`` for :func:`sorted` so a single stable pass handles multiple
    ``ORDER BY`` terms, each with its own direction, without needing a series of
    reverse-then-stable-sort passes (which would also have to special-case where NULLs land).
    """

    __slots__ = ("descending", "value")

    def __init__(self, value: Any, descending: bool) -> None:
        self.value = value
        self.descending = descending

    def __lt__(self, other: _SortKey) -> bool:
        if self.value is None:
            return False  # NULLs sort last: never "less than" anything
        if other.value is None:
            return True
        return bool(self.value > other.value) if self.descending else bool(self.value < other.value)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _SortKey):
            return NotImplemented
        return bool(self.value == other.value)


def order_and_limit(
    records: Iterable[ExecutionRecord], query: Query, schema: SearchAttributes, now: datetime
) -> list[ExecutionRecord]:
    """Apply a checked query's ``ORDER BY`` and ``LIMIT`` to an already-filtered iterable.

    Only built-in fields may appear in ``ORDER BY`` (enforced by the type checker), so no
    search attributes are needed here.
    """
    checked = check_query(query, schema, now)
    result: Sequence[ExecutionRecord] = list(records)
    if checked.order_by:
        getters = [_BUILTIN_GETTERS[term.field.name] for term in checked.order_by]
        directions = [term.descending for term in checked.order_by]
        result = sorted(
            result,
            key=lambda r: tuple(
                _SortKey(g(r), d) for g, d in zip(getters, directions, strict=True)
            ),
        )
    if checked.limit is not None:
        result = result[: checked.limit]
    return list(result)


__all__ = ["Predicate", "Trilean", "compile_predicate", "order_and_limit"]
