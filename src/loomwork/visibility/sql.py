"""Compile a checked query into a WHERE clause for ``lw_executions``.

Design
    Every value the query text supplies is bound as a ``?`` placeholder parameter -- never
    interpolated into the SQL text. The only things ever written directly into the SQL
    string are: fixed keywords/operators, and column or value-type names drawn from a
    small, fixed internal table (:mod:`loomwork.visibility.ddl`), never from user input.
    Field *names* (including search attribute names) are always parameters, even though
    they happen to satisfy an identifier-safe regex, so a change to that regex can never
    turn into an injection vector.

    Built-in fields compile to a direct reference to the matching ``lw_executions`` column.
    Search attributes compile to a scalar subquery against ``lw_search_attributes`` (see
    ``loomwork.visibility.ddl`` for the proposed schema): ``(SELECT sa.<col> FROM
    lw_search_attributes sa WHERE sa.run_id = lw_executions.run_id AND sa.name = ? AND
    sa.value_type = ?)``. When the attribute was never set, that subquery returns SQL NULL,
    and every comparison operator then propagates NULL exactly the way
    :mod:`loomwork.visibility.evaluator`'s three-valued logic does -- for free, from the
    database's own NULL handling. This relies on at most one row per ``(run_id, name)`` for
    scalar attribute types, which the proposed schema enforces with a partial unique index.

    ``CONTAINS`` (keyword-list attributes) is the one operator that cannot use a scalar
    subquery, because a keyword list is stored as several rows. It compiles to a ``CASE``
    that yields NULL when the attribute was never set at all, 1 when a matching element
    exists, and 0 otherwise, compared with ``= 1`` so the whole expression is a normal
    three-valued boolean rather than a bare integer (which PostgreSQL would refuse in a
    boolean context).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any, NamedTuple

from loomwork.store.sql.migrations import Dialect
from loomwork.visibility.ddl import VALUE_COLUMNS, VALUE_TYPES
from loomwork.visibility.nodes import (
    And,
    Between,
    BoolExpr,
    Compare,
    Contains,
    InList,
    IntervalLiteral,
    IsNull,
    Like,
    Literal,
    Not,
    NowLiteral,
    NullLiteral,
    Or,
    OrderTerm,
    Query,
    TimeArith,
    TimestampLiteral,
)
from loomwork.visibility.schema import BUILTIN_FIELDS, AttributeType, SearchAttributes
from loomwork.visibility.typecheck import ResolvedField, check_query, resolve_field

#: Converts an (aware, UTC) datetime to whatever a backend's timestamp column expects --
#: integer microseconds for SQLite, the datetime itself (or a string) for PostgreSQL. Mirrors
#: ``SQLStore.ts_in``; pass that method directly when compiling against a real store.
TsIn = Callable[[datetime], Any]


class CompiledSql(NamedTuple):
    """A compiled query, ready to splice into a larger statement.

    Callers compose the final statement themselves, for example::

        where_sql, params, order_by_sql = compile_sql(
            query, schema, dialect=..., now=..., ts_in=...
        )
        sql = f"SELECT * FROM lw_executions WHERE {where_sql}"
        if order_by_sql:
            sql += f" ORDER BY {order_by_sql}"
        sql += " LIMIT ?"
        params = [*params, query.limit or default_limit]

    ``params`` covers every ``?`` placeholder in ``where_sql`` *and then* ``order_by_sql``,
    in that order -- matching the order those fragments appear in the composition above.
    Do not reorder them relative to each other.
    """

    where_sql: str
    params: list[Any]
    order_by_sql: str


def _bind_value_type(builder: _SqlBuilder, attr_type: AttributeType) -> str:
    return builder.bind(VALUE_TYPES[attr_type.value])


class _SqlBuilder:
    """Accumulates SQL text and its bound parameters, in the order both are produced."""

    def __init__(self, dialect: Dialect, ts_in: TsIn) -> None:
        self.dialect = dialect
        self.ts_in = ts_in
        self.params: list[Any] = []

    def bind(self, value: Any) -> str:
        self.params.append(value)
        return "?"

    def bind_literal(self, lit: Literal) -> str:
        # now()/interval nodes must already be folded away by check_query before a query
        # reaches the SQL compiler.
        if isinstance(lit, NowLiteral | TimeArith | IntervalLiteral):
            raise AssertionError(  # pragma: no cover - guarded by check_query
                f"unresolved literal {lit!r} reached the SQL compiler; call check_query first"
            )
        if isinstance(lit, NullLiteral):
            return self.bind(None)
        if isinstance(lit, TimestampLiteral):
            return self.bind(self.ts_in(lit.value))
        return self.bind(lit.value)

    def field_expr(self, resolved: ResolvedField) -> str:
        """SQL for reading a field's value. Every call binds its own fresh parameters, so
        callers that need the expression more than once (see ``order_by``) must call this
        again rather than reusing the returned string -- reusing it would duplicate a ``?``
        in the SQL text without a matching entry in ``params``.
        """
        if not resolved.field.is_attr:
            spec = BUILTIN_FIELDS[resolved.field.name]
            return f"lw_executions.{spec.column}"
        col = VALUE_COLUMNS[resolved.type.value]
        name_param = self.bind(resolved.field.name)
        type_param = _bind_value_type(self, resolved.type)
        # `col` is one of a fixed, small set of column names in VALUE_COLUMNS, never user
        # input; the attribute name and type are bound parameters (name_param, type_param).
        return (
            f"(SELECT sa.{col} FROM lw_search_attributes sa "  # noqa: S608
            f"WHERE sa.run_id = lw_executions.run_id AND sa.name = {name_param} "
            f"AND sa.value_type = {type_param})"
        )

    def attr_exists(self, name: str, attr_type: AttributeType) -> str:
        name_param = self.bind(name)
        type_param = _bind_value_type(self, attr_type)
        # Same as field_expr above: no user input is interpolated, only bound parameters.
        return (
            "EXISTS (SELECT 1 FROM lw_search_attributes sa "  # noqa: S608
            f"WHERE sa.run_id = lw_executions.run_id AND sa.name = {name_param} "
            f"AND sa.value_type = {type_param})"
        )

    # ------------------------------------------------------------------ leaf predicates

    def compare(self, node: Compare, resolved: ResolvedField) -> str:
        left = self.field_expr(resolved)
        right = self.bind_literal(node.value)
        return f"({left} {node.op} {right})"

    def in_list(self, node: InList, resolved: ResolvedField) -> str:
        left = self.field_expr(resolved)
        placeholders = ", ".join(self.bind_literal(v) for v in node.values)
        sql = f"({left} IN ({placeholders}))"
        return f"(NOT {sql})" if node.negated else sql

    def like(self, node: Like, resolved: ResolvedField) -> str:
        left = self.field_expr(resolved)
        pattern = self.bind(node.pattern.value)
        # ``ESCAPE '\'`` is identical, valid syntax on both dialects: PostgreSQL already
        # treats backslash as the default LIKE escape, and SQLite has none unless told.
        sql = f"({left} LIKE {pattern} ESCAPE '\\')"
        return f"(NOT {sql})" if node.negated else sql

    def is_null(self, node: IsNull, resolved: ResolvedField) -> str:
        if resolved.field.is_attr and resolved.type is AttributeType.KEYWORD_LIST:
            # A keyword list is stored as zero or more rows; there is no single value for a
            # scalar subquery to return, so "unset" is "no rows at all" instead.
            exists = self.attr_exists(resolved.field.name, resolved.type)
            return f"({exists})" if node.negated else f"(NOT {exists})"
        left = self.field_expr(resolved)
        return f"({left} IS NOT NULL)" if node.negated else f"({left} IS NULL)"

    def between(self, node: Between, resolved: ResolvedField) -> str:
        left = self.field_expr(resolved)
        low = self.bind_literal(node.low)
        high = self.bind_literal(node.high)
        sql = f"({left} BETWEEN {low} AND {high})"
        return f"(NOT {sql})" if node.negated else sql

    def contains(self, node: Contains) -> str:
        exists_any = self.attr_exists(node.field.name, AttributeType.KEYWORD_LIST)
        name_param = self.bind(node.field.name)
        type_param = _bind_value_type(self, AttributeType.KEYWORD_LIST)
        element_param = self.bind(node.value.value)
        # Same as field_expr/attr_exists: only bound parameters are interpolated.
        exists_match = (
            "EXISTS (SELECT 1 FROM lw_search_attributes sa "  # noqa: S608
            f"WHERE sa.run_id = lw_executions.run_id AND sa.name = {name_param} "
            f"AND sa.value_type = {type_param} AND sa.str_value = {element_param})"
        )
        # NULL when the attribute was never set at all, else a definite 1/0 -- matching the
        # in-memory evaluator, where a missing attribute is unknown but an empty/non-matching
        # list is a definite false.
        return f"((CASE WHEN NOT {exists_any} THEN NULL WHEN {exists_match} THEN 1 ELSE 0 END) = 1)"

    # ------------------------------------------------------------------ tree walk

    def compile_bool(self, node: BoolExpr, schema: SearchAttributes) -> str:
        if isinstance(node, Compare):
            return self.compare(node, resolve_field(node.field, schema, ""))
        if isinstance(node, InList):
            return self.in_list(node, resolve_field(node.field, schema, ""))
        if isinstance(node, Like):
            return self.like(node, resolve_field(node.field, schema, ""))
        if isinstance(node, IsNull):
            return self.is_null(node, resolve_field(node.field, schema, ""))
        if isinstance(node, Between):
            return self.between(node, resolve_field(node.field, schema, ""))
        if isinstance(node, Contains):
            return self.contains(node)
        if isinstance(node, And):
            left_sql = self.compile_bool(node.left, schema)
            right_sql = self.compile_bool(node.right, schema)
            return f"({left_sql} AND {right_sql})"
        if isinstance(node, Or):
            left_sql = self.compile_bool(node.left, schema)
            right_sql = self.compile_bool(node.right, schema)
            return f"({left_sql} OR {right_sql})"
        if isinstance(node, Not):
            return f"(NOT {self.compile_bool(node.operand, schema)})"
        raise AssertionError(f"unhandled boolean node: {node!r}")  # pragma: no cover

    def compile_order_by(self, terms: tuple[OrderTerm, ...], schema: SearchAttributes) -> str:
        parts: list[str] = []
        for term in terms:
            resolved = resolve_field(term.field, schema, "")
            # Two independent calls: each binds its own parameters, keeping every ``?`` in
            # the emitted text lined up with exactly one entry in ``params``.
            null_check = self.field_expr(resolved)
            order_expr = self.field_expr(resolved)
            direction = "DESC" if term.descending else "ASC"
            parts.append(f"({null_check} IS NULL) ASC, {order_expr} {direction}")
        return ", ".join(parts)


def compile_sql(
    query: Query,
    schema: SearchAttributes,
    *,
    dialect: Dialect,
    now: datetime,
    ts_in: TsIn,
    source: str = "",
) -> CompiledSql:
    """Check ``query`` and compile it to a portable SQL ``WHERE``/``ORDER BY`` pair.

    ``ts_in`` must match the backend's own conversion (``SQLStore.ts_in``) so a
    :class:`~loomwork.visibility.nodes.TimestampLiteral` is bound in the same representation
    already stored in ``created_at``/``closed_at``/the search attribute ``time_value`` column.
    ``query.limit`` is not part of the returned tuple (see :class:`CompiledSql`); read it
    from ``query`` directly, it needs no dialect-specific handling.
    """
    checked = check_query(query, schema, now, source)
    builder = _SqlBuilder(dialect, ts_in)
    where_sql = (
        builder.compile_bool(checked.where, schema) if checked.where is not None else "1 = 1"
    )
    order_by_sql = builder.compile_order_by(checked.order_by, schema) if checked.order_by else ""
    return CompiledSql(where_sql, builder.params, order_by_sql)


__all__ = ["CompiledSql", "TsIn", "compile_sql"]
