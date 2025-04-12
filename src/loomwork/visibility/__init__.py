"""A small, safe query language for finding workflow runs.

Operators need to answer questions like "all failed ``orders.process`` runs in the last day
where ``customer_tier`` is gold". This package parses, type-checks, evaluates and compiles
queries such as::

    status = 'failed' AND workflow_name = 'orders.process'
      AND created_at >= now() - interval '1 day'
      AND attr.customer_tier IN ('gold', 'platinum')
      AND NOT (close_reason LIKE 'ops%')
    ORDER BY created_at DESC

See ``docs/visibility-queries.md`` for the full grammar, examples and NULL semantics.

Pipeline
    1. :func:`~loomwork.visibility.parser.parse_query` turns text into an immutable AST.
    2. :func:`~loomwork.visibility.typecheck.check_query` validates it against a
       :class:`~loomwork.visibility.schema.SearchAttributes` schema and folds ``now()`` /
       interval arithmetic into concrete timestamps, using an injected clock.
    3. :func:`~loomwork.visibility.evaluator.compile_predicate` (in-memory) or
       :func:`~loomwork.visibility.sql.compile_sql` (SQL) evaluate the checked query.

Both consumers call :func:`~loomwork.visibility.typecheck.check_query` themselves, so a
caller normally only needs steps 1 and 3.
"""

from __future__ import annotations

from loomwork.visibility.errors import QuerySyntaxError, QueryTypeError, VisibilityQueryError
from loomwork.visibility.evaluator import Predicate, Trilean, compile_predicate, order_and_limit
from loomwork.visibility.nodes import Query
from loomwork.visibility.parser import parse_query
from loomwork.visibility.schema import BUILTIN_FIELDS, AttributeType, SchemaError, SearchAttributes
from loomwork.visibility.sql import CompiledSql, TsIn, compile_sql
from loomwork.visibility.typecheck import check_query

__all__ = [
    "BUILTIN_FIELDS",
    "AttributeType",
    "CompiledSql",
    "Predicate",
    "Query",
    "QuerySyntaxError",
    "QueryTypeError",
    "SchemaError",
    "SearchAttributes",
    "Trilean",
    "TsIn",
    "VisibilityQueryError",
    "check_query",
    "compile_predicate",
    "compile_sql",
    "order_and_limit",
    "parse_query",
]
