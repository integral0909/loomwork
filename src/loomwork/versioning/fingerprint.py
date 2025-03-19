"""Code fingerprints of workflow definitions.

A fingerprint is a short hash of a workflow function's *normalised* source: comments,
docstrings, formatting and type annotations do not change it, while any change to the
logic does. Every tick records the fingerprint of the code that executed it
(``TickCompleted.code_version``), so an operator can tell which deploy made each decision
and the compatibility checker can group recorded histories by code version.
"""

from __future__ import annotations

import ast
import hashlib
import inspect
import textwrap
from collections.abc import Callable
from typing import Any

FINGERPRINT_LENGTH = 12


class _Normalizer(ast.NodeTransformer):
    """Removes parts of the AST that cannot affect behaviour."""

    def __init__(self) -> None:
        self._depth = 0

    def _strip_docstring(self, body: list[ast.stmt]) -> list[ast.stmt]:
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            return body[1:] or [ast.Pass()]
        return body

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.AST:  # noqa: N802
        return self._function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> ast.AST:  # noqa: N802
        return self._function(node)

    def _function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> ast.AST:
        if self._depth == 0:
            # The workflow's recorded name is independent of the Python function name.
            node.name = ""
        self._depth += 1
        node.body = self._strip_docstring(node.body)
        node.returns = None
        node.decorator_list = []
        node.type_params = []
        self.generic_visit(node)
        self._depth -= 1
        return node

    def visit_arg(self, node: ast.arg) -> ast.AST:
        node.annotation = None
        return node

    def visit_AnnAssign(self, node: ast.AnnAssign) -> ast.AST | None:  # noqa: N802
        if node.value is None:
            return None  # a bare annotation has no runtime effect in a function
        return ast.Assign(targets=[node.target], value=node.value)


def _source_fingerprint(fn: Callable[..., Any]) -> str | None:
    try:
        source = textwrap.dedent(inspect.getsource(inspect.unwrap(fn)))
        tree = ast.parse(source)
    except (OSError, TypeError, SyntaxError):
        return None
    normalized = _Normalizer().visit(tree)
    dump = ast.dump(normalized, annotate_fields=False, include_attributes=False)
    return hashlib.sha256(dump.encode()).hexdigest()


def _bytecode_fingerprint(fn: Callable[..., Any]) -> str:
    code = getattr(inspect.unwrap(fn), "__code__", None)
    if code is None:
        return hashlib.sha256(repr(fn).encode()).hexdigest()
    digest = hashlib.sha256()

    def feed(co: Any) -> None:
        digest.update(co.co_code)
        digest.update(repr(co.co_names).encode())
        for const in co.co_consts:
            if inspect.iscode(const):
                feed(const)
            else:
                digest.update(repr(const).encode())

    feed(code)
    return digest.hexdigest()


def code_fingerprint(fn: Callable[..., Any]) -> str:
    """A short, stable identifier of ``fn``'s logic.

    Falls back to hashing bytecode when the source is unavailable (for example in a
    frozen application); such fingerprints change between Python versions.
    """
    full = _source_fingerprint(fn) or _bytecode_fingerprint(fn)
    return full[:FINGERPRINT_LENGTH]
