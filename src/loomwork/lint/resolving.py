"""Static resolution of names to dotted paths.

The linter never imports the code it analyzes (that would defeat the point of a
*static* analyzer, and would execute arbitrary user code). Instead it resolves the
handful of things it cares about - "what module-level function does this decorator
refer to", "what does this call actually call" - purely from the AST: it tracks import
aliases and a light approximation of local variable origins, then walks attribute
chains textually.

This is deliberately not full type inference. It resolves the patterns that show up in
real code (``import x as y``, ``from x import y as z``, ``obj = Cls(); obj.method()``)
and silently gives up on anything it cannot follow (an attribute of a function
parameter, a dict lookup, ...), which trades a few false negatives for zero false
positives from guessing.
"""

from __future__ import annotations

import ast
from collections.abc import Mapping

#: Built-in callables that matter to at least one rule. A bare, unshadowed reference to
#: one of these resolves to ``builtins.<name>`` even though nothing imported it.
BUILTIN_CALL_NAMES = frozenset(
    {"open", "print", "input", "id", "hash", "set", "frozenset", "eval", "exec", "compile"}
)


def build_import_map(module: ast.Module) -> dict[str, str]:
    """Map names bound at module scope to the dotted path they refer to.

    Only top-level ``import`` / ``from ... import`` statements are considered: imports
    performed inside a function or an ``if`` block are rare for the names this linter
    cares about (the workflow/step decorators, ``time``, ``random``, ...) and tracking
    them would require flow analysis this linter intentionally does not do.
    """
    mapping: dict[str, str] = {}
    for stmt in module.body:
        if isinstance(stmt, ast.Import):
            for alias in stmt.names:
                if alias.asname is None:
                    # ``import a.b.c`` binds the top-level package ``a`` to itself;
                    # ``a.b.c.whatever`` is still resolved by walking the attribute
                    # chain from that root.
                    bound = alias.name.split(".")[0]
                    mapping[bound] = bound
                else:
                    mapping[alias.asname] = alias.name
        elif isinstance(stmt, ast.ImportFrom) and stmt.module is not None and stmt.level == 0:
            for alias in stmt.names:
                if alias.name == "*":
                    continue
                bound = alias.asname or alias.name
                mapping[bound] = f"{stmt.module}.{alias.name}"
    return mapping


def resolve_dotted(node: ast.expr, names: Mapping[str, str]) -> str | None:
    """Resolve a ``Name``/``Attribute`` chain to a dotted path using ``names``.

    ``names`` is consulted for every bare name at the root of the chain: it is either
    the module's import map, a local-origin map (see :mod:`loomwork.lint.checker`), or a
    combination of both. A call in the middle of the chain (``f().attr``) is resolved
    through its own callee, so ``requests.Session().get`` still resolves to
    ``requests.Session.get`` even though nothing assigned the session to a variable.
    """
    if isinstance(node, ast.Name):
        return names.get(node.id)
    if isinstance(node, ast.Attribute):
        base = resolve_dotted(node.value, names)
        if base is None and isinstance(node.value, ast.Call):
            base = resolve_dotted(node.value.func, names)
        if base is None:
            return None
        return f"{base}.{node.attr}"
    return None


def resolve_call_target(call: ast.Call, names: Mapping[str, str]) -> str | None:
    """Resolve what a :class:`ast.Call` calls, falling back to known builtins."""
    resolved = resolve_dotted(call.func, names)
    if resolved is None and isinstance(call.func, ast.Name) and call.func.id in BUILTIN_CALL_NAMES:
        return f"builtins.{call.func.id}"
    return resolved


#: Dotted paths that identify the ``@workflow`` decorator, however it was imported:
#: ``import loomwork`` + ``@loomwork.workflow``, ``import loomwork as lw`` + ``@lw.workflow``,
#: ``from loomwork import workflow`` + ``@workflow``, ``@loomwork.sdk.workflow``, etc.
#: ``resolve_dotted`` turns all of these spellings into one of the paths below because it
#: resolves through the import map rather than comparing source text.
WORKFLOW_DECORATOR_TARGETS = frozenset(
    {
        "loomwork.workflow",
        "loomwork.sdk.workflow",
        "loomwork.sdk.api.workflow",
        "loomwork.sdk.definitions.workflow",
    }
)

#: Same idea for ``@step``. Functions decorated with one of these are allowed to do the
#: I/O a step exists for, so the analyzer never recurses into their bodies.
STEP_DECORATOR_TARGETS = frozenset(
    {
        "loomwork.step",
        "loomwork.sdk.step",
        "loomwork.sdk.api.step",
        "loomwork.sdk.definitions.step",
    }
)


def _decorator_target(decorator: ast.expr, names: Mapping[str, str]) -> str | None:
    node = decorator.func if isinstance(decorator, ast.Call) else decorator
    return resolve_dotted(node, names)


def classify_decorators(
    fn: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef, names: Mapping[str, str]
) -> str | None:
    """Return ``"workflow"``, ``"step"`` or ``None`` for a function's (or a class's)
    decorators."""
    for decorator in fn.decorator_list:
        target = _decorator_target(decorator, names)
        if target in WORKFLOW_DECORATOR_TARGETS:
            return "workflow"
        if target in STEP_DECORATOR_TARGETS:
            return "step"
    return None
