"""The per-function AST checker.

:class:`FunctionChecker` is the workhorse: given one function body, the module's import
map and the combined forbidden-call table, it walks the function once and collects every
finding plus every call to another same-module function (for the analyzer to follow
transitively). A fresh instance is used per function so that local-variable origin
tracking and the "declared global" set never leak between unrelated functions.
"""

# ``ast.NodeVisitor`` dispatches on the exact, mixed-case node class name (``visit_If``,
# ``visit_FunctionDef``, ...); every method named that way in this file is deliberate.
# ruff: noqa: N802

from __future__ import annotations

import ast
from collections.abc import Iterable, Iterator, Mapping

from loomwork.lint.models import Finding, Location
from loomwork.lint.registry import RuleRegistry
from loomwork.lint.resolving import resolve_call_target, resolve_dotted
from loomwork.lint.rules import (
    OPAQUE_RECEIVER_METHODS,
    RULE_ENVIRONMENT_ACCESS,
    RULE_GLOBAL_MUTATION,
    RULE_HASH_IN_CONTROL_FLOW,
    RULE_HASH_ORDER_ITERATION,
)
from loomwork.lint.rules.concurrency_rules import RULE_BLOCKING_CONCURRENCY
from loomwork.lint.rules.tables import ForbiddenCall

_HASH_SEED_CALLS = frozenset({"builtins.id", "builtins.hash"})
_SET_BUILTIN_CALLS = frozenset({"builtins.set", "builtins.frozenset"})


class FunctionChecker(ast.NodeVisitor):
    """Checks one function body for determinism violations."""

    def __init__(
        self,
        *,
        filename: str,
        import_map: Mapping[str, str],
        forbidden_calls: Iterable[ForbiddenCall],
        allowed_calls: frozenset[str],
        registry: RuleRegistry,
        local_function_names: frozenset[str],
    ) -> None:
        self._filename = filename
        self._import_map = import_map
        self._forbidden_calls = tuple(forbidden_calls)
        self._allowed_calls = allowed_calls
        self._registry = registry
        self._local_function_names = local_function_names
        self._origins: dict[str, str] = {}
        self._global_names: frozenset[str] = frozenset()
        self._parents: dict[int, ast.AST] = {}
        self.findings: list[Finding] = []
        #: Names of same-module functions called by simple name (``helper()``), for the
        #: analyzer to follow into the call graph.
        self.local_calls: list[str] = []

    def check(self, fn: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        """Analyze ``fn``'s body. Populates :attr:`findings` and :attr:`local_calls`."""
        self._parents = {}
        for parent in ast.walk(fn):
            for child in ast.iter_child_nodes(parent):
                self._parents[id(child)] = parent
        self._check_scope(fn)

    # ------------------------------------------------------------------ scope handling

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_scope(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_scope(node)

    def _check_scope(self, fn: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        # ``global`` declarations are scoped to the (possibly nested) function they
        # appear in, so each function body gets its own declared-globals set.
        previous = self._global_names
        self._global_names = _collect_global_names(fn)
        self.generic_visit(fn)
        self._global_names = previous

    def _names(self) -> dict[str, str]:
        # Local origins shadow the module's import map: a variable that happens to share
        # a name with an imported module hides the import once assigned, matching
        # ordinary Python scoping.
        combined = dict(self._import_map)
        combined.update(self._origins)
        return combined

    # ------------------------------------------------------------------ reporting

    def _location(self, node: ast.AST) -> Location:
        end_col = getattr(node, "end_col_offset", None)
        return Location(
            file=self._filename,
            line=getattr(node, "lineno", 1),
            column=getattr(node, "col_offset", 0) + 1,
            end_line=getattr(node, "end_lineno", None),
            end_column=end_col + 1 if end_col is not None else None,
        )

    def _report(self, rule_code: str, node: ast.AST, **fields: object) -> None:
        rule = self._registry[rule_code]
        self.findings.append(
            Finding(
                code=rule.code,
                name=rule.name,
                severity=rule.severity,
                message=rule.format(**fields),
                hint=rule.hint,
                location=self._location(node),
            )
        )

    # ------------------------------------------------------------------ variable origins

    def visit_Assign(self, node: ast.Assign) -> None:
        self._check_global_targets(node.targets)
        self._track_origin(node.targets, node.value)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        # ``x: int`` alone is a declaration, not an assignment; only check/track it when
        # there is a value, matching ``_check_global_targets``'s "assignment" contract.
        if node.value is not None:
            self._check_global_targets([node.target])
            self._track_origin([node.target], node.value)
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        self._check_global_targets([node.target])
        if isinstance(node.target, ast.Name):
            self._origins.pop(node.target.id, None)
        self.generic_visit(node)

    def _track_origin(self, targets: list[ast.expr], value: ast.expr) -> None:
        if len(targets) != 1 or not isinstance(targets[0], ast.Name):
            return
        name = targets[0].id
        resolved = (
            resolve_call_target(value, self._names()) if isinstance(value, ast.Call) else None
        )
        if resolved is not None:
            self._origins[name] = resolved
        else:
            # Anything else invalidates a stale origin: after ``p = compute()``, a later
            # ``p.read_text()`` must not still be attributed to pathlib.Path.
            self._origins.pop(name, None)

    # ------------------------------------------------------------------ LW113 global mutation

    def _check_global_targets(self, targets: list[ast.expr]) -> None:
        if not self._global_names:
            return
        for target in targets:
            for name in _iter_target_names(target):
                if name in self._global_names:
                    self._report(RULE_GLOBAL_MUTATION.code, target, name=name)

    # ------------------------------------------------------------------ forbidden calls

    def visit_Call(self, node: ast.Call) -> None:
        names = self._names()
        resolved = resolve_call_target(node, names)
        matched = False
        if resolved is not None and resolved not in self._allowed_calls:
            for entry in self._forbidden_calls:
                if entry.matches(resolved):
                    self._report(entry.code, node, name=resolved)
                    matched = True
                    break
        if (
            not matched
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in OPAQUE_RECEIVER_METHODS
        ):
            label = resolved if resolved is not None else f"...{node.func.attr}"
            self._report(RULE_BLOCKING_CONCURRENCY.code, node, name=label)
        if isinstance(node.func, ast.Name) and node.func.id in self._local_function_names:
            self.local_calls.append(node.func.id)
        self.generic_visit(node)

    # ------------------------------------------------------------------ LW110 os.environ

    def visit_Attribute(self, node: ast.Attribute) -> None:
        self._check_environ_reference(node)
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        self._check_environ_reference(node)
        self.generic_visit(node)

    def _check_environ_reference(self, node: ast.Attribute | ast.Name) -> None:
        resolved = resolve_dotted(node, self._names())
        if resolved != "os.environ":
            return
        parent = self._parents.get(id(node))
        if isinstance(parent, ast.Attribute):
            # Chained further (``os.environ.get(...)``); the outer call is reported by
            # the forbidden-call table instead, so reporting here too would be noise.
            return
        self._report(RULE_ENVIRONMENT_ACCESS.code, node, name="os.environ")

    # ------------------------------------------------------------------ LW111 iteration order

    def visit_For(self, node: ast.For) -> None:
        self._check_iteration_order(node.iter)
        self.generic_visit(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:
        self._check_iteration_order(node.iter)
        self.generic_visit(node)

    def _check_iteration_order(self, expr: ast.expr) -> None:
        label = self._describe_unordered(expr)
        if label is not None:
            self._report(RULE_HASH_ORDER_ITERATION.code, expr, name=label)

    def _describe_unordered(self, expr: ast.expr) -> str | None:
        if isinstance(expr, ast.Set):
            return "set literal"
        if isinstance(expr, ast.SetComp):
            return "set comprehension"
        if isinstance(expr, ast.Call):
            resolved = resolve_call_target(expr, self._names())
            if resolved == "builtins.set":
                return "set(...) call"
            if resolved == "builtins.frozenset":
                return "frozenset(...) call"
        return None

    # ------------------------------------------------------------------ LW112 hash in control flow

    def visit_If(self, node: ast.If) -> None:
        self._check_hash_in_condition(node.test)
        self.generic_visit(node)

    def visit_While(self, node: ast.While) -> None:
        self._check_hash_in_condition(node.test)
        self.generic_visit(node)

    def visit_IfExp(self, node: ast.IfExp) -> None:
        self._check_hash_in_condition(node.test)
        self.generic_visit(node)

    def _check_hash_in_condition(self, test: ast.expr) -> None:
        names = self._names()
        for call in ast.walk(test):
            if not isinstance(call, ast.Call):
                continue
            resolved = resolve_call_target(call, names)
            if resolved in _HASH_SEED_CALLS:
                func_name = resolved.rsplit(".", 1)[-1]
                self._report(RULE_HASH_IN_CONTROL_FLOW.code, call, name=func_name)


def _iter_target_names(target: ast.expr) -> Iterator[str]:
    if isinstance(target, ast.Name):
        yield target.id
    elif isinstance(target, ast.Tuple | ast.List):
        for elt in target.elts:
            yield from _iter_target_names(elt)
    elif isinstance(target, ast.Starred):
        yield from _iter_target_names(target.value)


class _GlobalCollector(ast.NodeVisitor):
    """Collects the names declared ``global`` directly in one function's own scope."""

    def __init__(self) -> None:
        self.names: set[str] = set()

    def visit_Global(self, node: ast.Global) -> None:
        self.names.update(node.names)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        pass  # nested scope: has its own global declarations, do not descend

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        pass

    def visit_Lambda(self, node: ast.Lambda) -> None:
        pass

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        pass


def _collect_global_names(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> frozenset[str]:
    collector = _GlobalCollector()
    for stmt in fn.body:
        collector.visit(stmt)
    return frozenset(collector.names)
