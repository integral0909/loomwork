"""Orchestration: turn source code into a sorted, filtered list of findings.

This is the one module that ties everything else together: parsing, import resolution,
workflow/step discovery, the per-function checker, the intra-module call graph, and
suppressions and config are all applied here so that :mod:`loomwork.lint` (the public
API) and :mod:`loomwork.lint.cli` stay thin.
"""

from __future__ import annotations

import ast
from dataclasses import replace
from pathlib import Path
from typing import Final

from loomwork.lint.checker import FunctionChecker
from loomwork.lint.config import LintConfig
from loomwork.lint.discovery import iter_python_files
from loomwork.lint.models import Finding, LintResult, Location, Severity
from loomwork.lint.registry import DEFAULT_REGISTRY, RuleRegistry
from loomwork.lint.resolving import build_import_map, classify_decorators
from loomwork.lint.rules import FORBIDDEN_CALLS, RULE_CUSTOM_FORBIDDEN_CALL, RULE_PARSE_ERROR
from loomwork.lint.rules.tables import ForbiddenCall, MatchKind
from loomwork.lint.suppressions import collect_suppressions

#: How many hops through the intra-module call graph the analyzer follows from a
#: workflow before giving up. Deep call chains are rare in practice; this bounds the
#: work done on pathological or mutually-recursive helpers.
MAX_CALL_DEPTH: Final[int] = 5

_FunctionDef = ast.FunctionDef | ast.AsyncFunctionDef


def analyze_source(
    source: str,
    filename: str,
    config: LintConfig | None = None,
    *,
    registry: RuleRegistry = DEFAULT_REGISTRY,
) -> list[Finding]:
    """Lint one file's already-read source. Returns findings, sorted and filtered."""
    cfg = config or LintConfig.default()
    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError as exc:
        return [_parse_error_finding(filename, exc)]

    suppressions = collect_suppressions(source)
    if suppressions.ignore_file:
        return []

    import_map = build_import_map(tree)
    functions: dict[str, _FunctionDef] = {
        stmt.name: stmt
        for stmt in tree.body
        if isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef)
    }
    decorator_kind = {name: classify_decorators(fn, import_map) for name, fn in functions.items()}
    forbidden_calls = _combined_forbidden_calls(cfg)
    local_function_names = frozenset(functions)

    findings: list[Finding] = []
    for name, fdef in functions.items():
        if decorator_kind[name] != "workflow":
            continue
        findings.extend(
            _analyze_workflow(
                fdef,
                filename=filename,
                import_map=import_map,
                functions=functions,
                decorator_kind=decorator_kind,
                forbidden_calls=forbidden_calls,
                allowed_calls=cfg.allowed_calls,
                registry=registry,
                local_function_names=local_function_names,
            )
        )

    findings = [f for f in findings if not suppressions.suppresses(f.code, f.location.line)]
    findings = [f for f in findings if cfg.is_enabled(f.code)]
    findings.sort(key=Finding.sort_key)
    return findings


def analyze_file(
    path: str | Path, config: LintConfig | None = None, *, registry: RuleRegistry = DEFAULT_REGISTRY
) -> list[Finding]:
    """Read and lint one file from disk."""
    file_path = Path(path)
    try:
        source = file_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        return [
            Finding(
                code=RULE_PARSE_ERROR.code,
                name=RULE_PARSE_ERROR.name,
                severity=Severity.ERROR,
                message=RULE_PARSE_ERROR.format(name=f"could not read file: {exc}"),
                hint=RULE_PARSE_ERROR.hint,
                location=Location(file=str(file_path), line=1, column=1),
            )
        ]
    return analyze_source(source, str(file_path), config, registry=registry)


def lint_source(
    source: str,
    filename: str = "<string>",
    config: LintConfig | None = None,
    *,
    registry: RuleRegistry = DEFAULT_REGISTRY,
) -> LintResult:
    """Lint a source string directly (no filesystem access). Mainly for tests."""
    findings = analyze_source(source, filename, config, registry=registry)
    return LintResult(findings=tuple(findings), files_scanned=1)


def lint_paths(
    paths: list[str] | list[Path] | list[str | Path],
    config: LintConfig | None = None,
    *,
    registry: RuleRegistry = DEFAULT_REGISTRY,
) -> LintResult:
    """Lint every ``.py`` file found under ``paths``."""
    cfg = config or LintConfig.default()
    findings: list[Finding] = []
    files_scanned = 0
    for file_path in iter_python_files(paths, cfg.exclude):
        findings.extend(analyze_file(file_path, cfg, registry=registry))
        files_scanned += 1
    findings.sort(key=Finding.sort_key)
    return LintResult(findings=tuple(findings), files_scanned=files_scanned)


# --------------------------------------------------------------------------- internals


def _parse_error_finding(filename: str, exc: SyntaxError) -> Finding:
    return Finding(
        code=RULE_PARSE_ERROR.code,
        name=RULE_PARSE_ERROR.name,
        severity=Severity.ERROR,
        message=RULE_PARSE_ERROR.format(name=exc.msg),
        hint=RULE_PARSE_ERROR.hint,
        location=Location(file=filename, line=exc.lineno or 1, column=(exc.offset or 1)),
    )


def _combined_forbidden_calls(config: LintConfig) -> tuple[ForbiddenCall, ...]:
    extra = tuple(
        ForbiddenCall(dotted, RULE_CUSTOM_FORBIDDEN_CALL.code, MatchKind.EXACT)
        for dotted in config.extend_forbidden_calls
    )
    return FORBIDDEN_CALLS + extra


def _analyze_workflow(
    entry: _FunctionDef,
    *,
    filename: str,
    import_map: dict[str, str],
    functions: dict[str, _FunctionDef],
    decorator_kind: dict[str, str | None],
    forbidden_calls: tuple[ForbiddenCall, ...],
    allowed_calls: frozenset[str],
    registry: RuleRegistry,
    local_function_names: frozenset[str],
) -> list[Finding]:
    """Analyze a workflow and every same-module helper it reaches, depth-limited.

    A helper is followed only if it is a plain function (no decorator this linter
    recognizes): a ``@step`` is expected to do real I/O, and another ``@workflow`` is
    analyzed as its own entry point, so descending into either here would duplicate or
    misattribute findings.
    """
    collected: list[Finding] = []
    visiting: set[str] = set()

    def visit(fdef: _FunctionDef, path: tuple[str, ...], depth: int) -> None:
        if fdef.name in visiting or depth > MAX_CALL_DEPTH:
            return
        visiting.add(fdef.name)
        checker = FunctionChecker(
            filename=filename,
            import_map=import_map,
            forbidden_calls=forbidden_calls,
            allowed_calls=allowed_calls,
            registry=registry,
            local_function_names=local_function_names,
        )
        checker.check(fdef)
        if path:
            collected.extend(replace(finding, call_path=path) for finding in checker.findings)
        else:
            collected.extend(checker.findings)
        next_path = (*path, fdef.name)
        for callee_name in dict.fromkeys(checker.local_calls):
            callee = functions.get(callee_name)
            if callee is None or decorator_kind.get(callee_name) is not None:
                continue
            visit(callee, next_path, depth + 1)
        visiting.discard(fdef.name)

    visit(entry, (), 0)
    return collected
