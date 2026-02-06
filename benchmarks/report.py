"""Turning result files into text: a console table, a Markdown table, and a
regression-aware diff between two result files.
"""

from __future__ import annotations

from collections.abc import Sequence

from benchmarks import stats
from benchmarks.results import ResultFile, ScenarioResult


def _point(result: ScenarioResult) -> str:
    return f"{result.point:.4g}"


def _ci(result: ScenarioResult) -> str:
    return f"[{result.ci_low:.4g}, {result.ci_high:.4g}]"


def _render_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt(cells: Sequence[str]) -> str:
        return "  ".join(cell.ljust(width) for cell, width in zip(cells, widths, strict=True))

    lines = [fmt(headers), "  ".join("-" * w for w in widths)]
    lines.extend(fmt(row) for row in rows)
    return "\n".join(lines)


def console_table(result_file: ResultFile) -> str:
    """A fixed-width table, one row per scenario/store/case."""
    headers = ["scenario", "store", "case", "metric", "point", "95% ci", "unit", "n"]
    rows = [
        [r.scenario, r.store, r.case, r.metric, _point(r), _ci(r), r.unit, str(r.n)]
        for r in result_file.results
    ]
    return _render_table(headers, rows)


def markdown_table(result_file: ResultFile) -> str:
    """The same table as GitHub-flavoured Markdown."""
    headers = ["scenario", "store", "case", "metric", "point", "95% CI", "unit", "n"]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for r in result_file.results:
        cells = [r.scenario, r.store, r.case, r.metric, _point(r), _ci(r), r.unit, str(r.n)]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def compare(old: ResultFile, new: ResultFile, *, threshold_pct: float) -> tuple[str, bool]:
    """A console report diffing ``old`` and ``new``. Returns ``(report, has_regression)``.

    A case present in only one file is reported as ``NEW``/``REMOVED`` rather than
    compared. Everything else goes through :func:`benchmarks.stats.compare_points`,
    which is what keeps noisy-but-unchanged results from being flagged (see its
    docstring): the report shows every case, but only ``REGRESSED`` sets the exit code.
    """
    old_by_key = {r.key: r for r in old.results}
    new_by_key = {r.key: r for r in new.results}
    keys = sorted(set(old_by_key) | set(new_by_key))

    headers = ["scenario", "store", "case", "old", "new", "delta", "verdict"]
    rows: list[list[str]] = []
    any_regression = False
    for key in keys:
        old_result = old_by_key.get(key)
        new_result = new_by_key.get(key)
        if old_result is None:
            assert new_result is not None
            rows.append([*key, "-", _point(new_result), "-", "NEW"])
            continue
        if new_result is None:
            rows.append([*key, _point(old_result), "-", "-", "REMOVED"])
            continue
        higher_is_better = old_result.metric == "throughput"
        verdict = stats.compare_points(
            old_result.point,
            (old_result.ci_low, old_result.ci_high),
            new_result.point,
            (new_result.ci_low, new_result.ci_high),
            threshold_pct=threshold_pct,
            higher_is_better=higher_is_better,
        )
        if verdict.regressed:
            label = "REGRESSED"
            any_regression = True
        elif verdict.improved:
            label = "improved"
        else:
            label = "-"
        rows.append(
            [*key, _point(old_result), _point(new_result), f"{verdict.delta_pct:+.1f}%", label]
        )

    header_line = (
        f"comparing {len(old.results)} old result(s) with {len(new.results)} new "
        f"result(s), threshold {threshold_pct:g}%"
    )
    return f"{header_line}\n\n{_render_table(headers, rows)}", any_regression
