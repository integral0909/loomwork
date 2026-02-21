"""Keeps the documentation honest as the code and the files around it change.

- Every relative link in a Markdown file points at a file that exists, and every
  ``#anchor`` matches a heading in its target (GitHub's slug rules).
- Every page under ``docs/`` is reachable from ``docs/index.md``.
- Every ``python`` code block at least parses (top-level ``await`` allowed, since many
  snippets show calls made from async code).
"""

from __future__ import annotations

import ast
import re
import textwrap
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
DOCS = REPO / "docs"

#: Markdown files that are published documentation (not generated or vendored).
MARKDOWN_FILES = sorted(
    [
        *REPO.glob("*.md"),
        *DOCS.rglob("*.md"),
        *(REPO / "examples").rglob("*.md"),
        *(REPO / "deploy").rglob("*.md"),
        *(REPO / "benchmarks").rglob("*.md"),
    ]
)

_LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
_FENCE = re.compile(r"^(```+|~~~+)(\w*)[^\n]*\n(.*?)^\1\s*$", re.MULTILINE | re.DOTALL)
_HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*#*\s*$", re.MULTILINE)


def _without_code(text: str) -> str:
    """The text with fenced code blocks and inline code removed (links inside them are
    examples, not links)."""
    text = _FENCE.sub("", text)
    return re.sub(r"`[^`\n]*`", "", text)


def _slug(heading: str) -> str:
    """GitHub's anchor for a heading: lower case, punctuation dropped, spaces to dashes."""
    text = re.sub(r"`([^`]*)`", r"\1", heading)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = text.strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def _anchors(path: Path) -> set[str]:
    anchors: set[str] = set()
    counts: dict[str, int] = {}
    for heading in _HEADING.findall(_FENCE.sub("", path.read_text(encoding="utf-8"))):
        slug = _slug(heading)
        n = counts.get(slug, 0)
        anchors.add(slug if n == 0 else f"{slug}-{n}")
        counts[slug] = n + 1
    return anchors


def _links(path: Path) -> list[str]:
    return _LINK.findall(_without_code(path.read_text(encoding="utf-8")))


def _rel(path: Path) -> str:
    return str(path.relative_to(REPO))


@pytest.mark.parametrize("path", MARKDOWN_FILES, ids=_rel)
def test_relative_links_resolve(path: Path) -> None:
    broken = []
    for target in _links(path):
        if re.match(r"^[a-z][a-z0-9+.-]*:", target) or target.startswith("//"):
            continue  # external (https:, mailto:, ...)
        file_part, _, anchor = target.partition("#")
        destination = (path.parent / file_part).resolve() if file_part else path
        if not destination.exists():
            broken.append(f"{target}: no such file")
            continue
        if anchor and destination.suffix == ".md" and anchor not in _anchors(destination):
            broken.append(f"{target}: no heading for #{anchor}")
    assert broken == []


def test_every_docs_page_is_reachable_from_the_index() -> None:
    seen: set[Path] = set()
    pending = [DOCS / "index.md"]
    while pending:
        page = pending.pop()
        if page in seen:
            continue
        seen.add(page)
        for target in _links(page):
            file_part = target.partition("#")[0]
            if not file_part or re.match(r"^[a-z][a-z0-9+.-]*:", file_part):
                continue
            destination = (page.parent / file_part).resolve()
            if destination.suffix == ".md" and destination.is_relative_to(DOCS):
                pending.append(destination)
    orphans = sorted(_rel(p) for p in DOCS.rglob("*.md") if p.resolve() not in seen)
    assert orphans == []


def _python_blocks() -> list[tuple[str, int, str]]:
    blocks = []
    for path in MARKDOWN_FILES:
        text = path.read_text(encoding="utf-8")
        for match in _FENCE.finditer(text):
            if match.group(2) in {"python", "py"}:
                line = text.count("\n", 0, match.start()) + 2
                blocks.append((_rel(path), line, match.group(3)))
    return blocks


@pytest.mark.parametrize(
    ("path", "line", "code"),
    _python_blocks(),
    ids=lambda v: str(v)[:60] if isinstance(v, str) else str(v),
)
def test_python_snippets_parse(path: str, line: int, code: str) -> None:
    try:
        compile(
            textwrap.dedent(code),  # fragments taken from inside a function are indented
            f"{path}:{line}",
            "exec",
            flags=ast.PyCF_ONLY_AST | ast.PyCF_ALLOW_TOP_LEVEL_AWAIT,
        )
    except SyntaxError as exc:
        pytest.fail(f"{path}:{line + (exc.lineno or 1) - 1}: {exc.msg}")


def test_slug_rules() -> None:
    assert _slug("`loomwork worker`") == "loomwork-worker"
    assert _slug("Id reuse and conflict policies for children") == (
        "id-reuse-and-conflict-policies-for-children"
    )
    assert _slug("What's new? (2024)") == "whats-new-2024"
