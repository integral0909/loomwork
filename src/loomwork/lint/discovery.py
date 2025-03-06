"""Finding the ``.py`` files to lint.

Separate from :mod:`loomwork.lint.analyzer`, which finds the *workflows* inside a file
once it has been parsed. This module only deals with the filesystem: turning a mix of
file and directory paths into a deterministic, exclude-filtered list of files.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Iterable, Iterator
from pathlib import Path

from loomwork.lint.errors import LintUsageError


def is_excluded(path: Path, patterns: Iterable[str]) -> bool:
    """Whether ``path`` matches any of ``patterns`` (fnmatch-style, POSIX separators)."""
    as_posix = path.as_posix()
    for pattern in patterns:
        if fnmatch.fnmatch(as_posix, pattern) or fnmatch.fnmatch(path.name, pattern):
            return True
    return False


def iter_python_files(paths: Iterable[str | Path], exclude: Iterable[str] = ()) -> Iterator[Path]:
    """Yield every ``.py`` file under ``paths``, in a stable order.

    Raises:
        LintUsageError: a given path does not exist. This is a usage error (exit code 2)
            rather than a finding: the invocation itself is wrong, independent of what
            any workflow code does.
    """
    patterns = tuple(exclude)
    seen: set[Path] = set()
    for raw in paths:
        path = Path(raw)
        if not path.exists():
            raise LintUsageError(f"path does not exist: {path}")
        if path.is_file():
            candidates: Iterable[Path] = (path,) if path.suffix == ".py" else ()
        else:
            candidates = sorted(path.rglob("*.py"))
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved in seen:
                continue
            if is_excluded(candidate, patterns):
                continue
            seen.add(resolved)
            yield candidate
