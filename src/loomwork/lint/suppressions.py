"""Inline suppression comments.

A finding can be silenced the same way most Python linters do it: a trailing comment on
the offending line. ``# loomwork: ignore`` silences every rule on that line;
``# loomwork: ignore[LW101]`` (or a comma-separated list) silences only those codes.
``# loomwork: ignore-file`` anywhere in the file silences the whole file - useful for
generated code or a file that is not actually workflow code despite living next to some.

Comments are found with :mod:`tokenize` rather than a line-based regex so that a
``#`` inside a string literal is never mistaken for a suppression directive.
"""

from __future__ import annotations

import io
import re
import tokenize
from dataclasses import dataclass, field

_IGNORE_RE = re.compile(r"#\s*loomwork\s*:\s*ignore\b(?:\[(?P<codes>[^\]]*)\])?")
_IGNORE_FILE_RE = re.compile(r"#\s*loomwork\s*:\s*ignore-file\b")
_CODE_SPLIT_RE = re.compile(r"[,\s]+")


@dataclass(slots=True)
class Suppressions:
    """Suppression directives found in one file."""

    ignore_file: bool = False
    #: Line number -> codes to ignore on that line, or ``None`` to ignore everything.
    lines: dict[int, frozenset[str] | None] = field(default_factory=dict)

    def suppresses(self, code: str, line: int) -> bool:
        if self.ignore_file:
            return True
        codes = self.lines.get(line, ())
        if codes is None:
            return True
        return code in codes


def collect_suppressions(source: str) -> Suppressions:
    """Scan ``source`` for ``# loomwork: ignore[...]`` / ``ignore-file`` comments."""
    result = Suppressions()
    try:
        tokens = tokenize.generate_tokens(io.StringIO(source).readline)
        for token in tokens:
            if token.type != tokenize.COMMENT:
                continue
            _record_comment(result, token.string, token.start[0])
    except (tokenize.TokenError, SyntaxError, IndentationError):
        # The caller also tries to ast.parse() the same source and will report a proper
        # syntax-error finding; suppressions just do not apply to an unparsable file.
        pass
    return result


def _record_comment(result: Suppressions, comment: str, line: int) -> None:
    if _IGNORE_FILE_RE.search(comment):
        result.ignore_file = True
        return
    match = _IGNORE_RE.search(comment)
    if match is None:
        return
    codes_text = match.group("codes")
    if codes_text is None:
        result.lines[line] = None
        return
    codes = frozenset(c for c in _CODE_SPLIT_RE.split(codes_text.strip()) if c)
    if line in result.lines and result.lines[line] is None:
        return  # a bare "ignore" on this line already silences everything
    existing = result.lines.get(line)
    result.lines[line] = (existing | codes) if existing else codes
