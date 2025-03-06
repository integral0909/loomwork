"""The forbidden-call table: the declarative half of the rule set.

Most determinism rules boil down to "calling this dotted name is forbidden", which does
not need a bespoke AST visitor per rule - it needs one row in a table. Each
:class:`ForbiddenCall` says which dotted path it matches (and how) and which
:class:`~loomwork.lint.registry.Rule` to report when it does. The checker
(:mod:`loomwork.lint.checker`) resolves every call it sees to a dotted path once and
matches it against the combined table from all rule modules plus any
``extend-forbidden-calls`` entries from configuration.

A handful of rules cannot be expressed this way (non-deterministic iteration order,
``id()``/``hash()`` in a condition, ``global`` assignment) because they are about
*shape*, not about which name was called; those are plain AST checks in
:mod:`loomwork.lint.checker`.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class MatchKind(Enum):
    """How a :class:`ForbiddenCall` compares against a resolved dotted path."""

    #: The resolved path must equal ``dotted`` exactly (``time.sleep``).
    EXACT = "exact"
    #: The resolved path must equal ``dotted`` or start with ``dotted + "."``
    #: (``socket`` also matches ``socket.socket.connect``).
    PREFIX = "prefix"
    #: The resolved path must end with ``"." + dotted`` (``run_in_executor`` matches
    #: ``asyncio.get_event_loop().run_in_executor`` even though the base is opaque).
    SUFFIX = "suffix"


@dataclass(slots=True, frozen=True)
class ForbiddenCall:
    """One entry in the forbidden-call table."""

    dotted: str
    code: str
    kind: MatchKind = MatchKind.EXACT

    def matches(self, resolved: str) -> bool:
        if self.kind is MatchKind.EXACT:
            return resolved == self.dotted
        if self.kind is MatchKind.PREFIX:
            return resolved == self.dotted or resolved.startswith(self.dotted + ".")
        return resolved == self.dotted or resolved.endswith("." + self.dotted)
