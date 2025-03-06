"""LW111 and LW112: nondeterminism from hash randomization.

Both rules trace back to the same root cause - ``PYTHONHASHSEED`` (and, for ``id()``,
the memory allocator) differs between processes - but they show up as different code
shapes, so :mod:`loomwork.lint.checker` implements them as two dedicated AST checks
rather than table entries. They live together in one module because they share that
cause and because fixing one often surfaces the other in the same function.
"""

from __future__ import annotations

from loomwork.lint.models import Severity
from loomwork.lint.registry import DEFAULT_REGISTRY, Rule

RULE_HASH_ORDER_ITERATION = DEFAULT_REGISTRY.register(
    Rule(
        code="LW111",
        name="hash-order-iteration",
        severity=Severity.WARNING,
        message="iterating a {name} has an order that depends on the process hash seed",
        hint="iterate sorted(...) of it instead, or use a list/tuple if order was never "
        "meaningful",
    )
)

RULE_HASH_IN_CONTROL_FLOW = DEFAULT_REGISTRY.register(
    Rule(
        code="LW112",
        name="hash-in-control-flow",
        severity=Severity.WARNING,
        message="{name}() is randomized per process and must not drive control flow",
        hint="branch on a stable value (an id field, an enum, an explicit key) instead",
    )
)
