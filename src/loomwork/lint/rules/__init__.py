"""All built-in rules, aggregated for the analyzer.

Importing this package imports every rule module, which registers each :class:`Rule`
into :data:`loomwork.lint.registry.DEFAULT_REGISTRY` as a side effect. It also exposes
the combined forbidden-call table (:data:`FORBIDDEN_CALLS`) that
:mod:`loomwork.lint.checker` matches every call against.

To add a new forbidden-call rule: create (or extend) a module here with a ``Rule`` and a
tuple of ``ForbiddenCall`` entries, then add that tuple to :data:`FORBIDDEN_CALLS` below.
To add a new structural rule (like LW111/LW112/LW113): register the ``Rule`` here and
implement the check as a visitor method in :mod:`loomwork.lint.checker`.
"""

from __future__ import annotations

from loomwork.lint.registry import DEFAULT_REGISTRY
from loomwork.lint.rules import (
    concurrency_rules,
    environment_rules,
    io_rules,
    iteration_rules,
    meta_rules,
    random_rules,
    state_rules,
    time_rules,
)
from loomwork.lint.rules.concurrency_rules import OPAQUE_RECEIVER_METHODS
from loomwork.lint.rules.environment_rules import RULE_ENVIRONMENT_ACCESS
from loomwork.lint.rules.iteration_rules import RULE_HASH_IN_CONTROL_FLOW, RULE_HASH_ORDER_ITERATION
from loomwork.lint.rules.meta_rules import RULE_CUSTOM_FORBIDDEN_CALL, RULE_PARSE_ERROR
from loomwork.lint.rules.state_rules import RULE_GLOBAL_MUTATION
from loomwork.lint.rules.tables import ForbiddenCall

#: Every declarative forbidden-call rule, combined. Order does not matter for matching
#: (the checker tries all of them), but it is kept in the same order as the bullet list
#: in the module docstrings for readability.
FORBIDDEN_CALLS: tuple[ForbiddenCall, ...] = (
    *time_rules.FORBIDDEN_CALLS,
    *random_rules.FORBIDDEN_CALLS,
    *io_rules.FORBIDDEN_CALLS,
    *concurrency_rules.FORBIDDEN_CALLS,
    *environment_rules.FORBIDDEN_CALLS,
)

__all__ = [
    "DEFAULT_REGISTRY",
    "FORBIDDEN_CALLS",
    "OPAQUE_RECEIVER_METHODS",
    "RULE_CUSTOM_FORBIDDEN_CALL",
    "RULE_ENVIRONMENT_ACCESS",
    "RULE_GLOBAL_MUTATION",
    "RULE_HASH_IN_CONTROL_FLOW",
    "RULE_HASH_ORDER_ITERATION",
    "RULE_PARSE_ERROR",
    "meta_rules",
    "state_rules",
    "iteration_rules",
]
