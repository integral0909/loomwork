"""Correlating ordinary log records with the workflow or step that emitted them.

``loomwork.logger`` already tags its own records with run identity (see
:mod:`loomwork.sdk.api`). Application code inside a step, or a library it calls, usually
logs through its own ``logging.getLogger(__name__)`` instead, and those records get none
of that context. :class:`CorrelationFilter` closes the gap: attach it to a handler (or the
root logger) and every record passing through it while a workflow or step is running
gains the same fields ``loomwork.logger`` would have added, including the propagated
trace ids, so a log aggregator can group everything from one run or one step attempt
regardless of which logger produced it.
"""

from __future__ import annotations

import logging

from loomwork.sdk.api import correlation_fields


class CorrelationFilter(logging.Filter):
    """Stamps ``workflow_id``, ``run_id``, ``step_name``, ``step_attempt``, ``trace_id``
    and ``span_id`` onto records emitted from workflow or step code.

    A filter never drops a record here (it always returns ``True``); it only adds
    attributes, so attaching it is safe on any handler::

        handler.addFilter(CorrelationFilter())

    Fields that do not apply (for example ``step_name`` inside a workflow, or any of
    them outside a run) are left unset rather than set to ``None``, so a formatter
    referencing ``%(run_id)s`` needs a ``logging.Formatter(..., defaults={"run_id": ""})``
    or similar if it also has to handle records from outside Loomwork.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in correlation_fields().items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


__all__ = ["CorrelationFilter"]
