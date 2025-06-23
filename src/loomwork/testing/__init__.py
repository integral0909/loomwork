"""Testkit for Loomwork workflows.

:class:`WorkflowEnvironment` gives tests an in-memory, deterministic place to run
workflows that also skips durable waits automatically, so a workflow sleeping for
thirty days, or a schedule firing daily for a year, completes in milliseconds::

    from loomwork.testing import WorkflowEnvironment

    async def test_process_order() -> None:
        async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card]) as env:
            env.mock_step(charge_card, return_value="ch_123")
            receipt = await env.execute(process_order, an_order)
            assert receipt.charge_id == "ch_123"

See ``docs/testing.md`` for the full guide, including assertions over recorded
histories, golden-history regression tests, and the optional pytest fixtures in
:mod:`loomwork.testing.pytest_plugin`.
"""

from __future__ import annotations

from loomwork.testing.assertions import HistoryAssertion, HistorySource, HistoryView, assert_history
from loomwork.testing.environment import TESTING_CONFIG, WorkflowEnvironment
from loomwork.testing.golden import assert_replays, record_golden, replay_check
from loomwork.testing.mocks import StepCall, StepMock

__all__ = [
    "TESTING_CONFIG",
    "HistoryAssertion",
    "HistorySource",
    "HistoryView",
    "StepCall",
    "StepMock",
    "WorkflowEnvironment",
    "assert_history",
    "assert_replays",
    "record_golden",
    "replay_check",
]
