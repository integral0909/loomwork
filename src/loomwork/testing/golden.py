"""Golden histories: pin a run's recorded history and check that future workflow code
still replays it identically.

A golden file is the same JSON document :func:`~loomwork.history.history_to_json`
produces, so it can be committed to a repository and checked in CI without a live
store::

    await record_golden(handle, "tests/golden/process_order_v1.json")
    ...
    # later, in CI, with no store or environment at all:
    assert_replays("tests/golden/process_order_v1.json", process_order)

:func:`replay_check` is the same check without a file: it replays a history fetched
directly from a handle (or an in-memory history), which is convenient inside a single
test that both runs a workflow and wants to check a *different* set of workflow
definitions (for example the "new" version of a workflow being migrated to) would have
handled its history the same way.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from loomwork.codec.converter import DataConverter
from loomwork.history.serde import history_to_json
from loomwork.sdk.definitions import Registry, WorkflowDefinition
from loomwork.testing.assertions import HistorySource, events_of
from loomwork.versioning.compat import RunCheck, check_history, check_history_file


def _describe_failure(check: RunCheck) -> str:
    where = f" at event {check.diverged_at_event}" if check.diverged_at_event else ""
    versions = ", ".join(check.code_versions) or "unknown"
    return (
        f"run {check.run_id} ({check.workflow_name}, recorded by code {versions}){where}: "
        f"{check.error}"
    )


async def record_golden(source: HistorySource, path: str | Path) -> Path:
    """Write ``source``'s history to ``path`` as a golden JSON document.

    Overwrites any file already there (creating parent directories as needed); commit
    the result like any other test fixture and re-record deliberately when a workflow's
    recorded shape is meant to change.
    """
    events = await events_of(source)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(history_to_json(events), encoding="utf-8")
    return target


def assert_replays(
    path: str | Path,
    *workflows: WorkflowDefinition[..., Any],
    converter: DataConverter | None = None,
) -> None:
    """Replay a golden history file against ``workflows`` and fail on divergence.

    Needs no live environment or store: it only reads ``path`` and runs the workflow
    code, so it is cheap to call for every golden file a project keeps, in a plain
    (synchronous) test.
    """
    registry = Registry(workflows, ())
    check = check_history_file(path, registry, converter=converter)
    if not check.ok:
        raise AssertionError(f"golden history {path} no longer replays: {_describe_failure(check)}")


async def replay_check(
    source: HistorySource,
    *workflows: WorkflowDefinition[..., Any],
    converter: DataConverter | None = None,
) -> None:
    """Replay a history (fetched from ``source`` if needed) from scratch against
    ``workflows`` and raise an :class:`AssertionError` if it would diverge.

    Unlike :func:`assert_replays`, this takes the history directly -- a handle, a
    :class:`~loomwork.testing.assertions.HistoryView`, or a plain event sequence --
    instead of a golden file, so it also works for a run that was never recorded to disk.
    """
    events = await events_of(source)
    registry = Registry(workflows, ())
    check = check_history(events, registry, converter=converter)
    if not check.ok:
        raise AssertionError(
            f"history would not replay on the given code: {_describe_failure(check)}"
        )
