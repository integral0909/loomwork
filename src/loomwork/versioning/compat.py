"""Checking new workflow code against recorded histories before deploying it.

Deploying workflow code that makes different decisions than the code that recorded a
running history breaks that run: its next tick fails with a nondeterminism error. The
compatibility checker replays recorded histories with the code about to be deployed and
reports every run that would break, with the event at which it diverges and the code
versions that wrote the history.

Typical use in CI::

    report = await check_compatibility(store, registry)
    if not report.ok:
        print(report.format_text())
        raise SystemExit(1)

Histories can also be checked from exported JSON files (``history_to_json``), which lets
teams keep "golden" histories in their repository and check every change against them
without database access.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Collection, Iterable
from dataclasses import dataclass, field
from pathlib import Path

from loomwork.codec.converter import DataConverter, default_converter
from loomwork.codec.transform import CodecBoundary
from loomwork.errors import LoomworkError, NondeterminismError
from loomwork.history.events import Event, TickCompleted, WorkflowStarted
from loomwork.history.serde import history_from_json
from loomwork.runtime.instance import WorkflowInstance
from loomwork.runtime.replayer import Replayer
from loomwork.sdk.definitions import Registry
from loomwork.store.base import Store
from loomwork.store.records import ExecutionStatus


@dataclass(frozen=True, slots=True)
class RunCheck:
    """The outcome of replaying one history."""

    run_id: str
    workflow_name: str
    status: str
    code_versions: tuple[str, ...]
    ok: bool
    error: str | None = None
    diverged_at_event: int | None = None
    nondeterministic: bool = False


@dataclass(slots=True)
class CompatibilityReport:
    checks: list[RunCheck] = field(default_factory=list)
    unknown_workflows: Counter[str] = field(default_factory=Counter)

    @property
    def ok(self) -> bool:
        return all(check.ok for check in self.checks)

    @property
    def failures(self) -> list[RunCheck]:
        return [check for check in self.checks if not check.ok]

    def summary(self) -> dict[str, tuple[int, int]]:
        """``{workflow_name: (compatible, incompatible)}``."""
        counts: dict[str, list[int]] = {}
        for check in self.checks:
            entry = counts.setdefault(check.workflow_name, [0, 0])
            entry[0 if check.ok else 1] += 1
        return {name: (counts[name][0], counts[name][1]) for name in sorted(counts)}

    def format_text(self) -> str:
        lines = []
        for name, (ok, bad) in self.summary().items():
            mark = "ok " if not bad else "FAIL"
            lines.append(f"[{mark}] {name}: {ok} compatible, {bad} incompatible")
        for check in self.failures:
            where = f" at event {check.diverged_at_event}" if check.diverged_at_event else ""
            versions = ", ".join(check.code_versions) or "unknown"
            lines.append(
                f"  run {check.run_id} ({check.status}, recorded by code {versions}){where}: "
                f"{check.error}"
            )
        for name, count in sorted(self.unknown_workflows.items()):
            lines.append(f"[skip] {name}: {count} run(s) of a workflow not in this registry")
        if not lines:
            lines.append("no histories to check")
        return "\n".join(lines)


def _instance_factory(registry: Registry, converter: DataConverter) -> object:
    def make(started: WorkflowStarted) -> WorkflowInstance:
        return WorkflowInstance(
            registry.workflow(started.workflow_name), started, converter=converter
        )

    return make


def check_history(
    events: Iterable[Event],
    registry: Registry,
    *,
    converter: DataConverter | None = None,
    status: str = "unknown",
) -> RunCheck:
    """Replay one (already decoded) history with the code in ``registry``."""
    history = list(events)
    started = history[0] if history else None
    if not isinstance(started, WorkflowStarted):
        return RunCheck("", "", status, (), ok=False, error="history does not start a workflow")
    versions = tuple(
        dict.fromkeys(
            e.code_version for e in history if isinstance(e, TickCompleted) and e.code_version
        )
    )
    replayer = Replayer(_instance_factory(registry, converter or default_converter))  # type: ignore[arg-type]
    try:
        replayer.feed(history)
        replayer.finish()
    except NondeterminismError as exc:
        return RunCheck(
            started.run_id,
            started.workflow_name,
            status,
            versions,
            ok=False,
            error=str(exc),
            diverged_at_event=replayer.last_event_id + 1,
            nondeterministic=True,
        )
    except LoomworkError as exc:
        return RunCheck(
            started.run_id, started.workflow_name, status, versions, ok=False, error=str(exc)
        )
    finally:
        replayer.close()
    return RunCheck(started.run_id, started.workflow_name, status, versions, ok=True)


def check_history_file(
    path: str | Path, registry: Registry, *, converter: DataConverter | None = None
) -> RunCheck:
    """Check an exported history document (see :func:`loomwork.history.history_to_json`)."""
    events = history_from_json(Path(path).read_text(encoding="utf-8"))
    return check_history(events, registry, converter=converter, status="file")


async def check_compatibility(
    store: Store,
    registry: Registry,
    *,
    converter: DataConverter | None = None,
    workflow_names: Collection[str] | None = None,
    running_limit: int = 1000,
    closed_limit: int = 50,
) -> CompatibilityReport:
    """Replay recent histories from ``store`` with the code in ``registry``.

    Running executions matter most (they will replay on the new code), so up to
    ``running_limit`` of them are checked, plus the ``closed_limit`` most recent closed
    runs of each workflow as a sample of the paths the code takes.
    """
    converter = converter or default_converter
    boundary = CodecBoundary(converter.codec)
    report = CompatibilityReport()
    names = set(workflow_names) if workflow_names is not None else None

    candidates = await store.list_executions(status=ExecutionStatus.RUNNING, limit=running_limit)
    closed_seen: Counter[str] = Counter()
    for record in await store.list_executions(limit=max(closed_limit * 20, 100)):
        if record.is_closed and closed_seen[record.workflow_name] < closed_limit:
            closed_seen[record.workflow_name] += 1
            candidates.append(record)

    for record in candidates:
        if names is not None and record.workflow_name not in names:
            continue
        if not registry.has_workflow(record.workflow_name):
            report.unknown_workflows[record.workflow_name] += 1
            continue
        history = await boundary.decode(await store.load_history(record.run_id))
        report.checks.append(
            check_history(history, registry, converter=converter, status=record.status.value)
        )
    return report
