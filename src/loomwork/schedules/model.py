"""Schedules: recurring workflow starts.

A schedule pairs a :class:`ScheduleSpec` (when) with a :class:`ScheduleAction` (which
workflow, with which arguments) and a :class:`SchedulePolicy` (what to do when a run is
still going, or fire times were missed while nothing was running).

The whole schedule is persisted as one versioned document (:class:`ScheduleState`).
Stores only index ``next_fire_at``; everything else is interpreted by the planner in
:mod:`loomwork.schedules.planner`.
"""

from __future__ import annotations

import enum
import hashlib
import re
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from loomwork.codec.payload import Payload
from loomwork.options import as_timedelta
from loomwork.schedules.cron import CronError, CronExpression, parse_cron

_ID_RE = re.compile(r"^[A-Za-z0-9_.:\-]{1,128}$")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MAX_RECENT_ACTIONS = 10


class ScheduleError(ValueError):
    """A schedule definition is invalid."""


def validate_schedule_id(schedule_id: str) -> str:
    if not _ID_RE.match(schedule_id):
        raise ScheduleError(
            f"invalid schedule id {schedule_id!r}: use 1-128 letters, digits and _ . : -"
        )
    return schedule_id


class OverlapPolicy(enum.StrEnum):
    """What happens when a fire time arrives while the previous run is still running."""

    SKIP = "skip"
    """Do not start a new run; count the fire time as skipped."""
    BUFFER_ONE = "buffer_one"
    """Start one run as soon as the running one closes (later fire times are skipped)."""
    ALLOW_ALL = "allow_all"
    """Start a new run regardless; runs may overlap."""
    CANCEL_OTHER = "cancel_other"
    """Ask the running run to cancel, then start the new one."""
    TERMINATE_OTHER = "terminate_other"
    """Terminate the running run, then start the new one."""


# --------------------------------------------------------------------------- spec


def _utc(value: datetime | None, name: str) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ScheduleError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True, init=False)
class ScheduleSpec:
    """When a schedule fires: a cron expression *or* a fixed interval.

    Attributes:
        cron: cron expression (see :mod:`loomwork.schedules.cron`).
        interval: fire every ``interval``, aligned to the Unix epoch plus ``offset``
            (so ``interval=1h, offset=15m`` fires at quarter past every hour).
        timezone: IANA zone the cron expression is evaluated in.
        start_at, end_at: optional bounds; no fire times outside them.
        jitter: spread each start by up to this much (deterministically per fire time),
            to avoid many schedules hitting a downstream system at the same second.
    """

    cron: str | None
    interval: timedelta | None
    offset: timedelta
    timezone: str
    start_at: datetime | None
    end_at: datetime | None
    jitter: timedelta

    def __init__(
        self,
        *,
        cron: str | None = None,
        interval: timedelta | float | None = None,
        offset: timedelta | float = 0,
        timezone: str = "UTC",
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        jitter: timedelta | float = 0,
    ) -> None:
        if (cron is None) == (interval is None):
            raise ScheduleError("a schedule needs exactly one of cron and interval")
        interval_td = as_timedelta(interval)
        offset_td = as_timedelta(offset) or timedelta(0)
        jitter_td = as_timedelta(jitter) or timedelta(0)
        if interval_td is not None and interval_td < timedelta(seconds=1):
            raise ScheduleError("interval must be at least one second")
        if interval_td is not None and not timedelta(0) <= offset_td < interval_td:
            raise ScheduleError("offset must be in [0, interval)")
        if jitter_td < timedelta(0):
            raise ScheduleError("jitter must not be negative")
        start = _utc(start_at, "start_at")
        end = _utc(end_at, "end_at")
        if start and end and end <= start:
            raise ScheduleError("end_at must be after start_at")
        if cron is not None:
            try:
                parse_cron(cron, timezone)
            except CronError as exc:
                raise ScheduleError(str(exc)) from exc
        object.__setattr__(self, "cron", cron)
        object.__setattr__(self, "interval", interval_td)
        object.__setattr__(self, "offset", offset_td)
        object.__setattr__(self, "timezone", timezone)
        object.__setattr__(self, "start_at", start)
        object.__setattr__(self, "end_at", end)
        object.__setattr__(self, "jitter", jitter_td)

    def _expression(self) -> CronExpression:
        assert self.cron is not None
        return parse_cron(self.cron, self.timezone)

    def next_after(self, moment: datetime) -> datetime | None:
        """First nominal fire time strictly after ``moment``, or ``None`` if the
        schedule has ended."""
        if self.start_at is not None and moment < self.start_at:
            moment = self.start_at - timedelta(microseconds=1)
        if self.interval is not None:
            step = self.interval.total_seconds()
            elapsed = (moment - _EPOCH - self.offset).total_seconds()
            index = int(elapsed // step) + 1
            candidate = _EPOCH + self.offset + timedelta(seconds=index * step)
        else:
            try:
                candidate = self._expression().next_after(moment)
            except CronError:
                return None
        if self.end_at is not None and candidate > self.end_at:
            return None
        return candidate

    def fire_times(self, after: datetime, until: datetime, limit: int = 1000) -> list[datetime]:
        """Nominal fire times in ``(after, until]``, at most ``limit``."""
        out: list[datetime] = []
        current = after
        while len(out) < limit:
            nxt = self.next_after(current)
            if nxt is None or nxt > until:
                break
            out.append(nxt)
            current = nxt
        return out

    def jitter_for(self, schedule_id: str, nominal: datetime) -> timedelta:
        """A stable delay in ``[0, jitter)`` for one fire time."""
        if not self.jitter:
            return timedelta(0)
        digest = hashlib.sha256(f"{schedule_id}|{nominal.isoformat()}".encode()).digest()
        fraction = int.from_bytes(digest[:8], "big") / 2**64
        return timedelta(seconds=self.jitter.total_seconds() * fraction)

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"timezone": self.timezone}
        if self.cron is not None:
            data["cron"] = self.cron
        if self.interval is not None:
            data["interval"] = self.interval.total_seconds()
            data["offset"] = self.offset.total_seconds()
        if self.start_at is not None:
            data["start_at"] = self.start_at.isoformat()
        if self.end_at is not None:
            data["end_at"] = self.end_at.isoformat()
        if self.jitter:
            data["jitter"] = self.jitter.total_seconds()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ScheduleSpec:
        return cls(
            cron=data.get("cron"),
            interval=data.get("interval"),
            offset=data.get("offset", 0),
            timezone=data.get("timezone", "UTC"),
            start_at=datetime.fromisoformat(data["start_at"]) if "start_at" in data else None,
            end_at=datetime.fromisoformat(data["end_at"]) if "end_at" in data else None,
            jitter=data.get("jitter", 0),
        )

    def describe(self) -> str:
        if self.cron is not None:
            zone = "" if self.timezone == "UTC" else f" ({self.timezone})"
            return f"cron {self.cron!r}{zone}"
        assert self.interval is not None
        return f"every {self.interval}" + (f" + {self.offset}" if self.offset else "")


# --------------------------------------------------------------------------- action / policy


@dataclass(frozen=True, slots=True)
class ScheduleAction:
    """Start ``workflow_name`` with ``args`` (already-encoded payloads)."""

    workflow_name: str
    args: list[Payload] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"workflow_name": self.workflow_name, "args": [p.to_dict() for p in self.args]}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ScheduleAction:
        return cls(
            workflow_name=str(data["workflow_name"]),
            args=[Payload.from_dict(p) for p in data.get("args", [])],
        )


@dataclass(frozen=True, slots=True, init=False)
class SchedulePolicy:
    """How a schedule reacts to overlaps and outages.

    Attributes:
        overlap: see :class:`OverlapPolicy`.
        catchup_window: after an outage, fire times older than this are skipped instead
            of being started late.
        pause_on_failure: pause the schedule when one of its runs fails.
    """

    overlap: OverlapPolicy
    catchup_window: timedelta
    pause_on_failure: bool

    def __init__(
        self,
        overlap: OverlapPolicy | str = OverlapPolicy.SKIP,
        catchup_window: timedelta | float = timedelta(minutes=1),
        pause_on_failure: bool = False,
    ) -> None:
        window = as_timedelta(catchup_window) or timedelta(0)
        if window < timedelta(0):
            raise ScheduleError("catchup_window must not be negative")
        object.__setattr__(self, "overlap", OverlapPolicy(overlap))
        object.__setattr__(self, "catchup_window", window)
        object.__setattr__(self, "pause_on_failure", pause_on_failure)

    def to_dict(self) -> dict[str, Any]:
        return {
            "overlap": self.overlap.value,
            "catchup_window": self.catchup_window.total_seconds(),
            "pause_on_failure": self.pause_on_failure,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SchedulePolicy:
        return cls(
            overlap=data.get("overlap", OverlapPolicy.SKIP),
            catchup_window=data.get("catchup_window", 60),
            pause_on_failure=bool(data.get("pause_on_failure", False)),
        )


# --------------------------------------------------------------------------- state


@dataclass(frozen=True, slots=True)
class ScheduledRun:
    """One action a schedule took."""

    nominal_time: datetime
    started_at: datetime
    workflow_id: str
    run_id: str
    status: str = "running"

    def to_dict(self) -> dict[str, Any]:
        return {
            "nominal_time": self.nominal_time.isoformat(),
            "started_at": self.started_at.isoformat(),
            "workflow_id": self.workflow_id,
            "run_id": self.run_id,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ScheduledRun:
        return cls(
            nominal_time=datetime.fromisoformat(data["nominal_time"]),
            started_at=datetime.fromisoformat(data["started_at"]),
            workflow_id=str(data["workflow_id"]),
            run_id=str(data["run_id"]),
            status=str(data.get("status", "running")),
        )


@dataclass(frozen=True, slots=True)
class ScheduleState:
    """The persisted document describing one schedule and its progress.

    Attributes:
        next_fire_at: when the scheduler should next look at this schedule (the next
            nominal fire time plus jitter); ``None`` once the schedule has ended.
        last_evaluated: nominal fire times up to here have been handled.
        buffered: a fire time waiting for the running run to close (``BUFFER_ONE``).
        running: runs started by this schedule that have not closed yet.
        recent: the last few actions, newest last.
        version: incremented on every write; used for optimistic concurrency.
    """

    schedule_id: str
    spec: ScheduleSpec
    action: ScheduleAction
    policy: SchedulePolicy = field(default_factory=SchedulePolicy)
    paused: bool = False
    note: str = ""
    created_at: datetime = _EPOCH
    updated_at: datetime = _EPOCH
    next_fire_at: datetime | None = None
    last_evaluated: datetime | None = None
    buffered: datetime | None = None
    running: tuple[ScheduledRun, ...] = ()
    recent: tuple[ScheduledRun, ...] = ()
    total_started: int = 0
    total_skipped: int = 0
    version: int = 0

    def with_(self, **changes: Any) -> ScheduleState:
        return replace(self, **changes)

    def to_dict(self) -> dict[str, Any]:
        def ts(value: datetime | None) -> str | None:
            return value.isoformat() if value is not None else None

        return {
            "schedule_id": self.schedule_id,
            "spec": self.spec.to_dict(),
            "action": self.action.to_dict(),
            "policy": self.policy.to_dict(),
            "paused": self.paused,
            "note": self.note,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "next_fire_at": ts(self.next_fire_at),
            "last_evaluated": ts(self.last_evaluated),
            "buffered": ts(self.buffered),
            "running": [r.to_dict() for r in self.running],
            "recent": [r.to_dict() for r in self.recent],
            "total_started": self.total_started,
            "total_skipped": self.total_skipped,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ScheduleState:
        def ts(value: str | None) -> datetime | None:
            return datetime.fromisoformat(value) if value else None

        return cls(
            schedule_id=str(data["schedule_id"]),
            spec=ScheduleSpec.from_dict(data["spec"]),
            action=ScheduleAction.from_dict(data["action"]),
            policy=SchedulePolicy.from_dict(data.get("policy", {})),
            paused=bool(data.get("paused", False)),
            note=str(data.get("note", "")),
            created_at=ts(data.get("created_at")) or _EPOCH,
            updated_at=ts(data.get("updated_at")) or _EPOCH,
            next_fire_at=ts(data.get("next_fire_at")),
            last_evaluated=ts(data.get("last_evaluated")),
            buffered=ts(data.get("buffered")),
            running=tuple(ScheduledRun.from_dict(r) for r in data.get("running", [])),
            recent=tuple(ScheduledRun.from_dict(r) for r in data.get("recent", [])),
            total_started=int(data.get("total_started", 0)),
            total_skipped=int(data.get("total_skipped", 0)),
            version=int(data.get("version", 0)),
        )


def scheduled_workflow_id(schedule_id: str, nominal: datetime) -> str:
    """Workflow id of the run started for ``nominal``; stable, so a retried firing
    after a crash finds the run it already started instead of starting another."""
    return f"{schedule_id}@{nominal.astimezone(UTC):%Y-%m-%dT%H:%M:%SZ}"
