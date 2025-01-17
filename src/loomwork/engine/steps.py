"""Executing step jobs.

For every claimed job the :class:`StepExecutor`:

1. sets the lease to cover one heartbeat interval (or the whole attempt),
2. refuses to start if the attempt budget or total timeout is already exhausted (this
   happens when a previous attempt was abandoned by a crashed worker),
3. runs the step as its own task, with the :func:`loomwork.step_info` and heartbeat
   context, under the attempt timeout and, if configured, a heartbeat watchdog,
4. reports success, schedules a retry with backoff, or reports the final failure as a
   ``StepError`` whose cause is the last attempt's error.

An attempt task can end in three ways besides returning or raising:

* **cancelled by the workflow** (a heartbeat learned that the result is no longer
  wanted): the job is released and deleted, no event is recorded;
* **heartbeat timeout**: treated as a retryable failure;
* **worker shutdown**: the job is released for immediate re-claim and the attempt is
  not counted, because it was interrupted by us rather than failed by the step.
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import inspect
import logging
import random
from collections.abc import Collection
from datetime import datetime, timedelta
from typing import Any

from loomwork.clock import Clock
from loomwork.codec.converter import DataConverter
from loomwork.engine.config import EngineConfig
from loomwork.failure import FailureInfo
from loomwork.history.events import StepCompleted, StepFailed
from loomwork.info import StepInfo
from loomwork.retry import RetryPolicy
from loomwork.sdk.api import _reset_attempt, _reset_step_info, _set_attempt, _set_step_info
from loomwork.sdk.definitions import Registry
from loomwork.store.base import Store
from loomwork.store.records import StepJob

logger = logging.getLogger("loomwork.engine.steps")

ABANDONED_TYPE = "StepAttemptAbandoned"
TIMEOUT_TYPE = "StepTimeout"
TOTAL_TIMEOUT_TYPE = "StepTotalTimeout"
HEARTBEAT_TIMEOUT_TYPE = "StepHeartbeatTimeout"

_NO_HINT: Any = inspect.Parameter.empty
# Without a heartbeat timeout, heartbeats are still sent (to carry details and learn
# about cancellation), at most once per this many seconds.
_DEFAULT_HEARTBEAT_THROTTLE = 10.0


class _Interruption(enum.Enum):
    CANCELLED_BY_WORKFLOW = "cancelled"
    HEARTBEAT_TIMEOUT = "heartbeat-timeout"
    LEASE_LOST = "lease-lost"


class _Attempt:
    """Heartbeat bookkeeping for one running attempt (the SDK's ``AttemptContext``)."""

    def __init__(self, executor: StepExecutor, job: StepJob, lease: timedelta) -> None:
        self._executor = executor
        self._job = job
        self._lease = lease
        self._loop = asyncio.get_running_loop()
        self.last_beat = self._loop.time()
        self._last_sent = float("-inf")
        heartbeat_timeout = job.options.heartbeat_timeout
        self._throttle = (
            heartbeat_timeout.total_seconds() / 3
            if heartbeat_timeout
            else _DEFAULT_HEARTBEAT_THROTTLE
        )
        self._pending_details: Any = None
        self._has_pending = False
        self.task: asyncio.Task[Any] | None = None
        self.interruption: _Interruption | None = None
        self.heartbeats_sent = 0

    async def heartbeat(self, details: Any) -> None:
        self.last_beat = self._loop.time()
        if details is not None:
            self._pending_details = details
            self._has_pending = True
        if self.last_beat - self._last_sent < self._throttle:
            return
        await self.flush()

    async def flush(self) -> None:
        executor = self._executor
        payload = None
        if self._has_pending:
            payload = executor.converter.encode(self._pending_details)
            self._has_pending = False
        self._last_sent = self._loop.time()
        now = executor.clock.now()
        result = await executor.store.heartbeat_step(
            self._job.job_id, executor.worker_id, now + self._lease, payload, now
        )
        self.heartbeats_sent += 1
        if result is None:
            self.interrupt(_Interruption.LEASE_LOST)
        elif result.cancel_requested:
            self.interrupt(_Interruption.CANCELLED_BY_WORKFLOW)

    @property
    def has_pending_details(self) -> bool:
        return self._has_pending

    def previous_details(self, hint: Any) -> Any:
        details = self._job.heartbeat_details
        if details is None:
            return None
        return self._executor.converter.decode(details, _NO_HINT if hint is Any else hint)

    def interrupt(self, reason: _Interruption) -> None:
        if self.interruption is None:
            self.interruption = reason
            if self.task is not None and not self.task.done():
                self.task.cancel()


class StepExecutor:
    """Claims and runs step jobs for the steps in ``registry``."""

    def __init__(
        self,
        store: Store,
        registry: Registry,
        *,
        clock: Clock,
        converter: DataConverter,
        config: EngineConfig,
        worker_id: str,
    ) -> None:
        self.store = store
        self.registry = registry
        self.clock = clock
        self.converter = converter
        self.config = config
        self.worker_id = worker_id
        self._rng = random.Random()
        self.attempts_started = 0
        self.steps_completed = 0
        self.steps_failed = 0
        self.retries_scheduled = 0
        self.attempts_released = 0

    async def claim(self, queues: Collection[str] | None = None) -> StepJob | None:
        names = self.registry.step_names
        if not names:
            return None
        return await self.store.claim_step(
            self.worker_id,
            self.clock.now(),
            self.config.step_lease(self.config.default_step_options),
            names,
            queues,
        )

    async def run_once(self, queues: Collection[str] | None = None) -> bool:
        """Execute at most one step attempt. Returns whether a job was claimed."""
        job = await self.claim(queues)
        if job is None:
            return False
        await self.execute(job)
        return True

    async def execute(self, job: StepJob) -> None:
        now = self.clock.now()
        policy = job.options.retry or self.config.default_step_retry
        lease = self.config.step_lease(job.options)
        if not await self.store.renew_step_lease(job.job_id, self.worker_id, now + lease):
            logger.info("lost lease on step job %s before starting", job.job_id)
            return

        refusal = self._refuse_reason(job, policy, now)
        if refusal is not None:
            await self._finish_failed(job, refusal, now)
            return

        timeout = job.options.timeout or self.config.default_step_timeout
        if job.deadline is not None:
            timeout = min(timeout, job.deadline - now)

        self.attempts_started += 1
        attempt = _Attempt(self, job, lease)
        try:
            outcome = await self._run_attempt(job, attempt, timeout)
        except asyncio.CancelledError:
            # The worker is shutting down: hand the job back without counting the
            # attempt, so another worker can start it immediately.
            await self._release(job, consume_attempt=False)
            raise
        finished_at = self.clock.now()

        if attempt.interruption is _Interruption.CANCELLED_BY_WORKFLOW:
            await self._release(job, consume_attempt=True)
            return
        if attempt.interruption is _Interruption.LEASE_LOST:
            logger.warning(
                "step %s (run %s, seq %d) lost its lease mid-attempt; result discarded",
                job.step_name,
                job.run_id,
                job.seq,
            )
            return
        if isinstance(outcome, StepCompleted):
            if await self.store.finish_step(job.job_id, self.worker_id, outcome, finished_at):
                self.steps_completed += 1
            return

        failure = outcome
        if attempt.has_pending_details:
            # Heartbeats are throttled; persist the latest checkpoint so the next
            # attempt resumes from it rather than from an older one.
            with contextlib.suppress(Exception):
                await attempt.flush()
        if policy.should_retry(job.attempt, failure):
            ready_at = finished_at + policy.delay_for(job.attempt, self._rng)
            if job.deadline is None or ready_at < job.deadline:
                self.retries_scheduled += 1
                logger.info(
                    "step %s (run %s, seq %d) attempt %d failed, retrying at %s: %s",
                    job.step_name,
                    job.run_id,
                    job.seq,
                    job.attempt,
                    ready_at.isoformat(),
                    failure.message,
                )
                await self.store.retry_step(
                    job.job_id, self.worker_id, ready_at, failure, finished_at
                )
                return
            failure = FailureInfo(
                message=(
                    f"total timeout would be exceeded before the next attempt: {failure.message}"
                ),
                type=TOTAL_TIMEOUT_TYPE,
                kind="timeout",
                cause=failure,
            )
        await self._finish_failed(job, failure, finished_at)

    async def _release(self, job: StepJob, *, consume_attempt: bool) -> None:
        # Shielded: this must complete even while the worker is being cancelled.
        release = asyncio.ensure_future(
            self.store.release_step(
                job.job_id, self.worker_id, self.clock.now(), consume_attempt=consume_attempt
            )
        )
        try:
            if await asyncio.shield(release):
                self.attempts_released += 1
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await release
            raise
        except Exception:
            logger.warning("could not release step job %s", job.job_id, exc_info=True)

    def _refuse_reason(
        self, job: StepJob, policy: RetryPolicy, now: datetime
    ) -> FailureInfo | None:
        if job.deadline is not None and now >= job.deadline:
            return FailureInfo(
                message=f"step exceeded its total timeout of {job.options.total_timeout}",
                type=TOTAL_TIMEOUT_TYPE,
                kind="timeout",
                cause=job.last_failure,
            )
        if policy.max_attempts and job.attempt > policy.max_attempts:
            # Only reachable when the previous attempt was abandoned (the worker died
            # or lost its lease), since reported failures are checked before retrying.
            return FailureInfo(
                message=(
                    f"attempt {job.attempt - 1} was abandoned and the retry policy allows "
                    f"at most {policy.max_attempts} attempts"
                ),
                type=ABANDONED_TYPE,
                kind="timeout",
                cause=job.last_failure,
            )
        return None

    async def _run_attempt(
        self, job: StepJob, attempt: _Attempt, timeout: timedelta
    ) -> StepCompleted | FailureInfo:
        definition = self.registry.step(job.step_name)
        info = StepInfo(
            workflow_id=job.workflow_id,
            run_id=job.run_id,
            workflow_name=job.workflow_name,
            step_name=job.step_name,
            seq=job.seq,
            attempt=job.attempt,
            scheduled_at=job.scheduled_at,
        )
        try:
            args = self.converter.decode_args(definition.fn, job.args)
        except Exception as exc:
            return FailureInfo.from_exception(exc)

        # The attempt runs as its own task (which copies these context variables) so
        # that it can be cancelled without cancelling the executor itself.
        info_token = _set_step_info(info)
        attempt_token = _set_attempt(attempt)
        try:
            task: asyncio.Task[Any] = asyncio.ensure_future(definition.invoke(args))
        finally:
            _reset_attempt(attempt_token)
            _reset_step_info(info_token)
        attempt.task = task

        heartbeat_timeout = job.options.heartbeat_timeout
        watchdog = (
            asyncio.ensure_future(self._watch(attempt, heartbeat_timeout))
            if heartbeat_timeout is not None
            else None
        )
        try:
            done, _ = await asyncio.wait({task}, timeout=max(timeout.total_seconds(), 0.0))
        except asyncio.CancelledError:
            await _cancel_and_wait(task)
            raise
        finally:
            if watchdog is not None:
                watchdog.cancel()

        if not done:
            await _cancel_and_wait(task)
            return FailureInfo(
                message=f"attempt {job.attempt} timed out after {timeout.total_seconds():g}s",
                type=TIMEOUT_TYPE,
                kind="timeout",
            )
        if task.cancelled():
            if attempt.interruption is _Interruption.HEARTBEAT_TIMEOUT and heartbeat_timeout:
                return FailureInfo(
                    message=(
                        f"attempt {job.attempt} sent no heartbeat for "
                        f"{heartbeat_timeout.total_seconds():g}s"
                    ),
                    type=HEARTBEAT_TIMEOUT_TYPE,
                    kind="timeout",
                )
            return FailureInfo(message="attempt was cancelled", type="Cancelled", kind="canceled")
        error = task.exception()
        if error is not None:
            return FailureInfo.from_exception(error)
        try:
            payload = self.converter.encode(task.result())
        except Exception as encode_error:
            return FailureInfo.from_exception(encode_error)
        return StepCompleted(seq=job.seq, result=payload, attempts=job.attempt)

    @staticmethod
    async def _watch(attempt: _Attempt, heartbeat_timeout: timedelta) -> None:
        limit = heartbeat_timeout.total_seconds()
        loop = asyncio.get_running_loop()
        while True:
            remaining = attempt.last_beat + limit - loop.time()
            if remaining <= 0:
                attempt.interrupt(_Interruption.HEARTBEAT_TIMEOUT)
                return
            await asyncio.sleep(remaining)

    async def _finish_failed(self, job: StepJob, cause: FailureInfo, now: datetime) -> None:
        failure = FailureInfo(
            message=(
                f"step {job.step_name!r} failed after {job.attempt} attempt(s): {cause.message}"
            ),
            type="StepError",
            kind="step",
            non_retryable=cause.non_retryable,
            attributes={"step_name": job.step_name, "seq": job.seq, "attempts": job.attempt},
            cause=cause,
        )
        event = StepFailed(seq=job.seq, failure=failure, attempts=job.attempt)
        if await self.store.finish_step(job.job_id, self.worker_id, event, now):
            self.steps_failed += 1
            logger.warning(
                "step %s (run %s, seq %d) failed permanently: %s",
                job.step_name,
                job.run_id,
                job.seq,
                cause.message,
            )


async def _cancel_and_wait(task: asyncio.Task[Any]) -> None:
    """Cancel ``task`` and wait for it; sync steps in threads cannot be interrupted."""
    task.cancel()
    with contextlib.suppress(BaseException):
        await task
