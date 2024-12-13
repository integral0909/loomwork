"""Executing step jobs.

For every claimed job the :class:`StepExecutor`:

1. extends the lease to cover the step's timeout,
2. refuses to start if the attempt budget or total timeout is already exhausted (this
   happens when a previous attempt was abandoned by a crashed worker),
3. runs the step with a per-attempt timeout and the :func:`loomwork.step_info` context,
4. reports success, schedules a retry with backoff, or reports the final failure as a
   ``StepError`` whose cause is the last attempt's error.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timedelta

from loomwork.clock import Clock
from loomwork.codec.converter import DataConverter
from loomwork.engine.config import EngineConfig
from loomwork.failure import FailureInfo
from loomwork.history.events import StepCompleted, StepFailed
from loomwork.info import StepInfo
from loomwork.retry import RetryPolicy
from loomwork.sdk.api import _reset_step_info, _set_step_info
from loomwork.sdk.definitions import Registry
from loomwork.store.base import Store
from loomwork.store.records import StepJob

logger = logging.getLogger("loomwork.engine.steps")

ABANDONED_TYPE = "StepAttemptAbandoned"
TIMEOUT_TYPE = "StepTimeout"
TOTAL_TIMEOUT_TYPE = "StepTotalTimeout"


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

    async def run_once(self) -> bool:
        """Execute at most one step attempt. Returns whether a job was claimed."""
        names = self.registry.step_names
        if not names:
            return False
        job = await self.store.claim_step(
            self.worker_id,
            self.clock.now(),
            self.config.step_lease(self.config.default_step_options),
            names,
        )
        if job is None:
            return False
        await self.execute(job)
        return True

    async def execute(self, job: StepJob) -> None:
        now = self.clock.now()
        timeout = job.options.timeout or self.config.default_step_timeout
        policy = job.options.retry or self.config.default_step_retry

        lease_until = now + timeout + self.config.step_lease_grace
        if not await self.store.extend_step_lease(job.job_id, self.worker_id, lease_until):
            logger.info("lost lease on step job %s before starting", job.job_id)
            return

        refusal = self._refuse_reason(job, policy, now)
        if refusal is not None:
            await self._finish_failed(job, refusal, now)
            return

        attempt_timeout = timeout
        if job.deadline is not None:
            attempt_timeout = min(attempt_timeout, job.deadline - now)

        self.attempts_started += 1
        outcome = await self._run_attempt(job, attempt_timeout)
        finished_at = self.clock.now()
        if isinstance(outcome, StepCompleted):
            if await self.store.finish_step(job.job_id, self.worker_id, outcome, finished_at):
                self.steps_completed += 1
            return

        failure = outcome

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

    async def _run_attempt(self, job: StepJob, timeout: timedelta) -> StepCompleted | FailureInfo:
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
        token = _set_step_info(info)
        scope: asyncio.Timeout | None = None
        try:
            args = self.converter.decode_args(definition.fn, job.args)
            async with asyncio.timeout(max(timeout.total_seconds(), 0.0)) as scope:
                result = await definition.invoke(args)
            payload = self.converter.encode(result)
        except TimeoutError as exc:
            # Distinguish our attempt timeout from a TimeoutError raised by the step.
            if scope is not None and scope.expired():
                return FailureInfo(
                    message=f"attempt {job.attempt} timed out after {timeout.total_seconds():g}s",
                    type=TIMEOUT_TYPE,
                    kind="timeout",
                )
            return FailureInfo.from_exception(exc)
        except Exception as exc:
            return FailureInfo.from_exception(exc)
        finally:
            _reset_step_info(token)
        return StepCompleted(seq=job.seq, result=payload, attempts=job.attempt)

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
