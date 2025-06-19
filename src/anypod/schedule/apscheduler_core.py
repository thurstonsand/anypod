"""Type-safe wrapper around APScheduler for Anypod's scheduling needs.

This module provides a type-safe abstraction layer over APScheduler,
handling job scheduling, event listening, and lifecycle management
while isolating the rest of the codebase from direct APScheduler dependencies.
"""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
import logging
from typing import Any

from apscheduler.events import (  # type: ignore
    EVENT_JOB_ERROR,  # type: ignore
    EVENT_JOB_EXECUTED,  # type: ignore
    EVENT_JOB_MISSED,  # type: ignore
    JobExecutionEvent,  # type: ignore
)
from apscheduler.executors.asyncio import AsyncIOExecutor  # type: ignore
from apscheduler.jobstores.memory import MemoryJobStore  # type: ignore
from apscheduler.schedulers.asyncio import AsyncIOScheduler  # type: ignore
from apscheduler.triggers.cron import CronTrigger  # type: ignore
from apscheduler.triggers.date import DateTrigger  # type: ignore

from ..config.types import CronExpression

logger = logging.getLogger(__name__)


class APSchedulerCore:
    """Wrapper around APScheduler to provide a typesafe interface.

    This class provides a typesafe interface for scheduling and managing jobs.
    It wraps the APScheduler library and provides a typesafe interface for
    scheduling and managing jobs. It makes a couple assumptions:

    - It stores jobs in memory -- no persistent storage.
    - It uses the AsyncIOScheduler scheduler.
    - It schedules jobs for based on cron expressions.

    It also provides a typesafe interface for adding listeners to the scheduler.
    """

    def __init__(self):
        self._scheduler = AsyncIOScheduler(  # type: ignore
            jobstores={"default": MemoryJobStore()},
            executors={"default": AsyncIOExecutor()},
            job_defaults={
                "coalesce": True,  # Merge multiple missed executions into one
                "max_instances": 1,  # Prevent overlapping feed processing
                "misfire_grace_time": 300,  # 5 minutes grace for missed jobs
                "replace_existing": True,  # Replace jobs with same ID on restart
            },
            timezone="UTC",
        )

        self._job_completed_type_listeners: dict[
            type, Callable[[str, datetime, Any], None]
        ] = {}

        self._scheduler.add_listener(  # type: ignore
            self._dispatch_job_completed_event,  # type: ignore
            EVENT_JOB_EXECUTED,
        )

    @staticmethod
    def _trigger_from_cron_expression(expr: CronExpression, jitter: int) -> CronTrigger:  # type: ignore
        """Convert a CronExpression to a CronTrigger.

        Args:
            expr: The CronExpression to convert.
            jitter: The jitter to apply to the cron trigger.

        Returns:
            CronTrigger instance with appropriate fields set.
        """
        return CronTrigger(  # type: ignore
            minute=expr.minute,
            hour=expr.hour,
            day=expr.day,
            month=expr.month,
            day_of_week=expr.day_of_week,
            second=expr.second,
            jitter=jitter,
        )

    def _dispatch_job_completed_event(self, event: JobExecutionEvent) -> None:  # type: ignore
        """Dispatch job completion events to registered type-specific listeners.

        Args:
            event: The job execution event from APScheduler.
        """
        for return_type, callback in self._job_completed_type_listeners.items():
            if isinstance(event.retval, return_type):  # type: ignore
                callback(
                    event.job_id,  # type: ignore
                    event.scheduled_run_time,  # type: ignore
                    event.retval,  # type: ignore
                )
                return

        logger.warning(
            "No registered listener for job completed event",
            extra={
                "job_id": event.job_id,  # type: ignore
                "scheduled_run_time": event.scheduled_run_time,  # type: ignore
                "retval": event.retval,  # type: ignore
            },
        )

    def add_job_completed_listener[R](
        self, return_type: type[R], callback: Callable[[str, datetime, R], None]
    ) -> None:
        """Add a listener for job completed events.

        Can be called multiple times with different return types.

        TypeVar R: The return type of the job to listen for.

        Args:
            return_type: The type of the return value to listen for.
            callback: The callback function to call when a job is completed, with args:
                - job_id: The job identifier.
                - scheduled_run_time: The scheduled run time of the job.
                - retval: The return value of the job.
        """
        self._job_completed_type_listeners[return_type] = callback

    def add_job_failed_listener(
        self, callback: Callable[[str, datetime, Exception], None]
    ) -> None:
        """Add a listener for job failed events.

        Args:
            callback: The callback function to call when a job fails, with args:
                - job_id: The job identifier.
                - scheduled_run_time: The scheduled run time of the job.
                - exception: The exception that occurred.
        """

        def callback_wrapper(event: JobExecutionEvent) -> None:  # type: ignore
            callback(
                event.job_id,  # type: ignore
                event.scheduled_run_time,  # type: ignore
                event.exception,  # type: ignore
            )

        self._scheduler.add_listener(  # type: ignore
            callback_wrapper,  # type: ignore
            EVENT_JOB_ERROR,  # type: ignore
        )

    def add_job_missed_listener(
        self, callback: Callable[[str, datetime], None]
    ) -> None:
        """Add a listener for job missed events.

        Args:
            callback: The callback function to call when a job is missed, with args:
                - job_id: The job identifier.
                - scheduled_run_time: The scheduled run time of the job.
        """

        def callback_wrapper(event: JobExecutionEvent) -> None:  # type: ignore
            callback(
                event.job_id,  # type: ignore
                event.scheduled_run_time,  # type: ignore
            )

        self._scheduler.add_listener(  # type: ignore
            callback_wrapper,  # type: ignore
            EVENT_JOB_MISSED,  # type: ignore
        )

    def schedule_job[**P, R](
        self,
        job_id: str,
        cron_expression: CronExpression,
        jitter: int,
        callback: Callable[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        """Schedule a job to be executed based on a cron expression.

        Args:
            job_id: The job identifier.
            cron_expression: The CronExpression to schedule the job.
            jitter: The jitter to apply to the cron trigger.
            callback: The callback function to call when the job is executed.
            args: The arguments to pass to the callback function.
            kwargs: The keyword arguments to pass to the callback function.

        Raises:
            SchedulerError: If the cron expression is invalid.
        """
        trigger = self._trigger_from_cron_expression(cron_expression, jitter=jitter)  # type: ignore
        self._scheduler.add_job(  # type: ignore
            callback,
            args=args,
            kwargs=kwargs,
            trigger=trigger,
            id=job_id,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=300,
            replace_existing=True,
        )

    async def run_job_now[**P, R](
        self,
        callback: Callable[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> tuple[str, asyncio.Future[R]]:
        """Run a job immediately and return a future for its completion.

        Args:
            callback: The callback function to call.
            args: The arguments to pass to the callback function.
            kwargs: The keyword arguments to pass to the callback function.

        Returns:
            A tuple of (job_id, future) where the future will resolve with the job's return value.
        """
        future: asyncio.Future[R] = asyncio.get_running_loop().create_future()
        job_id: str | None = None  # This will be set once the job is created

        # Define the listeners first - they need to be defined so they can refer to each other for cleanup.
        def success_listener(event: JobExecutionEvent) -> None:  # type: ignore
            # We only care about the event for the specific job we just launched
            if event.job_id == job_id:  # type: ignore
                if not future.done():
                    future.set_result(event.retval)  # type: ignore

                # CLEANUP: The job is done, so we no longer need these listeners.
                self._scheduler.remove_listener(success_listener, EVENT_JOB_EXECUTED)  # type: ignore
                self._scheduler.remove_listener(error_listener, EVENT_JOB_ERROR)  # type: ignore

        def error_listener(event: JobExecutionEvent) -> None:  # type: ignore
            # We only care about the event for the specific job we just launched
            if event.job_id == job_id and not future.done():  # type: ignore
                if not future.done():
                    future.set_exception(event.exception)  # type: ignore

                # CLEANUP: The job is done (it failed), so we still clean up.
                self._scheduler.remove_listener(success_listener, EVENT_JOB_EXECUTED)  # type: ignore
                self._scheduler.remove_listener(error_listener, EVENT_JOB_ERROR)  # type: ignore

        # Add the listeners to the scheduler
        self._scheduler.add_listener(success_listener, EVENT_JOB_EXECUTED)  # type: ignore
        self._scheduler.add_listener(error_listener, EVENT_JOB_ERROR)  # type: ignore

        try:
            # Schedule the job and get its ID
            job = self._scheduler.add_job(  # type: ignore
                callback,
                trigger=DateTrigger(run_date=datetime.now(UTC)),  # type: ignore
                args=args,
                kwargs=kwargs,
            )
            job_id = job.id  # type: ignore # Now the listeners know which job ID to look for
            return job_id, future  # type: ignore
        except Exception:
            # If scheduling itself fails, we must clean up immediately.
            self._scheduler.remove_listener(success_listener, EVENT_JOB_EXECUTED)  # type: ignore
            self._scheduler.remove_listener(error_listener, EVENT_JOB_ERROR)  # type: ignore
            raise

    def start(self) -> None:
        """Start the scheduler."""
        self._scheduler.start()  # type: ignore

    def pause_job(self, job_id: str) -> None:
        """Pause a scheduled job.

        Args:
            job_id: The job identifier to pause.
        """
        self._scheduler.pause_job(job_id)  # type: ignore

    def resume_job(self, job_id: str) -> None:
        """Resume a paused job.

        Args:
            job_id: The job identifier to resume.
        """
        self._scheduler.resume_job(job_id)  # type: ignore

    def remove_job(self, job_id: str) -> None:
        """Remove a scheduled job.

        Args:
            job_id: The job identifier to remove.
        """
        self._scheduler.remove_job(job_id)  # type: ignore

    def get_job_ids(self) -> list[str]:
        """Get all scheduled job IDs.

        Returns:
            List of scheduled job IDs.
        """
        return [job.id for job in self._scheduler.get_jobs()]  # type: ignore

    @property
    def running(self) -> bool:
        """Check if the scheduler is currently running.

        Returns:
            True if the scheduler is running, False otherwise.
        """
        return self._scheduler.running  # type: ignore

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the scheduler.

        Args:
            wait: Whether to wait for running jobs to complete before shutting down.
        """
        self._scheduler.shutdown(wait=wait)  # type: ignore
