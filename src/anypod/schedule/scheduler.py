"""Scheduler module for periodic feed processing.

This module provides the FeedScheduler class which manages the scheduling
of periodic feed processing jobs using APScheduler with async support,
graceful error handling, and proper lifecycle management.
"""

from datetime import datetime
import logging
import time

from ..config import FeedConfig
from ..data_coordinator import DataCoordinator
from ..data_coordinator.types import ProcessingResults
from ..logging_config import set_context_id
from .apscheduler_core import APSchedulerCore

logger = logging.getLogger(__name__)


class FeedScheduler:
    """Manage scheduled feed processing using APScheduler.

    The FeedScheduler class provides comprehensive scheduling functionality
    for periodic feed processing, including job lifecycle management,
    graceful shutdown, and event monitoring.

    Attributes:
        _scheduler: APSchedulerCore instance.
    """

    def __init__(
        self,
        ready_feed_ids: list[str],
        feed_configs: dict[str, FeedConfig],
        data_coordinator: DataCoordinator,
    ):
        self._scheduler = APSchedulerCore()

        for ready_feed_id in ready_feed_ids:
            self._scheduler.schedule_job(
                job_id=FeedScheduler._feed_to_job_id(ready_feed_id),
                cron_expression=feed_configs[ready_feed_id].schedule,
                # TODO: evaluate if we want jitter
                jitter=0,
                callback=FeedScheduler._process_feed_with_context,
                run_immediately=True,
                data_coordinator=data_coordinator,
                feed_id=ready_feed_id,
                feed_config=feed_configs[ready_feed_id],
            )

        # Register event listeners
        self._scheduler.add_job_completed_listener(
            ProcessingResults, self._job_completed_callback
        )
        self._scheduler.add_job_failed_listener(self._job_failed_callback)
        self._scheduler.add_job_missed_listener(self._job_missed_callback)

        logger.debug(
            "FeedScheduler initialized.",
            extra={"ready_feed_count": len(ready_feed_ids)},
        )

    async def start(self) -> None:
        """Start the scheduler."""
        # Start the scheduler
        self._scheduler.start()
        logger.info(
            "Feed scheduler started successfully.",
        )

    async def stop(self, wait_for_jobs: bool = True) -> None:
        """Stop the scheduler gracefully.

        Args:
            wait_for_jobs: Whether to wait for running jobs to complete.
        """
        if not self._scheduler.running:
            logger.debug("Scheduler is not running, nothing to stop.")
            return

        logger.info(
            "Stopping feed scheduler.",
            extra={"wait_for_jobs": wait_for_jobs},
        )

        self._scheduler.shutdown(wait=wait_for_jobs)
        logger.info("Feed scheduler stopped successfully.")

    @property
    def running(self) -> bool:
        """Check if the scheduler is currently running.

        Returns:
            True if scheduler is running, False otherwise.
        """
        return self._scheduler.running

    def get_scheduled_feed_ids(self) -> list[str]:
        """Get list of currently scheduled feed IDs.

        Returns:
            List of feed IDs that have scheduled jobs.
        """
        job_ids = self._scheduler.get_job_ids()
        feed_ids: list[str] = []

        for job_id in job_ids:
            feed_id = self._job_to_feed_id(job_id)
            feed_ids.append(feed_id or f"<invalid job id: {job_id}>")

        return feed_ids

    @staticmethod
    def _feed_to_job_id(feed_id: str) -> str:
        """Convert a feed ID to a job ID.

        Args:
            feed_id: The feed identifier.

        Returns:
            Job ID string for APScheduler.
        """
        return f"feed_{feed_id}"

    @staticmethod
    def _job_to_feed_id(job_id: str) -> str | None:
        """Convert a job ID to a feed ID.

        Args:
            job_id: The job identifier.

        Returns:
            Feed ID string if the job ID is a feed job, None otherwise.
        """
        if job_id.startswith("feed_"):
            return job_id.replace("feed_", "")
        return None

    @staticmethod
    def _process_feed_with_context(
        data_coordinator: DataCoordinator, feed_id: str, feed_config: FeedConfig
    ) -> ProcessingResults:
        """Process a feed with context ID set for logging.

        Args:
            data_coordinator: The DataCoordinator instance.
            feed_id: The feed identifier.
            feed_config: The feed configuration.

        Returns:
            ProcessingResults from the DataCoordinator.
        """
        # Generate unique context ID for this job execution
        timestamp = int(time.time())
        context_id = f"{feed_id}-{timestamp}"

        # Set context ID for automatic log correlation
        set_context_id(context_id)

        logger.info(
            "Starting scheduled feed processing job.",
            extra={
                "feed_id": feed_id,
            },
        )

        # Execute the main feed processing logic
        return data_coordinator.process_feed(feed_id, feed_config)

    @staticmethod
    def _job_completed_callback(
        job_id: str, scheduled_run_time: datetime, retval: ProcessingResults
    ) -> None:
        """Handle job completion events.

        Args:
            job_id: The job identifier.
            scheduled_run_time: The scheduled run time of the job.
            retval: The ProcessingResults from the job execution.
        """
        feed_id = FeedScheduler._job_to_feed_id(job_id)
        log_params = {
            "job_id": job_id,
            "feed_id": feed_id,
            "scheduled_run_time": scheduled_run_time.isoformat(),
            **retval.summary_dict(),
        }

        if retval.overall_success:
            logger.info(
                "Scheduled feed processing job completed successfully.",
                extra=log_params,
            )
        else:
            logger.warning(
                "Scheduled feed processing job completed with errors.",
                extra=log_params,
            )

    @staticmethod
    def _job_failed_callback(
        job_id: str, scheduled_run_time: datetime, exception: Exception
    ) -> None:
        """Handle job failure events.

        Args:
            job_id: The job identifier.
            scheduled_run_time: The scheduled run time of the job.
            exception: The exception that caused the failure.
        """
        feed_id = FeedScheduler._job_to_feed_id(job_id)
        logger.error(
            "Scheduled job failed with error.",
            extra={
                "job_id": job_id,
                "feed_id": feed_id,
                "scheduled_run_time": scheduled_run_time.isoformat(),
                "exception_type": type(exception).__name__,
            },
            exc_info=exception,
        )

    @staticmethod
    def _job_missed_callback(job_id: str, scheduled_run_time: datetime) -> None:
        """Handle missed job execution.

        Args:
            job_id: The job identifier.
            scheduled_run_time: The scheduled run time that was missed.
        """
        feed_id = FeedScheduler._job_to_feed_id(job_id)
        logger.warning(
            "Scheduled job missed execution window.",
            extra={
                "job_id": job_id,
                "feed_id": feed_id,
                "scheduled_run_time": scheduled_run_time.isoformat(),
            },
        )
