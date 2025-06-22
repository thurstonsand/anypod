"""Main orchestrator for feed processing operations.

This module defines the DataCoordinator class, which orchestrates the entire
feed processing lifecycle including enqueueing, downloading, pruning, and
RSS generation.
"""

from datetime import UTC, datetime
import logging
from pathlib import Path
import time

from ..config import FeedConfig
from ..db import FeedDatabase
from ..exceptions import (
    CoordinatorExecutionError,
    DatabaseOperationError,
    DataCoordinatorError,
    DownloadError,
    EnqueueError,
    FeedNotFoundError,
    PruneError,
    RSSGenerationError,
)
from ..rss import RSSFeedGenerator
from .downloader import Downloader
from .enqueuer import Enqueuer
from .pruner import Pruner
from .types import PhaseResult, ProcessingResults

logger = logging.getLogger(__name__)


class DataCoordinator:
    """Orchestrate the complete feed processing lifecycle.

    The DataCoordinator manages the sequence of operations required to process
    a feed: enqueue new downloads, download queued media, prune old content,
    and generate RSS feeds. It handles errors gracefully and provides
    comprehensive result tracking.

    Attributes:
        _enqueuer: Service for enqueueing new downloads from feed sources.
        _downloader: Service for downloading queued media files.
        _pruner: Service for pruning old downloads based on retention policies.
        _rss_generator: Service for generating RSS feed XML.
        _feed_db: Database manager for feed record operations.
        _cookies_path: Path to cookies.txt file for yt-dlp authentication.
    """

    def __init__(
        self,
        enqueuer: Enqueuer,
        downloader: Downloader,
        pruner: Pruner,
        rss_generator: RSSFeedGenerator,
        feed_db: FeedDatabase,
        cookies_path: Path | None = None,
    ):
        self._enqueuer = enqueuer
        self._downloader = downloader
        self._pruner = pruner
        self._rss_generator = rss_generator
        self._feed_db = feed_db
        self._cookies_path = cookies_path
        logger.debug("DataCoordinator initialized.")

    def _calculate_fetch_since_date(self, feed_id: str) -> datetime:
        """Calculate the date to use for fetching new downloads.

        Uses the feed's last_successful_sync timestamp. This method expects
        that the feed has been successfully synced at least once before.
        Startup logic elsewhere should guarantee this precondition.

        Args:
            feed_id: The feed identifier.

        Returns:
            The datetime to use for fetch_since_date.

        Raises:
            CoordinatorExecutionError: If the feed is not found in the database
                or if last_successful_sync is not defined.
        """
        try:
            feed = self._feed_db.get_feed_by_id(feed_id)
        except (FeedNotFoundError, DatabaseOperationError) as e:
            raise CoordinatorExecutionError(
                f"Cannot retrieve feed for sync date calculation: {e}",
                feed_id=feed_id,
            ) from e

        # Use last successful sync timestamp
        fetch_since_date = feed.last_successful_sync
        if not fetch_since_date:
            raise CoordinatorExecutionError(
                "No last successful sync found for feed. Expected last_successful_sync to be defined.",
                feed_id=feed_id,
            )

        logger.debug(
            "Calculated fetch_since_date for feed.",
            extra={
                "feed_id": feed_id,
                "last_successful_sync": fetch_since_date,
            },
        )

        return fetch_since_date

    async def _execute_enqueue_phase(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        fetch_since_date: datetime,
        fetch_until_date: datetime,
    ) -> PhaseResult:
        """Execute the enqueue phase of feed processing.

        Args:
            feed_id: The feed identifier.
            feed_config: The feed configuration.
            fetch_since_date: Date to use for filtering new downloads (from last_successful_sync).
            fetch_until_date: Upper bound date for filtering new downloads (current time).

        Returns:
            PhaseResult with enqueue phase results.
        """
        phase_start = time.time()
        log_params = {"feed_id": feed_id, "phase": "enqueue"}

        try:
            enqueued_count = await self._enqueuer.enqueue_new_downloads(
                feed_id,
                feed_config,
                fetch_since_date,
                fetch_until_date,
                self._cookies_path,
            )
        except EnqueueError as e:
            duration = time.time() - phase_start
            logger.error(
                "Enqueue phase failed.",
                extra={**log_params, "duration_seconds": duration},
                exc_info=e,
            )

            return PhaseResult(
                success=False,
                count=0,
                errors=[e],
                duration_seconds=duration,
            )
        else:
            duration = time.time() - phase_start
            logger.info(
                "Enqueue phase completed successfully.",
                extra={
                    **log_params,
                    "enqueued_count": enqueued_count,
                    "duration_seconds": duration,
                },
            )

            return PhaseResult(
                success=True,
                count=enqueued_count,
                duration_seconds=duration,
            )

    async def _execute_download_phase(
        self, feed_id: str, feed_config: FeedConfig
    ) -> PhaseResult:
        """Execute the download phase of feed processing.

        Args:
            feed_id: The feed identifier.
            feed_config: The feed configuration.

        Returns:
            PhaseResult with download phase results.
        """
        phase_start = time.time()
        log_params = {"feed_id": feed_id, "phase": "download"}

        logger.info("Starting download phase.", extra=log_params)

        try:
            success_count, failure_count = await self._downloader.download_queued(
                feed_id, feed_config, self._cookies_path
            )
        except DownloadError as e:
            duration = time.time() - phase_start
            logger.error(
                "Download phase failed with infrastructure error.",
                extra={**log_params, "duration_seconds": duration},
                exc_info=e,
            )

            return PhaseResult(
                success=False,
                count=0,
                errors=[e],
                duration_seconds=duration,
            )
        else:
            duration = time.time() - phase_start
            logger.info(
                "Download phase completed.",
                extra={
                    **log_params,
                    "success_count": success_count,
                    "failure_count": failure_count,
                    "duration_seconds": duration,
                },
            )

            # Consider download phase successful even if some individual downloads failed
            # The downloader handles individual failures appropriately
            return PhaseResult(
                success=True,
                count=success_count,
                duration_seconds=duration,
            )

    def _execute_prune_phase(
        self, feed_id: str, feed_config: FeedConfig
    ) -> PhaseResult:
        """Execute the prune phase of feed processing.

        Args:
            feed_id: The feed identifier.
            feed_config: The feed configuration.

        Returns:
            PhaseResult with prune phase results.
        """
        phase_start = time.time()
        log_params = {"feed_id": feed_id, "phase": "prune"}

        logger.info("Starting prune phase.", extra=log_params)

        try:
            # Use feed_config.since as prune_before_date (different from fetch_since_date)
            archived_count, files_deleted_count = self._pruner.prune_feed_downloads(
                feed_id, feed_config.keep_last, feed_config.since
            )
        except PruneError as e:
            duration = time.time() - phase_start
            logger.error(
                "Prune phase failed.",
                extra={**log_params, "duration_seconds": duration},
                exc_info=e,
            )

            return PhaseResult(
                success=False,
                count=0,
                errors=[e],
                duration_seconds=duration,
            )
        else:
            duration = time.time() - phase_start
            logger.info(
                "Prune phase completed successfully.",
                extra={
                    **log_params,
                    "archived_count": archived_count,
                    "files_deleted_count": files_deleted_count,
                    "duration_seconds": duration,
                },
            )

            return PhaseResult(
                success=True,
                count=archived_count,
                duration_seconds=duration,
            )

    def _execute_rss_generation_phase(self, feed_id: str) -> PhaseResult:
        """Execute the RSS generation phase of feed processing.

        Args:
            feed_id: The feed identifier.

        Returns:
            PhaseResult with RSS generation phase results.
        """
        phase_start = time.time()
        log_params = {"feed_id": feed_id, "phase": "rss_generation"}

        logger.info("Starting RSS generation phase.", extra=log_params)

        try:
            # Get Feed object from database for RSS generation
            feed = self._feed_db.get_feed_by_id(feed_id)
            self._rss_generator.update_feed(feed_id, feed)
            self._feed_db.mark_rss_generated(feed_id)
        except (RSSGenerationError, FeedNotFoundError, DatabaseOperationError) as e:
            duration = time.time() - phase_start
            logger.error(
                "RSS generation phase failed.",
                extra={**log_params, "duration_seconds": duration},
                exc_info=e,
            )

            return PhaseResult(
                success=False,
                count=0,
                errors=[e],
                duration_seconds=duration,
            )
        else:
            duration = time.time() - phase_start
            logger.info(
                "RSS generation phase completed successfully.",
                extra={**log_params, "duration_seconds": duration},
            )

            return PhaseResult(
                success=True,
                count=1,  # Successfully generated 1 feed
                duration_seconds=duration,
            )

    def _update_feed_sync_status(
        self,
        feed_id: str,
        success: bool,
        fetch_until_date: datetime,
    ) -> bool:
        """Update the feed's sync status in the database.

        Args:
            feed_id: The feed identifier.
            success: Whether the sync was successful.
            fetch_until_date: The upper bound date used during fetch. Used as new sync time if successful.

        Returns:
            True if the database update was successful.
        """
        log_params = {"feed_id": feed_id, "sync_success": success}

        try:
            if success:
                self._feed_db.mark_sync_success(feed_id, fetch_until_date)
                logger.info(
                    "Feed sync status updated to success.",
                    extra={**log_params, "sync_time": fetch_until_date.isoformat()},
                )
            else:
                self._feed_db.mark_sync_failure(feed_id)
                logger.info(
                    "Feed sync status updated to failure.",
                    extra=log_params,
                )
        except (FeedNotFoundError, DatabaseOperationError) as e:
            logger.error(
                "Failed to update feed sync status.",
                extra=log_params,
                exc_info=e,
            )
            return False
        else:
            return True

    async def process_feed(
        self, feed_id: str, feed_config: FeedConfig
    ) -> ProcessingResults:
        """Process a feed through all phases of the pipeline.

        Executes the complete feed processing workflow:
        1. Calculate fetch_since_date from last successful sync
        2. Enqueue new downloads from the feed source
        3. Download queued media files
        4. Prune old downloads based on retention policies
        5. Generate updated RSS feed
        6. Update feed sync status

        The method uses graceful degradation - if a phase fails, subsequent
        phases may still execute where appropriate (e.g., RSS generation
        can proceed even if downloads failed).

        Args:
            feed_id: The unique identifier for the feed to process.
            feed_config: The configuration object for the feed.

        Returns:
            ProcessingResults containing comprehensive results from all phases.
        """
        start_time = datetime.now(UTC)
        log_params = {"feed_id": feed_id}

        logger.info("Starting feed processing.", extra=log_params)

        # Initialize results object
        results = ProcessingResults(
            feed_id=feed_id,
            start_time=start_time,
        )

        # Calculate fetch dates (used in finally block too)
        fetch_until_date = datetime.now(UTC)

        try:
            fetch_since_date = self._calculate_fetch_since_date(feed_id)

            # Phase 1: Enqueue new downloads
            results.enqueue_result = await self._execute_enqueue_phase(
                feed_id, feed_config, fetch_since_date, fetch_until_date
            )

            # Phase 2: Download queued media (always attempt, even if enqueue failed)
            results.download_result = await self._execute_download_phase(
                feed_id, feed_config
            )

            # Phase 3: Prune old downloads (always attempt)
            results.prune_result = self._execute_prune_phase(feed_id, feed_config)

            # Phase 4: Generate RSS feed (always attempt)
            results.rss_generation_result = self._execute_rss_generation_phase(feed_id)

            # Determine overall success
            # Consider successful if at least RSS generation succeeded
            results.overall_success = results.rss_generation_result.success

        except (DataCoordinatorError, CoordinatorExecutionError) as e:
            # Fatal error that prevented processing
            logger.error(
                "Fatal error during feed processing.",
                extra=log_params,
                exc_info=e,
            )
            results.fatal_error = e
            results.overall_success = False

        except Exception as e:
            # Unexpected error
            logger.error(
                "Unexpected error during feed processing.",
                extra=log_params,
                exc_info=e,
            )
            results.fatal_error = e
            results.overall_success = False

        finally:
            # Calculate total duration
            end_time = datetime.now(UTC)
            results.total_duration_seconds = (end_time - start_time).total_seconds()

            # Update feed sync status
            results.feed_sync_updated = self._update_feed_sync_status(
                feed_id, results.overall_success, fetch_until_date
            )

            # Log final results
            logger.info(
                "Feed processing completed.",
                extra={**log_params, **results.summary_dict()},
            )

        return results
