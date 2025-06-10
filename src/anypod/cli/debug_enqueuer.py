"""Debug mode for testing the Enqueuer functionality.

This module provides functionality to test the Enqueuer in isolation,
processing all configured feeds and reporting on the results.
"""

from datetime import UTC, datetime
import logging
from pathlib import Path

from ..config import AppSettings
from ..data_coordinator.enqueuer import Enqueuer
from ..db import Download, DownloadDatabase, DownloadStatus, FeedDatabase
from ..exceptions import DatabaseOperationError, EnqueueError
from ..path_manager import PathManager
from ..ytdlp_wrapper import YtdlpWrapper

logger = logging.getLogger(__name__)


def run_debug_enqueuer_mode(
    settings: AppSettings,
    debug_db_path: Path,
    paths: PathManager,
) -> None:
    """Run the Enqueuer in debug mode to process feed metadata.

    Initializes the Enqueuer, processes all configured feeds by calling
    enqueue_new_downloads, and then logs the state of downloads in the
    database.

    Args:
        settings: Application settings containing feed configurations.
        debug_db_path: Path to the database file.
        paths: PathManager instance containing data and temporary directories.
    """
    logger.info(
        "Initializing Anypod in Enqueuer debug mode.",
        extra={
            "config_file": str(settings.config_file),
            "debug_db_path": str(debug_db_path.resolve()),
        },
    )

    try:
        feed_db = FeedDatabase(db_path=debug_db_path)
        download_db = DownloadDatabase(db_path=debug_db_path)
        ytdlp_wrapper = YtdlpWrapper(paths)
        enqueuer = Enqueuer(feed_db, download_db, ytdlp_wrapper)
    except Exception as e:
        logger.critical(
            "Failed to initialize components for Enqueuer debug mode.", exc_info=e
        )
        return

    logger.debug("Enqueuer and its dependencies initialized for debug mode.")

    if not settings.feeds:
        logger.info("No feeds configured. Enqueuer debug mode has nothing to process.")
        feed_db.close()
        download_db.close()
        return

    total_newly_queued_count = 0
    processed_feeds_count = 0

    for feed_id, feed_config in settings.feeds.items():
        log_params = {"feed_id": feed_id, "feed_url": feed_config.url}
        logger.info(f"Processing feed: {feed_id}", extra=log_params)
        try:
            # this is not normally how this field is used, but for debug mode, let's reuse this field
            fetch_since_date = feed_config.since or datetime.min.replace(tzinfo=UTC)

            newly_queued_count = enqueuer.enqueue_new_downloads(
                feed_id=feed_id,
                feed_config=feed_config,
                fetch_since_date=fetch_since_date,
            )
            total_newly_queued_count += newly_queued_count
            processed_feeds_count += 1
            logger.info(
                f"Finished processing feed: {feed_id}. Newly queued: {newly_queued_count}",
                extra=log_params,
            )
        except EnqueueError as e:
            logger.error(
                "Could not enqueue feed.",
                extra=log_params,
                exc_info=e,
            )

    logger.info(
        "Finished processing all configured feeds.",
        extra={
            "feed_count": processed_feeds_count,
            "queued_count": total_newly_queued_count,
        },
    )

    logger.info("Fetching final download states from database.")
    try:
        status_counts: dict[str, int] = {str(status): 0 for status in DownloadStatus}
        all_downloads_by_status: dict[DownloadStatus, list[Download]] = {
            status: [] for status in DownloadStatus
        }

        for status in DownloadStatus:
            try:
                downloads_in_status = download_db.get_downloads_by_status(
                    status_to_filter=status,
                    limit=-1,  # get all
                )
            except DatabaseOperationError as e:
                logger.error(
                    "Failed to fetch downloads for status.",
                    extra={"status": str(status)},
                    exc_info=e,
                )
            else:
                all_downloads_by_status[status].extend(downloads_in_status)
                status_counts[str(status)] = len(downloads_in_status)

        logger.info("Current download counts by status:", extra=status_counts)

        if any(all_downloads_by_status.values()):
            logger.info("Listing up to 5 downloads per status:")
            for status, downloads_list in all_downloads_by_status.items():
                if downloads_list:
                    logger.info(
                        f"--- Status: {status.value} ({len(downloads_list)} total) ---"
                    )
                    for i, dl in enumerate(downloads_list[:5]):
                        logger.info(
                            f"  {i + 1}. ID: {dl.id}, Title: {dl.title}, Feed: {dl.feed}"
                        )
        else:
            logger.info("No downloads found in the database.")
    finally:
        feed_db.close()
        download_db.close()

    logger.info("Enqueuer debug mode processing complete.")
