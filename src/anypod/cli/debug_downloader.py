"""Debug mode for testing the Downloader functionality.

This module provides functionality to test the Downloader in isolation,
processing queued downloads for all configured feeds and reporting on
the results.

Recommend running debug_enqueuer first to populate the database.
"""

import logging

from ..config import AppSettings
from ..data_coordinator.downloader import Downloader
from ..db import DownloadDatabase
from ..db.sqlalchemy_core import SqlalchemyCore
from ..db.types import Download, DownloadStatus
from ..exceptions import DatabaseOperationError, DownloadError
from ..file_manager import FileManager
from ..path_manager import PathManager
from ..ytdlp_wrapper import YtdlpWrapper

logger = logging.getLogger(__name__)


async def run_debug_downloader_mode(
    settings: AppSettings,
    paths: PathManager,
) -> None:
    """Run the Downloader in debug mode to process queued downloads.

    Initializes the Downloader, processes all queued downloads by calling
    download_queued for each configured feed, and then logs the state of
    downloads in the database.

    Args:
        settings: Application settings containing feed configurations.
        paths: PathManager instance containing data and temporary directories.
    """
    db_dir = await paths.db_dir()
    logger.info(
        "Initializing Anypod in Downloader debug mode.",
        extra={
            "config_file": str(settings.config_file),
            "db_dir": str(db_dir.resolve()),
            "debug_downloads_path": str(paths.base_data_dir.resolve()),
        },
    )

    try:
        db_core = SqlalchemyCore(db_dir)
        download_db = DownloadDatabase(db_core)

        file_manager = FileManager(paths)

        ytdlp_wrapper = YtdlpWrapper(paths)
        downloader = Downloader(download_db, file_manager, ytdlp_wrapper)
    except Exception as e:
        logger.critical(
            "Failed to initialize components for Downloader debug mode.", exc_info=e
        )
        return

    logger.debug("Downloader and its dependencies initialized for debug mode.")

    if not settings.feeds:
        logger.info(
            "No feeds configured. Downloader debug mode has nothing to process."
        )
        await db_core.close()
        return

    total_success_count = 0
    total_failure_count = 0
    processed_feeds_count = 0

    for feed_id, feed_config in settings.feeds.items():
        log_params = {"feed_id": feed_id, "feed_url": feed_config.url}
        logger.info(
            f"Processing queued downloads for feed: {feed_id}", extra=log_params
        )

        try:
            success_count, failure_count = await downloader.download_queued(
                feed_id=feed_id,
                feed_config=feed_config,
                cookies_path=settings.cookies_path,
                limit=-1,  # Process all queued downloads
            )
            total_success_count += success_count
            total_failure_count += failure_count
            processed_feeds_count += 1
            logger.info(
                f"Finished processing feed: {feed_id}. "
                f"Successful downloads: {success_count}, Failed downloads: {failure_count}",
                extra=log_params,
            )
        except DownloadError as e:
            logger.error(
                "Could not process downloads for feed.",
                extra=log_params,
                exc_info=e,
            )

    logger.info(
        "Finished processing all configured feeds.",
        extra={
            "feed_count": processed_feeds_count,
            "total_success_count": total_success_count,
            "total_failure_count": total_failure_count,
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
                downloads_in_status = await download_db.get_downloads_by_status(
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
                            f"  {i + 1}. ID: {dl.id}, Title: {dl.title}, Feed: {dl.feed_id}, "
                            f"Ext: {dl.ext}, Filesize: {dl.filesize or 'N/A'}, "
                            f"Duration: {dl.duration}s, Quality: {dl.quality_info or 'N/A'}, "
                            f"Published: {dl.published.isoformat() if dl.published else 'N/A'}"
                        )
        else:
            logger.info("No downloads found in the database.")

        # Show downloaded files on filesystem
        if paths.base_data_dir.exists():
            logger.info("Files found in debug downloads directory:")
            for feed_dir in paths.base_data_dir.iterdir():
                if feed_dir.is_dir():
                    files = list(feed_dir.glob("*"))
                    if files:
                        logger.info(f"  Feed '{feed_dir.name}': {len(files)} files")
                        for i, file_path in enumerate(files[:3]):  # Show first 3 files
                            size_mb = (
                                file_path.stat().st_size / (1024 * 1024)
                                if file_path.is_file()
                                else 0
                            )
                            logger.info(
                                f"    {i + 1}. {file_path.name} ({size_mb:.2f} MB)"
                            )
                        if len(files) > 3:
                            logger.info(f"    ... and {len(files) - 3} more files")
                    else:
                        logger.info(f"  Feed '{feed_dir.name}': no files")
        else:
            logger.info("Debug downloads directory does not exist.")

    finally:
        await db_core.close()

    logger.info("Downloader debug mode processing complete.")
