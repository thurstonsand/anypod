"""Debug mode for testing the Enqueuer functionality.

This module provides functionality to test the Enqueuer in isolation,
processing all configured feeds and reporting on the results.
"""

from datetime import UTC, datetime
import logging

from ..config import AppSettings
from ..data_coordinator.enqueuer import Enqueuer
from ..data_coordinator.pruner import Pruner
from ..db import AppStateDatabase, DownloadDatabase, FeedDatabase
from ..db.sqlalchemy_core import SqlalchemyCore
from ..db.types import Download, DownloadStatus
from ..exceptions import DatabaseOperationError, EnqueueError, StateReconciliationError
from ..ffmpeg import FFmpeg
from ..ffprobe import FFProbe
from ..file_manager import FileManager
from ..image_downloader import ImageDownloader
from ..path_manager import PathManager
from ..state_reconciler import StateReconciler
from ..ytdlp_wrapper import YtdlpWrapper

logger = logging.getLogger(__name__)


async def run_debug_enqueuer_mode(
    settings: AppSettings,
    paths: PathManager,
) -> None:
    """Run the Enqueuer in debug mode to process feed metadata.

    Initializes the Enqueuer, processes all configured feeds by calling
    enqueue_new_downloads, and then logs the state of downloads in the
    database.

    Args:
        settings: Application settings containing feed configurations.
        paths: PathManager instance containing data and temporary directories.
    """
    db_dir = await paths.db_dir()
    logger.info(
        "Initializing Anypod in Enqueuer debug mode.",
        extra={
            "config_file": str(settings.config_file),
            "db_dir": str(db_dir.resolve()),
        },
    )

    try:
        db_core = SqlalchemyCore(db_dir)
        feed_db = FeedDatabase(db_core)
        download_db = DownloadDatabase(db_core)
        file_manager = FileManager(paths)
        app_state_db = AppStateDatabase(db_core)
        ytdlp_wrapper = YtdlpWrapper(
            paths,
            pot_provider_url=settings.pot_provider_url,
            app_state_db=app_state_db,
            yt_channel=settings.yt_channel,
            yt_update_freq=settings.yt_dlp_update_freq,
        )
        pruner = Pruner(feed_db, download_db, file_manager)
        ffprobe = FFProbe()
        ffmpeg = FFmpeg()
        image_downloader = ImageDownloader(paths, ytdlp_wrapper, ffprobe, ffmpeg)
        state_reconciler = StateReconciler(
            file_manager,
            image_downloader,
            feed_db,
            download_db,
            ytdlp_wrapper,
            pruner,
        )
        enqueuer = Enqueuer(feed_db, download_db, ytdlp_wrapper)
    except Exception as e:
        logger.critical(
            "Failed to initialize components for Enqueuer debug mode.", exc_info=e
        )
        return

    logger.debug("Enqueuer and its dependencies initialized for debug mode.")

    if not settings.feeds:
        logger.info("No feeds configured. Enqueuer debug mode has nothing to process.")
        await db_core.close()
        return

    # Run state reconciliation first to ensure feeds exist in database
    logger.info("Running state reconciliation to set up feeds in database.")
    try:
        ready_feed_ids = await state_reconciler.reconcile_startup_state(
            settings.feeds, settings.cookies_path
        )
        logger.info(
            f"State reconciliation completed. {len(ready_feed_ids)} feeds ready for processing.",
            extra={"ready_feed_count": len(ready_feed_ids)},
        )
    except StateReconciliationError as e:
        logger.error("Failed to reconcile state.", exc_info=e)
        await db_core.close()
        return

    total_newly_queued_count = 0
    processed_feeds_count = 0

    for feed_id, feed_config in settings.feeds.items():
        log_params = {"feed_id": feed_id, "feed_url": feed_config.url}
        logger.info(f"Processing feed: {feed_id}", extra=log_params)
        try:
            # this is not normally how this field is used, but for debug mode, let's reuse this field
            fetch_since_date = feed_config.since or datetime.min.replace(tzinfo=UTC)

            newly_queued_count, _ = await enqueuer.enqueue_new_downloads(
                feed_id=feed_id,
                feed_config=feed_config,
                fetch_since_date=fetch_since_date,
                cookies_path=settings.cookies_path,
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
                            f"Ext: {dl.ext}, Duration: {dl.duration}s, "
                            f"Quality: {dl.quality_info or 'N/A'}, "
                            f"Published: {dl.published.isoformat() if dl.published else 'N/A'}"
                        )
        else:
            logger.info("No downloads found in the database.")
    finally:
        await db_core.close()

    logger.info("Enqueuer debug mode processing complete.")
