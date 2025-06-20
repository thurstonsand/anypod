"""Default mode implementation for Anypod.

This module provides the default execution mode that initializes all components,
runs state reconciliation, starts the scheduler, and manages the application lifecycle.
"""

import asyncio
import logging
import time

from ..config import AppSettings
from ..data_coordinator import DataCoordinator, Downloader, Enqueuer, Pruner
from ..db import DownloadDatabase, FeedDatabase
from ..exceptions import (
    DatabaseOperationError,
    StateReconciliationError,
)
from ..file_manager import FileManager
from ..logging_config import set_context_id
from ..path_manager import PathManager
from ..rss import RSSFeedGenerator
from ..schedule import FeedScheduler
from ..state_reconciler import StateReconciler
from ..ytdlp_wrapper import YtdlpWrapper

logger = logging.getLogger(__name__)


def _perform_initial_sync(
    data_coordinator: DataCoordinator,
    ready_feeds: list[str],
    settings: AppSettings,
) -> None:
    """Perform initial sync for all enabled feeds to populate RSS.

    Args:
        data_coordinator: The data coordinator instance.
        ready_feeds: List of feed IDs ready for processing.
        settings: Application settings.
    """
    logger.info(
        "Starting initial sync for all enabled feeds.",
        extra={"feed_count": len(ready_feeds)},
    )

    success_count = 0
    error_count = 0

    for feed_id in ready_feeds:
        feed_config = settings.feeds[feed_id]

        # Set context ID for initial sync logging correlation
        timestamp = int(time.time())
        context_id = f"initial-{feed_id}-{timestamp}"
        set_context_id(context_id)

        logger.info("Performing initial sync for feed.", extra={"feed_id": feed_id})

        result = data_coordinator.process_feed(feed_id, feed_config)
        if result.overall_success:
            success_count += 1
        else:
            error_count += 1
            logger.warning(
                "Initial sync completed with errors.",
                extra={"feed_id": feed_id, **result.summary_dict()},
            )

    logger.info(
        "Initial sync completed for all feeds.",
        extra={
            "total_feeds": len(ready_feeds),
            "successful": success_count,
            "failed": error_count,
        },
    )


def _init(
    settings: AppSettings,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    path_manager: PathManager,
) -> FeedScheduler:
    file_manager = FileManager(path_manager)
    ytdlp_wrapper = YtdlpWrapper(paths=path_manager)
    rss_generator = RSSFeedGenerator(download_db=download_db, paths=path_manager)

    # Initialize data coordinator components
    enqueuer = Enqueuer(
        ytdlp_wrapper=ytdlp_wrapper,
        download_db=download_db,
        feed_db=feed_db,
    )
    downloader = Downloader(
        ytdlp_wrapper=ytdlp_wrapper,
        download_db=download_db,
        file_manager=file_manager,
    )
    pruner = Pruner(feed_db=feed_db, download_db=download_db, file_manager=file_manager)

    data_coordinator = DataCoordinator(
        enqueuer=enqueuer,
        downloader=downloader,
        pruner=pruner,
        rss_generator=rss_generator,
        feed_db=feed_db,
        cookie_path=settings.cookie_path,
    )

    # Run state reconciliation
    logger.info("Running state reconciliation.")
    state_reconciler = StateReconciler(
        feed_db=feed_db, download_db=download_db, pruner=pruner
    )

    try:
        ready_feeds = state_reconciler.reconcile_startup_state(settings.feeds)
    except StateReconciliationError as e:
        logger.error("State reconciliation failed, cannot continue.", exc_info=e)
        raise

    if not ready_feeds:
        logger.warning(
            "No enabled feeds found after reconciliation, exiting.",
            extra={"configured_feeds": len(settings.feeds)},
        )
        raise RuntimeError("No enabled feeds found in config")

    # Perform initial sync for all feeds
    _perform_initial_sync(data_coordinator, ready_feeds, settings)

    # Initialize and start scheduler
    logger.info("Initializing feed scheduler.", extra={"ready_feeds": len(ready_feeds)})
    return FeedScheduler(
        ready_feed_ids=ready_feeds,
        feed_configs=settings.feeds,
        data_coordinator=data_coordinator,
    )


async def default(settings: AppSettings) -> None:
    """Main async entry point for default mode.

    Initializes all components, runs state reconciliation, starts scheduler,
    and manages application lifecycle.

    Args:
        settings: Application settings object containing configuration.
    """
    logger.info(
        "Starting Anypod in default mode.",
        extra={"config_file": str(settings.config_file)},
    )

    # Initialize components
    path_manager = PathManager(
        base_data_dir=settings.data_dir,
        base_url=settings.base_url,
    )

    # Ensure data directory exists before database initialization
    try:
        settings.data_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error(
            "Failed to create data directory.",
            extra={"data_dir": str(settings.data_dir)},
            exc_info=e,
        )
        raise DatabaseOperationError("Failed to create data directory.") from e

    logger.info(
        "Initializing database components.",
        extra={"db_path": str(path_manager.db_file_path)},
    )

    feed_db = FeedDatabase(db_path=path_manager.db_file_path)
    download_db = DownloadDatabase(db_path=path_manager.db_file_path)

    try:
        scheduler = _init(settings, feed_db, download_db, path_manager)

        await scheduler.start()

        logger.info(
            "Anypod is running. Press Ctrl+C to shutdown.",
            extra={
                "scheduled_feeds": scheduler.get_scheduled_feed_ids(),
            },
        )

        # Keep running forever until interrupted
        await asyncio.Event().wait()
    finally:
        # Cleanup database connections
        try:
            feed_db.close()
            download_db.close()
        except DatabaseOperationError as e:
            logger.error("Error closing database connections.", exc_info=e)
