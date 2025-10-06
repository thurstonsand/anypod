"""Default mode implementation for Anypod.

This module provides the default execution mode that initializes all components,
runs state reconciliation, starts the scheduler, and manages the application lifecycle.
"""

import asyncio
import logging

from ..config import AppSettings
from ..data_coordinator import DataCoordinator, Downloader, Enqueuer, Pruner
from ..db import AppStateDatabase, DownloadDatabase, FeedDatabase
from ..db.sqlalchemy_core import SqlalchemyCore
from ..exceptions import (
    DatabaseOperationError,
    StateReconciliationError,
)
from ..ffmpeg import FFmpeg
from ..ffprobe import FFProbe
from ..file_manager import FileManager
from ..image_downloader import ImageDownloader
from ..path_manager import PathManager
from ..rss import RSSFeedGenerator
from ..schedule import FeedScheduler
from ..server import create_admin_server, create_server
from ..state_reconciler import StateReconciler
from ..ytdlp_wrapper import YtdlpWrapper
from ..ytdlp_wrapper.handlers import HandlerSelector

logger = logging.getLogger(__name__)


async def graceful_shutdown(
    scheduler: FeedScheduler | None,
    db_core: SqlalchemyCore | None,
) -> None:
    """Perform graceful shutdown of all components in correct order.

    Args:
        scheduler: The feed scheduler instance to shutdown.
        db_core: The database core instance to close.
    """
    logger.info("Shutdown signal received.")

    # Step 1: Stop scheduler (finish current jobs, no new ones)
    if scheduler:
        try:
            await scheduler.stop(wait_for_jobs=True)
            logger.info("Scheduler shutdown completed.")
        except Exception as e:
            logger.error("Error shutting down scheduler.", exc_info=e)

    # Step 2: Close database connections
    if db_core:
        try:
            await db_core.close()
            logger.info("Database connections closed.")
        except Exception as e:
            logger.error("Error closing database connections.", exc_info=e)

    logger.info("Anypod shutdown completed.")


async def _init(
    settings: AppSettings,
) -> tuple[
    SqlalchemyCore,
    FileManager,
    FeedDatabase,
    DownloadDatabase,
    FeedScheduler,
]:
    # Initialize path manager
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

    logger.debug("Initializing database components.")

    # Initialize low-level components
    db_dir = await path_manager.db_dir()
    db_core = SqlalchemyCore(db_dir)
    file_manager = FileManager(path_manager)

    # Initialize database layers
    app_state_db = AppStateDatabase(db_core)
    feed_db = FeedDatabase(db_core)
    download_db = DownloadDatabase(db_core)

    # Initialize application components
    ffmpeg = FFmpeg()
    ffprobe = FFProbe()
    handler_selector = HandlerSelector(ffprobe)
    ytdlp_wrapper = YtdlpWrapper(
        paths=path_manager,
        pot_provider_url=settings.pot_provider_url,
        app_state_db=app_state_db,
        yt_channel=settings.yt_channel,
        yt_update_freq=settings.yt_dlp_update_freq,
        ffmpeg=ffmpeg,
        handler_selector=handler_selector,
    )
    rss_generator = RSSFeedGenerator(download_db=download_db, paths=path_manager)
    image_downloader = ImageDownloader(
        paths=path_manager,
        ytdlp_wrapper=ytdlp_wrapper,
        ffprobe=ffprobe,
        ffmpeg=ffmpeg,
    )

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
        cookies_path=settings.cookies_path,
    )

    # Run state reconciliation
    logger.debug("Running state reconciliation.")
    state_reconciler = StateReconciler(
        file_manager=file_manager,
        image_downloader=image_downloader,
        feed_db=feed_db,
        download_db=download_db,
        ytdlp_wrapper=ytdlp_wrapper,
        pruner=pruner,
    )

    try:
        ready_feeds = await state_reconciler.reconcile_startup_state(
            settings.feeds, settings.cookies_path
        )
    except StateReconciliationError as e:
        logger.error("State reconciliation failed, cannot continue.", exc_info=e)
        raise

    if not ready_feeds:
        logger.warning(
            "No enabled feeds found after reconciliation, exiting.",
            extra={"configured_feeds": len(settings.feeds)},
        )
        raise RuntimeError(
            f"No enabled feeds ready after reconciliation. "
            f"Configured {len(settings.feeds)} feed(s), but all failed during setup. "
            f"Check the logs above for specific errors (e.g., yt-dlp, network, or configuration issues)."
        )

    # Initialize and start scheduler
    logger.debug(
        "Initializing feed scheduler.", extra={"ready_feeds": len(ready_feeds)}
    )
    scheduler = FeedScheduler(
        ready_feed_ids=ready_feeds,
        feed_configs=settings.feeds,
        data_coordinator=data_coordinator,
    )

    return db_core, file_manager, feed_db, download_db, scheduler


async def default(settings: AppSettings) -> None:
    """Main async entry point for default mode.

    Initializes all components, runs state reconciliation, starts scheduler,
    and manages application lifecycle.

    Args:
        settings: Application settings object containing configuration.
    """
    logger.debug(
        "Starting Anypod in default mode.",
        extra={"config_file": str(settings.config_file)},
    )

    db_core: SqlalchemyCore | None = None
    scheduler: FeedScheduler | None = None
    try:
        (
            db_core,
            file_manager,
            feed_db,
            download_db,
            scheduler,
        ) = await _init(settings)

        # Create HTTP server with shutdown callback
        server = create_server(
            settings=settings,
            file_manager=file_manager,
            feed_database=feed_db,
            download_database=download_db,
            shutdown_callback=lambda: graceful_shutdown(scheduler, db_core),
        )

        # Create admin HTTP server (no shutdown callback to avoid double-close)
        admin_server = create_admin_server(
            settings=settings,
            file_manager=file_manager,
            feed_database=feed_db,
            download_database=download_db,
        )

        logger.info(
            "Starting scheduler and HTTP servers...",
            extra={
                "scheduled_feeds": scheduler.get_scheduled_feed_ids(),
                "server_host": settings.server_host,
                "server_port": settings.server_port,
                "admin_port": settings.admin_server_port,
            },
        )

        await scheduler.start()

        # Will gracefully shutdown on SIGINT/SIGTERM
        await asyncio.gather(server.serve(), admin_server.serve())
    except Exception as e:
        logger.error("Unexpected error during execution.", exc_info=e)
        await graceful_shutdown(scheduler, db_core)
