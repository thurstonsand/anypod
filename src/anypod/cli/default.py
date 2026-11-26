"""Default mode implementation for Anypod.

This module provides the default execution mode that initializes all components,
runs state reconciliation, starts the scheduler, and manages the application lifecycle.
"""

import asyncio
import logging
import os
from pathlib import Path
from urllib.parse import urlparse

from alembic.config import Config as AlembicConfig

from alembic import command as alembic_command

from ..config import AppSettings, MediaConversionPreset
from ..data_coordinator import DataCoordinator, Downloader, Enqueuer, Pruner
from ..db import AppStateDatabase, DownloadDatabase, FeedDatabase
from ..db.sqlalchemy_core import SqlalchemyCore
from ..exceptions import (
    ConfigLoadError,
    DatabaseOperationError,
    StateReconciliationError,
)
from ..ffmpeg import FFmpeg
from ..ffprobe import FFProbe
from ..file_manager import FileManager
from ..image_downloader import ImageDownloader
from ..manual_feed_runner import ManualFeedRunner
from ..manual_submission_service import ManualSubmissionService
from ..media_processor import MediaProcessor
from ..path_manager import PathManager
from ..rss import RSSFeedGenerator
from ..schedule import FeedScheduler
from ..server import create_admin_server, create_server
from ..state_reconciler import StateReconciler
from ..ytdlp_wrapper import YtdlpWrapper
from ..ytdlp_wrapper.handlers import HandlerSelector

logger = logging.getLogger(__name__)


def _find_alembic_ini() -> Path | None:
    """Locate alembic.ini in the working directory or project root."""
    candidates = [
        Path.cwd() / "alembic.ini",
        Path(__file__).resolve().parents[3] / "alembic.ini",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _run_migrations_sync(db_dir: Path) -> None:
    """Run Alembic migrations synchronously against the SQLite database."""
    alembic_ini = _find_alembic_ini()
    if alembic_ini is None:
        logger.debug("alembic.ini not found; skipping automatic startup migrations.")
        return

    db_path = (db_dir / "anypod.db").resolve()
    alembic_cfg = AlembicConfig(str(alembic_ini))
    alembic_cfg.set_main_option("script_location", str(alembic_ini.parent / "alembic"))
    alembic_cfg.attributes["configure_logger"] = False
    alembic_cfg.attributes["database_url"] = f"sqlite:///{db_path}"
    alembic_command.upgrade(alembic_cfg, "head")


def _validate_runtime_paths(settings: AppSettings) -> None:
    """Validate runtime-only filesystem paths and emit helpful startup warnings."""
    if settings.cookies_path is not None:
        if settings.cookies_path.is_dir():
            raise ConfigLoadError(
                f"COOKIES_PATH '{settings.cookies_path}' is a directory, not a file. "
                "If using Docker, make sure the cookies file exists on the host before "
                "starting the container so Docker does not auto-create an empty directory.",
                config_file=str(settings.cookies_path),
            )
        if not settings.cookies_path.is_file():
            raise ConfigLoadError(
                f"COOKIES_PATH '{settings.cookies_path}' does not exist or is not a regular file.",
                config_file=str(settings.cookies_path),
            )

    custom_feeds = [
        fid
        for fid, cfg in settings.feeds.items()
        if cfg.enabled and cfg.media_conversion == MediaConversionPreset.CUSTOM
    ]
    if custom_feeds:
        script_path = settings.effective_custom_conversion_script
        if not script_path.is_file():
            raise ConfigLoadError(
                f"Feeds {custom_feeds} use 'media_conversion: custom', but hook script "
                f"'{script_path}' does not exist. Create it or set MEDIA_CONVERSION_SCRIPT.",
                config_file=str(script_path),
            )
        if not os.access(script_path, os.X_OK):
            raise ConfigLoadError(
                f"Custom media conversion hook script '{script_path}' is not executable "
                "(run chmod +x).",
                config_file=str(script_path),
            )

    parsed_base = urlparse(settings.base_url)
    if parsed_base.hostname in ("localhost", "127.0.0.1", "::1"):
        logger.warning(
            "BASE_URL points to a loopback address (%s). RSS enclosures and artwork URLs "
            "will not be reachable from external podcast apps (and apps like Pocket Casts "
            "cache feed URLs permanently). Set BASE_URL to your reachable URL.",
            settings.base_url,
            extra={"base_url": settings.base_url},
        )


async def graceful_shutdown(
    scheduler: FeedScheduler | None,
    manual_feed_runner: ManualFeedRunner | None,
    db_core: SqlalchemyCore | None,
    reconcile_task: asyncio.Task[None] | None = None,
) -> None:
    """Perform graceful shutdown of all components in correct order.

    Args:
        scheduler: The feed scheduler instance to shutdown.
        manual_feed_runner: The manual feed runner instance to shutdown.
        db_core: The database core instance to close.
        reconcile_task: Optional background startup reconciliation task to cancel.
    """
    logger.info("Shutdown signal received.")

    # Step 0: Cancel background startup reconciliation if still running
    if reconcile_task is not None and not reconcile_task.done():
        reconcile_task.cancel()
        try:
            await reconcile_task
        except asyncio.CancelledError:
            logger.info("Background startup reconciliation cancelled.")
        except Exception as e:
            logger.error("Error cancelling startup reconciliation.", exc_info=e)

    # Step 1: Stop scheduler (finish current jobs, no new ones)
    if scheduler:
        try:
            await scheduler.stop(wait_for_jobs=True)
            logger.info("Scheduler shutdown completed.")
        except Exception as e:
            logger.error("Error shutting down scheduler.", exc_info=e)

    # Step 2: Cancel manual feed tasks
    if manual_feed_runner:
        try:
            await manual_feed_runner.shutdown()
            logger.info("Manual feed runner shutdown completed.")
        except Exception as e:
            logger.error("Error shutting down manual feed runner.", exc_info=e)

    # Step 3: Close database connections
    if db_core:
        try:
            await db_core.close()
            logger.info("Database connections closed.")
        except Exception as e:
            logger.error("Error closing database connections.", exc_info=e)

    logger.info("Anypod shutdown completed.")


async def _reconcile_and_start_scheduler(
    state_reconciler: StateReconciler,
    scheduler: FeedScheduler,
    settings: AppSettings,
    known_scheduled_feeds: list[str],
) -> None:
    """Run upstream startup reconciliation in the background and start the scheduler.

    Runs after the HTTP server has started listening so health checks and
    cached RSS/media endpoints are immediately available. Feeds that were
    already initialized in SQLite remain scheduled (on their next cron tick)
    even if upstream metadata refresh transiently fails during startup.

    Args:
        state_reconciler: StateReconciler for upstream feed reconciliation.
        scheduler: FeedScheduler to register jobs on and start.
        settings: Application settings containing feed configs and cookies path.
        known_scheduled_feeds: Enabled, non-manual feed IDs already present in SQLite.
    """
    ready_feeds: list[str] = []
    try:
        ready_feeds = await state_reconciler.reconcile_startup_state(
            settings.feeds, settings.cookies_path
        )
    except StateReconciliationError as e:
        logger.error(
            "Background state reconciliation encountered a critical error; "
            "existing feeds will still be scheduled for future ticks.",
            exc_info=e,
        )
    except Exception as e:
        logger.error(
            "Unexpected error during background state reconciliation.",
            exc_info=e,
        )

    ready_set = set(ready_feeds)
    for feed_id in ready_feeds:
        scheduler.schedule_feed(feed_id, settings.feeds[feed_id], run_immediately=True)

    # Preserve cron scheduling for already-known DB feeds whose startup fetch
    # failed transiently, so they self-heal on their next cron tick.
    for feed_id in known_scheduled_feeds:
        if feed_id not in ready_set and feed_id in settings.feeds:
            feed_cfg = settings.feeds[feed_id]
            if feed_cfg.enabled and not feed_cfg.is_manual:
                logger.warning(
                    "Startup reconciliation failed for existing feed; scheduling for "
                    "next cron tick without immediate run.",
                    extra={"feed_id": feed_id},
                )
                scheduler.schedule_feed(feed_id, feed_cfg, run_immediately=False)

    manual_feed_ids = [
        fid for fid, cfg in settings.feeds.items() if cfg.enabled and cfg.is_manual
    ]
    scheduled_ids = scheduler.get_scheduled_feed_ids()
    if not scheduled_ids and manual_feed_ids:
        logger.info(
            "All enabled feeds are manual; scheduler will stay idle until manual submissions arrive.",
            extra={"manual_feeds": manual_feed_ids},
        )
    elif not scheduled_ids and not manual_feed_ids:
        logger.warning(
            "No enabled feeds ready after startup reconciliation. "
            "HTTP server remains running; check feed configuration or upstream errors above.",
            extra={"configured_feeds": len(settings.feeds)},
        )

    await scheduler.start()
    logger.info(
        "Background reconciliation finished and scheduler started.",
        extra={"scheduled_feeds": scheduled_ids},
    )


async def _init(
    settings: AppSettings,
) -> tuple[
    SqlalchemyCore,
    FileManager,
    FeedDatabase,
    DownloadDatabase,
    FeedScheduler,
    DataCoordinator,
    YtdlpWrapper,
    ManualFeedRunner,
    ManualSubmissionService,
    StateReconciler,
    list[str],
]:
    _validate_runtime_paths(settings)

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
    await asyncio.to_thread(_run_migrations_sync, db_dir)
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
        ffprobe=ffprobe,
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
    media_processor = MediaProcessor(
        ffmpeg=ffmpeg,
        ffprobe=ffprobe,
        paths=path_manager,
        custom_script_path=settings.effective_custom_conversion_script,
    )
    downloader = Downloader(
        download_db=download_db,
        file_manager=file_manager,
        ytdlp_wrapper=ytdlp_wrapper,
        ffprobe=ffprobe,
        ffmpeg=ffmpeg,
        media_processor=media_processor,
    )
    pruner = Pruner(feed_db=feed_db, download_db=download_db, file_manager=file_manager)

    data_coordinator = DataCoordinator(
        enqueuer=enqueuer,
        downloader=downloader,
        pruner=pruner,
        rss_generator=rss_generator,
        download_db=download_db,
        feed_db=feed_db,
        cookies_path=settings.cookies_path,
    )

    # Phase 1: fast local-only reconciliation before binding HTTP servers
    state_reconciler = StateReconciler(
        file_manager=file_manager,
        image_downloader=image_downloader,
        feed_db=feed_db,
        download_db=download_db,
        ytdlp_wrapper=ytdlp_wrapper,
        downloader=downloader,
        pruner=pruner,
    )
    known_scheduled_feeds = await state_reconciler.reconcile_local_state(settings.feeds)

    # Initialize scheduler (feeds will be registered by background reconciliation)
    feed_semaphore = asyncio.Semaphore(1)
    manual_feed_runner = ManualFeedRunner(
        data_coordinator=data_coordinator,
        feed_configs=settings.feeds,
        feed_semaphore=feed_semaphore,
    )
    manual_submission_service = ManualSubmissionService(ytdlp_wrapper)
    scheduler = FeedScheduler(
        ready_feed_ids=[],
        feed_configs=settings.feeds,
        data_coordinator=data_coordinator,
        feed_semaphore=feed_semaphore,
        timezone=settings.effective_timezone,
    )
    scheduler.schedule_maintenance_job(
        job_id="yt_dlp_update",
        interval=settings.yt_dlp_update_freq,
        callback=ytdlp_wrapper.update_yt_dlp,
        run_immediately=True,
    )

    return (
        db_core,
        file_manager,
        feed_db,
        download_db,
        scheduler,
        data_coordinator,
        ytdlp_wrapper,
        manual_feed_runner,
        manual_submission_service,
        state_reconciler,
        known_scheduled_feeds,
    )


async def default(settings: AppSettings) -> None:
    """Main async entry point for default mode.

    Initializes local state and HTTP servers immediately, then runs upstream
    feed reconciliation and starts the scheduler in the background so startup
    never blocks health checks or cached RSS/media serving.

    Args:
        settings: Application settings object containing configuration.
    """
    logger.debug(
        "Starting Anypod in default mode.",
        extra={"config_file": str(settings.config_file)},
    )

    db_core: SqlalchemyCore | None = None
    scheduler: FeedScheduler | None = None
    manual_feed_runner: ManualFeedRunner | None = None
    reconcile_task: asyncio.Task[None] | None = None
    try:
        (
            db_core,
            file_manager,
            feed_db,
            download_db,
            scheduler,
            data_coordinator,
            ytdlp_wrapper,
            manual_feed_runner,
            manual_submission_service,
            state_reconciler,
            known_scheduled_feeds,
        ) = await _init(settings)

        # Create HTTP server with shutdown callback
        server = create_server(
            settings=settings,
            file_manager=file_manager,
            feed_database=feed_db,
            download_database=download_db,
            data_coordinator=data_coordinator,
            ytdlp_wrapper=ytdlp_wrapper,
            manual_feed_runner=manual_feed_runner,
            manual_submission_service=manual_submission_service,
            feed_configs=settings.feeds,
            cookies_path=settings.cookies_path,
            shutdown_callback=lambda: graceful_shutdown(
                scheduler, manual_feed_runner, db_core, reconcile_task
            ),
            include_admin=settings.single_server_mode,
        )

        servers = [server]
        log_extra: dict[str, object] = {
            "configured_feeds": len(settings.feeds),
            "server_host": settings.server_host,
            "server_port": settings.server_port,
        }

        if settings.single_server_mode:
            logger.warning(
                "Single-server mode enabled: admin APIs are exposed on the public server port. "
                "Ensure admin routes are protected at the infrastructure level (e.g., Cloudflare Access).",
                extra={
                    "server_host": settings.server_host,
                    "server_port": settings.server_port,
                },
            )
        else:
            admin_server = create_admin_server(
                settings=settings,
                file_manager=file_manager,
                feed_database=feed_db,
                download_database=download_db,
                data_coordinator=data_coordinator,
                ytdlp_wrapper=ytdlp_wrapper,
                manual_feed_runner=manual_feed_runner,
                manual_submission_service=manual_submission_service,
                feed_configs=settings.feeds,
                cookies_path=settings.cookies_path,
            )
            servers.append(admin_server)
            log_extra["admin_port"] = settings.admin_server_port

        logger.info(
            "Starting HTTP server and background state reconciliation.",
            extra=log_extra,
        )

        # Log actionable startup summary with RSS and admin URLs
        enabled_feed_ids = [fid for fid, cfg in settings.feeds.items() if cfg.enabled]
        summary_paths = PathManager(
            base_data_dir=settings.data_dir, base_url=settings.base_url
        )
        for fid in enabled_feed_ids:
            logger.info(
                "Feed RSS available at %s",
                summary_paths.feed_url(fid),
                extra={"feed_id": fid, "rss_url": summary_paths.feed_url(fid)},
            )
        if not settings.single_server_mode:
            logger.info(
                "Admin API docs available at http://%s:%d/docs",
                settings.server_host,
                settings.admin_server_port,
            )

        # Phase 2: Run upstream reconciliation + scheduler start asynchronously
        reconcile_task = asyncio.create_task(
            _reconcile_and_start_scheduler(
                state_reconciler,
                scheduler,
                settings,
                known_scheduled_feeds,
            )
        )

        # Will gracefully shutdown on SIGINT/SIGTERM
        await asyncio.gather(*(s.serve() for s in servers))
    except Exception as e:
        logger.error("Unexpected error during execution.", exc_info=e)
        await graceful_shutdown(scheduler, manual_feed_runner, db_core, reconcile_task)
