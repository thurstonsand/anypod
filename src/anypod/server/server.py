"""HTTP server initialization and configuration for Anypod.

This module provides functions for creating and configuring the uvicorn HTTP server
with the FastAPI application and all necessary dependencies.
"""

from collections.abc import Awaitable, Callable
import logging
from pathlib import Path

import uvicorn

from ..config import AppSettings, FeedConfig
from ..data_coordinator import DataCoordinator
from ..db.download_db import DownloadDatabase
from ..db.feed_db import FeedDatabase
from ..file_manager import FileManager
from ..logging_config import LOGGING_CONFIG
from ..manual_feed_runner import ManualFeedRunner
from ..manual_submission_service import ManualSubmissionService
from ..ytdlp_wrapper import YtdlpWrapper
from .app import create_admin_app, create_app

logger = logging.getLogger(__name__)


def create_server(
    settings: AppSettings,
    file_manager: FileManager,
    feed_database: FeedDatabase,
    download_database: DownloadDatabase,
    data_coordinator: DataCoordinator,
    ytdlp_wrapper: YtdlpWrapper,
    manual_feed_runner: ManualFeedRunner,
    manual_submission_service: ManualSubmissionService,
    feed_configs: dict[str, FeedConfig],
    cookies_path: Path | None,
    shutdown_callback: Callable[[], Awaitable[None]] | None = None,
) -> uvicorn.Server:
    """Create and configure a uvicorn HTTP server with FastAPI app.

    Args:
        settings: Application settings containing server configuration.
        file_manager: The file manager instance.
        feed_database: The feed database instance.
        download_database: The download database instance.
        data_coordinator: The data coordinator instance.
        ytdlp_wrapper: The yt-dlp wrapper instance.
        manual_feed_runner: Shared manual feed runner.
        manual_submission_service: Service for manual submission metadata lookups.
        feed_configs: The feed configurations.
        cookies_path: Path to cookies.txt file for authentication.
        shutdown_callback: Optional callback to execute during shutdown.

    Returns:
        Configured uvicorn server ready to run.
    """
    logger.debug("Creating FastAPI application.")
    app = create_app(
        file_manager=file_manager,
        feed_database=feed_database,
        download_database=download_database,
        feed_configs=feed_configs,
        data_coordinator=data_coordinator,
        ytdlp_wrapper=ytdlp_wrapper,
        manual_feed_runner=manual_feed_runner,
        manual_submission_service=manual_submission_service,
        cookies_path=cookies_path,
        shutdown_callback=shutdown_callback,
    )

    # Configure proxy settings based on trusted_proxies
    proxy_headers = settings.trusted_proxies is not None
    forwarded_allow_ips = settings.trusted_proxies or ["*"] if proxy_headers else None

    config = uvicorn.Config(
        app=app,
        host=settings.server_host,
        port=settings.server_port,
        log_config=LOGGING_CONFIG,  # Use our own logging configuration
        access_log=False,  # We have our own logging middleware
        ws="none",  # We don't need websockets
        lifespan="on",  # Enable lifespan for shutdown handling
        proxy_headers=proxy_headers,  # Honor X-Forwarded-For, X-Forwarded-Proto, etc.
        forwarded_allow_ips=forwarded_allow_ips,  # Allow requests from reverse proxy
    )
    server = uvicorn.Server(config)

    logger.debug(
        "HTTP server configured.",
        extra={
            "host": settings.server_host,
            "port": settings.server_port,
        },
    )

    return server


def create_admin_server(
    settings: AppSettings,
    file_manager: FileManager,
    feed_database: FeedDatabase,
    download_database: DownloadDatabase,
    data_coordinator: DataCoordinator,
    ytdlp_wrapper: YtdlpWrapper,
    manual_feed_runner: ManualFeedRunner,
    manual_submission_service: ManualSubmissionService,
    feed_configs: dict[str, FeedConfig],
    cookies_path: Path | None,
) -> uvicorn.Server:
    """Create and configure a uvicorn HTTP server for the admin FastAPI app.

    Args:
        settings: Application settings containing admin server configuration.
        file_manager: The file manager instance.
        feed_database: The feed database instance.
        download_database: The download database instance.
        data_coordinator: The data coordinator instance.
        ytdlp_wrapper: The yt-dlp wrapper instance.
        manual_feed_runner: Shared manual feed runner.
        manual_submission_service: Service for manual submission metadata lookups.
        feed_configs: The feed configurations.
        cookies_path: Path to cookies.txt file for authentication.

    Returns:
        Configured uvicorn server ready to run the admin app.
    """
    logger.debug("Creating FastAPI admin application.")
    app = create_admin_app(
        file_manager=file_manager,
        feed_database=feed_database,
        download_database=download_database,
        feed_configs=feed_configs,
        data_coordinator=data_coordinator,
        ytdlp_wrapper=ytdlp_wrapper,
        manual_feed_runner=manual_feed_runner,
        manual_submission_service=manual_submission_service,
        cookies_path=cookies_path,
    )

    config = uvicorn.Config(
        app=app,
        host=settings.server_host,
        port=settings.admin_server_port,
        log_config=LOGGING_CONFIG,  # Use our own logging configuration
        access_log=False,  # We have our own logging middleware
        ws="none",  # We don't need websockets
        lifespan="off",  # Disable signal handling - main server handles shutdown
        proxy_headers=False,  # We don't need proxy headers
    )
    server = uvicorn.Server(config)

    logger.debug(
        "Admin HTTP server configured.",
        extra={"host": settings.server_host, "port": settings.admin_server_port},
    )

    return server
