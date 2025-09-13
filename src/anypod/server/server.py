"""HTTP server initialization and configuration for Anypod.

This module provides functions for creating and configuring the uvicorn HTTP server
with the FastAPI application and all necessary dependencies.
"""

from collections.abc import Awaitable, Callable
import logging

import uvicorn

from ..config import AppSettings
from ..db.download_db import DownloadDatabase
from ..db.feed_db import FeedDatabase
from ..file_manager import FileManager
from ..logging_config import LOGGING_CONFIG
from .app import create_admin_app, create_app

logger = logging.getLogger(__name__)


def create_server(
    settings: AppSettings,
    file_manager: FileManager,
    feed_database: FeedDatabase,
    download_database: DownloadDatabase,
    shutdown_callback: Callable[[], Awaitable[None]] | None = None,
) -> uvicorn.Server:
    """Create and configure a uvicorn HTTP server with FastAPI app.

    Args:
        settings: Application settings containing server configuration.
        rss_generator: The RSS feed generator instance.
        file_manager: The file manager instance.
        feed_database: The feed database instance.
        download_database: The download database instance.
        shutdown_callback: Optional callback to execute during shutdown.

    Returns:
        Configured uvicorn server ready to run.
    """
    logger.debug("Creating FastAPI application.")
    app = create_app(
        file_manager=file_manager,
        feed_database=feed_database,
        download_database=download_database,
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
) -> uvicorn.Server:
    """Create and configure a uvicorn HTTP server for the admin FastAPI app.

    Args:
        settings: Application settings containing admin server configuration.
        rss_generator: The RSS feed generator instance.
        file_manager: The file manager instance.
        feed_database: The feed database instance.
        download_database: The download database instance.

    Returns:
        Configured uvicorn server ready to run the admin app.
    """
    logger.debug("Creating FastAPI admin application.")
    app = create_admin_app(
        file_manager=file_manager,
        feed_database=feed_database,
        download_database=download_database,
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
