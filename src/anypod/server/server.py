"""HTTP server initialization and configuration for Anypod.

This module provides functions for creating and configuring the uvicorn HTTP server
with the FastAPI application and all necessary dependencies.
"""

import logging

import uvicorn

from ..config import AppSettings
from ..db.download_db import DownloadDatabase
from ..db.feed_db import FeedDatabase
from ..file_manager import FileManager
from ..rss import RSSFeedGenerator
from .app import create_app

logger = logging.getLogger(__name__)


def create_server(
    settings: AppSettings,
    rss_generator: RSSFeedGenerator,
    file_manager: FileManager,
    feed_database: FeedDatabase,
    download_database: DownloadDatabase,
) -> uvicorn.Server:
    """Create and configure a uvicorn HTTP server with FastAPI app.

    Args:
        settings: Application settings containing server configuration.
        rss_generator: The RSS feed generator instance.
        file_manager: The file manager instance.
        feed_database: The feed database instance.
        download_database: The download database instance.

    Returns:
        Configured uvicorn server ready to run.
    """
    logger.debug("Creating FastAPI application.")
    app = create_app(
        rss_generator=rss_generator,
        file_manager=file_manager,
        feed_database=feed_database,
        download_database=download_database,
    )

    # Configure proxy settings based on trusted_proxies
    proxy_headers = settings.trusted_proxies is not None
    forwarded_allow_ips = settings.trusted_proxies or ["*"] if proxy_headers else None

    config = uvicorn.Config(
        app=app,
        host=settings.server_host,
        port=settings.server_port,
        log_level="info",
        access_log=False,  # We have our own logging middleware
        ws="none",  # We don't need websockets
        lifespan="off",  # We don't need a lifespan
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
