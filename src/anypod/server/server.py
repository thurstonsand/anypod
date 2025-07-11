"""HTTP server initialization and configuration for Anypod.

This module provides functions for creating and configuring the uvicorn HTTP server
with the FastAPI application and all necessary dependencies.
"""

import logging

import uvicorn

from ..config import AppSettings
from ..file_manager import FileManager
from ..rss import RSSFeedGenerator
from .app import create_app

logger = logging.getLogger(__name__)


def create_server(
    settings: AppSettings,
    rss_generator: RSSFeedGenerator,
    file_manager: FileManager,
) -> uvicorn.Server:
    """Create and configure a uvicorn HTTP server with FastAPI app.

    Args:
        settings: Application settings containing server configuration.
        rss_generator: The RSS feed generator instance.
        file_manager: The file manager instance.

    Returns:
        Configured uvicorn server ready to run.
    """
    logger.debug("Creating FastAPI application.")
    app = create_app(rss_generator=rss_generator, file_manager=file_manager)

    config = uvicorn.Config(
        app=app,
        host=settings.server_host,
        port=settings.server_port,
        log_level="info",
        access_log=False,  # We have our own logging middleware
        ws="none",  # We don't need websockets
        lifespan="off",  # We don't need a lifespan
        # TODO: make this configurable
        # proxy_headers=True,  # Honor X-Forwarded-For, X-Forwarded-Proto, etc.
        # forwarded_allow_ips=[],  # Allow requests from reverse proxy
        workers=2,  # We can run multiple workers to handle requests in parallel
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
