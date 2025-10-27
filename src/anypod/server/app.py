"""FastAPI application factory for Anypod HTTP server.

This module provides the factory function for creating and configuring
the FastAPI application instance with all necessary middleware and routers.
"""

from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
import logging
from pathlib import Path

from fastapi import FastAPI

# from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from ..config import FeedConfig
from ..data_coordinator import DataCoordinator
from ..db.download_db import DownloadDatabase
from ..db.feed_db import FeedDatabase
from ..file_manager import FileManager
from ..manual_feed_runner import ManualFeedRunner
from ..manual_submission_service import ManualSubmissionService
from ..ytdlp_wrapper import YtdlpWrapper
from .routers import admin, health, static

logger = logging.getLogger(__name__)


class LoggingMiddleware(BaseHTTPMiddleware):
    """Middleware to log HTTP requests and responses."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Process request and log details.

        Args:
            request: The incoming HTTP request.
            call_next: The next middleware or endpoint to call.

        Returns:
            The HTTP response.
        """
        logger.debug(
            "HTTP request received",
            extra={
                "method": request.method,
                "path": request.url.path,
                "client": request.client.host if request.client else None,
            },
        )

        response = await call_next(request)

        logger.debug(
            "HTTP response sent",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
            },
        )

        return response


def create_app(
    file_manager: FileManager,
    feed_database: FeedDatabase,
    download_database: DownloadDatabase,
    feed_configs: dict[str, FeedConfig],
    data_coordinator: DataCoordinator,
    ytdlp_wrapper: YtdlpWrapper,
    manual_feed_runner: ManualFeedRunner,
    manual_submission_service: ManualSubmissionService,
    cookies_path: Path | None = None,
    shutdown_callback: Callable[[], Awaitable[None]] | None = None,
) -> FastAPI:
    """Create and configure a FastAPI application instance.

    Creates a FastAPI app with necessary middleware, routers, and configuration
    for serving Anypod's HTTP endpoints.

    Args:
        file_manager: The file manager instance.
        feed_database: The feed database instance.
        download_database: The download database instance.
        feed_configs: The feed configurations.
        data_coordinator: The data coordinator instance.
        ytdlp_wrapper: The yt-dlp wrapper instance.
        manual_feed_runner: The manual feed runner instance.
        manual_submission_service: Service for manual submission metadata fetches.
        cookies_path: Path to cookies.txt file for authentication.
        shutdown_callback: Optional callback function for graceful shutdown.

    Returns:
        Configured FastAPI application instance.
    """

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
        """Handle application lifespan events."""
        try:
            yield
        finally:
            if shutdown_callback:
                await shutdown_callback()

    app = FastAPI(
        title="Anypod",
        description="Thin yt-dlp -> podcast solution",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Add CORS middleware with permissive settings for development
    # TODO: Enable CORS middleware when admin dashboard API is implemented
    # app.add_middleware(
    #     CORSMiddleware,
    #     allow_origins=["http://localhost:3000"],  # Local admin dashboard only
    #     allow_credentials=False,
    #     allow_methods=["GET", "POST", "PUT", "DELETE"],
    #     allow_headers=["Accept", "Accept-Language", "Content-Type"],
    # )

    # Add custom logging middleware
    app.add_middleware(LoggingMiddleware)

    # Attach dependencies to app state
    app.state.file_manager = file_manager
    app.state.feed_database = feed_database
    app.state.download_database = download_database
    app.state.feed_configs = feed_configs
    app.state.data_coordinator = data_coordinator
    app.state.ytdlp_wrapper = ytdlp_wrapper
    app.state.manual_feed_runner = manual_feed_runner
    app.state.manual_submission_service = manual_submission_service
    app.state.cookies_path = cookies_path

    # Include public routers
    app.include_router(static.router, tags=["static"])
    app.include_router(health.router, tags=["health"])  # /api/health on public server

    logger.debug("FastAPI application created successfully")

    return app


def create_admin_app(
    file_manager: FileManager,
    feed_database: FeedDatabase,
    download_database: DownloadDatabase,
    feed_configs: dict[str, FeedConfig],
    data_coordinator: DataCoordinator,
    ytdlp_wrapper: YtdlpWrapper,
    manual_feed_runner: ManualFeedRunner,
    manual_submission_service: ManualSubmissionService,
    cookies_path: Path | None = None,
) -> FastAPI:
    """Create and configure the admin FastAPI application instance.

    The admin app exposes private administration endpoints and should be bound
    to a private interface/port (e.g., 127.0.0.1). It intentionally includes
    only the admin router.

    Args:
        rss_generator: The RSS feed generator instance.
        file_manager: The file manager instance.
        feed_database: The feed database instance.
        download_database: The download database instance.
        feed_configs: The feed configurations.
        data_coordinator: The data coordinator instance.
        ytdlp_wrapper: The yt-dlp wrapper instance.
        manual_feed_runner: The manual feed runner instance.
        manual_submission_service: Service for manual submission metadata fetches.
        cookies_path: Path to cookies.txt file for authentication.

    Returns:
        Configured FastAPI application instance for admin APIs.
    """
    app = FastAPI(
        title="Anypod Admin",
        description="Private admin API for Anypod",
        version="0.1.0",
    )

    # Reuse logging middleware for consistency
    app.add_middleware(LoggingMiddleware)

    # Attach dependencies to app state
    app.state.file_manager = file_manager
    app.state.feed_database = feed_database
    app.state.download_database = download_database
    app.state.feed_configs = feed_configs
    app.state.data_coordinator = data_coordinator
    app.state.ytdlp_wrapper = ytdlp_wrapper
    app.state.manual_feed_runner = manual_feed_runner
    app.state.manual_submission_service = manual_submission_service
    app.state.cookies_path = cookies_path

    # Include admin and health routers
    app.include_router(admin.router, tags=["admin"])
    app.include_router(health.router, tags=["health"])

    logger.debug("FastAPI admin application created successfully")
    return app
