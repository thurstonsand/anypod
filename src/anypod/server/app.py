"""FastAPI application factory for Anypod HTTP server.

This module provides the factory function for creating and configuring
the FastAPI application instance with all necessary middleware and routers.
"""

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from ..file_manager import FileManager
from ..rss import RSSFeedGenerator
from .routers import health, static

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
    rss_generator: RSSFeedGenerator,
    file_manager: FileManager,
) -> FastAPI:
    """Create and configure a FastAPI application instance.

    Creates a FastAPI app with necessary middleware, routers, and configuration
    for serving Anypod's HTTP endpoints.

    Args:
        rss_generator: The RSS feed generator instance.
        file_manager: The file manager instance.
        **kwargs: Additional arguments passed to the FastAPI constructor.

    Returns:
        Configured FastAPI application instance.
    """
    app = FastAPI(
        title="Anypod",
        description="Thin yt-dlp -> podcast solution",
        version="0.1.0",
    )

    # Add CORS middleware with permissive settings for development
    # TODO: Tighten CORS settings for production
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Add custom logging middleware
    app.add_middleware(LoggingMiddleware)

    # Attach dependencies to app state
    app.state.rss_generator = rss_generator
    app.state.file_manager = file_manager

    # Include routers
    app.include_router(health.router, tags=["health"])
    app.include_router(static.router, tags=["static"])

    logger.debug("FastAPI application created successfully")

    return app
