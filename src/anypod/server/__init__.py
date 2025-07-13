"""HTTP server module for Anypod.

This module provides the FastAPI-based HTTP server implementation
for serving RSS feeds, media files, and API endpoints.
"""

from .server import create_server

__all__ = ["create_server"]
