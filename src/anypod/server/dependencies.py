"""Dependency provider functions for FastAPI endpoints.

This module contains functions that retrieve dependencies from the
application state for use in FastAPI endpoints via the Depends system.
"""

from typing import Annotated

from fastapi import Depends, Request

from anypod.db.download_db import DownloadDatabase
from anypod.db.feed_db import FeedDatabase
from anypod.file_manager import FileManager
from anypod.rss import RSSFeedGenerator


def get_file_manager(request: Request) -> FileManager:
    """Retrieve file manager instance from app state.

    Args:
        request: The FastAPI request object.

    Returns:
        The file manager instance.
    """
    return request.app.state.file_manager


def get_rss_generator(request: Request) -> RSSFeedGenerator:
    """Retrieve RSS feed generator instance from app state.

    Args:
        request: The FastAPI request object.

    Returns:
        The RSS feed generator instance.
    """
    return request.app.state.rss_generator


def get_feed_database(request: Request) -> FeedDatabase:
    """Retrieve feed database instance from app state.

    Args:
        request: The FastAPI request object.

    Returns:
        The feed database instance.
    """
    return request.app.state.feed_database


def get_download_database(request: Request) -> DownloadDatabase:
    """Retrieve download database instance from app state.

    Args:
        request: The FastAPI request object.

    Returns:
        The download database instance.
    """
    return request.app.state.download_database


FileManagerDep = Annotated[FileManager, Depends(get_file_manager)]
RSSFeedGeneratorDep = Annotated[RSSFeedGenerator, Depends(get_rss_generator)]
FeedDatabaseDep = Annotated[FeedDatabase, Depends(get_feed_database)]
DownloadDatabaseDep = Annotated[DownloadDatabase, Depends(get_download_database)]
