"""Dependency provider functions for FastAPI endpoints.

This module contains functions that retrieve dependencies from the
application state for use in FastAPI endpoints via the Depends system.
"""

from pathlib import Path
from typing import Annotated

from fastapi import Depends, Request

from anypod.config import FeedConfig
from anypod.data_coordinator import DataCoordinator
from anypod.db.download_db import DownloadDatabase
from anypod.db.feed_db import FeedDatabase
from anypod.file_manager import FileManager
from anypod.manual_feed_runner import ManualFeedRunner
from anypod.manual_submission_service import ManualSubmissionService
from anypod.ytdlp_wrapper import YtdlpWrapper


def get_file_manager(request: Request) -> FileManager:
    """Return the shared :class:`FileManager` from application state.

    Args:
        request: Incoming FastAPI request.

    Returns:
        File manager stored on ``app.state``.
    """
    return request.app.state.file_manager


def get_feed_database(request: Request) -> FeedDatabase:
    """Return the :class:`FeedDatabase` bound to the app.

    Args:
        request: Incoming FastAPI request.

    Returns:
        Feed database reference.
    """
    return request.app.state.feed_database


def get_download_database(request: Request) -> DownloadDatabase:
    """Return the :class:`DownloadDatabase` stored on ``app.state``.

    Args:
        request: Incoming FastAPI request.

    Returns:
        Download database reference.
    """
    return request.app.state.download_database


def get_feed_configs(request: Request) -> dict[str, FeedConfig]:
    """Return the configured feeds mapping.

    Args:
        request: Incoming FastAPI request.

    Returns:
        Mapping of feed IDs to :class:`FeedConfig`.
    """
    return request.app.state.feed_configs


def get_data_coordinator(request: Request) -> DataCoordinator:
    """Return the shared :class:`DataCoordinator` instance.

    Args:
        request: Incoming FastAPI request.

    Returns:
        DataCoordinator stored on ``app.state``.
    """
    return request.app.state.data_coordinator


def get_ytdlp_wrapper(request: Request) -> YtdlpWrapper:
    """Return the process-wide :class:`YtdlpWrapper`.

    Args:
        request: Incoming FastAPI request.

    Returns:
        Wrapper stored on ``app.state``.
    """
    return request.app.state.ytdlp_wrapper


def get_manual_feed_runner(request: Request) -> ManualFeedRunner:
    """Return the manual feed runner utility.

    Args:
        request: Incoming FastAPI request.

    Returns:
        Manual feed runner stored on ``app.state``.
    """
    return request.app.state.manual_feed_runner


def get_manual_submission_service(
    request: Request,
) -> ManualSubmissionService:
    """Return helper responsible for manual submission metadata fetches.

    Args:
        request: Incoming FastAPI request.

    Returns:
        ManualSubmissionService from ``app.state``.
    """
    return request.app.state.manual_submission_service


def get_cookies_path(request: Request) -> Path | None:
    """Return optional cookies path for yt-dlp operations.

    Args:
        request: Incoming FastAPI request.

    Returns:
        Path to ``cookies.txt`` or ``None`` when not configured.
    """
    return request.app.state.cookies_path


FileManagerDep = Annotated[FileManager, Depends(get_file_manager)]
# RSS feed serving no longer depends on RSSFeedGenerator; feeds are served from disk
FeedDatabaseDep = Annotated[FeedDatabase, Depends(get_feed_database)]
DownloadDatabaseDep = Annotated[DownloadDatabase, Depends(get_download_database)]
FeedConfigsDep = Annotated[dict[str, FeedConfig], Depends(get_feed_configs)]
DataCoordinatorDep = Annotated[DataCoordinator, Depends(get_data_coordinator)]
YtdlpWrapperDep = Annotated[YtdlpWrapper, Depends(get_ytdlp_wrapper)]
ManualFeedRunnerDep = Annotated[ManualFeedRunner, Depends(get_manual_feed_runner)]
ManualSubmissionServiceDep = Annotated[
    ManualSubmissionService,
    Depends(get_manual_submission_service),
]
CookiesPathDep = Annotated[Path | None, Depends(get_cookies_path)]
