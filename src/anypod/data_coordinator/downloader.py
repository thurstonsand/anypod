"""Handles the downloading of media files for queued items.

This module defines the Downloader class, which is responsible for processing
downloads marked as 'queued' in the database. It interacts with the YtdlpWrapper
to fetch media, the FileManager to handle file storage, and the DatabaseManager
to update download statuses and metadata.
"""

import logging
from pathlib import Path
from typing import Any

from ..config import FeedConfig
from ..db import DatabaseManager, Download, DownloadStatus
from ..exceptions import (
    DatabaseOperationError,
    DownloaderError,
    DownloadNotFoundError,
    YtdlpApiError,
)
from ..file_manager import FileManager
from ..ytdlp_wrapper import YtdlpWrapper

logger = logging.getLogger(__name__)


class Downloader:
    """Orchestrates the download process for media items.

    The Downloader retrieves queued download items, manages the download process
    using YtdlpWrapper, handles file system operations via FileManager (including
    temporary file management and moving to final storage), and updates the
    database via DatabaseManager upon success or failure.
    """

    def __init__(
        self,
        db_manager: DatabaseManager,
        file_manager: FileManager,
        ytdlp_wrapper: YtdlpWrapper,
    ):
        self.db_manager = db_manager
        self.file_manager = file_manager
        self.ytdlp_wrapper = ytdlp_wrapper
        logger.debug("Downloader initialized.")

    def _handle_download_success(
        self, download: Download, downloaded_file_path: Path
    ) -> None:
        """Process a successfully downloaded file.

        Moves the file to permanent storage and updates its database record to DOWNLOADED,
        including its final extension and filesize.

        Args:
            download: The Download object.
            downloaded_file_path: Path to the successfully downloaded (temporary) file.

        Raises:
            DownloaderError: If moving the file or updating the database fails.
        """
        log_params: dict[str, Any] = {
            "feed_id": download.feed,
            "download_id": download.id,
            "downloaded_file_path": downloaded_file_path,
        }
        logger.info("Download successful, processing file.", extra=log_params)

        try:
            self.db_manager.mark_as_downloaded(
                feed=download.feed,
                id=download.id,
                ext=downloaded_file_path.suffix.lstrip("."),
                filesize=downloaded_file_path.stat().st_size,
            )
        except (DownloadNotFoundError, DatabaseOperationError) as e:
            raise DownloaderError(
                message="Failed to update database record to DOWNLOADED.",
                feed_id=download.feed,
                download_id=download.id,
            ) from e
        logger.info("Database record updated to DOWNLOADED.", extra=log_params)

    def _handle_download_failure(
        self, download: Download, feed_config: FeedConfig, error: Exception
    ) -> None:
        """Handle failures during the download process for a single item.

        Logs the error and bumps the retry count for the download in the database.

        Args:
            download: The Download object that failed.
            feed_config: The configuration for the feed.
            error: The exception that occurred.
        """
        logger.error(
            "Could not complete download.",
            exc_info=error,
            extra={
                "feed_id": download.feed,
                "download_id": download.id,
            },
        )
        try:
            self.db_manager.bump_retries(
                feed_id=download.feed,
                download_id=download.id,
                error_message=str(error),
                max_allowed_errors=feed_config.max_errors,
            )
        except (DownloadNotFoundError, DatabaseOperationError) as e:
            logger.error(
                "Failed to bump retries.",
                exc_info=e,
                extra={"feed_id": download.feed, "download_id": download.id},
            )

    def _process_single_download(
        self, download_to_process: Download, feed_config: FeedConfig
    ) -> None:
        """Manage the download lifecycle for a single Download object.

        This includes attempting the download via `YtdlpWrapper`, and then
        handling success or failure.

        Args:
            download_to_process: The Download object to process.
            feed_config: The configuration for the feed.

        Raises:
            DownloaderError: If a step in the download process fails critically
                             (e.g., ytdlp error, file move error, DB update error).
        """
        log_params: dict[str, Any] = {
            "feed_id": download_to_process.feed,
            "download_id": download_to_process.id,
        }
        logger.info("Processing single download.", extra=log_params)

        try:
            downloaded_file_path = self.ytdlp_wrapper.download_media_to_file(
                download_to_process,
                feed_config.yt_args,
            )
            self._handle_download_success(download_to_process, downloaded_file_path)
        except YtdlpApiError as e:
            raise DownloaderError(
                message="Failed to download media to file.",
                feed_id=download_to_process.feed,
                download_id=download_to_process.id,
            ) from e

    # TODO: do i need to think about race conditions for retrieve/modify/update?
    def download_queued(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        limit: int = -1,
    ) -> tuple[int, int]:
        """Process and download media items in 'queued' status for a specific feed.

        Retrieves 'queued' Download objects from the database. For each item:
        1. It attempts to download the media content using yt-dlp,
           saving it to a temporary location.
        2. If successful, the media file is moved to permanent storage via `FileManager`,
           and the database record is updated (status, ext, filesize) via `DatabaseManager`.
        3. If any step fails, the Download's status is managed by `DatabaseManager.bump_retries`,
           logging the error and incrementing its retry count.
        4. Temporary files are cleaned up regardless of the outcome.

        Args:
            feed_id: The unique identifier for the feed whose queued items are to be processed.
            feed_config: The configuration object for the feed, containing yt-dlp arguments
                         and max error count.
            limit: The maximum number of queued items to process. If -1 (default),
                   processes all queued items for the feed.

        Returns:
            A tuple (success_count, failure_count).

        Raises:
            DownloaderError: If fetching queued items from the database fails.
        """
        log_params: dict[str, Any] = {
            "feed_id": feed_id,
            "feed_url": feed_config.url,
        }
        logger.info(
            "Starting download_queued process.",
            extra=log_params,
        )
        success_count = 0
        failure_count = 0

        try:
            queued_downloads = self.db_manager.get_downloads_by_status(
                DownloadStatus.QUEUED,
                feed_id,
                limit,
            )
        except DatabaseOperationError as e:
            raise DownloaderError(
                message="Failed to fetch queued downloads from database.",
                feed_id=feed_id,
            ) from e

        if not queued_downloads:
            logger.info("No queued downloads found for feed.", extra=log_params)
            return 0, 0

        logger.info(
            "Found queued items for feed. Processing...",
            extra={**log_params, "num_queued": len(queued_downloads)},
        )

        for download in queued_downloads:
            try:
                self._process_single_download(download, feed_config)
                success_count += 1
            except DownloaderError as e:
                logger.error(
                    "Failed to process download.",
                    exc_info=e,
                    extra=log_params,
                )
                failure_count += 1

        logger.info(
            "Finished processing queued downloads.",
            extra={
                **log_params,
                "success_count": success_count,
                "failure_count": failure_count,
            },
        )
        return success_count, failure_count
