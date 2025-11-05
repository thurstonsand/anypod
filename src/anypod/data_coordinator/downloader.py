"""Handles the downloading of media files for queued items.

This module defines the Downloader class, which is responsible for processing
downloads marked as 'queued' in the database. It interacts with the YtdlpWrapper
to fetch media, the FileManager to handle file storage, and the DownloadDatabase
to update download statuses and metadata.
"""

import logging
from pathlib import Path
from typing import Any

import aiofiles.os

from ..config import FeedConfig
from ..db.download_db import DownloadDatabase
from ..db.types import Download, DownloadStatus
from ..exceptions import (
    DatabaseOperationError,
    DownloadError,
    DownloadNotFoundError,
    FileOperationError,
    YtdlpApiError,
)
from ..file_manager import FileManager
from ..ytdlp_wrapper import YtdlpWrapper

logger = logging.getLogger(__name__)


class Downloader:
    """Orchestrate the download process for media items.

    The Downloader retrieves queued download items, manages the download process
    using YtdlpWrapper, handles file system operations via FileManager (including
    temporary file management and moving to final storage), and updates the
    database via DownloadDatabase upon success or failure.

    Attributes:
        download_db: Database manager for download record operations.
        file_manager: File manager for file system operations.
        ytdlp_wrapper: Wrapper for yt-dlp media download operations.
    """

    def __init__(
        self,
        download_db: DownloadDatabase,
        file_manager: FileManager,
        ytdlp_wrapper: YtdlpWrapper,
    ):
        self.download_db = download_db
        self.file_manager = file_manager
        self.ytdlp_wrapper = ytdlp_wrapper
        logger.debug("Downloader initialized.")

    async def _handle_download_success(
        self, download: Download, downloaded_file_path: Path
    ) -> None:
        """Process a successfully downloaded file.

        Moves the file to permanent storage and updates its database record to DOWNLOADED,
        including its final extension and filesize.

        Args:
            download: The Download object.
            downloaded_file_path: Path to the successfully downloaded (temporary) file.

        Raises:
            DownloadError: If moving the file or updating the database fails.
        """
        log_params: dict[str, Any] = {
            "feed_id": download.feed_id,
            "download_id": download.id,
            "downloaded_file_path": downloaded_file_path,
        }
        logger.debug("Download successful, processing file.", extra=log_params)

        try:
            file_stat = await aiofiles.os.stat(downloaded_file_path)
            await self.download_db.mark_as_downloaded(
                feed_id=download.feed_id,
                download_id=download.id,
                ext=downloaded_file_path.suffix.lstrip("."),
                filesize=file_stat.st_size,
            )
        except (DownloadNotFoundError, DatabaseOperationError) as e:
            raise DownloadError(
                message="Failed to update database record to DOWNLOADED.",
                feed_id=download.feed_id,
                download_id=download.id,
            ) from e
        # If a thumbnail was saved by yt-dlp, persist its extension
        # Check for existence rather than assuming success
        try:
            has_thumb = await self.file_manager.image_exists(
                download.feed_id, download.id, "jpg"
            )
        except FileOperationError as e:
            raise DownloadError(
                message="Failed to check for thumbnail.",
                feed_id=download.feed_id,
                download_id=download.id,
            ) from e
        else:
            if has_thumb:
                await self.download_db.set_thumbnail_extension(
                    download.feed_id, download.id, "jpg"
                )
        logger.info("Successfully downloaded media.", extra=log_params)

    async def _handle_download_failure(
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
                "feed_id": download.feed_id,
                "download_id": download.id,
            },
        )
        try:
            await self.download_db.bump_retries(
                feed_id=download.feed_id,
                download_id=download.id,
                error_message=str(error),
                max_allowed_errors=feed_config.max_errors,
            )
        except (DownloadNotFoundError, DatabaseOperationError) as e:
            logger.error(
                "Failed to bump retries.",
                exc_info=e,
                extra={"feed_id": download.feed_id, "download_id": download.id},
            )

    async def _persist_download_logs(self, download: Download, logs: str) -> None:
        """Persist yt-dlp logs for a download when available."""
        log_params = {"feed_id": download.feed_id, "download_id": download.id}
        try:
            await self.download_db.set_download_logs(
                feed_id=download.feed_id,
                download_id=download.id,
                logs=logs,
            )
        except DownloadNotFoundError as e:
            logger.warning(
                "Download disappeared before logs could be stored.",
                extra=log_params,
                exc_info=e,
            )
        except DatabaseOperationError as e:
            logger.warning(
                "Failed to store yt-dlp logs for download.",
                extra=log_params,
                exc_info=e,
            )

    async def _process_single_download(
        self,
        download_to_process: Download,
        feed_config: FeedConfig,
        cookies_path: Path | None = None,
    ) -> None:
        """Manage the download lifecycle for a single Download object.

        This includes re-fetching metadata to check for updates, attempting
        the download via `YtdlpWrapper`, and then handling success or failure.

        Args:
            download_to_process: The Download object to process.
            feed_config: The configuration for the feed.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

        Raises:
            DownloadError: If a step in the download process fails critically
                             (e.g., ytdlp error, file move error, DB update error).
        """
        log_params: dict[str, Any] = {
            "feed_id": download_to_process.feed_id,
            "download_id": download_to_process.id,
        }
        logger.debug("Processing single download.", extra=log_params)

        try:
            (
                downloaded_file_path,
                download_logs,
            ) = await self.ytdlp_wrapper.download_media_to_file(
                download_to_process,
                feed_config.yt_args,
                cookies_path=cookies_path,
            )
        except YtdlpApiError as e:
            await self._persist_download_logs(download_to_process, e.logs or "")
            raise DownloadError(
                message="Failed to download media to file.",
                feed_id=download_to_process.feed_id,
                download_id=download_to_process.id,
            ) from e

        await self._persist_download_logs(download_to_process, download_logs)
        await self._handle_download_success(download_to_process, downloaded_file_path)

    # TODO: do i need to think about race conditions for retrieve/modify/update?
    async def download_queued(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        cookies_path: Path | None = None,
        limit: int = -1,
    ) -> tuple[int, int]:
        """Process and download media items in 'queued' status for a feed.

        Retrieves 'queued' Download objects from the database. For each item:
        1. It attempts to download the media content using yt-dlp,
           saving it to a temporary location.
        2. If successful, the media file is moved to permanent storage via `FileManager`,
           and the database record is updated (status, ext, filesize) via `DownloadDatabase`.
        3. If any step fails, the Download's status is managed by `DownloadDatabase.bump_retries`,
           logging the error and incrementing its retry count.
        4. Temporary files are cleaned up regardless of the outcome.

        Args:
            feed_id: The unique identifier for the feed whose queued items are to be processed.
            feed_config: The configuration object for the feed, containing yt-dlp arguments
                         and max error count.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.
            limit: The maximum number of queued items to process. If -1 (default),
                   processes all queued items for the feed.

        Returns:
            A tuple (success_count, failure_count).

        Raises:
            DownloadError: If fetching queued items from the database fails.
        """
        log_params: dict[str, Any] = {
            "feed_id": feed_id,
            "feed_url": feed_config.url,
        }
        logger.debug(
            "Starting download_queued process.",
            extra=log_params,
        )
        success_count = 0
        failure_count = 0

        try:
            queued_downloads = await self.download_db.get_downloads_by_status(
                DownloadStatus.QUEUED,
                feed_id,
                limit,
            )
        except DatabaseOperationError as e:
            raise DownloadError(
                message="Failed to fetch queued downloads from database.",
                feed_id=feed_id,
            ) from e

        if not queued_downloads:
            logger.debug("No queued downloads found for feed.", extra=log_params)
            return 0, 0

        logger.debug(
            "Found queued items for feed. Processing...",
            extra={**log_params, "num_queued": len(queued_downloads)},
        )

        for download in queued_downloads:
            try:
                await self._process_single_download(download, feed_config, cookies_path)
                success_count += 1
            except DownloadError as e:
                await self._handle_download_failure(download, feed_config, e)
                failure_count += 1

        logger.debug(
            "Finished processing queued downloads.",
            extra={
                **log_params,
                "success_count": success_count,
                "failure_count": failure_count,
            },
        )
        return success_count, failure_count
