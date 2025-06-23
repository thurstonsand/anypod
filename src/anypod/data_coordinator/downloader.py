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
            "feed_id": download.feed,
            "download_id": download.id,
            "downloaded_file_path": downloaded_file_path,
        }
        logger.info("Download successful, processing file.", extra=log_params)

        try:
            file_stat = await aiofiles.os.stat(downloaded_file_path)
            self.download_db.mark_as_downloaded(
                feed=download.feed,
                id=download.id,
                ext=downloaded_file_path.suffix.lstrip("."),
                filesize=file_stat.st_size,
            )
        except (DownloadNotFoundError, DatabaseOperationError) as e:
            raise DownloadError(
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
            self.download_db.bump_retries(
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

    async def _check_and_update_metadata(
        self,
        download: Download,
        feed_config: FeedConfig,
        cookies_path: Path | None = None,
    ) -> Download:
        """Re-fetch metadata and update if values have changed.

        Re-fetches the metadata for a download before actually downloading it,
        and updates the database if any values have changed (except for the
        primary keys and published date).

        TODO: determine if this is necessary.

        Args:
            download: The Download object to check.
            feed_config: The configuration for the feed.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

        Returns:
            Updated Download object with latest metadata.

        Raises:
            DownloadError: If metadata fetch fails.
        """
        log_params: dict[str, Any] = {
            "feed_id": download.feed,
            "download_id": download.id,
        }
        logger.debug("Re-fetching metadata to check for updates.", extra=log_params)

        try:
            # Re-fetch metadata for this specific download
            _, fetched_downloads = await self.ytdlp_wrapper.fetch_metadata(
                download.feed,
                download.source_url,
                feed_config.yt_args,
                cookies_path=cookies_path,
            )
        except YtdlpApiError as e:
            logger.warning(
                "Failed to re-fetch metadata for update check. Proceeding with cached values.",
                extra=log_params,
                exc_info=e,
            )
            return download

        # Find the matching download in the fetched results
        matching_download = None
        for fetched in fetched_downloads:
            if fetched.id == download.id and fetched.feed == download.feed:
                matching_download = fetched
                break

        if not matching_download:
            logger.warning(
                "Could not find matching download in re-fetched metadata. Proceeding with cached values.",
                extra=log_params,
            )
            return download

        # Check for changes (excluding pks, published date, and status-related fields)
        changes: dict[str, tuple[Any, Any]] = {}
        fields_to_check = [
            "source_url",
            "title",
            "published",
            "ext",
            "mime_type",
            "filesize",
            "duration",
            "thumbnail",
            "description",
        ]

        for field in fields_to_check:
            old_value = getattr(download, field)
            new_value = getattr(matching_download, field)
            if old_value != new_value:
                changes[field] = (old_value, new_value)

        if changes:
            # Create a simpler changes dict for logging
            changes_for_log = {
                field: {"old": old_val, "new": new_val}
                for field, (old_val, new_val) in changes.items()
            }
            logger.info(
                "Detected metadata changes, updating database.",
                extra={
                    **log_params,
                    "changes": changes_for_log,
                },
            )

            # Update the download object with new values
            for field, (_, new_value) in changes.items():
                setattr(download, field, new_value)

            # Persist changes to database
            try:
                self.download_db.upsert_download(download)
            except DatabaseOperationError as e:
                logger.error(
                    "Failed to update changed metadata in database.",
                    extra=log_params,
                    exc_info=e,
                )
                # Continue with download even if update fails

        return download

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
            "feed_id": download_to_process.feed,
            "download_id": download_to_process.id,
        }
        logger.info("Processing single download.", extra=log_params)

        try:
            # Check for metadata updates before downloading
            download_to_process = await self._check_and_update_metadata(
                download_to_process, feed_config, cookies_path
            )

            downloaded_file_path = await self.ytdlp_wrapper.download_media_to_file(
                download_to_process,
                feed_config.yt_args,
                cookies_path=cookies_path,
            )
            await self._handle_download_success(
                download_to_process, downloaded_file_path
            )
        except YtdlpApiError as e:
            raise DownloadError(
                message="Failed to download media to file.",
                feed_id=download_to_process.feed,
                download_id=download_to_process.id,
            ) from e

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
        logger.info(
            "Starting download_queued process.",
            extra=log_params,
        )
        success_count = 0
        failure_count = 0

        try:
            queued_downloads = self.download_db.get_downloads_by_status(
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
            logger.info("No queued downloads found for feed.", extra=log_params)
            return 0, 0

        logger.info(
            "Found queued items for feed. Processing...",
            extra={**log_params, "num_queued": len(queued_downloads)},
        )

        for download in queued_downloads:
            try:
                await self._process_single_download(download, feed_config, cookies_path)
                success_count += 1
            except DownloadError as e:
                self._handle_download_failure(download, feed_config, e)
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
