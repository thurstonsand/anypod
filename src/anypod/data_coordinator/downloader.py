"""Handles the downloading of media files for queued items.

This module defines the Downloader class, which is responsible for processing
downloads marked as 'queued' in the database. It interacts with the YtdlpWrapper
to fetch media, the FileManager to handle file storage, and the DownloadDatabase
to update download statuses and metadata.
"""

from datetime import UTC, datetime
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
    FFProbeError,
    FileOperationError,
    YtdlpApiError,
)
from ..ffprobe import FFProbe
from ..file_manager import FileManager
from ..ytdlp_wrapper import TranscriptInfo, YtdlpWrapper

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
        ffprobe: FFProbe,
    ):
        self.download_db = download_db
        self.file_manager = file_manager
        self.ytdlp_wrapper = ytdlp_wrapper
        self._ffprobe = ffprobe
        logger.debug("Downloader initialized.")

    async def _probe_download_duration(
        self, download: Download, downloaded_file_path: Path
    ) -> int | None:
        """Return actual duration for the downloaded media when possible."""
        log_params = {
            "feed_id": download.feed_id,
            "download_id": download.id,
            "downloaded_file_path": str(downloaded_file_path),
        }
        try:
            duration = await self._ffprobe.get_duration_seconds_from_file(
                downloaded_file_path
            )
        except (FFProbeError, FileNotFoundError) as e:
            logger.warning(
                "Unable to probe duration via ffprobe; keeping metadata value.",
                extra=log_params,
                exc_info=e,
            )
            return None

        if duration <= 0:
            logger.warning(
                "ffprobe reported non-positive duration; keeping metadata value.",
                extra={**log_params, "probed_duration": duration},
            )
            return None

        return duration

    async def _handle_download_success(
        self,
        download: Download,
        downloaded_file_path: Path,
        logs: str,
        transcript: TranscriptInfo | None = None,
    ) -> None:
        """Process a successfully downloaded file.

        Updates the download record to DOWNLOADED status with all metadata
        (extension, filesize, duration, thumbnail, transcript, logs) in a single
        database upsert.

        Args:
            download: The Download object to update.
            downloaded_file_path: Path to the successfully downloaded file.
            logs: yt-dlp execution logs.
            transcript: Transcript metadata if downloaded, None otherwise.

        Raises:
            DownloadError: If file operations or database update fails.
        """
        log_params: dict[str, Any] = {
            "feed_id": download.feed_id,
            "download_id": download.id,
            "downloaded_file_path": downloaded_file_path,
        }
        logger.debug("Download successful, processing file.", extra=log_params)

        try:
            file_stat = await aiofiles.os.stat(downloaded_file_path)
        except OSError as e:
            raise DownloadError(
                message="Failed to stat downloaded file.",
                feed_id=download.feed_id,
                download_id=download.id,
            ) from e

        duration_seconds = await self._probe_download_duration(
            download, downloaded_file_path
        )

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

        download.status = DownloadStatus.DOWNLOADED
        download.ext = downloaded_file_path.suffix.lstrip(".")
        download.filesize = file_stat.st_size
        if duration_seconds is not None:
            download.duration = duration_seconds
        download.retries = 0
        download.last_error = None
        download.download_logs = logs
        download.thumbnail_ext = "jpg" if has_thumb else None
        if transcript:
            download.transcript_ext = transcript.ext
            download.transcript_lang = transcript.lang
            download.transcript_source = transcript.source

        try:
            await self.download_db.update_download(download)
        except (DownloadNotFoundError, DatabaseOperationError) as e:
            raise DownloadError(
                message="Failed to update database record to DOWNLOADED.",
                feed_id=download.feed_id,
                download_id=download.id,
            ) from e

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

    async def download_thumbnail_for_existing_download(
        self,
        download: Download,
        yt_args: list[str],
        cookies_path: Path | None = None,
    ) -> bool:
        """Download thumbnail for an existing download.

        Used during metadata refresh when remote_thumbnail_url changes.
        Downloads the new thumbnail without re-downloading the media file.

        Args:
            download: The Download object to fetch thumbnail for.
            yt_args: User-configured yt-dlp command-line arguments.
            cookies_path: Path to cookies.txt file for authentication.

        Returns:
            True if thumbnail was successfully downloaded, False otherwise.

        Raises:
            DownloadError: If a critical infrastructure error occurs
                (e.g., database update failure).
        """
        log_params: dict[str, Any] = {
            "feed_id": download.feed_id,
            "download_id": download.id,
        }
        logger.debug("Downloading thumbnail for existing download.", extra=log_params)

        try:
            await self.ytdlp_wrapper.download_media_thumbnail(
                download, yt_args, cookies_path
            )
        except YtdlpApiError as e:
            logger.warning(
                "Thumbnail download failed via yt-dlp.",
                extra=log_params,
                exc_info=e,
            )
            return False

        # Verify thumbnail file was created before updating database
        try:
            has_thumb = await self.file_manager.image_exists(
                download.feed_id, download.id, "jpg"
            )
        except FileOperationError as e:
            raise DownloadError(
                message="Failed to verify thumbnail file exists.",
                feed_id=download.feed_id,
                download_id=download.id,
            ) from e

        if not has_thumb:
            logger.warning(
                "Thumbnail file not found after download.",
                extra=log_params,
            )
            return False

        try:
            await self.download_db.set_thumbnail_extension(
                download.feed_id, download.id, "jpg"
            )
        except (DownloadNotFoundError, DatabaseOperationError) as e:
            raise DownloadError(
                message="Failed to update thumbnail extension in database.",
                feed_id=download.feed_id,
                download_id=download.id,
            ) from e

        logger.info("Thumbnail downloaded for existing download.", extra=log_params)
        return True

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
            downloaded_media = await self.ytdlp_wrapper.download_media_to_file(
                download_to_process,
                feed_config.yt_args,
                cookies_path=cookies_path,
                transcript_lang=feed_config.transcript_lang,
            )
        except YtdlpApiError as e:
            await self._persist_download_logs(download_to_process, e.logs or "")
            raise DownloadError(
                message="Failed to download media to file.",
                feed_id=download_to_process.feed_id,
                download_id=download_to_process.id,
            ) from e

        await self._handle_download_success(
            download_to_process,
            downloaded_media.file_path,
            downloaded_media.logs,
            downloaded_media.transcript,
        )

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

        # Apply download_delay filtering if configured
        ready_downloads = queued_downloads
        if feed_config.download_delay is not None:
            now = datetime.now(UTC)
            ready_downloads = [
                dl
                for dl in queued_downloads
                if dl.published + feed_config.download_delay <= now
            ]
            deferred_count = len(queued_downloads) - len(ready_downloads)
            if deferred_count > 0:
                logger.debug(
                    "Deferred downloads due to download_delay.",
                    extra={
                        **log_params,
                        "deferred_count": deferred_count,
                        "download_delay": str(feed_config.download_delay),
                    },
                )

        if not ready_downloads:
            logger.debug(
                "No downloads ready for processing (all deferred by download_delay).",
                extra=log_params,
            )
            return 0, 0

        logger.debug(
            "Found queued items for feed. Processing...",
            extra={**log_params, "num_queued": len(ready_downloads)},
        )

        for download in ready_downloads:
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
