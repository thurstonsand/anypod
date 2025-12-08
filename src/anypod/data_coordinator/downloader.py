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
from ..db.types import Download, DownloadStatus, TranscriptSource
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
from .types import ArtifactDownloadResult, DownloadArtifact

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

    async def _download_thumbnail_artifact(
        self,
        download: Download,
        yt_args: list[str],
        cookies_path: Path | None,
        log_params: dict[str, Any],
    ) -> bool:
        """Download thumbnail and persist to database.

        Args:
            download: The Download object to fetch thumbnail for.
            yt_args: User-configured yt-dlp command-line arguments.
            cookies_path: Path to cookies.txt file for authentication.
            log_params: Logging context dictionary.

        Returns:
            True if thumbnail was successfully downloaded and persisted.

        Raises:
            DownloadError: If database update fails.
        """
        logger.debug("Downloading thumbnail.", extra=log_params)

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

        # Verify thumbnail file was created
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

        logger.info("Thumbnail artifact downloaded.", extra=log_params)
        return True

    async def _download_transcript_artifact(
        self,
        download: Download,
        transcript_lang: str,
        transcript_source: TranscriptSource,
        cookies_path: Path | None,
        log_params: dict[str, Any],
    ) -> bool:
        """Download transcript and persist to database.

        Args:
            download: The Download object to fetch transcript for.
            transcript_lang: Language code for transcripts.
            transcript_source: Source type (creator or auto-generated).
            cookies_path: Path to cookies.txt file for authentication.
            log_params: Logging context dictionary.

        Returns:
            True if transcript was successfully downloaded and persisted.

        Raises:
            DownloadError: If database update fails.
        """
        logger.debug(
            "Downloading transcript.",
            extra={**log_params, "transcript_lang": transcript_lang},
        )

        try:
            transcript_ext = await self.ytdlp_wrapper.download_transcript_only(
                feed_id=download.feed_id,
                download_id=download.id,
                source_url=download.source_url,
                transcript_lang=transcript_lang,
                transcript_source=transcript_source,
                cookies_path=cookies_path,
            )
        except YtdlpApiError as e:
            logger.warning(
                "Transcript download failed via yt-dlp.",
                extra=log_params,
                exc_info=e,
            )
            return False

        if transcript_ext is None:
            logger.warning(
                "Transcript file not found after download attempt.",
                extra=log_params,
            )
            return False

        try:
            await self.download_db.set_transcript_metadata(
                feed_id=download.feed_id,
                download_id=download.id,
                transcript_ext=transcript_ext,
                transcript_lang=transcript_lang,
                transcript_source=transcript_source,
            )
        except (DownloadNotFoundError, DatabaseOperationError) as e:
            raise DownloadError(
                message="Failed to update transcript metadata in database.",
                feed_id=download.feed_id,
                download_id=download.id,
            ) from e

        logger.info(
            "Transcript artifact downloaded.",
            extra={**log_params, "transcript_ext": transcript_ext},
        )
        return True

    async def refresh_artifacts(
        self,
        download: Download,
        feed_config: FeedConfig,
        thumbnail_url_changed: bool,
        transcript_metadata_changed: bool,
        force_transcript: bool = False,
        cookies_path: Path | None = None,
    ) -> tuple[Download, bool | None, bool | None]:
        """Refresh artifacts based on metadata changes.

        Determines which artifacts need refresh based on metadata change flags
        and download state, then downloads them via download_artifacts.

        Thumbnail is refreshed if:
        - thumbnail_url_changed is True, OR
        - download.thumbnail_ext is None AND download.remote_thumbnail_url exists

        Transcript is refreshed if:
        - (force_transcript OR transcript_metadata_changed) AND
        - feed_config.transcript_lang is set AND
        - download.transcript_source is CREATOR or AUTO

        Args:
            download: The Download object to refresh artifacts for.
            feed_config: The feed configuration.
            thumbnail_url_changed: Whether the thumbnail URL changed during metadata refresh.
            transcript_metadata_changed: Whether transcript metadata changed during refresh.
            force_transcript: Force transcript re-download even if metadata unchanged.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

        Returns:
            Tuple of (updated_download, thumbnail_result, transcript_result) where
            each result is True (success), False (failed), or None (not attempted).

        Raises:
            DownloadError: If database re-fetch fails after artifact download.
        """
        log_params: dict[str, Any] = {
            "feed_id": download.feed_id,
            "download_id": download.id,
        }

        # Determine which artifacts to refresh
        artifacts = DownloadArtifact.NONE

        should_refresh_thumbnail = thumbnail_url_changed or (
            download.thumbnail_ext is None and download.remote_thumbnail_url is not None
        )
        if should_refresh_thumbnail:
            artifacts |= DownloadArtifact.THUMBNAIL
            logger.info(
                "Thumbnail refresh needed.",
                extra={
                    **log_params,
                    "url_changed": thumbnail_url_changed,
                    "missing_thumbnail": download.thumbnail_ext is None,
                },
            )

        should_refresh_transcript = force_transcript or transcript_metadata_changed
        can_download_transcript = (
            feed_config.transcript_lang is not None
            and download.transcript_source
            in [TranscriptSource.CREATOR, TranscriptSource.AUTO]
        )
        if should_refresh_transcript and can_download_transcript:
            artifacts |= DownloadArtifact.TRANSCRIPT
            logger.info(
                "Transcript refresh needed.",
                extra={
                    **log_params,
                    "force_refresh": force_transcript,
                    "metadata_changed": transcript_metadata_changed,
                },
            )

        # Nothing to refresh
        if artifacts == DownloadArtifact.NONE:
            return download, None, None

        # Download the artifacts
        result = await self.download_artifacts(
            download, feed_config, artifacts, cookies_path
        )

        # Re-fetch download from DB if any artifact was downloaded
        updated_download = download
        if result.thumbnail_downloaded or result.transcript_downloaded:
            try:
                updated_download = await self.download_db.get_download_by_id(
                    download.feed_id, download.id
                )
            except (DownloadNotFoundError, DatabaseOperationError) as e:
                raise DownloadError(
                    message="Failed to re-fetch download after artifact refresh.",
                    feed_id=download.feed_id,
                    download_id=download.id,
                ) from e

        return (
            updated_download,
            result.thumbnail_downloaded,
            result.transcript_downloaded,
        )

    async def download_artifacts(
        self,
        download: Download,
        feed_config: FeedConfig,
        artifacts: DownloadArtifact,
        cookies_path: Path | None = None,
    ) -> ArtifactDownloadResult:
        """Download selected artifacts for a download.

        When downloading MEDIA (with ALL or MEDIA flag), this method handles
        the complete download lifecycle including status transitions. For
        THUMBNAIL or TRANSCRIPT only downloads, only the specific artifact
        is downloaded and persisted without changing download status.

        Args:
            download: The Download object to process.
            feed_config: The configuration for the feed.
            artifacts: Which artifacts to download.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

        Returns:
            ArtifactDownloadResult with success/failure for each requested artifact.

        Raises:
            DownloadError: If a critical error occurs (e.g., database failure).
        """
        log_params: dict[str, Any] = {
            "feed_id": download.feed_id,
            "download_id": download.id,
            "artifacts": str(artifacts),
        }
        logger.debug("Downloading artifacts.", extra=log_params)

        # Handle MEDIA download (includes thumbnail + transcript if configured)
        if DownloadArtifact.MEDIA in artifacts:
            try:
                downloaded_media = await self.ytdlp_wrapper.download_media_to_file(
                    download,
                    feed_config.yt_args,
                    cookies_path=cookies_path,
                    transcript_lang=feed_config.transcript_lang,
                )
            except YtdlpApiError as e:
                await self._persist_download_logs(download, e.logs or "")
                logger.warning(
                    "Media artifact download failed.",
                    extra=log_params,
                    exc_info=e,
                )
                return ArtifactDownloadResult(
                    media_downloaded=False,
                    thumbnail_downloaded=False,
                    transcript_downloaded=False,
                    errors=[
                        DownloadError(
                            message="Failed to download media to file.",
                            feed_id=download.feed_id,
                            download_id=download.id,
                        )
                    ],
                )

            await self._handle_download_success(
                download,
                downloaded_media.file_path,
                downloaded_media.logs,
                downloaded_media.transcript,
            )
            logger.debug("Media artifact downloaded successfully.", extra=log_params)

            thumbnail_downloaded = download.thumbnail_ext is not None
            if feed_config.transcript_lang is None:
                transcript_downloaded = None
            else:
                transcript_downloaded = downloaded_media.transcript is not None

            return ArtifactDownloadResult(
                media_downloaded=True,
                thumbnail_downloaded=thumbnail_downloaded,
                transcript_downloaded=transcript_downloaded,
            )

        result = ArtifactDownloadResult()

        # Handle THUMBNAIL-only download
        if DownloadArtifact.THUMBNAIL in artifacts:
            try:
                result.thumbnail_downloaded = await self._download_thumbnail_artifact(
                    download, feed_config.yt_args, cookies_path, log_params
                )
            except DownloadError as e:
                result.thumbnail_downloaded = False
                result.errors.append(e)

        # Handle TRANSCRIPT-only download
        if DownloadArtifact.TRANSCRIPT in artifacts:
            # Transcript download requires transcript_lang and transcript_source
            if feed_config.transcript_lang and download.transcript_source in [
                TranscriptSource.CREATOR,
                TranscriptSource.AUTO,
            ]:
                try:
                    result.transcript_downloaded = (
                        await self._download_transcript_artifact(
                            download,
                            feed_config.transcript_lang,
                            download.transcript_source,
                            cookies_path,
                            log_params,
                        )
                    )
                except DownloadError as e:
                    result.transcript_downloaded = False
                    result.errors.append(e)
            else:
                # Transcript download not applicable (no lang configured or no source)
                result.transcript_downloaded = None
                logger.debug(
                    "Transcript artifact skipped (not configured or no source).",
                    extra=log_params,
                )

        return result

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
                result = await self.download_artifacts(
                    download, feed_config, DownloadArtifact.ALL, cookies_path
                )
            except DownloadError as e:
                await self._handle_download_failure(download, feed_config, e)
                failure_count += 1
                continue

            if result.all_succeeded:
                success_count += 1
            else:
                await self._handle_download_failure(
                    download,
                    feed_config,
                    result.errors[0]
                    if result.errors
                    else DownloadError(
                        "Unknown error",
                        feed_id=download.feed_id,
                        download_id=download.id,
                    ),
                )
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
