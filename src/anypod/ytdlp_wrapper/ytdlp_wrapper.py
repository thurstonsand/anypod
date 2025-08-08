"""High-level wrapper for yt-dlp operations.

This module provides the YtdlpWrapper class that orchestrates yt-dlp operations
for metadata fetching and media downloading, integrating with source-specific
handlers for different platforms.
"""

from datetime import datetime
import logging
from pathlib import Path
from typing import Any

import aiofiles.os

from ..db.types import Download, Feed, SourceType
from ..exceptions import FileOperationError, YtdlpApiError
from ..path_manager import PathManager
from .base_handler import SourceHandlerBase
from .core import YtdlpArgs, YtdlpCore
from .youtube_handler import (
    YoutubeHandler,
    YtdlpYoutubeDataError,
    YtdlpYoutubeVideoFilteredOutError,
)

logger = logging.getLogger(__name__)


class YtdlpWrapper:
    """Wrapper around yt-dlp for fetching and parsing metadata and downloading media.

    Provides high-level methods for metadata extraction and media downloading,
    integrating with source-specific handlers to support different platforms
    and URL types.

    Attributes:
        _source_handler: Source-specific handler for URL processing. For now,
            only YoutubeHandler is implemented.
        _app_tmp_dir: Temporary directory for yt-dlp operations.
        _app_data_dir: Data directory for downloaded files.
    """

    _source_handler: SourceHandlerBase = YoutubeHandler()

    def __init__(self, paths: PathManager):
        self._paths = paths
        logger.debug(
            "YtdlpWrapper initialized.",
            extra={
                "app_tmp_dir": str(self._paths.base_tmp_dir),
                "app_data_dir": str(self._paths.base_data_dir),
            },
        )

    async def discover_feed_properties(
        self,
        feed_id: str,
        url: str,
        cookies_path: Path | None = None,
    ) -> tuple[SourceType, str | None]:
        """Discover feed properties: source type and resolved URL.

        Determine the feed's source type and the final resolved URL to use for
        metadata fetching.

        Args:
            feed_id: The feed identifier.
            url: The original feed URL from configuration.
            cookies_path: Path to cookies.txt file for authentication, or None if not needed.

        Returns:
            Tuple of (source_type, resolved_url) where:
            - source_type: The categorized type of the feed source
            - resolved_url: The final URL to use for metadata fetching

        Raises:
            YtdlpApiError: If discovery fails or no fetchable URL is determined.
        """
        log_config: dict[str, Any] = {
            "feed_id": feed_id,
            "url": url,
        }

        logger.debug("Discovering feed properties.", extra=log_config)

        resolved_url, source_type = await self._source_handler.determine_fetch_strategy(
            feed_id, url, cookies_path
        )

        logger.debug(
            "Successfully discovered feed properties.",
            extra={
                **log_config,
                "source_type": source_type.value,
                "resolved_url": resolved_url,
            },
        )

        return source_type, resolved_url

    async def _prepare_download_dir(self, feed_id: str) -> tuple[Path, Path]:
        try:
            feed_temp_path = await self._paths.feed_tmp_dir(feed_id)
            feed_data_path = await self._paths.feed_data_dir(feed_id)
        except ValueError as e:
            raise YtdlpApiError(
                message="Invalid feed identifier for yt-dlp paths.",
                feed_id=feed_id,
            ) from e
        except FileOperationError as e:
            raise YtdlpApiError(
                message="Failed to create directories for yt-dlp paths.",
                feed_id=feed_id,
            ) from e

        return feed_temp_path, feed_data_path

    def _match_filter_since_date(self, since_date: datetime) -> str:
        """Create break-match-filters expression for date filtering.

        Args:
            since_date: The datetime to filter from.

        Returns:
            Filter expression in the format "upload_date >= YYYYMMDD".
        """
        date_str = since_date.strftime("%Y%m%d")
        return f"upload_date >= {date_str}"

    async def fetch_playlist_metadata(
        self,
        feed_id: str,
        source_type: SourceType,
        source_url: str,
        resolved_url: str | None,
        user_yt_cli_args: list[str],
        cookies_path: Path | None = None,
    ) -> Feed:
        """Get playlist metadata from yt-dlp. Does not retrieve download metadata.

        Args:
            feed_id: The identifier for the feed.
            source_type: The source type of the feed.
            source_url: The original source URL from configuration.
            resolved_url: The resolved URL to fetch from.
            user_yt_cli_args: User-configured command-line arguments for yt-dlp.
            cookies_path: Path to cookies.txt file for authentication, or None if not needed.

        Returns:
            A Feed object with extracted metadata.

        Raises:
            YtdlpApiError: If yt-dlp fails to execute or encounters an error.
        """
        resolved_url = resolved_url or source_url
        log_config: dict[str, Any] = {
            "feed_id": feed_id,
            "source_url": source_url,
            "source_type": str(source_type),
            "num_user_yt_cli_args": len(user_yt_cli_args),
        }

        logger.debug("Fetching playlist metadata for feed.", extra=log_config)

        args = YtdlpArgs(user_yt_cli_args).convert_thumbnails("jpg")
        if cookies_path:
            args.cookies(cookies_path)

        logger.debug(
            "Acquiring playlist metadata.",
            extra=log_config,
        )

        ytdlp_info = await YtdlpCore.extract_playlist_info(args, resolved_url)

        extracted_feed = self._source_handler.extract_feed_metadata(
            feed_id,
            ytdlp_info,
            source_type,
            source_url,
        )

        logger.debug(
            "Successfully processed playlist metadata.",
            extra=log_config,
        )

        return extracted_feed

    async def fetch_new_downloads_metadata(
        self,
        feed_id: str,
        source_type: SourceType,
        source_url: str,
        resolved_url: str | None,
        user_yt_cli_args: list[str],
        fetch_since_date: datetime | None = None,
        keep_last: int | None = None,
        cookies_path: Path | None = None,
    ) -> list[Download]:
        """Get download metadata for enqueuing. Does not retrieve playlist metadata.

        Args:
            feed_id: The identifier for the feed.
            source_type: The source type of the feed.
            source_url: The original source URL from configuration.
            resolved_url: The resolved URL to fetch from.
            user_yt_cli_args: User-configured command-line arguments for yt-dlp.
            fetch_since_date: The cutoff date for fetching videos (inclusive).
            keep_last: Maximum number of recent playlist items to fetch.
            cookies_path: Path to cookies.txt file for authentication.

        Returns:
            A list of Download objects. Empty list if no downloads are found.

        Raises:
            YtdlpApiError: If yt-dlp fails to execute or encounters an error.
        """
        resolved_url = resolved_url or source_url
        log_config: dict[str, Any] = {
            "feed_id": feed_id,
            "source_url": source_url,
            "source_type": str(source_type),
            "num_user_yt_cli_args": len(user_yt_cli_args),
        }

        logger.debug("Fetching new downloads metadata for feed.", extra=log_config)

        args = YtdlpArgs(user_yt_cli_args).convert_thumbnails("jpg")

        # Apply filtering for playlists/channels
        if source_type != SourceType.SINGLE_VIDEO:
            if keep_last:
                args.playlist_limit(keep_last)
                log_config["keep_last"] = keep_last
            if fetch_since_date:
                # Use lazy_playlist with break_match_filters for early termination
                args.lazy_playlist()
                date_filter_expr = self._match_filter_since_date(fetch_since_date)
                args.break_match_filters(date_filter_expr)
                log_config["fetch_since_date_day_aligned"] = fetch_since_date.strftime(
                    "%Y%m%d"
                )

        if cookies_path:
            args.cookies(cookies_path)

        logger.debug(
            "Acquiring downloads metadata.",
            extra=log_config,
        )

        ytdlp_infos = await YtdlpCore.extract_downloads_info(args, resolved_url)

        if not ytdlp_infos:
            logger.debug(
                "No new downloads found.",
                extra=log_config,
            )
            return []

        # Parse all downloads from individual video infos
        parsed_downloads: list[Download] = []
        for ytdlp_info in ytdlp_infos:
            try:
                download = self._source_handler.extract_download_metadata(
                    feed_id,
                    ytdlp_info,
                )
            except YtdlpYoutubeVideoFilteredOutError:
                # Video was filtered out by yt-dlp, skip it
                logger.debug(
                    "Video filtered out by yt-dlp, skipping.",
                    extra={"feed_id": feed_id},
                )
            except YtdlpYoutubeDataError as e:
                # Critical: Required metadata is missing, this shouldn't happen
                logger.error(
                    "Failed to extract required metadata from video.",
                    exc_info=e,
                    extra={"feed_id": feed_id},
                )
                raise
            else:
                parsed_downloads.append(download)

        logger.debug(
            "Successfully processed downloads metadata.",
            extra={
                "feed_id": feed_id,
                "fetch_url": resolved_url,
                "source_url": source_url,
                "num_downloads_identified": len(parsed_downloads),
            },
        )

        return parsed_downloads

    async def download_media_to_file(
        self,
        download: Download,
        user_yt_cli_args: list[str],
        cookies_path: Path | None = None,
    ) -> Path:
        """Download the media for a given Download to a target directory.

        yt-dlp will place the final file in a feed-specific subdirectory within
        the application's configured data directory.

        Args:
            download: The Download object containing metadata.
            user_yt_cli_args: User-provided yt-dlp CLI arguments for this feed.
            cookies_path: Path to cookies.txt file for authentication, or None if not needed.

        Returns:
            The absolute path to the successfully downloaded media file.

        Raises:
            YtdlpApiError: If the download fails or the downloaded file is not found.
        """
        download_temp_dir, download_data_dir = await self._prepare_download_dir(
            download.feed_id
        )

        logger.debug(
            "Requesting media download via yt-dlp.",
            extra={
                "feed_id": download.feed_id,
                "download_id": download.id,
                "download_target_dir": str(download_data_dir),
                "source_url": download.source_url,
            },
        )

        # Inline download options
        download_opts = (
            YtdlpArgs(user_yt_cli_args)
            .convert_thumbnails("jpg")
            .output(f"{download.id}.%(ext)s")
            .paths_temp(download_temp_dir)
            .paths_home(download_data_dir)
        )

        if cookies_path:
            download_opts.cookies(cookies_path)

        url_to_download = download.source_url

        try:
            await YtdlpCore.download(download_opts, url_to_download)
        except YtdlpApiError as e:
            logger.error(
                "yt-dlp download call failed.",
                exc_info=e,
                extra={
                    "feed_id": download.feed_id,
                    "download_id": download.id,
                    "url": url_to_download,
                    "download_target_dir": str(download_data_dir),
                },
            )
            raise
        except Exception as e:
            logger.error(
                "Unexpected error during yt-dlp download call.",
                exc_info=e,
                extra={
                    "feed_id": download.feed_id,
                    "download_id": download.id,
                    "url": url_to_download,
                },
            )
            raise YtdlpApiError(
                message="Unexpected error during media download.",
                feed_id=download.feed_id,
                download_id=download.id,
                url=url_to_download,
            ) from e

        downloaded_files = list(
            await aiofiles.os.wrap(download_data_dir.glob)(f"{download.id}.*")
        )

        if not downloaded_files:
            raise YtdlpApiError(
                message="Downloaded file not found after attempted download. yt-dlp might have filtered.",
                feed_id=download.feed_id,
                download_id=download.id,
                url=url_to_download,
            )
        if len(downloaded_files) > 1:
            logger.warning(
                "Multiple files found after attempting download. Using the first one.",
                extra={
                    "feed_id": download.feed_id,
                    "download_id": download.id,
                    "files_found": [str(f) for f in downloaded_files],
                },
            )

        downloaded_file = downloaded_files[0]

        # Use aiofiles.os.stat for file size check
        if not await aiofiles.os.path.isfile(downloaded_file):
            raise YtdlpApiError(
                message="Downloaded file is invalid (not a file).",
                feed_id=download.feed_id,
                download_id=download.id,
                url=url_to_download,
            )

        file_stat = await aiofiles.os.stat(downloaded_file)
        if file_stat.st_size == 0:
            raise YtdlpApiError(
                message="Downloaded file is invalid (empty).",
                feed_id=download.feed_id,
                download_id=download.id,
                url=url_to_download,
            )

        logger.debug(
            "Download complete.",
            extra={
                "feed_id": download.feed_id,
                "download_id": download.id,
                "file_path": str(downloaded_file),
            },
        )

        return downloaded_file
