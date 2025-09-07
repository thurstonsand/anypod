"""High-level wrapper for yt-dlp operations.

This module provides the YtdlpWrapper class that orchestrates yt-dlp operations
for metadata fetching and media downloading, integrating with source-specific
handlers for different platforms.
"""

from datetime import datetime, timedelta
import logging
from pathlib import Path
from typing import Any

import aiofiles.os

from ..db.app_state_db import AppStateDatabase
from ..db.types import Download, Feed, SourceType
from ..exceptions import FileOperationError, YtdlpApiError
from ..path_manager import PathManager
from .base_handler import SourceHandlerBase
from .core import YtdlpArgs, YtdlpCore
from .youtube_handler import (
    YoutubeHandler,
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

    def __init__(
        self,
        paths: PathManager,
        pot_provider_url: str | None,
        app_state_db: AppStateDatabase,
        yt_channel: str,
        yt_update_freq: timedelta,
    ):
        self._paths = paths
        self._pot_provider_url = pot_provider_url if pot_provider_url else None
        self._app_state_db = app_state_db
        self._yt_channel = yt_channel
        self._yt_update_freq = yt_update_freq
        logger.debug(
            "YtdlpWrapper initialized.",
            extra={
                "app_tmp_dir": str(self._paths.base_tmp_dir),
                "app_data_dir": str(self._paths.base_data_dir),
                "pot_provider_url": self._pot_provider_url or "<disabled>",
                "yt_channel": self._yt_channel,
                "yt_update_freq_seconds": int(self._yt_update_freq.total_seconds()),
            },
        )

    async def _update_to(self, args: YtdlpArgs) -> YtdlpArgs:
        """Apply --update-to if allowed by rate limiter and record timestamp."""
        if await self._app_state_db.update_yt_dlp_timestamp_if_stale(
            self._yt_update_freq
        ):
            args.update_to(self._yt_channel)
        return args

    def _pot_extractor_args(self, args: YtdlpArgs) -> YtdlpArgs:
        """Apply POT provider related extractor args to the given builder.

        When ``self._pot_provider_url`` is unset/empty, force yt-dlp to never
        fetch POT. Otherwise, configure the youtubepot HTTP provider base URL.

        Args:
            args: The :class:`YtdlpArgs` builder to modify.

        Returns:
            The same builder instance for chaining.
        """
        extractor_arg = (
            f"youtubepot-bgutilhttp:base_url={self._pot_provider_url}"
            if self._pot_provider_url
            else "youtube:fetch_pot=never"
        )
        return args.extractor_args(extractor_arg)

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

        # Prepare discovery args with centralized configuration and universal options
        discovery_args = YtdlpArgs().quiet().no_warnings()
        discovery_args = await self._update_to(discovery_args)
        discovery_args = self._pot_extractor_args(discovery_args)
        if cookies_path:
            discovery_args = discovery_args.cookies(cookies_path)

        resolved_url, source_type = await self._source_handler.determine_fetch_strategy(
            feed_id, url, discovery_args
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

        info_args = YtdlpArgs(user_yt_cli_args)
        info_args = await self._update_to(info_args)
        info_args = self._pot_extractor_args(info_args)
        if cookies_path:
            info_args = info_args.cookies(cookies_path)

        logger.debug(
            "Acquiring playlist metadata.",
            extra=log_config,
        )
        ytdlp_info = await YtdlpCore.extract_playlist_info(info_args, resolved_url)

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

    async def download_feed_thumbnail(
        self,
        feed_id: str,
        source_type: SourceType,
        source_url: str,
        resolved_url: str | None,
        user_yt_cli_args: list[str],
        cookies_path: Path | None = None,
    ) -> str | None:
        """Download the feed-level thumbnail.

        For single videos, this downloads the video's thumbnail. For playlists
        and channels, it downloads the playlist-level thumbnail.

        Args:
            feed_id: The identifier for the feed.
            source_type: The source type of the feed.
            source_url: The original source URL from configuration.
            resolved_url: The resolved URL to fetch from.
            user_yt_cli_args: User-configured command-line arguments for yt-dlp.
            cookies_path: Path to cookies.txt file for authentication, or None if not needed.

        Returns:
            Extension string (e.g., "jpg") if successful, None if failed.

        Raises:
            YtdlpApiError: If the thumbnail download fails.
        """
        resolved_url = resolved_url or source_url
        log_config: dict[str, Any] = {
            "feed_id": feed_id,
            "source_url": source_url,
            "resolved_url": resolved_url,
            "source_type": str(source_type),
        }

        logger.debug("Downloading feed thumbnail.", extra=log_config)

        feed_images_dir = await self._paths.feed_images_dir(feed_id)
        feed_tmp_dir = await self._paths.feed_tmp_dir(feed_id)

        # Base args for thumbnail download
        thumb_args = (
            YtdlpArgs(user_yt_cli_args)
            .skip_download()
            .write_thumbnail()
            .convert_thumbnails("jpg")
            .paths_temp(feed_tmp_dir)
        )
        thumb_args = await self._update_to(thumb_args)
        thumb_args = self._pot_extractor_args(thumb_args)

        # For single video feeds, use the video's thumbnail as the feed image.
        if source_type == SourceType.SINGLE_VIDEO:
            thumb_args.paths_thumbnail(feed_images_dir).output_thumbnail(
                f"{feed_id}.%(ext)s"
            )
        else:
            thumb_args.paths_pl_thumbnail(feed_images_dir).output_pl_thumbnail(
                f"{feed_id}.%(ext)s"
            ).output_thumbnail("").playlist_limit(0)

        if cookies_path:
            thumb_args = thumb_args.cookies(cookies_path)

        await YtdlpCore.download(thumb_args, resolved_url)

        # Verify the file was created successfully
        try:
            image_path = await self._paths.image_path(feed_id, None, "jpg")
        except ValueError:
            logger.warning(
                "Failed to get image path for verification.", extra=log_config
            )
            return None

        try:
            file_exists = await aiofiles.os.path.isfile(image_path)
        except OSError:
            logger.warning("Failed to check if image file exists.", extra=log_config)
            return None

        if file_exists:
            logger.debug("Feed thumbnail downloaded successfully.", extra=log_config)
            return "jpg"
        else:
            logger.warning(
                "Feed thumbnail download appeared to succeed but file not found.",
                extra=log_config,
            )
            return None

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

        info_args = YtdlpArgs(user_yt_cli_args).convert_thumbnails("jpg")
        info_args = await self._update_to(info_args)
        info_args = self._pot_extractor_args(info_args)

        # Apply filtering for playlists/channels
        if source_type != SourceType.SINGLE_VIDEO:
            if keep_last:
                info_args.playlist_limit(keep_last)
                log_config["keep_last"] = keep_last
            if fetch_since_date:
                # Use lazy_playlist with break_match_filters for early termination
                info_args.lazy_playlist()
                date_filter_expr = self._match_filter_since_date(fetch_since_date)
                info_args.break_match_filters(date_filter_expr)
                log_config["fetch_since_date_day_aligned"] = fetch_since_date.strftime(
                    "%Y%m%d"
                )

        if cookies_path:
            info_args = info_args.cookies(cookies_path)

        logger.debug(
            "Acquiring downloads metadata.",
            extra=log_config,
        )

        ytdlp_infos = await YtdlpCore.extract_downloads_info(info_args, resolved_url)

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
        thumbnails_dir = await self._paths.download_images_dir(download.feed_id)
        download_args = (
            YtdlpArgs(user_yt_cli_args)
            .convert_thumbnails("jpg")
            .write_thumbnail()
            .paths_thumbnail(thumbnails_dir)
            .output_thumbnail(f"{download.id}.%(ext)s")
            .output(f"{download.id}.%(ext)s")
            .paths_temp(download_temp_dir)
            .paths_home(download_data_dir)
        )
        download_args = await self._update_to(download_args)
        download_args = self._pot_extractor_args(download_args)

        if cookies_path:
            download_args = download_args.cookies(cookies_path)

        url_to_download = download.source_url

        await YtdlpCore.download(download_args, url_to_download)

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
