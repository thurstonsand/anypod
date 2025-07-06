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
from .base_handler import FetchPurpose, ReferenceType, SourceHandlerBase
from .core import YtdlpArgs, YtdlpCore
from .youtube_handler import YoutubeHandler

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

        resolved_url, ref_type = await self._source_handler.determine_fetch_strategy(
            feed_id, url, cookies_path
        )

        match ref_type:
            case ReferenceType.SINGLE:
                source_type = SourceType.SINGLE_VIDEO
            case ReferenceType.CHANNEL:
                source_type = SourceType.CHANNEL
            case ReferenceType.COLLECTION:
                source_type = SourceType.PLAYLIST
            case (
                ReferenceType.UNKNOWN_RESOLVED_URL | ReferenceType.UNKNOWN_DIRECT_FETCH
            ):
                source_type = SourceType.UNKNOWN

        logger.debug(
            "Successfully discovered feed properties.",
            extra={
                **log_config,
                "source_type": source_type.value,
                "resolved_url": resolved_url,
                "reference_type": ref_type.value,
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

    def _prepare_ytdlp_options(
        self,
        args: YtdlpArgs,
        purpose: FetchPurpose,
        ref_type: ReferenceType | None = None,
        fetch_since_date: datetime | None = None,
        fetch_until_date: datetime | None = None,
        keep_last: int | None = None,
        download_temp_dir: Path | None = None,
        download_data_dir: Path | None = None,
        download_id: str | None = None,
        cookies_path: Path | None = None,
    ) -> YtdlpArgs:
        log_params: dict[str, Any] = {
            "purpose": purpose,
            "num_user_provided_opts": args.additional_args_count,
            "ref_type": ref_type,
        }
        logger.debug("Preparing yt-dlp options.", extra=log_params)

        # Add base options
        args.quiet().no_warnings()

        # Add date filtering and playlist limits (only for non-SINGLE references)
        if ref_type != ReferenceType.SINGLE:
            if fetch_since_date:
                args.dateafter(fetch_since_date)
                log_params["fetch_since_date_day_aligned"] = fetch_since_date.strftime(
                    "%Y%m%d"
                )
            if fetch_until_date:
                args.datebefore(fetch_until_date)
                log_params["fetch_until_date_day_aligned"] = fetch_until_date.strftime(
                    "%Y%m%d"
                )
            if keep_last:
                args.playlist_limit(keep_last)
                log_params["keep_last"] = keep_last

        # Add purpose-specific options
        match purpose:
            case FetchPurpose.DISCOVERY:
                args.skip_download().flat_playlist().playlist_limit(5)
            case FetchPurpose.METADATA_FETCH:
                args.skip_download()
            case FetchPurpose.MEDIA_DOWNLOAD:
                if (
                    download_temp_dir is None
                    or download_data_dir is None
                    or download_id is None
                ):
                    raise YtdlpApiError(
                        message="download_temp_dir, download_data_dir, and download_id are required for MEDIA_DOWNLOAD purpose",
                        download_id=download_id,
                    )
                args.output(f"{download_id}.%(ext)s").paths_temp(
                    download_temp_dir
                ).paths_home(download_data_dir)

        # Add cookies if provided
        if cookies_path is not None:
            args.cookies(cookies_path)

        logger.debug(
            f"Prepared {purpose!s} options.",
            extra={
                **log_params,
                "final_cli_args_count": len(args.to_list()),
                "cookies_provided": cookies_path is not None,
            },
        )
        return args

    async def fetch_metadata(
        self,
        feed_id: str,
        source_type: SourceType,
        source_url: str,
        resolved_url: str | None,
        user_yt_cli_args: list[str],
        fetch_since_date: datetime | None = None,
        fetch_until_date: datetime | None = None,
        keep_last: int | None = None,
        cookies_path: Path | None = None,
    ) -> tuple[Feed, list[Download]]:
        """Fetches metadata for a given feed using yt-dlp.

        Uses the pre-discovered source type and resolved URL to fetch metadata.

        Date filtering uses yt-dlp's day-level precision: both fetch_since_date and
        fetch_until_date are converted to YYYYMMDD format before being passed to yt-dlp.
        This means overlapping date windows that fall on the same day will use identical
        date ranges in yt-dlp, potentially returning duplicate results that are handled
        by the deduplication logic in the Enqueuer. NOTE: This only applies to playlists
        and channels, not single videos.

        Args:
            feed_id: The identifier for the feed.
            source_type: The source type of the feed.
            source_url: The original source URL from configuration.
            resolved_url: The resolved URL to fetch from.
            user_yt_cli_args: User-configured command-line arguments for yt-dlp.
            fetch_since_date: The lower bound date for the fetch operation (inclusive).
                Converted to YYYYMMDD format for yt-dlp compatibility.
            fetch_until_date: The upper bound date for the fetch operation (exclusive).
                Converted to YYYYMMDD format for yt-dlp compatibility.
            keep_last: Maximum number of recent playlist items to fetch, or None for no limit.
                Uses `playlist_items` to get the first N items
            cookies_path: Path to cookies.txt file for authentication, or None if not needed.

        Returns:
            A tuple of (feed, downloads) where feed is a Feed object with extracted
            metadata and downloads is a list of Download objects.

        Raises:
            YtdlpApiError: If no information is extracted.
        """
        # fallback to source_url if no resolved_url is provided
        resolved_url = resolved_url or source_url
        log_config: dict[str, Any] = {
            "feed_id": feed_id,
            "source_url": source_url,
            "source_type": str(source_type),
            "num_user_yt_cli_args": len(user_yt_cli_args),
        }

        logger.debug("Fetching metadata for feed.", extra=log_config)

        # Convert SourceType back to ReferenceType for internal processing
        match source_type:
            case SourceType.SINGLE_VIDEO:
                ref_type = ReferenceType.SINGLE
            case SourceType.CHANNEL:
                ref_type = ReferenceType.CHANNEL
            case SourceType.PLAYLIST:
                ref_type = ReferenceType.COLLECTION
            case SourceType.UNKNOWN:
                ref_type = ReferenceType.UNKNOWN_RESOLVED_URL

        # Prepare CLI args with date filtering and source-specific options
        metadata_fetch_args = YtdlpArgs(user_yt_cli_args)

        # Apply source-specific options
        metadata_fetch_args = self._source_handler.set_source_specific_ytdlp_options(
            metadata_fetch_args, FetchPurpose.METADATA_FETCH
        )

        # Apply metadata fetch options
        metadata_fetch_args = self._prepare_ytdlp_options(
            args=metadata_fetch_args,
            purpose=FetchPurpose.METADATA_FETCH,
            ref_type=ref_type,
            fetch_since_date=fetch_since_date,
            fetch_until_date=fetch_until_date,
            keep_last=keep_last,
            cookies_path=cookies_path,
        )

        logger.debug(
            "Acquiring metadata.",
            extra={
                "feed_id": feed_id,
                "fetch_url": resolved_url,
                "source_url": source_url,
                "reference_type": ref_type.name,
            },
        )

        ytdlp_info = await YtdlpCore.extract_info(metadata_fetch_args, resolved_url)
        if ytdlp_info is None:
            raise YtdlpApiError(
                message="No information extracted by yt-dlp. This might be due to filters or content unavailability.",
                feed_id=feed_id,
                url=resolved_url,
            )

        extracted_feed = self._source_handler.extract_feed_metadata(
            feed_id,
            ytdlp_info,
            ref_type,
            source_url,
            fetch_until_date,
        )

        parsed_downloads = self._source_handler.parse_metadata_to_downloads(
            feed_id,
            ytdlp_info,
            source_identifier=feed_id,
            ref_type=ref_type,
        )

        logger.debug(
            "Successfully processed metadata.",
            extra={
                "feed_id": feed_id,
                "fetch_url": resolved_url,
                "source_url": source_url,
                "num_downloads_identified": len(parsed_downloads),
            },
        )
        return extracted_feed, parsed_downloads

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

        try:
            # Create download args
            download_opts = YtdlpArgs(user_yt_cli_args)

            # Apply source-specific options
            download_opts = self._source_handler.set_source_specific_ytdlp_options(
                download_opts, FetchPurpose.MEDIA_DOWNLOAD
            )

            # Apply download-specific options
            download_opts = self._prepare_ytdlp_options(
                args=download_opts,
                purpose=FetchPurpose.MEDIA_DOWNLOAD,
                download_temp_dir=download_temp_dir,
                download_data_dir=download_data_dir,
                download_id=download.id,
                cookies_path=cookies_path,
            )
        except YtdlpApiError as e:
            e.feed_id = download.feed_id
            e.url = download.source_url
            raise

        url_to_download = download.source_url

        try:
            # TODO: maybe we can get the filepath from here?
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
