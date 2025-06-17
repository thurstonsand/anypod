"""High-level wrapper for yt-dlp operations.

This module provides the YtdlpWrapper class that orchestrates yt-dlp operations
for metadata fetching and media downloading, integrating with source-specific
handlers for different platforms.
"""

from datetime import datetime
import logging
from pathlib import Path
from typing import Any

from ..db.types import Download, Feed
from ..exceptions import YtdlpApiError
from ..path_manager import PathManager
from .base_handler import FetchPurpose, ReferenceType, SourceHandlerBase
from .youtube_handler import YoutubeHandler
from .ytdlp_core import YtdlpCore, YtdlpInfo

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

    def _prepare_download_dir(self, feed_id: str) -> tuple[Path, Path]:
        try:
            feed_temp_path = self._paths.feed_tmp_dir(feed_id)
            feed_data_path = self._paths.feed_data_dir(feed_id)
        except ValueError as e:
            raise YtdlpApiError(
                message="Invalid feed identifier for yt-dlp paths.",
                feed_id=feed_id,
            ) from e
        except OSError as e:
            raise YtdlpApiError(
                message="Failed to create directories for yt-dlp paths.",
                feed_id=feed_id,
            ) from e

        return feed_temp_path, feed_data_path

    def _prepare_ydl_options(
        self,
        user_cli_args: dict[str, Any],
        purpose: FetchPurpose,
        source_specific_opts: dict[str, Any],
        download_temp_dir: Path | None = None,
        download_data_dir: Path | None = None,
        download_id: str | None = None,
    ) -> dict[str, Any]:
        log_params: dict[str, Any] = {
            "purpose": purpose,
            "num_user_provided_opts": len(user_cli_args),
            "source_specific_opts": list(source_specific_opts.keys()),
        }
        logger.debug("Preparing yt-dlp options.", extra=log_params)

        base_opts: dict[str, Any] = {
            "logger": logger,
            "quiet": True,
            "ignoreerrors": True,
            "no_warnings": True,
            "verbose": False,
            **source_specific_opts,
        }

        match purpose:
            case FetchPurpose.DISCOVERY:
                final_opts = {
                    **user_cli_args,
                    **base_opts,
                    "skip_download": True,
                    "extract_flat": "in_playlist",
                    "playlist_items": "1-5",
                }
            case FetchPurpose.METADATA_FETCH:
                final_opts = {
                    **user_cli_args,
                    **base_opts,
                    "skip_download": True,
                    "extract_flat": False,
                }
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
                final_opts = {
                    **user_cli_args,
                    **base_opts,
                    "skip_download": False,
                    "outtmpl": f"{download_id}.%(ext)s",
                    "paths": {
                        "temp": str(download_temp_dir),
                        "home": str(download_data_dir),
                    },
                    "extract_flat": False,
                }

        logger.debug(
            f"Prepared {purpose!s} options.",
            extra={
                **log_params,
                "final_opts_keys": list(final_opts.keys()),
            },
        )
        return final_opts

    def fetch_metadata(
        self,
        feed_id: str,
        url: str,
        user_yt_cli_args: dict[str, Any],
        fetch_since_date: datetime | None = None,
        fetch_until_date: datetime | None = None,
    ) -> tuple[Feed, list[Download]]:
        """Fetches metadata for a given feed and URL using yt-dlp.

        This method determines the appropriate fetch strategy for the provided URL,
        acquires metadata, and parses it into feed metadata and a list of found downloads.
        Date filtering is applied automatically using dateafter and datebefore arguments when dates are provided.

        Args:
            feed_id: The identifier for the feed.
            url: The URL to fetch metadata from.
            user_yt_cli_args: User-configured command-line arguments for yt-dlp.
            fetch_since_date: The lower bound date for the fetch operation (inclusive). Optional.
            fetch_until_date: The upper bound date for the fetch operation (exclusive). Optional.

        Returns:
            A tuple of (feed, downloads) where feed is a Feed object with extracted
            metadata and downloads is a list of Download objects.

        Raises:
            YtdlpApiError: If no fetchable URL is determined or if no information is extracted.
        """
        # Add date filtering to user-provided arguments if dates are provided
        yt_cli_args = dict(user_yt_cli_args)  # Make a copy
        log_extra = {
            "feed_id": feed_id,
            "url": url,
            "num_user_yt_cli_args": len(user_yt_cli_args),
        }

        if fetch_since_date is not None or fetch_until_date is not None:
            start_date = (
                fetch_since_date.strftime("%Y%m%d") if fetch_since_date else None
            )
            end_date = fetch_until_date.strftime("%Y%m%d") if fetch_until_date else None

            YtdlpCore.set_date_range(yt_cli_args, start_date, end_date)

            if fetch_since_date:
                log_extra["fetch_since_date"] = fetch_since_date.isoformat()
            if fetch_until_date:
                log_extra["fetch_until_date"] = fetch_until_date.isoformat()

        logger.info("Fetching metadata for feed.", extra=log_extra)

        source_specific_discovery_opts = (
            self._source_handler.get_source_specific_ydl_options(FetchPurpose.DISCOVERY)
        )

        def discovery_caller(
            handler_discovery_opts: dict[str, Any], url_to_discover: str
        ) -> YtdlpInfo | None:
            logger.debug(
                "Discovery caller invoked by strategy handler.",
                extra={
                    "feed_id": feed_id,
                    "original_url": url,
                    "url_to_discover": url_to_discover,
                    "handler_discovery_opts": list(handler_discovery_opts.keys()),
                },
            )
            effective_discovery_opts = self._prepare_ydl_options(
                user_cli_args={},  # Pass empty because this is just for discovery
                purpose=FetchPurpose.DISCOVERY,
                source_specific_opts=source_specific_discovery_opts,
            )
            effective_discovery_opts.update(handler_discovery_opts)
            return YtdlpCore.extract_info(effective_discovery_opts, url_to_discover)

        fetch_url, ref_type = self._source_handler.determine_fetch_strategy(
            feed_id, url, discovery_caller
        )
        actual_fetch_url = fetch_url or url
        if ref_type == ReferenceType.UNKNOWN_DIRECT_FETCH and not fetch_url:
            logger.info(
                "Discovery indicated direct fetch, using original URL.",
                extra={"feed_id": feed_id, "url": url},
            )
        elif not fetch_url:
            raise YtdlpApiError(
                message="Strategy determination returned no fetchable URL. Aborting.",
                feed_id=feed_id,
                url=url,
            )

        logger.info(
            "Acquiring metadata.",
            extra={
                "feed_id": feed_id,
                "actual_fetch_url": actual_fetch_url,
                "original_url": url,
                "reference_type": ref_type.name,
            },
        )

        source_specific_metadata_opts = (
            self._source_handler.get_source_specific_ydl_options(
                FetchPurpose.METADATA_FETCH
            )
        )
        main_fetch_opts = self._prepare_ydl_options(
            yt_cli_args, FetchPurpose.METADATA_FETCH, source_specific_metadata_opts
        )

        ytdlp_info = YtdlpCore.extract_info(main_fetch_opts, actual_fetch_url)
        if ytdlp_info is None:
            raise YtdlpApiError(
                message="No information extracted by yt-dlp. This might be due to filters or content unavailability.",
                feed_id=feed_id,
                url=actual_fetch_url,
            )

        extracted_feed = self._source_handler.extract_feed_metadata(
            feed_id,
            ytdlp_info,
            ref_type,
            url,
            fetch_until_date,
        )

        parsed_downloads = self._source_handler.parse_metadata_to_downloads(
            feed_id,
            ytdlp_info,
            source_identifier=feed_id,
            ref_type=ref_type,
        )

        logger.info(
            "Successfully processed metadata.",
            extra={
                "feed_id": feed_id,
                "fetch_url": actual_fetch_url,
                "original_url": url,
                "num_downloads_identified": len(parsed_downloads),
            },
        )
        return extracted_feed, parsed_downloads

    def download_media_to_file(
        self,
        download: Download,
        yt_cli_args: dict[str, Any],
    ) -> Path:
        """Download the media for a given Download to a target directory.

        yt-dlp will place the final file in a feed-specific subdirectory within
        the application's configured data directory.

        Args:
            download: The Download object containing metadata.
            yt_cli_args: User-provided yt-dlp CLI arguments for this feed.

        Returns:
            The absolute path to the successfully downloaded media file.

        Raises:
            YtdlpApiError: If the download fails or the downloaded file is not found.
        """
        download_temp_dir, download_data_dir = self._prepare_download_dir(download.feed)

        logger.info(
            "Requesting media download via yt-dlp.",
            extra={
                "feed_id": download.feed,
                "download_id": download.id,
                "download_target_dir": str(download_data_dir),
                "source_url": download.source_url,
            },
        )

        source_specific_download_opts = (
            self._source_handler.get_source_specific_ydl_options(
                FetchPurpose.MEDIA_DOWNLOAD
            )
        )
        try:
            download_opts = self._prepare_ydl_options(
                user_cli_args=yt_cli_args,
                purpose=FetchPurpose.MEDIA_DOWNLOAD,
                source_specific_opts=source_specific_download_opts,
                download_temp_dir=download_temp_dir,
                download_data_dir=download_data_dir,
                download_id=download.id,
            )
        except YtdlpApiError as e:
            e.feed_id = download.feed
            e.url = download.source_url
            raise

        url_to_download = download.source_url

        try:
            # TODO: maybe we can get the filepath from here?
            YtdlpCore.download(download_opts, url_to_download)
        except YtdlpApiError as e:
            logger.error(
                "yt-dlp download call failed.",
                exc_info=e,
                extra={
                    "feed_id": download.feed,
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
                    "feed_id": download.feed,
                    "download_id": download.id,
                    "url": url_to_download,
                },
            )
            raise YtdlpApiError(
                message="Unexpected error during media download.",
                feed_id=download.feed,
                download_id=download.id,
                url=url_to_download,
            ) from e

        downloaded_files = list(download_data_dir.glob(f"{download.id}.*"))

        if not downloaded_files:
            raise YtdlpApiError(
                message="Downloaded file not found after attempted download. yt-dlp might have filtered.",
                feed_id=download.feed,
                download_id=download.id,
                url=url_to_download,
            )
        if len(downloaded_files) > 1:
            logger.warning(
                "Multiple files found after attempting download. Using the first one.",
                extra={
                    "feed_id": download.feed,
                    "download_id": download.id,
                    "files_found": [str(f) for f in downloaded_files],
                },
            )

        downloaded_file = downloaded_files[0]

        if not downloaded_file.is_file() or downloaded_file.stat().st_size == 0:
            raise YtdlpApiError(
                message="Downloaded file is invalid (not a file or empty).",
                feed_id=download.feed,
                download_id=download.id,
                url=url_to_download,
            )

        logger.info(
            "Download complete.",
            extra={
                "feed_id": download.feed,
                "download_id": download.id,
                "file_path": str(downloaded_file),
            },
        )

        return downloaded_file
