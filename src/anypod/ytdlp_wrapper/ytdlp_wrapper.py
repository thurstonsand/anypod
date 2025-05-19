import logging
from pathlib import Path
from typing import Any

from ..db import Download
from ..exceptions import YtdlpApiError
from .base_handler import FetchPurpose, ReferenceType, SourceHandlerBase
from .youtube_handler import YoutubeHandler
from .ytdlp_core import YtdlpCore

logger = logging.getLogger(__name__)


class YtdlpWrapper:
    """
    Wrapper around yt-dlp for fetching and parsing metadata.
    """

    _source_handler: SourceHandlerBase = YoutubeHandler()

    def _prepare_ydl_options(
        self,
        user_cli_args: list[str],
        purpose: FetchPurpose,
        source_specific_opts: dict[str, Any],
        download_target_path: Path | None = None,
    ) -> dict[str, Any]:
        log_params: dict[str, Any] = {
            "purpose": purpose,
            "num_user_cli_args": len(user_cli_args),
            "source_specific_opts": list(source_specific_opts.keys()),
            "download_target_path": download_target_path,
        }
        logger.debug("Preparing yt-dlp options.", extra=log_params)

        try:
            parsed_user_opts = YtdlpCore.parse_options(user_cli_args)
            logger.debug(
                "Successfully parsed user CLI arguments for yt-dlp options.",
                extra={**log_params, "parsed_user_opts": list(parsed_user_opts.keys())},
            )
        except Exception as e:
            raise YtdlpApiError(
                message="Invalid yt-dlp CLI arguments provided.",
            ) from e

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
                    **base_opts,
                    "skip_download": True,
                    "extract_flat": "in_playlist",
                    "playlist_items": "1-5",
                }
                logger.debug(
                    "Prepared DISCOVERY options.",
                    extra={**log_params, "final_opts": list(final_opts.keys())},
                )
            case FetchPurpose.METADATA_FETCH:
                final_opts = {  # type: ignore
                    **parsed_user_opts,
                    **base_opts,
                    "skip_download": True,
                    "extract_flat": False,
                }
                logger.debug(
                    "Prepared METADATA_FETCH options.",
                    extra={**log_params, "final_opts": list(final_opts.keys())},  # type: ignore
                )
            case FetchPurpose.MEDIA_DOWNLOAD:
                if not download_target_path:
                    raise ValueError(
                        "download_target_path is required for MEDIA_DOWNLOAD purpose"
                    )
                final_opts = {  # type: ignore
                    **parsed_user_opts,
                    **base_opts,
                    "skip_download": False,
                    "outtmpl": str(download_target_path),
                    "extract_flat": False,
                }
                logger.debug(
                    "Prepared MEDIA_DOWNLOAD options.",
                    extra={**log_params, "final_opts": list(final_opts.keys())},  # type: ignore
                )

        return final_opts  # type: ignore

    def fetch_metadata(
        self,
        feed_id: str,
        url: str,
        yt_cli_args: list[str],
    ) -> list[Download]:
        """Fetches metadata for a given feed and URL using yt-dlp.

        This method determines the appropriate fetch strategy for the provided URL,
        acquires metadata, and parses it into a list of Download objects.

        Args:
            feed_id: The identifier for the feed.
            url: The URL to fetch metadata from.
            yt_cli_args: Additional command-line arguments for yt-dlp.

        Returns:
            A list of Download objects containing the fetched metadata.

        Raises:
            YtdlpApiError: If no fetchable URL is determined or if no information is extracted.
        """
        logger.info(
            "Fetching metadata for feed.",
            extra={
                "feed_id": feed_id,
                "url": url,
                "num_yt_cli_args": len(yt_cli_args),
            },
        )

        source_specific_discovery_opts = (
            self._source_handler.get_source_specific_ydl_options(FetchPurpose.DISCOVERY)
        )

        def discovery_caller(
            handler_discovery_opts: dict[str, Any], url_to_discover: str
        ) -> dict[str, Any] | None:
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
                user_cli_args=[],  # Pass empty because this is just for discovery
                purpose=FetchPurpose.DISCOVERY,
                source_specific_opts=source_specific_discovery_opts,
            )
            effective_discovery_opts.update(handler_discovery_opts)
            return YtdlpCore.extract_info(effective_discovery_opts, url_to_discover)

        fetch_url, ref_type = self._source_handler.determine_fetch_strategy(
            url, discovery_caller
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

        raw_info_dict = YtdlpCore.extract_info(main_fetch_opts, actual_fetch_url)

        if raw_info_dict is None:
            raise YtdlpApiError(
                message="No information extracted by yt-dlp. This might be due to filters or content unavailability.",
                feed_id=feed_id,
                url=actual_fetch_url,
            )

        parsed_downloads = self._source_handler.parse_metadata_to_downloads(
            raw_info_dict,
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
        return parsed_downloads

    def download_media_to_file(
        self,
        download: Download,
        yt_cli_args: list[str],
        download_target_dir: Path,
    ) -> Path:
        """Downloads the media for a given Download to a target directory.

        Uses yt-dlp's download() method.

        Args:
            download: The Download object containing metadata.
            yt_cli_args: User-provided yt-dlp CLI arguments for this feed.
            download_target_dir: The temporary directory path to download into.

        Returns:
            A tuple containing:
                - str: The absolute path to the downloaded media file.
                - dict: A dictionary of potentially updated metadata fields
                        (e.g., filesize, ext) obtained after download.

        Raises:
            YtdlpApiError: If the download or subsequent metadata fetch fails.
            ValueError: If download_target_dir is not provided.
        """
        target_path = (
            download_target_dir / download.feed / f"{download.id}.{download.ext}"
        )
        logger.info(
            "Downloading media.",
            extra={
                "feed_id": download.feed,
                "download_id": download.id,
                "target_path": target_path,
            },
        )

        source_specific_download_opts = (
            self._source_handler.get_source_specific_ydl_options(
                FetchPurpose.MEDIA_DOWNLOAD
            )
        )
        download_opts = self._prepare_ydl_options(
            user_cli_args=yt_cli_args,
            purpose=FetchPurpose.MEDIA_DOWNLOAD,
            source_specific_opts=source_specific_download_opts,
            download_target_path=target_path,
        )

        url_to_download = download.source_url

        try:
            YtdlpCore.download(download_opts, url_to_download)
        except YtdlpApiError as e:
            logger.error(
                "Download failed during internal download call.",
                exc_info=e,
                extra={
                    "feed_id": download.feed,
                    "download_id": download.id,
                    "url": url_to_download,
                    "file_path": target_path,
                },
            )
            raise
        except Exception as e:
            logger.error(
                "Unexpected error during internal download call wrapper.",
                exc_info=e,
                extra={
                    "feed_id": download.feed,
                    "download_id": download.id,
                    "url": url_to_download,
                    "file_path": target_path,
                },
            )
            raise YtdlpApiError(
                message="Failed to download media.",
                feed_id=download.feed,
                url=url_to_download,
            ) from e

        if not target_path.exists():
            raise YtdlpApiError(
                message=f"File not downloaded to {target_path}; may have been filtered out by yt-dlp.",
                feed_id=download.feed,
                url=url_to_download,
            )

        logger.info(
            "Download complete.",
            extra={
                "feed_id": download.feed,
                "download_id": download.id,
                "file_path": target_path,
            },
        )

        return target_path
