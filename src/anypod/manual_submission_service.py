"""Utilities for validating and preparing manual download submissions."""

import logging
from pathlib import Path

from .config import FeedConfig
from .db.types import Download, DownloadStatus, SourceType
from .exceptions import (
    ManualSubmissionUnavailableError,
    ManualSubmissionUnsupportedURLError,
    YtdlpError,
)
from .ytdlp_wrapper import YtdlpWrapper

logger = logging.getLogger(__name__)


class ManualSubmissionService:
    """Validate manual submissions and extract yt-dlp metadata.

    Attributes:
        _ytdlp_wrapper: Thin wrapper around yt-dlp utilities for metadata fetches.
    """

    def __init__(self, ytdlp_wrapper: YtdlpWrapper) -> None:
        self._ytdlp_wrapper = ytdlp_wrapper

    async def fetch_submission_download(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        url: str,
        cookies_path: Path | None,
    ) -> Download:
        """Fetch metadata for a manually submitted URL.

        Args:
            feed_id: Feed receiving the submission.
            feed_config: Configuration attached to the feed.
            url: User-provided video URL.
            cookies_path: Optional cookies file forwarded to yt-dlp.

        Returns:
            Parsed :class:`Download` metadata ready for persistence.

        Raises:
            ManualSubmissionUnsupportedURLError: When yt-dlp cannot process the URL.
            ManualSubmissionUnavailableError: When the URL is not yet downloadable.
        """
        logger.debug(
            "Fetching manual submission metadata.",
            extra={"feed_id": feed_id, "url": url},
        )

        try:
            downloads = await self._ytdlp_wrapper.fetch_new_downloads_metadata(
                feed_id=feed_id,
                source_type=SourceType.SINGLE_VIDEO,
                source_url=url,
                resolved_url=url,
                user_yt_cli_args=feed_config.yt_args,
                fetch_since_date=None,
                keep_last=None,
                cookies_path=cookies_path,
            )
        except YtdlpError as e:
            raise ManualSubmissionUnsupportedURLError(
                "URL could not be processed by yt-dlp.",
                feed_id=feed_id,
                url=url,
            ) from e

        if not downloads:
            raise ManualSubmissionUnavailableError(
                "No downloadable media found for URL.",
                feed_id=feed_id,
                url=url,
            )

        download = downloads[0]
        if download.status != DownloadStatus.QUEUED:
            raise ManualSubmissionUnavailableError(
                "URL is not yet available as on-demand media.",
                feed_id=feed_id,
                url=url,
            )

        return download
