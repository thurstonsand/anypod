"""Base handler protocol and types for yt-dlp source-specific processing.

This module defines the protocol interface and supporting types for
implementing source-specific strategies for yt-dlp operations, including
fetch strategy determination and metadata parsing.
"""

from pathlib import Path
from typing import Protocol

from ..db.types import Download, Feed, SourceType
from .core import YtdlpInfo


class SourceHandlerBase(Protocol):
    """Protocol defining the interface for source-specific strategy and parsing logic.

    Implementations of this protocol provide source-specific behavior for
    different media platforms, handling URL classification, option customization,
    and metadata parsing into Download objects.
    """

    async def determine_fetch_strategy(
        self,
        feed_id: str,
        initial_url: str,
        cookies_path: Path | None = None,
    ) -> tuple[str | None, SourceType]:
        """Classify the initial URL and determine the final URL to fetch downloads from.

        Args:
            feed_id: The feed identifier.
            initial_url: The initial URL to classify.
            cookies_path: Path to cookies.txt file for authentication, or None if not needed.

        Returns:
            Tuple of (final_url, source_type).
        """
        ...

    def extract_feed_metadata(
        self,
        feed_id: str,
        ytdlp_info: YtdlpInfo,
        source_type: SourceType,
        source_url: str,
    ) -> Feed:
        """Extract feed-level metadata from yt-dlp response.

        Args:
            feed_id: The feed identifier.
            ytdlp_info: The yt-dlp metadata information.
            source_type: The type of source being parsed.
            source_url: The original source URL for this feed.

        Returns:
            Feed object with extracted metadata populated.
        """
        ...

    def extract_download_metadata(
        self,
        feed_id: str,
        ytdlp_info: YtdlpInfo,
    ) -> Download:
        """Extract metadata from a single yt-dlp video entry into a Download object.

        Args:
            feed_id: The feed identifier.
            ytdlp_info: The yt-dlp metadata for a single video.

        Returns:
            A Download object parsed from the metadata.
        """
        ...
