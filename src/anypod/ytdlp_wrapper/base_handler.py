"""Base handler protocol and types for yt-dlp source-specific processing.

This module defines the protocol interface and supporting types for
implementing source-specific strategies for yt-dlp operations, including
fetch strategy determination and metadata parsing.
"""

from typing import Protocol
from urllib.parse import urlparse

from ..db.types import Download, Feed, SourceType
from ..exceptions import YtdlpError
from ..ffprobe import FFProbe
from .core import YtdlpArgs, YtdlpInfo


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
        base_args: YtdlpArgs,
    ) -> tuple[str | None, SourceType]:
        """Classify the initial URL and determine the final URL to fetch downloads from.

        Args:
            feed_id: The feed identifier.
            initial_url: The initial URL to classify.
            base_args: Pre-configured YtdlpArgs with shared settings (POT, updates, cookies, etc.).

        Returns:
            Tuple of (final_url, source_type).
        """
        ...

    def prepare_playlist_info_args(
        self,
        args: YtdlpArgs,
    ) -> YtdlpArgs:
        """Prepare args for playlist/feed metadata extraction.

        Args:
            args: Builder instance that will be sent to yt-dlp.

        Returns:
            The same builder instance for chaining.
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

    def prepare_thumbnail_args(
        self,
        args: YtdlpArgs,
    ) -> YtdlpArgs:
        """Prepare args for thumbnail download operations.

        Args:
            args: Builder instance that will be sent to yt-dlp.

        Returns:
            The same builder instance for chaining.
        """
        ...

    def prepare_downloads_info_args(
        self,
        args: YtdlpArgs,
    ) -> YtdlpArgs:
        """Prepare args for downloads metadata enumeration.

        Args:
            args: Builder instance that will be sent to yt-dlp.

        Returns:
            The same builder instance for chaining.
        """
        ...

    async def extract_download_metadata(
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

    def prepare_media_download_args(
        self,
        args: YtdlpArgs,
    ) -> YtdlpArgs:
        """Prepare args for media download operations."""
        ...


class HandlerSelector:
    """Resolve source handlers based on URL hostnames."""

    def __init__(self, ffprobe: FFProbe):
        from .patreon_handler import PatreonHandler
        from .youtube_handler import YoutubeHandler

        self._default_handler = YoutubeHandler()
        self._hostname_handlers = {
            "patreon.com": PatreonHandler(ffprobe),
        }

    def select(self, url: str) -> SourceHandlerBase:
        """Return the registered handler for `url`.

        Falls back to the default handler when no hostname-specific handler matches.
        """
        try:
            hostname = urlparse(url).hostname
        except ValueError as e:
            raise YtdlpError(f"Invalid url found: {url}") from e

        if not hostname:
            raise YtdlpError(f"URL has no hostname: {url}")

        hostname = hostname.lower()
        for suffix, handler in self._hostname_handlers.items():
            if hostname == suffix or hostname.endswith(f".{suffix}"):
                return handler

        return self._default_handler
