"""Base handler protocol and types for yt-dlp source-specific processing.

This module defines the protocol interface and supporting types for
implementing source-specific strategies for yt-dlp operations, including
fetch strategy determination and metadata parsing.
"""

from collections.abc import Awaitable, Callable
from datetime import datetime
from enum import Enum
from typing import Protocol

from ..db.types import Download, Feed
from .ytdlp_core import YtdlpArgs, YtdlpInfo


class ReferenceType(Enum):
    """Represent the type of reference being requested.

    Indicates whether the URL points to a single item, a collection
    of items, or an unknown reference type that requires special handling.
    """

    SINGLE = "single"
    CHANNEL = "channel"  # For channel videos tab or main channel pages
    COLLECTION = "collection"  # For playlists, other channel tabs, etc.
    UNKNOWN_RESOLVED_URL = "unknown_resolved_url"
    UNKNOWN_DIRECT_FETCH = "unknown_direct_fetch"

    def __str__(self) -> str:
        return self.value


class FetchPurpose(Enum):
    """Indicate the purpose of the yt-dlp fetch operation.

    Used to determine appropriate options and behavior for different
    types of yt-dlp operations.
    """

    DISCOVERY = "discovery"
    METADATA_FETCH = "metadata_fetch"
    MEDIA_DOWNLOAD = "media_download"

    def __str__(self) -> str:
        return self.value


# Type alias for the function YtdlpWrapper passes to handlers for discovery calls
YdlApiCaller = Callable[[list[str], str], Awaitable[YtdlpInfo | None]]


class SourceHandlerBase(Protocol):
    """Protocol defining the interface for source-specific strategy and parsing logic.

    Implementations of this protocol provide source-specific behavior for
    different media platforms, handling URL classification, option customization,
    and metadata parsing into Download objects.
    """

    def set_source_specific_ydl_options(
        self, args: YtdlpArgs, purpose: FetchPurpose
    ) -> YtdlpArgs:
        """Apply source-specific CLI options to yt-dlp arguments.

        Args:
            args: YtdlpArgs object to modify with source-specific options.
            purpose: The purpose of the fetch operation.

        Returns:
            Modified YtdlpArgs object with source-specific options applied.
        """
        ...

    async def determine_fetch_strategy(
        self,
        feed_id: str,
        initial_url: str,
        ydl_caller_for_discovery: YdlApiCaller,
    ) -> tuple[str | None, ReferenceType]:
        """Classify the initial URL and determine the final URL to fetch downloads from.

        Args:
            feed_id: The feed identifier.
            initial_url: The initial URL to classify.
            ydl_caller_for_discovery: Function to call yt-dlp for discovery.

        Returns:
            Tuple of (final_url, reference_type).
        """
        ...

    def extract_feed_metadata(
        self,
        feed_id: str,
        ytdlp_info: YtdlpInfo,
        ref_type: ReferenceType,
        source_url: str,
        fetch_until_date: datetime | None = None,
    ) -> Feed:
        """Extract feed-level metadata from yt-dlp response.

        Args:
            feed_id: The feed identifier.
            ytdlp_info: The yt-dlp metadata information.
            ref_type: The type of reference being parsed.
            source_url: The original source URL for this feed.
            fetch_until_date: The upper bound date for the fetch operation, used for setting last_successful_sync. Optional.

        Returns:
            Feed object with extracted metadata populated.
        """
        ...

    def parse_metadata_to_downloads(
        self,
        feed_id: str,
        ytdlp_info: YtdlpInfo,
        source_identifier: str,
        ref_type: ReferenceType,
    ) -> list[Download]:
        """Parse the full metadata dictionary from yt-dlp into Download objects.

        Args:
            feed_id: The feed identifier.
            ytdlp_info: The yt-dlp metadata information.
            source_identifier: Identifier for the source being parsed.
            ref_type: The type of reference being parsed.

        Returns:
            List of Download objects parsed from the metadata.
        """
        ...
