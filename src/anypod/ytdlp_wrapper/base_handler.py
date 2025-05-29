"""Base handler protocol and types for yt-dlp source-specific processing.

This module defines the protocol interface and supporting types for
implementing source-specific strategies for yt-dlp operations, including
fetch strategy determination and metadata parsing.
"""

from collections.abc import Callable
from enum import Enum
from typing import Any, Protocol

from ..db import Download
from .ytdlp_core import YtdlpInfo


class ReferenceType(Enum):
    """Represent the type of reference being requested.

    Indicates whether the URL points to a single item, a collection
    of items, or an unknown reference type that requires special handling.
    """

    SINGLE = "single"
    COLLECTION = "collection"  # For playlists, channel tabs listing videos, etc.
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
YdlApiCaller = Callable[[dict[str, Any], str], YtdlpInfo | None]


class SourceHandlerBase(Protocol):
    """Protocol defining the interface for source-specific strategy and parsing logic.

    Implementations of this protocol provide source-specific behavior for
    different media platforms, handling URL classification, option customization,
    and metadata parsing into Download objects.
    """

    def get_source_specific_ydl_options(self, purpose: FetchPurpose) -> dict[str, Any]:
        """Return source-specific options to be merged into yt-dlp opts.

        Args:
            purpose: The purpose of the fetch operation.

        Returns:
            Dictionary of yt-dlp options specific to this source.
        """
        ...

    def determine_fetch_strategy(
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
