from collections.abc import Callable
from enum import Enum
from typing import Any, Protocol

from ..db import Download
from .ytdlp_core import YtdlpInfo


class ReferenceType(Enum):
    """Represents the what kind of reference is being requested."""

    SINGLE = "single"
    COLLECTION = "collection"  # For playlists, channel tabs listing videos, etc.
    UNKNOWN_RESOLVED_URL = "unknown_resolved_url"
    UNKNOWN_DIRECT_FETCH = "unknown_direct_fetch"

    def __str__(self) -> str:
        return self.value


class FetchPurpose(Enum):
    """Indicates the purpose of the yt-dlp fetch operation."""

    DISCOVERY = "discovery"
    METADATA_FETCH = "metadata_fetch"
    MEDIA_DOWNLOAD = "media_download"

    def __str__(self) -> str:
        return self.value


# Type alias for the function YtdlpWrapper passes to handlers for discovery calls
YdlApiCaller = Callable[[dict[str, Any], str], YtdlpInfo | None]


class SourceHandlerBase(Protocol):
    """Protocol defining the interface for source-specific strategy and parsing logic."""

    def get_source_specific_ydl_options(self, purpose: FetchPurpose) -> dict[str, Any]:
        """Returns source-specific options to be merged into yt-dlp opts.

        Example: {'match_filter': '!is_live'} for YouTube.
        """
        ...

    def determine_fetch_strategy(
        self,
        feed_id: str,
        initial_url: str,
        ydl_caller_for_discovery: YdlApiCaller,
    ) -> tuple[str | None, ReferenceType]:
        """Classifies the initial URL and determines the final URL to fetch downloads from."""
        ...

    def parse_metadata_to_downloads(
        self,
        feed_id: str,
        ytdlp_info: YtdlpInfo,
        source_identifier: str,
        ref_type: ReferenceType,
    ) -> list[Download]:
        """Parses the full metadata dictionary from yt-dlp into a list of Download objects."""
        ...
