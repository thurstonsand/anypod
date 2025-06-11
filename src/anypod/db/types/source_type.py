"""Enumeration of feed source types."""

from enum import Enum

from ..sqlite_utils_core import register_adapter


class SourceType(Enum):
    """Represent the type of source for a feed.

    Indicates what kind of source the feed represents for proper handling
    and metadata extraction.
    """

    CHANNEL = "channel"
    PLAYLIST = "playlist"
    SINGLE_VIDEO = "single_video"
    UNKNOWN = "unknown"

    def __str__(self) -> str:
        return self.value


register_adapter(SourceType, lambda source_type: source_type.value)
