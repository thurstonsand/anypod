"""Enumeration of feed source types."""

from enum import Enum


class SourceType(Enum):
    """Represent the type of source for a feed.

    Indicates what kind of source the feed represents for proper handling
    and metadata extraction.
    """

    CHANNEL = "CHANNEL"
    PLAYLIST = "PLAYLIST"
    SINGLE_VIDEO = "SINGLE_VIDEO"
    MANUAL = "MANUAL"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return self.value
