"""Enumeration of transcript source types."""

from enum import Enum


class TranscriptSource(Enum):
    """Represent the source of a transcript.

    Indicates where the transcript originated from for proper handling
    during download and display.

    Values:
        NOT_AVAILABLE: No transcript is available for this download.
        CREATOR: Creator-provided subtitles.
        AUTO: Auto-generated captions.
    """

    NOT_AVAILABLE = "not_available"
    CREATOR = "creator"
    AUTO = "auto"

    def __str__(self) -> str:
        return self.value
