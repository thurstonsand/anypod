"""Transcript metadata type."""

from dataclasses import dataclass

from ...db.types import TranscriptSource


@dataclass(frozen=True, slots=True)
class TranscriptInfo:
    """Metadata about a downloaded transcript.

    Attributes:
        ext: File extension (e.g., "vtt").
        lang: Language code (e.g., "en").
        source: Source type (creator or auto-generated).
    """

    ext: str
    lang: str
    source: TranscriptSource
