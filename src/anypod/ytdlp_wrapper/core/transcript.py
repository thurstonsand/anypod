"""Core yt-dlp transcript functionality and typed data access."""

from typing import Any

from ...db.types import TranscriptSource


class YtdlpTranscriptFormat:
    """A wrapper around a single yt-dlp transcript format dictionary for strongly-typed access.

    Provides type-safe access to transcript format metadata fields with
    validation and error handling for missing or invalid field types.

    Attributes:
        _format_dict: The underlying transcript format metadata dictionary.
    """

    def __init__(self, format_dict: dict[str, Any]):
        self._format_dict = format_dict

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, YtdlpTranscriptFormat):
            return NotImplemented
        return self._format_dict == other._format_dict

    @property
    def ext(self) -> str | None:
        """Get the transcript file extension (e.g., 'vtt', 'json3', 'srv3')."""
        ext = self._format_dict.get("ext")
        return ext if isinstance(ext, str) else None

    @property
    def url(self) -> str | None:
        """Get the transcript download URL."""
        url = self._format_dict.get("url")
        return url if isinstance(url, str) else None

    @property
    def name(self) -> str | None:
        """Get the human-readable name (e.g., 'English')."""
        name = self._format_dict.get("name")
        return name if isinstance(name, str) else None


class YtdlpTranscript:
    """A collection wrapper for yt-dlp transcript data with source information.

    Provides type-safe access to transcript metadata from yt-dlp with
    source information (creator-provided vs auto-generated).

    Attributes:
        _formats: List of YtdlpTranscriptFormat objects.
        _source: The source type (creator or auto-generated).
    """

    def __init__(
        self,
        formats_list: list[dict[str, Any]],
        source: TranscriptSource,
    ):
        self._formats = [YtdlpTranscriptFormat(fmt) for fmt in formats_list]
        self._source = source

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, YtdlpTranscript):
            return NotImplemented
        return self._formats == other._formats and self._source == other._source

    def __len__(self) -> int:
        """Get the number of available transcript formats."""
        return len(self._formats)

    def __iter__(self):
        """Iterate over transcript formats."""
        return iter(self._formats)

    @property
    def source(self) -> TranscriptSource:
        """Get the transcript source type."""
        return self._source

    @property
    def all(self) -> list[YtdlpTranscriptFormat]:
        """Get all transcript formats."""
        return self._formats.copy()
