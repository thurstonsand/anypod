"""Core yt-dlp thumbnail functionality and typed data access."""

from typing import Any


class YtdlpThumbnail:
    """A wrapper around a single yt-dlp thumbnail dictionary for strongly-typed access.

    Provides type-safe access to thumbnail metadata fields with validation
    and error handling for missing or invalid field types.

    Attributes:
        _thumbnail_dict: The underlying thumbnail metadata dictionary.
    """

    def __init__(self, thumbnail_dict: dict[str, Any]):
        self._thumbnail_dict = thumbnail_dict

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, YtdlpThumbnail):
            return NotImplemented
        return self._thumbnail_dict == other._thumbnail_dict

    @property
    def url(self) -> str | None:
        """Get the thumbnail URL."""
        return self._thumbnail_dict.get("url")

    @property
    def preference(self) -> int | None:
        """Get the thumbnail preference (higher = better quality)."""
        pref = self._thumbnail_dict.get("preference")
        return pref if isinstance(pref, int) else None

    @property
    def width(self) -> int | None:
        """Get the thumbnail width in pixels."""
        width = self._thumbnail_dict.get("width")
        return width if isinstance(width, int) else None

    @property
    def height(self) -> int | None:
        """Get the thumbnail height in pixels."""
        height = self._thumbnail_dict.get("height")
        return height if isinstance(height, int) else None

    @property
    def format_id(self) -> str | None:
        """Get the thumbnail format identifier."""
        return self._thumbnail_dict.get("id")

    @property
    def is_jpg(self) -> bool:
        """Check if the thumbnail is in JPG format."""
        url = self.url
        return bool(url and url.endswith(".jpg"))

    @property
    def is_png(self) -> bool:
        """Check if the thumbnail is in PNG format."""
        url = self.url
        return bool(url and url.endswith(".png"))

    @property
    def is_supported_format(self) -> bool:
        """Check if the thumbnail is in a supported format (JPG or PNG)."""
        return self.is_jpg or self.is_png


class YtdlpThumbnails:
    """A collection wrapper for yt-dlp thumbnails with filtering capabilities.

    Provides type-safe access to thumbnail arrays from yt-dlp metadata
    with filtering methods for format selection and quality ranking.

    Attributes:
        _thumbnails: List of YtdlpThumbnail objects.
    """

    def __init__(self, thumbnails_list: list[dict[str, Any]]):
        self._thumbnails = [YtdlpThumbnail(thumb) for thumb in thumbnails_list]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, YtdlpThumbnails):
            return NotImplemented
        return self._thumbnails == other._thumbnails

    def __len__(self) -> int:
        """Get the number of thumbnails."""
        return len(self._thumbnails)

    def __iter__(self):
        """Iterate over thumbnails."""
        return iter(self._thumbnails)

    @property
    def all(self) -> list[YtdlpThumbnail]:
        """Get all thumbnails."""
        return self._thumbnails.copy()

    @property
    def supported_formats(self) -> list[YtdlpThumbnail]:
        """Get thumbnails in supported formats (JPG or PNG)."""
        return [thumb for thumb in self._thumbnails if thumb.is_supported_format]

    def best_supported(self) -> YtdlpThumbnail | None:
        """Get the highest preference thumbnail in a supported format."""
        supported = self.supported_formats
        if not supported:
            return None
        return max(supported, key=lambda x: x.preference or -999)
