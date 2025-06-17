"""Core yt-dlp wrapper functionality and typed data access.

This module provides core yt-dlp integration including strongly-typed access
to yt-dlp metadata and static methods for yt-dlp operations like option
parsing, metadata extraction, and media downloading.
"""

from types import UnionType
from typing import Any, Union, get_origin

import yt_dlp  # type: ignore
from yt_dlp.utils import DateRange, ExtractorError, UserNotLive  # type: ignore

from ..exceptions import YtdlpApiError, YtdlpFieldInvalidError, YtdlpFieldMissingError


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


class YtdlpInfo:
    """A wrapper around yt-dlp extract_info output for strongly-typed access.

    Provides type-safe access to fields in yt-dlp metadata dictionaries
    with validation and error handling for missing or invalid field types.

    Attributes:
        _info_dict: The underlying yt-dlp metadata dictionary.
    """

    def __init__(self, info_dict: dict[str, Any]):
        self._info_dict = info_dict

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, YtdlpInfo):
            return NotImplemented
        return self._info_dict == other._info_dict

    def get_raw(self, field_name: str) -> Any | None:
        """Retrieves a field's value directly from the dictionary without any type checking or validation by this class.

        Args:
            field_name: The name of the field to retrieve.

        Returns:
            The field's value if it exists, otherwise None.
        """
        return self._info_dict.get(field_name, None)

    def get[T](self, field_name: str, tpe: type[T] | tuple[type[T], ...]) -> T | None:
        """Retrieves a field value if it exists and matches the expected type(s).

        Args:
            field_name: The name of the field to retrieve.
            tpe: The expected type or a tuple of expected types for the field.

        Returns:
            The field's value if it exists, otherwise None.

        Raises:
            YtdlpFieldInvalidError: If the field exists but its type does not match.
        """
        if field_name not in self._info_dict or self._info_dict[field_name] is None:
            return None

        field = self._info_dict[field_name]

        origin = get_origin(tpe)
        # if it's a parameterized generic (e.g. list[int]), we need to use the origin type (e.g. list)
        # if it's anything else, including Union (e.g. int | str), we use the type itself
        # this is a limitation of isinstance
        check_type = origin if origin not in (None, Union, UnionType) else tpe

        if isinstance(field, check_type):
            return field
        else:
            raise YtdlpFieldInvalidError(
                field_name=field_name,
                expected_type=tpe,
                actual_value=field,
            )

    def required[T](self, field_name: str, tpe: type[T] | tuple[type[T], ...]) -> T:
        """Retrieves a required field value, ensuring it exists and matches the expected type(s).

        Args:
            field_name: The name of the field to retrieve.
            tpe: The expected type or a tuple of expected types for the field.

        Returns:
            The field's value, guaranteed to exist and match the type.

        Raises:
            YtdlpFieldMissingError: If the field does not exist.
            YtdlpFieldInvalidError: If the field exists but its type does not match
        """
        field = self.get(field_name, tpe)
        if field is None:
            raise YtdlpFieldMissingError(
                field_name=field_name,
            )
        return field

    def entries(self) -> list["YtdlpInfo | None"] | None:
        """Extract and wrap entries from a playlist or collection.

        Returns:
            List of YtdlpInfo objects for each entry, or None if no entries exist.

        Raises:
            YtdlpFieldInvalidError: If an entry has an invalid type.
        """
        raw_entries = self.get("entries", list[dict[str, Any] | None])  # type: ignore
        if raw_entries is None:
            return None

        entries: list[YtdlpInfo | None] = []
        for entry in raw_entries:  # type: ignore
            if entry is None:
                entries.append(None)
                continue

            # validate entry type at runtime
            if not isinstance(entry, dict):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise YtdlpFieldInvalidError(
                    field_name="entries",
                    expected_type=dict,
                    actual_value=entry,  # type: ignore
                )

            entries.append(YtdlpInfo(entry))

        return entries

    def thumbnails(self) -> YtdlpThumbnails | None:
        """Extract and wrap thumbnails from yt-dlp metadata.

        Returns:
            YtdlpThumbnails object for accessing thumbnail data, or None if no thumbnails exist.

        Raises:
            YtdlpFieldInvalidError: If thumbnails field has an invalid type.
        """
        raw_thumbnails = self.get("thumbnails", list[dict[str, Any]])  # type: ignore
        if raw_thumbnails is None:
            return None

        # Validate that each thumbnail is a dictionary
        for i, thumbnail in enumerate(raw_thumbnails):  # type: ignore
            if not isinstance(thumbnail, dict):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise YtdlpFieldInvalidError(
                    field_name=f"thumbnails[{i}]",
                    expected_type=dict,
                    actual_value=thumbnail,  # type: ignore
                )

        return YtdlpThumbnails(raw_thumbnails)  # type: ignore


class YtdlpCore:
    """Static methods for core yt-dlp operations.

    Provides a clean interface to yt-dlp functionality including option
    parsing, metadata extraction, and media downloading with proper
    error handling and conversion to application-specific exceptions.
    """

    @staticmethod
    def parse_options(user_cli_args: list[str]) -> dict[str, Any]:
        """Parse command-line arguments into yt-dlp options.

        Args:
            user_cli_args: List of command-line argument strings.

        Returns:
            Dictionary of parsed yt-dlp options.
        """
        _, _, _, parsed_user_opts = yt_dlp.parse_options(user_cli_args)  # type: ignore
        return parsed_user_opts  # type: ignore

    @staticmethod
    def extract_info(ydl_opts: dict[str, Any], url: str) -> YtdlpInfo | None:
        """Extract metadata information from a URL using yt-dlp.

        Args:
            ydl_opts: Dictionary of yt-dlp options.
            url: URL to extract information from.

        Returns:
            YtdlpInfo object with extracted metadata, or None if extraction failed.

        Raises:
            YtdlpApiError: If extraction fails or an unexpected error occurs.
        """
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:  # type: ignore
                extracted_info = ydl.extract_info(url, download=False)  # type: ignore
                return YtdlpInfo(extracted_info) if extracted_info else None  # type: ignore
        except (ExtractorError, UserNotLive) as e:  # type: ignore
            raise YtdlpApiError(
                message="Failed to extract metadata.",
                url=url,
            ) from e
        except Exception as e:
            raise YtdlpApiError(
                message="Unexpected error occurred while extracting metadata.",
                url=url,
            ) from e

    @staticmethod
    def set_date_range(
        ydl_opts: dict[str, Any],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> None:
        """Set date range filter in yt-dlp options using DateRange object.

        Args:
            ydl_opts: Dictionary of yt-dlp options to modify in place.
            start_date: Start date in YYYYMMDD format, or None for no start limit.
            end_date: End date in YYYYMMDD format, or None for no end limit.
        """
        if start_date is not None or end_date is not None:
            ydl_opts["daterange"] = DateRange(start=start_date, end=end_date)

    @staticmethod
    def download(ydl_opts: dict[str, Any], url: str) -> None:
        """Download media from a URL using yt-dlp.

        Args:
            ydl_opts: Dictionary of yt-dlp options.
            url: URL to download media from.

        Raises:
            YtdlpApiError: If download fails or returns a non-zero exit code.
        """
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:  # type: ignore
                ret_code: int = ydl.download([url])  # type: ignore
        except (ExtractorError, UserNotLive) as e:  # type: ignore
            raise YtdlpApiError(
                message="Failed to download media.",
                url=url,
            ) from e
        except Exception as e:
            raise YtdlpApiError(
                message="Unexpected error occurred while downloading media.",
                url=url,
            ) from e
        else:
            if ret_code != 0:
                raise YtdlpApiError(
                    message=f"Download failed with non-zero exit code: {ret_code}, may not exist",
                    url=url,
                )
