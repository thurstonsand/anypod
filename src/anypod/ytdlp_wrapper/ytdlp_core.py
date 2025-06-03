"""Core yt-dlp wrapper functionality and typed data access.

This module provides core yt-dlp integration including strongly-typed access
to yt-dlp metadata and static methods for yt-dlp operations like option
parsing, metadata extraction, and media downloading.
"""

from types import UnionType
from typing import Any, Union, get_origin

import yt_dlp  # type: ignore
from yt_dlp.utils import ExtractorError, UserNotLive  # type: ignore

from ..exceptions import YtdlpApiError, YtdlpFieldInvalidError, YtdlpFieldMissingError


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
        if field_name not in self._info_dict:
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

            if not isinstance(entry, dict):
                raise YtdlpFieldInvalidError(
                    field_name="entries",
                    expected_type=dict,
                    actual_value=entry,  # type: ignore
                )

            entries.append(YtdlpInfo(entry))

        return entries


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
