"""Core yt-dlp metadata functionality and typed data access."""

from types import UnionType
from typing import Any, Union, get_origin

from ...exceptions import YtdlpFieldInvalidError, YtdlpFieldMissingError
from .thumbnails import YtdlpThumbnails


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
