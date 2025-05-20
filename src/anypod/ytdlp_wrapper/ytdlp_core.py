from types import UnionType
from typing import Any, Union, get_origin

import yt_dlp  # type: ignore
from yt_dlp.utils import ExtractorError, UserNotLive  # type: ignore

from ..exceptions import YtdlpApiError, YtdlpFieldInvalidError, YtdlpFieldMissingError


class YtdlpInfo:
    """
    A wrapper around the output of yt-dlp extract_info
    to provide strongly-typed access to its fields.
    """

    def __init__(self, info_dict: dict[str, Any]):
        self._info_dict = info_dict

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, YtdlpInfo):
            return NotImplemented
        return self._info_dict == other._info_dict

    def get_raw(self, field_name: str) -> Any | None:
        """
        Retrieves a field's value directly from the dictionary
        without any type checking or validation by this class.
        Returns None if the field is not found.

        Args:
            field_name: The name of the field to retrieve.

        Returns:
            The field's value if it exists, otherwise None.
        """
        return self._info_dict.get(field_name, None)

    def get[T](self, field_name: str, tpe: type[T] | tuple[type[T], ...]) -> T | None:
        """
        Retrieves a field value if it exists and matches the expected type(s).

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
        """
        Retrieves a required field value, ensuring it exists and matches the expected type(s).

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
        raw_entries = self.get("entries", list[dict[str, Any] | None])  # type: ignore
        if raw_entries is None:
            return None
        entries: list[YtdlpInfo | None] = []
        for entry in raw_entries:  # type: ignore
            if not isinstance(entry, dict):
                raise YtdlpFieldInvalidError(
                    field_name="entries",
                    expected_type=dict,
                    actual_value=entry,  # type: ignore
                )
            entries.append(YtdlpInfo(entry)) if entry else None  # type: ignore
        return entries


class YtdlpCore:
    @staticmethod
    def parse_options(user_cli_args: list[str]) -> dict[str, Any]:
        _, _, _, parsed_user_opts = yt_dlp.parse_options(user_cli_args)  # type: ignore
        return parsed_user_opts  # type: ignore

    @staticmethod
    def extract_info(ydl_opts: dict[str, Any], url: str) -> YtdlpInfo | None:
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
