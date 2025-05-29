"""Custom exceptions for the Anypod application.

This module defines all custom exception classes used throughout the
application, organized by functional area and providing structured
error information for better debugging and error handling.
"""

from typing import Any


class AnypodError(Exception):
    """Base class for application-specific errors."""


class ConfigLoadError(AnypodError):
    """Raised when a configuration file fails to load.

    Attributes:
        config_file: Path to the configuration file that failed to load.
    """

    def __init__(
        self,
        message: str,
        config_file: str | None = None,
    ):
        super().__init__(message)
        self.config_file = config_file


class DataCoordinatorError(AnypodError):
    """Base class for errors originating from the DataCoordinator."""


class DatabaseOperationError(DataCoordinatorError):
    """Raised when a database operation fails within the DataCoordinator.

    Attributes:
        feed_id: The feed identifier associated with the error.
        download_id: The download identifier associated with the error.
    """

    def __init__(
        self,
        message: str,
        feed_id: str | None = None,
        download_id: str | None = None,
    ):
        super().__init__(message)
        self.feed_id = feed_id
        self.download_id = download_id


class DownloadNotFoundError(DataCoordinatorError):
    """Raised when a specific download is not found when expected.

    Attributes:
        feed_id: The feed identifier associated with the error.
        download_id: The download identifier associated with the error.
    """

    def __init__(
        self,
        message: str,
        feed_id: str | None = None,
        download_id: str | None = None,
    ):
        super().__init__(message)
        self.feed_id = feed_id
        self.download_id = download_id


class FileOperationError(DataCoordinatorError):
    """Raised when a file operation fails within the DataCoordinator.

    Attributes:
        feed_id: The feed identifier associated with the error.
        download_id: The download identifier associated with the error.
        file_name: The file name associated with the error.
    """

    def __init__(
        self,
        message: str,
        feed_id: str | None = None,
        download_id: str | None = None,
        file_name: str | None = None,
    ):
        super().__init__(message)
        self.feed_id = feed_id
        self.download_id = download_id
        self.file_name = file_name


class EnqueueError(DataCoordinatorError):
    """Raised when an error occurs during the enqueue process.

    Attributes:
        feed_id: The feed identifier associated with the error.
        feed_url: The feed URL associated with the error.
        download_id: The download identifier associated with the error.
    """

    def __init__(
        self,
        message: str,
        feed_id: str | None = None,
        feed_url: str | None = None,
        download_id: str | None = None,
    ):
        super().__init__(message)
        self.feed_id = feed_id
        self.feed_url = feed_url
        self.download_id = download_id


class DownloaderError(DataCoordinatorError):
    """Raised when an error occurs during the download process.

    Attributes:
        feed_id: The feed identifier associated with the error.
        download_id: The download identifier associated with the error.
    """

    def __init__(
        self,
        message: str,
        feed_id: str | None = None,
        download_id: str | None = None,
    ):
        super().__init__(message)
        self.feed_id = feed_id
        self.download_id = download_id


class PruneError(DataCoordinatorError):
    """Raised when an error occurs during the pruning process.

    Attributes:
        feed_id: The feed identifier associated with the error.
        download_id: The download identifier associated with the error.
    """

    def __init__(
        self,
        message: str,
        feed_id: str | None = None,
        download_id: str | None = None,
    ):
        super().__init__(message)
        self.feed_id = feed_id
        self.download_id = download_id


class YtdlpError(AnypodError):
    """Base class for yt-dlp errors."""


class YtdlpDataError(YtdlpError):
    """Raised when yt-dlp data extraction fails."""


class YtdlpFieldMissingError(YtdlpDataError):
    """Raised when a required field is missing from yt-dlp data.

    Attributes:
        field_name: The name of the missing field.
    """

    def __init__(
        self,
        field_name: str,
    ):
        super().__init__("Field is required")
        self.field_name = field_name


class YtdlpFieldInvalidError(YtdlpDataError):
    """Raised when a field has an invalid type.

    Attributes:
        field_name: The name of the field with invalid type.
        expected_type: The expected type(s) as a string.
        actual_type: The actual type as a string.
        actual_value: The actual value that caused the error.
    """

    def __init__(
        self,
        field_name: str,
        expected_type: type | tuple[type, ...],
        actual_value: Any,
    ):
        super().__init__("Invalid type for field.")
        self.field_name = field_name
        self.actual_value = actual_value
        self.actual_type = str(type(actual_value).__name__)

        if isinstance(expected_type, tuple):
            self.expected_type = ", ".join(t.__name__ for t in expected_type)
        else:
            self.expected_type = expected_type.__name__


class YtdlpApiError(YtdlpError):
    """Raised when yt-dlp API calls fail.

    Attributes:
        feed_id: The feed identifier associated with the error.
        download_id: The download identifier associated with the error.
        url: The URL associated with the error.
    """

    def __init__(
        self,
        message: str,
        feed_id: str | None = None,
        download_id: str | None = None,
        url: str | None = None,
    ):
        super().__init__(message)
        self.feed_id = feed_id
        self.download_id = download_id
        self.url = url
