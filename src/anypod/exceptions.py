from typing import Any


class AnypodError(Exception):
    """Base class for application-specific errors."""


class ConfigLoadError(AnypodError):
    """Raised when a configuration file fails to load."""

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
    """Raised when a database operation fails within the DataCoordinator."""

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
    """Raised when a specific download is not found when expected."""

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
    """Raised when a file operation fails within the DataCoordinator."""

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
    """Raised when an error occurs during the enqueue process."""

    def __init__(
        self,
        message: str,
        feed_id: str | None = None,
        feed_url: str | None = None,
    ):
        super().__init__(message)
        self.feed_id = feed_id
        self.feed_url = feed_url


class YtdlpError(AnypodError):
    """Base class for yt-dlp errors."""


class YtdlpDataError(YtdlpError):
    """Raised when yt-dlp data extraction fails."""


class YtdlpFieldMissingError(YtdlpDataError):
    """Raised when a required field is missing from yt-dlp data."""

    def __init__(
        self,
        field_name: str,
    ):
        super().__init__("Field is required")
        self.field_name = field_name


class YtdlpFieldInvalidError(YtdlpDataError):
    """Raised when a field has an invalid type."""

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
    """Raised when yt-dlp API calls fail."""

    def __init__(
        self,
        message: str,
        feed_id: str | None = None,
        url: str | None = None,
    ):
        super().__init__(message)
        self.feed_id = feed_id
        self.url = url
