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


class YtdlpError(AnypodError):
    """Base class for yt-dlp errors."""


class YtdlpDataError(YtdlpError):
    """Raised when yt-dlp data extraction fails."""


class YtdlpApiError(YtdlpError):
    """Raised when yt-dlp API calls fail."""

    def __init__(
        self,
        message: str,
        feed_name: str | None = None,
        url: str | None = None,
    ):
        super().__init__(message)
        self.feed_name = feed_name
        self.url = url
