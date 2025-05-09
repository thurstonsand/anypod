class AnypodError(Exception):
    """Base class for application-specific errors."""


class DataCoordinatorError(AnypodError):
    """Base class for errors originating from the DataCoordinator."""


class DatabaseOperationError(DataCoordinatorError):
    """Raised when a database operation fails within the DataCoordinator."""


class FileOperationError(DataCoordinatorError):
    """Raised when a file operation fails within the DataCoordinator."""


class DownloadNotFoundError(DataCoordinatorError):
    """Raised when a specific download is not found when expected."""


class YtdlpError(AnypodError):
    """Base class for yt-dlp errors."""


class YtdlpDataError(YtdlpError):
    """Raised when yt-dlp data extraction fails."""


class YtdlpApiError(YtdlpError):
    """Raised when yt-dlp API calls fail."""
