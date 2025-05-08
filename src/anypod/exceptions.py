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
