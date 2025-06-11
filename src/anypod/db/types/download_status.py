"""Download status lifecycle values."""

from enum import Enum

from ..sqlite_utils_core import register_adapter


class DownloadStatus(Enum):
    """Represent the status of a download in the processing lifecycle.

    Indicates the current state of a download item as it progresses through
    the system from discovery to completion or archival.
    """

    UPCOMING = "upcoming"
    QUEUED = "queued"
    DOWNLOADED = "downloaded"
    ERROR = "error"
    SKIPPED = "skipped"
    ARCHIVED = "archived"

    def __str__(self) -> str:
        return self.value


register_adapter(DownloadStatus, lambda status: status.value)
