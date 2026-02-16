"""Download status lifecycle values."""

from enum import StrEnum


class DownloadStatus(StrEnum):
    """Represent the status of a download in the processing lifecycle.

    Indicates the current state of a download item as it progresses through
    the system from discovery to completion or archival.
    """

    UPCOMING = "UPCOMING"
    QUEUED = "QUEUED"
    DOWNLOADED = "DOWNLOADED"
    ERROR = "ERROR"
    SKIPPED = "SKIPPED"
    ARCHIVED = "ARCHIVED"
