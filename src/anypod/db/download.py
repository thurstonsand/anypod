from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .base_db import parse_datetime, parse_required_datetime
from .download_status import DownloadStatus


@dataclass(eq=False)
class Download:
    """Represent a download's data for adding and updating.

    Attributes:
        feed: The feed identifier.
        id: The download identifier.
        source_url: The source URL for the download.
        title: The download title.
        published: Publication datetime (UTC).
        ext: File extension.
        mime_type: MIME type of the download.
        filesize: File size in bytes.
        duration: Duration in seconds.
        status: Current download status.
        discovered_at: When the download was first discovered (UTC).
        updated_at: When the download was last updated (UTC).

        Optional Media Metadata:
            thumbnail: Optional thumbnail URL.
            description: Optional description of the download.
            quality_info: Optional quality information for the download.

        Error Tracking:
            retries: Number of retry attempts.
            last_error: Last error message if any.

        Processing Timestamps:
            downloaded_at: When the download was completed (UTC).
    """

    # Core identifiers and metadata
    feed: str
    id: str
    source_url: str
    title: str
    published: datetime  # Should be UTC
    ext: str
    mime_type: str
    filesize: int  # Bytes
    duration: int  # in seconds
    status: DownloadStatus
    discovered_at: datetime | None = None
    updated_at: datetime | None = None

    # Optional media metadata
    thumbnail: str | None = None
    description: str | None = None
    quality_info: str | None = None

    # Error tracking
    retries: int = 0
    last_error: str | None = None

    # Processing timestamps
    downloaded_at: datetime | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Download":
        """Converts a row from the database to a Download.

        Args:
            row: A dictionary representing a row from the database.

        Returns:
            A Download object.

        Raises:
            ValueError: If the date format is invalid or the status value is invalid.
        """
        published_str = row["published"]
        try:
            published_dt = datetime.fromisoformat(published_str)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"Invalid date format for 'published' in DB row: {published_str}"
            ) from e

        status_str = row["status"]
        try:
            status_enum = DownloadStatus(status_str)
        except ValueError as e:
            raise ValueError(f"Invalid status value in DB row: {status_str}") from e

        return cls(
            feed=row["feed"],
            id=row["id"],
            source_url=row["source_url"],
            title=row["title"],
            published=published_dt,
            ext=row["ext"],
            mime_type=row["mime_type"],
            filesize=row["filesize"],
            duration=int(row["duration"]),
            status=status_enum,
            discovered_at=parse_required_datetime(row["discovered_at"]),
            updated_at=parse_required_datetime(row["updated_at"]),
            thumbnail=row.get("thumbnail"),
            description=row.get("description"),
            quality_info=row.get("quality_info"),
            retries=row.get("retries", 0),
            last_error=row.get("last_error"),
            downloaded_at=parse_datetime(row.get("downloaded_at")),
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Download):
            return NotImplemented
        # Equality is based solely on the composite primary key
        return self.feed == other.feed and self.id == other.id

    def __hash__(self) -> int:
        # Hash is based solely on the composite primary key
        return hash((self.feed, self.id))
