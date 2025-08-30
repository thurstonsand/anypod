"""Download table mapped with SQLModel."""

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import Column, Enum, Index, Integer, text
from sqlalchemy.sql.schema import FetchedValue
from sqlmodel import Field, Relationship, SQLModel

from .download_status import DownloadStatus
from .timezone_aware_datetime import SQLITE_DATETIME_NOW, TimezoneAwareDatetime

if TYPE_CHECKING:
    from .feed import Feed


class Download(SQLModel, table=True):
    """Represent a download.

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
            remote_thumbnail_url: Optional original thumbnail URL.
            thumbnail_ext: Optional hosted thumbnail file extension.
            description: Optional description of the download.
            quality_info: Optional quality information for the download.

        Error Tracking:
            retries: Number of retry attempts.
            last_error: Last error message if any.

        Processing Timestamps:
            downloaded_at: When the download was completed (UTC).

    Relationships:
        feed_rel: The feed associated with this download.
    """

    # Composite primary key: (feed, id)
    feed_id: str = Field(foreign_key="feed.id", primary_key=True)
    id: str = Field(primary_key=True)

    # Source + metadata
    source_url: str
    title: str
    published: datetime = Field(sa_column=Column(TimezoneAwareDatetime, nullable=False))

    # Media details
    ext: str
    mime_type: str
    filesize: int = Field(gt=0)
    duration: int = Field(gt=0)

    # Processing state
    status: DownloadStatus = Field(
        sa_column=Column(Enum(DownloadStatus), nullable=False)
    )

    discovered_at: datetime | None = Field(
        default=None,
        sa_column=Column(
            TimezoneAwareDatetime,
            nullable=False,
            server_default=text(SQLITE_DATETIME_NOW),
        ),
    )
    updated_at: datetime | None = Field(
        default=None,
        sa_column=Column(
            TimezoneAwareDatetime,
            nullable=False,
            server_default=text(SQLITE_DATETIME_NOW),
            server_onupdate=FetchedValue(),
        ),
    )

    # Optional media metadata
    remote_thumbnail_url: str | None = None
    thumbnail_ext: str | None = None
    description: str | None = None
    quality_info: str | None = None

    # Error tracking
    retries: int = Field(
        default=0, sa_column=Column(Integer, nullable=False, server_default="0")
    )
    last_error: str | None = None

    # When the file was actually downloaded
    downloaded_at: datetime | None = Field(
        default=None,
        sa_column=Column(
            TimezoneAwareDatetime,
            server_onupdate=FetchedValue(),
        ),
    )

    # --- Relationships ----------------------------------------------------

    feed: "Feed" = Relationship(back_populates="downloads")

    # Composite indexes
    __table_args__ = (
        Index("idx_feed_status", "feed_id", "status"),
        Index("idx_feed_published", "feed_id", "published"),
    )

    # --- Class Helpers -----------------------------------------------------

    def __hash__(self) -> int:
        return hash((self.feed_id, self.id))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Download):
            return False
        return self.feed_id == other.feed_id and self.id == other.id

    def model_dump_for_insert(self) -> dict[str, Any]:
        """Use in place of Pydantic's model_dump() for insert operations.

        This is necessary because certain fields are handled by the db directly,
        so should be treated differently. It also excludes computed fields.

        Returns:
            A dictionary representation of the download, excluding fields that
            the db handles directly.
        """
        dump = self.model_dump()
        if "discovered_at" in dump and dump["discovered_at"] is None:
            dump.pop("discovered_at")
        if "downloaded_at" in dump and dump["downloaded_at"] is None:
            dump.pop("downloaded_at")
        if "updated_at" in dump and dump["updated_at"] is None:
            dump.pop("updated_at")
        return dump

    def content_equals(self, other: "Download") -> bool:
        """Compare downloads excluding timestamp fields.

        Compares all fields except discovered_at, updated_at, and downloaded_at,
        which are automatically managed by the database.

        Args:
            other: The other Download to compare against.

        Returns:
            True if all content fields are equal.
        """
        exclude_fields = {"discovered_at", "updated_at", "downloaded_at"}
        return self.model_dump(exclude=exclude_fields) == other.model_dump(
            exclude=exclude_fields
        )
