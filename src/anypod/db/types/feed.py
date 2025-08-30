"""Feed table mapped with SQLModel."""

from datetime import datetime
from typing import TYPE_CHECKING, Any

from pydantic import computed_field
from sqlalchemy import (
    Boolean,
    Column,
    Enum,
    Integer,
    String,
    TypeDecorator,
    text,
)
from sqlalchemy.sql.schema import FetchedValue
from sqlmodel import Field, Relationship, SQLModel

from ...config.types import PodcastCategories, PodcastExplicit, PodcastType
from .source_type import SourceType
from .timezone_aware_datetime import SQLITE_DATETIME_NOW, TimezoneAwareDatetime

if TYPE_CHECKING:
    from .download import Download


class PodcastCategoriesType(TypeDecorator[PodcastCategories]):
    """SQLAlchemy type for PodcastCategories that stores as string."""

    impl = String
    cache_ok = True

    def process_bind_param(
        self, value: PodcastCategories | None, dialect: Any
    ) -> str | None:
        """Convert PodcastCategories to string for storage."""
        return str(value) if value is not None else None

    def process_result_value(
        self, value: str | None, dialect: Any
    ) -> PodcastCategories | None:
        """Convert string back to PodcastCategories."""
        if value is None:
            # Return default if somehow None is returned from database
            return PodcastCategories("TV & Film")
        return PodcastCategories(value)


class Feed(SQLModel, table=True):
    """ORM model representing a podcast/feed record.

    Attributes:
        id: The feed identifier.
        is_enabled: Whether the feed is enabled for processing.
        source_type: Type of source (e.g., channel, playlist, single_video).
        source_url: The original source URL for this feed.
        resolved_url: The URL after any redirects.

        Time Keeping:
            last_successful_sync: Last time a successful sync occurred (UTC).
            created_at: When the feed was created (UTC).
            updated_at: When the feed was last updated (UTC).
            last_rss_generation: Last time RSS was generated for this feed (UTC).

        Error Tracking:
            last_failed_sync: Last time a sync failed (UTC).
            consecutive_failures: Number of consecutive sync failures.

        Download Tracking:
            total_downloads: Total number of downloads for this feed.

        Retention Policies:
            since: Only process downloads published after this date (UTC).
            keep_last: Maximum number of downloads to keep (oldest will be pruned).

        Feed Metadata:
            title: Feed title.
            subtitle: Feed subtitle.
            description: Feed description.
            language: Feed language code.
            author: Feed author.
            author_email: Feed author email.
            remote_image_url: URL to feed image.
            category: List of podcast categories.
            podcast_type: Podcast type.
            explicit: Explicit content flag.

    Relationships:
        downloads: List of downloads associated with this feed.
    """

    id: str = Field(primary_key=True)
    is_enabled: bool = Field(
        default=True,
        sa_column=Column(Boolean, nullable=False, index=True, server_default="1"),
    )

    source_type: SourceType = Field(sa_column=Column(Enum(SourceType), nullable=False))
    source_url: str
    resolved_url: str | None = None

    # ----------------------------------------------------- time keeping ----
    last_successful_sync: datetime = Field(
        sa_column=Column(
            TimezoneAwareDatetime,
            nullable=False,
            server_default=text(SQLITE_DATETIME_NOW),
        ),
    )
    created_at: datetime | None = Field(
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
    last_rss_generation: datetime | None = Field(
        default=None, sa_column=Column(TimezoneAwareDatetime)
    )

    # ------------------------------------------------------ error tracking
    last_failed_sync: datetime | None = Field(
        default=None, sa_column=Column(TimezoneAwareDatetime)
    )
    consecutive_failures: int = Field(
        default=0,
        sa_column=Column(Integer, nullable=False, server_default="0"),
    )

    # --------------------------------------------------- download metrics
    total_downloads_internal: int = Field(
        default=0,
        exclude=True,
        sa_column=Column(
            Integer, nullable=False, server_default="0", name="total_downloads"
        ),
    )

    @computed_field
    @property
    def total_downloads(self) -> int:
        """Read-only property for total downloads."""
        return self.total_downloads_internal

    # ------------------------------------------------ retention policies
    since: datetime | None = Field(
        default=None, sa_column=Column(TimezoneAwareDatetime)
    )
    keep_last: int | None = None

    # ------------------------------------------------ feed metadata
    title: str | None = None
    subtitle: str | None = None
    description: str | None = None
    language: str | None = None
    author: str | None = None
    author_email: str | None = None
    remote_image_url: str | None = None
    image_ext: str | None = None
    category: PodcastCategories = Field(
        default_factory=lambda: PodcastCategories("TV & Film"),
        sa_column=Column(
            PodcastCategoriesType,
            nullable=False,
            server_default="TV & Film",
        ),
    )
    podcast_type: PodcastType = Field(
        default=PodcastType.EPISODIC,
        sa_column=Column(
            Enum(PodcastType), nullable=False, server_default=PodcastType.EPISODIC.value
        ),
    )
    explicit: PodcastExplicit = Field(
        default=PodcastExplicit.NO,
        sa_column=Column(
            Enum(PodcastExplicit),
            nullable=False,
            server_default=PodcastExplicit.NO.value,
        ),
    )

    # ---------------------------------------------------- relationships
    downloads: list["Download"] = Relationship(back_populates="feed")

    # --- Class Helpers -----------------------------------------------------

    def model_dump_for_insert(self) -> dict[str, Any]:
        """Use in place of Pydantic's model_dump() for insert operations.

        This is necessary because certain fields are handled by the db directly,
        so should be treated differently. It also excludes computed fields.
        """
        dump = self.model_dump()
        if "created_at" in dump and dump["created_at"] is None:
            dump.pop("created_at")
        if "updated_at" in dump and dump["updated_at"] is None:
            dump.pop("updated_at")

        # Don't include computed fields in database operations
        dump.pop("total_downloads", None)
        dump.pop("total_downloads_internal", None)
        return dump
