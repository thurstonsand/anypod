"""Data model representing a feed."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ...config.types import PodcastCategories, PodcastExplicit
from ..base_db import parse_datetime, parse_required_datetime
from .source_type import SourceType


@dataclass
class Feed:
    """Represent a feed's data for adding and updating.

    Attributes:
        id: The feed identifier.
        is_enabled: Whether the feed is enabled for processing.
        source_type: Type of source (e.g., channel, playlist, single_video).
        source_url: The original source URL for this feed.

        Time Keeping:
            last_successful_sync: Last time a successful sync occurred (UTC).
            created_at: When the feed was created (UTC).
            updated_at: When the feed was last updated (UTC).
            last_rss_generation: Last time RSS was generated for this feed (UTC).

        Error Tracking:
            last_failed_sync: Last time a sync failed (UTC).
            consecutive_failures: Number of consecutive sync failures.
            last_error: Last error message if any.

        Download Tracking:
            total_downloads: Total number of downloads for this feed.
            downloads_since_last_rss: Number of downloads since last RSS generation.

        Feed Metadata:
            title: Feed title.
            subtitle: Feed subtitle.
            description: Feed description.
            language: Feed language code.
            author: Feed author.
            image_url: URL to feed image.
            category: List of podcast categories.
            explicit: Explicit content flag.
    """

    id: str
    is_enabled: bool
    source_type: SourceType
    source_url: str

    # time keeping
    last_successful_sync: datetime
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_rss_generation: datetime | None = None

    # Error tracking
    last_failed_sync: datetime | None = None
    consecutive_failures: int = 0
    last_error: str | None = None

    # Download tracking
    total_downloads: int = 0
    downloads_since_last_rss: int = 0

    # Feed metadata
    title: str | None = None
    subtitle: str | None = None
    description: str | None = None
    language: str | None = None
    author: str | None = None
    image_url: str | None = None
    category: PodcastCategories | None = None
    explicit: PodcastExplicit | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Feed":
        """Converts a row from the database to a Feed.

        Args:
            row: A dictionary representing a row from the database.

        Returns:
            A Feed object.

        Raises:
            ValueError: If a date format is invalid or source_type value is invalid.
        """
        try:
            source_type_enum = SourceType(row["source_type"])
        except ValueError as e:
            raise ValueError(
                f"Invalid source_type value in DB row: {row['source_type']}"
            ) from e

        return cls(
            id=row["id"],
            is_enabled=bool(row["is_enabled"]),
            source_type=source_type_enum,
            source_url=row["source_url"],
            # time keeping
            last_successful_sync=parse_required_datetime(row["last_successful_sync"]),
            created_at=parse_required_datetime(row["created_at"]),
            updated_at=parse_required_datetime(row["updated_at"]),
            last_rss_generation=parse_datetime(row.get("last_rss_generation")),
            # error tracking
            last_failed_sync=parse_datetime(row.get("last_failed_sync")),
            consecutive_failures=row.get("consecutive_failures", 0),
            last_error=row.get("last_error"),
            # download tracking
            total_downloads=row.get("total_downloads", 0),
            downloads_since_last_rss=row.get("downloads_since_last_rss", 0),
            # feed metadata
            title=row.get("title"),
            subtitle=row.get("subtitle"),
            description=row.get("description"),
            language=row.get("language"),
            author=row.get("author"),
            image_url=row.get("image_url"),
            category=PodcastCategories(row["category"])
            if row.get("category")
            else None,
            explicit=PodcastExplicit.from_str(row["explicit"])
            if row.get("explicit")
            else None,
        )
