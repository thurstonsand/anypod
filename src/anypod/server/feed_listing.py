"""Shared feed listing query logic for public and admin HTTP endpoints.

Provides a single internal query flow for listing feeds from SQLite so public
HTML endpoints and JSON admin endpoints stay consistent and only differ in
filtering and output formatting.
"""

from datetime import datetime

from pydantic import BaseModel

from ..db import DownloadDatabase, FeedDatabase
from ..db.types import DownloadStatus, SourceType


class FeedDownloadCounts(BaseModel):
    """Per-status download counts for a single feed.

    Attributes:
        queued: Number of queued downloads awaiting download.
        downloaded: Number of completed downloads currently available.
        upcoming: Number of upcoming/scheduled downloads.
        error: Number of downloads in ERROR state.
        skipped: Number of skipped downloads.
        archived: Number of pruned/archived downloads.
    """

    queued: int = 0
    downloaded: int = 0
    upcoming: int = 0
    error: int = 0
    skipped: int = 0
    archived: int = 0


class FeedSummary(BaseModel):
    """Structured summary of a feed for public and admin listing endpoints.

    Attributes:
        id: Unique feed identifier.
        title: Human-readable feed title, if known.
        is_enabled: Whether the feed is currently enabled.
        source_type: Categorized feed source type.
        source_url: Configured source URL, or None for manual feeds.
        last_successful_sync: Timestamp of last successful sync.
        last_failed_sync: Timestamp of last failed sync, if any.
        consecutive_failures: Count of consecutive sync failures.
        last_rss_generation: Timestamp of last RSS generation, if any.
        total_downloads: Total downloaded items recorded for the feed.
        download_counts: Optional per-status breakdown of downloads.
    """

    id: str
    title: str | None
    is_enabled: bool
    source_type: SourceType
    source_url: str | None
    last_successful_sync: datetime
    last_failed_sync: datetime | None
    consecutive_failures: int
    last_rss_generation: datetime | None
    total_downloads: int
    download_counts: FeedDownloadCounts | None = None


async def list_feeds(
    feed_db: FeedDatabase,
    download_db: DownloadDatabase | None = None,
    *,
    enabled_only: bool = False,
) -> list[FeedSummary]:
    """Query feeds from the database with optional per-status download counts.

    Args:
        feed_db: Feed database instance.
        download_db: Optional download database instance. When provided,
            populates ``download_counts`` for every returned feed.
        enabled_only: If True, return only enabled feeds (for public routes).

    Returns:
        Ordered list of FeedSummary records.

    Raises:
        DatabaseOperationError: If database queries fail.
    """
    feeds = await feed_db.get_feeds(enabled=True if enabled_only else None)
    counts_by_feed: dict[str, dict[DownloadStatus, int]] | None = None
    if download_db is not None:
        counts_by_feed = await download_db.count_downloads_by_feed_and_status()

    summaries: list[FeedSummary] = []
    for feed in feeds:
        counts: FeedDownloadCounts | None = None
        if counts_by_feed is not None:
            raw = counts_by_feed.get(feed.id, {})
            counts = FeedDownloadCounts(
                queued=raw.get(DownloadStatus.QUEUED, 0),
                downloaded=raw.get(DownloadStatus.DOWNLOADED, 0),
                upcoming=raw.get(DownloadStatus.UPCOMING, 0),
                error=raw.get(DownloadStatus.ERROR, 0),
                skipped=raw.get(DownloadStatus.SKIPPED, 0),
                archived=raw.get(DownloadStatus.ARCHIVED, 0),
            )
        summaries.append(
            FeedSummary(
                id=feed.id,
                title=feed.title,
                is_enabled=feed.is_enabled,
                source_type=feed.source_type,
                source_url=feed.source_url,
                last_successful_sync=feed.last_successful_sync,
                last_failed_sync=feed.last_failed_sync,
                consecutive_failures=feed.consecutive_failures,
                last_rss_generation=feed.last_rss_generation,
                total_downloads=feed.total_downloads,
                download_counts=counts,
            )
        )
    return summaries
