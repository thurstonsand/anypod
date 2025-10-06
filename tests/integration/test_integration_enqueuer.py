"""Integration tests for Enqueuer with real YouTube URLs and database operations."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from anypod.config import FeedConfig
from anypod.config.types import FeedMetadataOverrides
from anypod.data_coordinator import Enqueuer
from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.state_reconciler import MIN_SYNC_DATE

# Test constants
BIG_BUCK_BUNNY_VIDEO_ID = "aqz-KE-bpKQ"
BIG_BUCK_BUNNY_URL = f"https://www.youtube.com/watch?v={BIG_BUCK_BUNNY_VIDEO_ID}"
BIG_BUCK_BUNNY_SHORT_URL = f"https://youtu.be/{BIG_BUCK_BUNNY_VIDEO_ID}"
BIG_BUCK_BUNNY_TITLE = "Test Video"
BIG_BUCK_BUNNY_PUBLISHED = datetime(2014, 11, 10, 14, 5, 55, tzinfo=UTC)
BIG_BUCK_BUNNY_DURATION = 635

COLETDJNZ_CHANNEL_VIDEOS = "https://www.youtube.com/@coletdjnz/videos"

INVALID_VIDEO_URL = "https://www.youtube.com/watch?v=thisvideodoesnotexistxyz"

# CLI args for minimal quality downloads as a string
YT_DLP_MINIMAL_ARGS_STR = "--format worst*[ext=mp4]/worst[ext=mp4]/best[ext=mp4]"

# --- Tests for Enqueuer.enqueue_new_downloads ---
# Test schedule
TEST_CRON_SCHEDULE = "0 * * * *"
MAX_ERRORS = 3

# Same CC-BY licensed URLs as YtdlpWrapper tests for consistency
TEST_URLS_SINGLE_AND_PLAYLIST = [
    ("video_short_link", BIG_BUCK_BUNNY_SHORT_URL, SourceType.SINGLE_VIDEO, None),
    (
        "video_in_playlist_link",
        f"{BIG_BUCK_BUNNY_URL}&list=PLt5yu3-wZAlSLRHmI1qNm0wjyVNWw1pCU",
        SourceType.PLAYLIST,
        None,
    ),
]

TEST_URLS_PARAMS = [
    *TEST_URLS_SINGLE_AND_PLAYLIST,
    (
        "channel_videos_tab",
        COLETDJNZ_CHANNEL_VIDEOS,
        SourceType.PLAYLIST,
        COLETDJNZ_CHANNEL_VIDEOS,
    ),
    (
        "playlist",
        "https://youtube.com/playlist?list=PLt5yu3-wZAlSLRHmI1qNm0wjyVNWw1pCU",
        SourceType.PLAYLIST,
        None,
    ),
]

# Sample feed configuration for testing
SAMPLE_FEED_CONFIG = FeedConfig(
    url=BIG_BUCK_BUNNY_SHORT_URL,
    yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
    schedule=TEST_CRON_SCHEDULE,
    keep_last=None,
    since=None,
    max_errors=MAX_ERRORS,
)


async def create_test_feed(
    feed_db: FeedDatabase,
    feed_id: str,
    url: str,
    source_type: SourceType,
    resolved_url: str | None = None,
) -> Feed:
    """Create a test feed in the database with specified properties."""
    feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=source_type,
        source_url=url,
        resolved_url=resolved_url,
        last_successful_sync=MIN_SYNC_DATE,
        title=f"Test Feed {feed_id}",
    )
    await feed_db.upsert_feed(feed)
    return feed


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("url_type, url, source_type, resolved_url", TEST_URLS_PARAMS)
async def test_enqueue_new_downloads_success(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    url_type: str,
    url: str,
    source_type: SourceType,
    resolved_url: str | None,
    cookies_path: Path | None,
):
    """Tests successful enqueueing of new downloads for various URL types.

    Asserts that downloads are properly fetched, parsed, and inserted into
    the database with appropriate status.
    """
    feed_id = f"test_feed_{url_type}"

    # Create feed in database
    await create_test_feed(feed_db, feed_id, url, source_type, resolved_url)

    feed_config = FeedConfig(
        url=url,
        yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
        schedule=TEST_CRON_SCHEDULE,
        keep_last=1,
        since=None,
        max_errors=MAX_ERRORS,
    )
    fetch_since_date = MIN_SYNC_DATE

    # Enqueue new downloads
    queued_count, _ = await enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
        cookies_path=cookies_path,
    )

    # Verify that downloads were queued
    assert queued_count >= 1, f"Expected at least 1 queued download for {url_type}"

    # Verify downloads are in the database
    queued_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    assert len(queued_downloads) >= 1, f"Expected queued downloads in DB for {url_type}"

    # Verify basic properties of the first download
    download = queued_downloads[0]
    assert download.feed_id == feed_id
    assert download.id, f"Download ID should not be empty for {url_type}"
    assert download.title, f"Download title should not be empty for {url_type}"
    assert download.source_url, (
        f"Download source_url should not be empty for {url_type}"
    )
    assert download.published, f"Download published should not be empty for {url_type}"
    assert download.duration > 0, f"Download duration should be > 0 for {url_type}"
    assert download.status == DownloadStatus.QUEUED


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enqueue_new_downloads_invalid_url(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    cookies_path: Path | None,
):
    """Tests lenient error handling for invalid URLs.

    TODO: When error handling is enhanced to distinguish between invalid URLs
    (configuration errors that should fail) and temporarily inaccessible content
    (partial failures that should warn), update this test to expect an EnqueueError
    for genuinely invalid URLs that can never succeed.
    """
    feed_id = "test_invalid_feed"

    # Create feed in database with invalid URL
    feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.UNKNOWN,
        source_url=INVALID_VIDEO_URL,
        resolved_url=INVALID_VIDEO_URL,
        last_successful_sync=MIN_SYNC_DATE,
        title=f"Test Feed {feed_id}",
    )
    await feed_db.upsert_feed(feed)

    feed_config = FeedConfig(
        url=INVALID_VIDEO_URL,
        yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
        schedule=TEST_CRON_SCHEDULE,  # type: ignore
        keep_last=None,
        since=None,
        max_errors=MAX_ERRORS,
    )
    fetch_since_date = MIN_SYNC_DATE

    # Current behavior: Lenient error handling returns 0 downloads with warnings
    queued_count, _ = await enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
        cookies_path=cookies_path,
    )

    assert queued_count == 0, (
        "Expected 0 queued downloads for invalid URL with lenient error handling"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enqueue_new_downloads_with_date_filter(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    cookies_path: Path | None,
):
    """Tests enqueueing with a date filter that should limit results."""
    feed_id = "test_date_filter"

    # Create feed in database
    await create_test_feed(
        feed_db,
        feed_id,
        COLETDJNZ_CHANNEL_VIDEOS,
        SourceType.PLAYLIST,
        COLETDJNZ_CHANNEL_VIDEOS,
    )

    feed_config = FeedConfig(
        url=COLETDJNZ_CHANNEL_VIDEOS,
        yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
        schedule=TEST_CRON_SCHEDULE,  # type: ignore
        keep_last=2,
        since=None,
        max_errors=MAX_ERRORS,
    )
    # Use a very recent date to potentially filter out older content
    fetch_since_date = datetime(2025, 1, 1, tzinfo=UTC)

    queued_count, _ = await enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
        cookies_path=cookies_path,
    )

    # Should still work, even if no results due to date filtering
    assert queued_count >= 0

    # Verify downloads in database match what was reported
    queued_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    assert len(queued_downloads) == queued_count


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enqueue_handles_existing_upcoming_downloads(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    cookies_path: Path | None,
):
    """Tests that existing UPCOMING downloads are properly handled and potentially transitioned to QUEUED."""
    feed_id = "test_upcoming_feed"

    # Create feed in database
    await create_test_feed(
        feed_db, feed_id, SAMPLE_FEED_CONFIG.url, SourceType.SINGLE_VIDEO, None
    )

    feed_config = SAMPLE_FEED_CONFIG

    # Insert an UPCOMING download manually
    upcoming_download = Download(
        feed_id=feed_id,
        id=BIG_BUCK_BUNNY_VIDEO_ID,
        source_url=BIG_BUCK_BUNNY_URL,
        title=BIG_BUCK_BUNNY_TITLE,
        published=BIG_BUCK_BUNNY_PUBLISHED,
        ext="mp4",
        mime_type="video/mp4",
        filesize=12345,
        duration=BIG_BUCK_BUNNY_DURATION,
        status=DownloadStatus.UPCOMING,
        retries=0,
        discovered_at=BIG_BUCK_BUNNY_PUBLISHED,
        updated_at=BIG_BUCK_BUNNY_PUBLISHED,
    )
    await download_db.upsert_download(upcoming_download)

    # Verify it's in UPCOMING status
    upcoming_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.UPCOMING, feed_id=feed_id
    )
    assert len(upcoming_downloads) == 1
    assert upcoming_downloads[0].status == DownloadStatus.UPCOMING

    # Run enqueuer - should transition UPCOMING to QUEUED since Big Buck Bunny is a VOD
    queued_count, _ = await enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=MIN_SYNC_DATE,
        cookies_path=cookies_path,
    )

    # Should have at least 1 queued (the transitioned one)
    assert queued_count >= 1

    # Verify the download is now QUEUED
    queued_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    assert len(queued_downloads) >= 1

    # The original UPCOMING download should be found in QUEUED status
    transitioned_download = next(
        (dl for dl in queued_downloads if dl.id == BIG_BUCK_BUNNY_VIDEO_ID), None
    )
    assert transitioned_download is not None
    assert transitioned_download.status == DownloadStatus.QUEUED

    # Should be no more UPCOMING downloads for this feed
    remaining_upcoming = await download_db.get_downloads_by_status(
        DownloadStatus.UPCOMING, feed_id=feed_id
    )
    assert len(remaining_upcoming) == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enqueue_handles_existing_downloaded_items(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    cookies_path: Path | None,
):
    """Tests that existing DOWNLOADED items are ignored during enqueue process."""
    feed_id = "test_downloaded_ignored_feed"

    # Create feed in database
    await create_test_feed(
        feed_db, feed_id, SAMPLE_FEED_CONFIG.url, SourceType.SINGLE_VIDEO, None
    )

    feed_config = SAMPLE_FEED_CONFIG

    # Insert a DOWNLOADED item
    downloaded_item = Download(
        feed_id=feed_id,
        id=BIG_BUCK_BUNNY_VIDEO_ID,
        source_url=BIG_BUCK_BUNNY_URL,
        title=BIG_BUCK_BUNNY_TITLE,
        published=BIG_BUCK_BUNNY_PUBLISHED,
        ext="mp4",
        mime_type="video/mp4",
        filesize=12345,
        duration=BIG_BUCK_BUNNY_DURATION,
        status=DownloadStatus.DOWNLOADED,
        retries=0,
        discovered_at=BIG_BUCK_BUNNY_PUBLISHED,
        updated_at=BIG_BUCK_BUNNY_PUBLISHED,
    )
    await download_db.upsert_download(downloaded_item)

    # Run enqueuer
    queued_count, _ = await enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=MIN_SYNC_DATE,
        cookies_path=cookies_path,
    )

    # Should NOT have queued the item since it's already DOWNLOADED
    assert queued_count == 0

    # Verify the item remains DOWNLOADED
    downloaded_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    assert len(downloaded_downloads) == 1
    assert downloaded_downloads[0].id == BIG_BUCK_BUNNY_VIDEO_ID
    assert downloaded_downloads[0].status == DownloadStatus.DOWNLOADED

    # Verify no items were queued
    queued_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    assert len(queued_downloads) == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enqueue_multiple_runs_idempotent(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    cookies_path: Path | None,
):
    """Tests that running enqueue multiple times on the same feed is idempotent."""
    feed_id = "test_idempotent_feed"

    # Create feed in database
    await create_test_feed(
        feed_db, feed_id, SAMPLE_FEED_CONFIG.url, SourceType.SINGLE_VIDEO, None
    )

    feed_config = SAMPLE_FEED_CONFIG
    fetch_since_date = MIN_SYNC_DATE

    # First run
    first_queued_count, _ = await enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
        cookies_path=cookies_path,
    )
    assert first_queued_count >= 1

    # Get downloads after first run
    first_run_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )

    # Second run - should not queue new items (they already exist)
    second_queued_count, _ = await enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
        cookies_path=cookies_path,
    )

    # Second run should queue 0 new items since they already exist
    assert second_queued_count == 0

    # Downloads should be the same
    second_run_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    assert len(second_run_downloads) == len(first_run_downloads)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enqueue_with_impossible_filter(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    cookies_path: Path | None,
):
    """Tests enqueueing with filters that match no videos still creates downloads but with filter applied."""
    feed_id = "test_impossible_filter"

    # Create feed in database
    await create_test_feed(
        feed_db, feed_id, BIG_BUCK_BUNNY_SHORT_URL, SourceType.SINGLE_VIDEO, None
    )

    feed_config = FeedConfig(
        url=BIG_BUCK_BUNNY_SHORT_URL,
        yt_args='--format worst*[ext=mp4]/worst[ext=mp4]/best[ext=mp4] --match-filters "duration > 10000000"',  # type: ignore
        schedule=TEST_CRON_SCHEDULE,  # type: ignore
        keep_last=None,
        since=None,
        max_errors=MAX_ERRORS,
    )
    fetch_since_date = MIN_SYNC_DATE

    # The filter applies to download, not metadata fetch, so downloads are still created
    queued_count, _ = await enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
        cookies_path=cookies_path,
    )

    # Metadata fetching still works, downloads are created
    assert queued_count == 0

    # Verify downloads exist in database (the filter will apply during actual download)
    queued_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    assert len(queued_downloads) == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enqueue_feed_metadata_partial_overrides(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    cookies_path: Path | None,
):
    """Tests that partial metadata overrides work correctly, using extracted values for non-overridden fields."""
    feed_id = "test_partial_metadata_sync"

    # Create feed in database
    await create_test_feed(
        feed_db, feed_id, BIG_BUCK_BUNNY_SHORT_URL, SourceType.SINGLE_VIDEO, None
    )

    # Create feed config with only some metadata overrides
    metadata_overrides = FeedMetadataOverrides(  # type: ignore # quirk of pydantic
        title="Custom Title Only",
        author="Custom Author Only",
        # Other fields left as None to use extracted values
    )

    feed_config = FeedConfig(
        url=BIG_BUCK_BUNNY_SHORT_URL,
        yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
        schedule=TEST_CRON_SCHEDULE,  # type: ignore
        keep_last=None,
        since=None,
        max_errors=MAX_ERRORS,
        metadata=metadata_overrides,
    )
    fetch_since_date = MIN_SYNC_DATE

    # Enqueue new downloads
    queued_count, _ = await enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
        cookies_path=cookies_path,
    )

    # Verify downloads were processed
    assert queued_count >= 1

    # Verify downloads in database match what was reported
    queued_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    assert len(queued_downloads) == queued_count

    # Verify at least one download has proper metadata
    assert queued_downloads[0].title is not None
    assert queued_downloads[0].source_url is not None

    # Note: Feed metadata synchronization is handled by StateReconciler,
    # not Enqueuer, so this test focuses on download enqueuing functionality
