# pyright: reportPrivateUsage=false

"""Integration tests for DataCoordinator process_feed orchestration method.

Tests focus on happy path scenarios and verify the complete end-to-end
feed processing pipeline including enqueue, download, prune, and RSS generation.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from anypod.config import FeedConfig
from anypod.data_coordinator import DataCoordinator, Downloader, Enqueuer, Pruner
from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.file_manager import FileManager
from anypod.rss import RSSFeedGenerator
from anypod.state_reconciler import MIN_SYNC_DATE

# Test constants - same as other integration tests for consistency
BIG_BUCK_BUNNY_VIDEO_ID = "aqz-KE-bpKQ"
BIG_BUCK_BUNNY_URL = f"https://www.youtube.com/watch?v={BIG_BUCK_BUNNY_VIDEO_ID}"
BIG_BUCK_BUNNY_SHORT_URL = f"https://youtu.be/{BIG_BUCK_BUNNY_VIDEO_ID}"
BIG_BUCK_BUNNY_TITLE = (
    "Big Buck Bunny 60fps 4K - Official Blender Foundation Short Film"
)
BIG_BUCK_BUNNY_PUBLISHED = datetime(2014, 11, 10, 14, 5, 55, tzinfo=UTC)

COLETDJNZ_CHANNEL_VIDEOS = "https://www.youtube.com/@coletdjnz/videos"
# Test playlist URL - small playlist with known content for date filtering tests
TEST_PLAYLIST_URL = (
    "https://youtube.com/playlist?list=PLt5yu3-wZAlQAaPZ5Z-rJoTdbT-45Q7c0"
)

# CLI args for minimal quality downloads as a string
YT_DLP_MINIMAL_ARGS_STR = "--format worst*[ext=mp4]/worst[ext=mp4]/best[ext=mp4]"

# Test schedule and config
TEST_CRON_SCHEDULE = "0 * * * *"
MAX_ERRORS = 3


@pytest.fixture
def data_coordinator(
    enqueuer: Enqueuer,
    downloader: Downloader,
    pruner: Pruner,
    rss_generator: RSSFeedGenerator,
    feed_db: FeedDatabase,
    cookies_path: Path | None,
) -> DataCoordinator:
    """Provides a DataCoordinator instance combining all services."""
    return DataCoordinator(
        enqueuer, downloader, pruner, rss_generator, feed_db, cookies_path=cookies_path
    )


async def create_test_feed(
    feed_db: FeedDatabase,
    feed_id: str,
    source_url: str,
    source_type: SourceType,
    resolved_url: str | None = None,
) -> Feed:
    """Create a test feed in the database."""
    feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=source_type,
        source_url=source_url,
        resolved_url=resolved_url,
        last_successful_sync=MIN_SYNC_DATE,
        title=f"Test Feed {feed_id}",
        description=f"Test description for {feed_id}",  # Required for RSS generation
    )
    await feed_db.upsert_feed(feed)
    return feed


def create_feed_config(
    url: str = BIG_BUCK_BUNNY_SHORT_URL,
    yt_args: str = YT_DLP_MINIMAL_ARGS_STR,
    keep_last: int | None = None,
    since: datetime | None = None,
    max_errors: int = MAX_ERRORS,
) -> FeedConfig:
    """Create a FeedConfig instance for testing."""
    return FeedConfig(
        url=url,
        yt_args=yt_args,  # type: ignore
        schedule=TEST_CRON_SCHEDULE,
        keep_last=keep_last,
        since=since,
        max_errors=max_errors,
    )


async def setup_feed_with_initial_sync(
    feed_db: FeedDatabase,
    feed_id: str,
    source_url: str,
    source_type: SourceType,
    resolved_url: str | None = None,
) -> Feed:
    """Create and setup a test feed with initial sync timestamp."""
    _ = await create_test_feed(feed_db, feed_id, source_url, source_type, resolved_url)

    # Set an initial last_successful_sync timestamp to enable process_feed
    # Use November 10, 2014 at 12:00 to ensure Big Buck Bunny (published 2014-11-10 14:05:55) is in range
    # With hourly cron, this creates a 2-hour window from 12:00-14:00 that includes the video
    initial_sync_time = datetime(2014, 11, 10, 12, 0, 0, tzinfo=UTC)
    await feed_db.mark_sync_success(feed_id, sync_time=initial_sync_time)

    return await feed_db.get_feed_by_id(feed_id)


async def setup_feed_with_channel_sync(
    feed_db: FeedDatabase,
    feed_id: str,
    source_url: str,
    source_type: SourceType,
    resolved_url: str | None = None,
) -> Feed:
    """Create and setup a test feed for channel testing with sync timestamp."""
    _ = await create_test_feed(feed_db, feed_id, source_url, source_type, resolved_url)

    # Set sync timestamp for July 9, 2024 to include newest coletdjnz videos (July 10, 2024)
    # With hourly cron, this creates a 2-hour window that includes the videos
    initial_sync_time = datetime(2024, 7, 9, 22, 0, 0, tzinfo=UTC)
    await feed_db.mark_sync_success(feed_id, sync_time=initial_sync_time)

    return await feed_db.get_feed_by_id(feed_id)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_process_feed_complete_success(
    data_coordinator: DataCoordinator,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    file_manager: FileManager,
    rss_generator: RSSFeedGenerator,
):
    """Tests complete end-to-end feed processing pipeline.

    Fresh feed with no existing data should run all phases:
    enqueue → download → prune → RSS generation.
    Verifies all phase results, database state, files created, and RSS generated.
    """
    feed_id = "test_complete_success"
    feed_config = create_feed_config()

    assert feed_config.url is not None, (
        "Scheduled feed configuration must define 'url'."
    )
    # Setup feed with initial sync timestamp
    await setup_feed_with_initial_sync(
        feed_db, feed_id, feed_config.url, SourceType.SINGLE_VIDEO
    )

    # Process the feed
    results = await data_coordinator.process_feed(feed_id, feed_config)

    # Verify ProcessingResults structure
    assert results.feed_id == feed_id
    assert results.overall_success is True
    assert results.total_duration_seconds > 0
    assert results.feed_sync_updated is True
    assert results.fatal_error is None

    # Verify enqueue phase results
    assert results.enqueue_result.success is True
    assert results.enqueue_result.count >= 1, "Should have enqueued at least 1 item"
    assert results.enqueue_result.duration_seconds > 0
    assert len(results.enqueue_result.errors) == 0

    # Verify download phase results
    assert results.download_result.success is True
    assert results.download_result.count >= 1, "Should have downloaded at least 1 item"
    assert results.download_result.duration_seconds > 0
    assert len(results.download_result.errors) == 0

    # Verify prune phase results (may or may not prune anything)
    assert results.prune_result.success is True
    assert results.prune_result.count >= 0, "Prune count should be non-negative"
    assert results.prune_result.duration_seconds > 0
    assert len(results.prune_result.errors) == 0

    # Verify RSS generation phase results
    assert results.rss_generation_result.success is True
    assert results.rss_generation_result.count == 1, "Should generate 1 feed"
    assert results.rss_generation_result.duration_seconds > 0
    assert len(results.rss_generation_result.errors) == 0

    # Verify database state consistency
    downloaded_items = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    assert len(downloaded_items) >= 1, "Should have downloaded items in database"
    assert len(downloaded_items) == results.total_downloaded

    # Verify downloaded files exist
    for downloaded_item in downloaded_items:
        assert await file_manager.download_exists(
            feed_id, downloaded_item.id, downloaded_item.ext
        ), f"Downloaded file should exist for {downloaded_item.id}"
        assert downloaded_item.filesize > 0, "Downloaded item should have filesize"

    # Verify no items left in QUEUED status
    queued_items = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    assert len(queued_items) == 0, (
        "Should have no queued items after successful processing"
    )

    # Verify RSS feed generation persisted to disk
    feed_xml_path = await file_manager.get_feed_xml_path(feed_id)
    feed_xml = Path(feed_xml_path).read_bytes()
    assert feed_xml, "RSS feed should be generated and have content"
    assert b"<?xml" in feed_xml, "RSS feed should be valid XML"

    # Verify feed sync status updated
    updated_feed = await feed_db.get_feed_by_id(feed_id)
    assert updated_feed.last_successful_sync is not None
    assert updated_feed.last_successful_sync > datetime(2024, 1, 1, tzinfo=UTC)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_process_feed_incremental_processing(
    data_coordinator: DataCoordinator,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    file_manager: FileManager,
):
    """Tests incremental processing of feed with existing data.

    Feed already has some downloaded items - should only process new items
    incrementally. Verifies existing items untouched, new items processed.
    """
    feed_id = "test_incremental"
    feed_config = create_feed_config()

    assert feed_config.url is not None, (
        "Scheduled feed configuration must define 'url'."
    )
    # Setup feed with initial sync timestamp
    await setup_feed_with_initial_sync(
        feed_db, feed_id, feed_config.url, SourceType.SINGLE_VIDEO
    )

    # Manually insert an existing downloaded item
    existing_download = Download(
        feed_id=feed_id,
        id=BIG_BUCK_BUNNY_VIDEO_ID,
        source_url=BIG_BUCK_BUNNY_URL,
        title=BIG_BUCK_BUNNY_TITLE,
        published=BIG_BUCK_BUNNY_PUBLISHED,
        ext="mp4",
        mime_type="video/mp4",
        filesize=12345,
        duration=635,
        status=DownloadStatus.DOWNLOADED,
        retries=0,
        discovered_at=BIG_BUCK_BUNNY_PUBLISHED,
        updated_at=BIG_BUCK_BUNNY_PUBLISHED,
    )
    await download_db.upsert_download(existing_download)

    # Create a dummy file for the existing download
    feed_data_dir = Path(file_manager._paths.base_data_dir) / feed_id
    feed_data_dir.mkdir(parents=True, exist_ok=True)
    dummy_file = feed_data_dir / f"{BIG_BUCK_BUNNY_VIDEO_ID}.mp4"
    dummy_file.write_bytes(b"dummy content")

    # Verify initial state
    initial_downloaded = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    assert len(initial_downloaded) == 1
    initial_download_id = initial_downloaded[0].id

    # Process the feed
    results = await data_coordinator.process_feed(feed_id, feed_config)

    # Verify overall success
    assert results.overall_success is True

    # Verify enqueue phase found no new items (since Big Buck Bunny already exists)
    assert results.enqueue_result.success is True
    assert results.enqueue_result.count == 0, "Should not enqueue existing items"

    # Verify download phase had nothing to download
    assert results.download_result.success is True
    assert results.download_result.count == 0, "Should not download existing items"

    # Verify existing download is untouched
    final_downloaded = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    assert len(final_downloaded) == 1
    assert final_downloaded[0].id == initial_download_id
    assert final_downloaded[0].status == DownloadStatus.DOWNLOADED

    # Verify file still exists
    assert dummy_file.exists(), "Existing file should be preserved"

    # RSS generation should still succeed
    assert results.rss_generation_result.success is True


@pytest.mark.asyncio
@pytest.mark.integration
async def test_process_feed_idempotency(
    data_coordinator: DataCoordinator,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    file_manager: FileManager,
):
    """Tests that repeated process_feed calls are idempotent.

    Run process_feed twice on same feed - second run should be essentially
    no-op. Verifies stable state and minimal processing.
    """
    feed_id = "test_idempotency"
    feed_config = create_feed_config()

    assert feed_config.url is not None, (
        "Scheduled feed configuration must define 'url'."
    )
    # Setup feed with initial sync timestamp
    await setup_feed_with_initial_sync(
        feed_db, feed_id, feed_config.url, SourceType.SINGLE_VIDEO
    )

    # First run - should process normally
    first_results = await data_coordinator.process_feed(feed_id, feed_config)
    assert first_results.overall_success is True
    assert first_results.total_enqueued >= 1
    assert first_results.total_downloaded >= 1

    # Capture state after first run
    first_downloaded = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    first_downloaded_count = len(first_downloaded)
    first_download_ids = {dl.id for dl in first_downloaded}

    # Verify files exist
    for dl in first_downloaded:
        assert await file_manager.download_exists(feed_id, dl.id, dl.ext)

    # Second run - should be minimal processing
    second_results = await data_coordinator.process_feed(feed_id, feed_config)

    # Verify second run succeeds
    assert second_results.overall_success is True

    # Verify minimal enqueue activity (should find no new items)
    assert second_results.enqueue_result.success is True
    assert second_results.enqueue_result.count == 0, (
        "Second run should find no new items"
    )

    # Verify minimal download activity (nothing new to download)
    assert second_results.download_result.success is True
    assert second_results.download_result.count == 0, (
        "Second run should download nothing new"
    )

    # Verify prune and RSS still execute successfully
    assert second_results.prune_result.success is True
    assert second_results.rss_generation_result.success is True

    # Verify stable state - same downloads exist
    second_downloaded = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    second_downloaded_count = len(second_downloaded)
    second_download_ids = {dl.id for dl in second_downloaded}

    assert second_downloaded_count == first_downloaded_count, (
        "Download count should be stable"
    )
    assert second_download_ids == first_download_ids, "Same items should be downloaded"

    # Verify all files still exist
    for dl in second_downloaded:
        assert await file_manager.download_exists(feed_id, dl.id, dl.ext)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_process_feed_with_retention_pruning(
    data_coordinator: DataCoordinator,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    file_manager: FileManager,
):
    """Tests feed processing with retention pruning integration.

    Feed with many items and keep_last config should download new items
    and prune old ones. Verifies pruning works correctly in full pipeline.
    """
    feed_id = "test_retention_pruning"
    # Use channel URL to get multiple items and set keep_last=1
    feed_config = create_feed_config(
        url=COLETDJNZ_CHANNEL_VIDEOS,
        keep_last=1,  # Keep only the most recent item
    )

    assert feed_config.url is not None, (
        "Scheduled feed configuration must define 'url'."
    )
    # Setup feed with channel sync timestamp (for 2024 videos)
    await setup_feed_with_channel_sync(
        feed_db, feed_id, feed_config.url, SourceType.PLAYLIST, feed_config.url
    )

    # Manually insert several old downloaded items to test pruning
    base_time = datetime(2020, 1, 1, tzinfo=UTC)
    old_downloads: list[Download] = []
    for i in range(3):
        old_download = Download(
            feed_id=feed_id,
            id=f"old_video_{i}",
            source_url=f"https://www.youtube.com/watch?v=old_video_{i}",
            title=f"Old Video {i}",
            published=base_time.replace(day=i + 1),
            ext="mp4",
            mime_type="video/mp4",
            filesize=12345,
            duration=100,
            status=DownloadStatus.DOWNLOADED,
            retries=0,
            discovered_at=base_time.replace(day=i + 1),
            updated_at=base_time.replace(day=i + 1),
        )
        await download_db.upsert_download(old_download)
        old_downloads.append(old_download)

        # Create dummy files for old downloads
        feed_data_dir = Path(file_manager._paths.base_data_dir) / feed_id
        feed_data_dir.mkdir(parents=True, exist_ok=True)
        dummy_file = feed_data_dir / f"old_video_{i}.mp4"
        dummy_file.write_bytes(b"old dummy content")

    # Verify initial state - 3 old downloads
    initial_downloaded = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    assert len(initial_downloaded) == 3

    # Process the feed
    results = await data_coordinator.process_feed(feed_id, feed_config)

    # Verify overall success
    assert results.overall_success is True

    # Verify enqueue phase found and queued new items
    assert results.enqueue_result.success is True
    assert results.enqueue_result.count >= 1, "Should enqueue new items from channel"

    # Verify download phase processed new items
    assert results.download_result.success is True
    assert results.download_result.count >= 1, "Should download new items"

    # Verify prune phase removed old items (should keep only 1 most recent)
    assert results.prune_result.success is True
    # Prune count should be positive since we had 3 old items + new items, keeping only 1
    assert results.prune_result.count >= 2, "Should have pruned old items"

    # Verify RSS generation succeeded
    assert results.rss_generation_result.success is True

    # Verify final state - should have only keep_last=1 downloaded items
    final_downloaded = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    assert len(final_downloaded) == 1, (
        f"Should have exactly 1 downloaded item (keep_last=1), got {len(final_downloaded)}"
    )

    # Verify the remaining item is the most recent one
    remaining_download = final_downloaded[0]
    assert remaining_download.published > base_time, (
        "Remaining download should be newer than old items"
    )

    # Verify file exists for remaining download
    assert await file_manager.download_exists(
        feed_id, remaining_download.id, remaining_download.ext
    ), "File should exist for remaining download"

    # Verify some old files were deleted
    old_files_remaining = 0
    for old_dl in old_downloads:
        if await file_manager.download_exists(feed_id, old_dl.id, old_dl.ext):
            old_files_remaining += 1
    assert old_files_remaining < 3, "Some old files should have been deleted"

    # Verify archived items exist in database
    archived_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.ARCHIVED, feed_id=feed_id
    )
    assert len(archived_downloads) >= 2, "Should have archived old downloads"


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.parametrize(
    "test_case, url, source_type, resolved_url, sync_timestamp, expected_enqueue_count, description",
    [
        # Single video tests - should always ignore date filtering
        (
            "single_video_ignores_date_filter_out_of_range",
            BIG_BUCK_BUNNY_SHORT_URL,
            SourceType.SINGLE_VIDEO,
            None,
            datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
            1,
            "Single video should be downloaded regardless of out-of-range date (ignores date filtering)",
        ),
        (
            "single_video_ignores_date_filter_in_range",
            BIG_BUCK_BUNNY_SHORT_URL,
            SourceType.SINGLE_VIDEO,
            None,
            datetime(2014, 11, 10, 12, 0, 0, tzinfo=UTC),
            1,
            "Single video should be downloaded when in date range (but still ignores date filtering)",
        ),
        # Channel tests - should respect date filtering (@coletdjnz has videos from 2024-07-10, 2024-03-08, 2022-07-29)
        (
            "channel_excludes_out_of_range",
            COLETDJNZ_CHANNEL_VIDEOS,
            SourceType.PLAYLIST,
            COLETDJNZ_CHANNEL_VIDEOS,
            datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
            0,
            "Channel should exclude all videos when sync date is after all uploads (2025 > 2024)",
        ),
        (
            "channel_includes_recent_videos",
            COLETDJNZ_CHANNEL_VIDEOS,
            SourceType.PLAYLIST,
            COLETDJNZ_CHANNEL_VIDEOS,
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            2,
            "Channel should include 2024 videos (uploaded 2024-07-10 and 2024-03-08) when sync from 2024-01-01",
        ),
        # Playlist tests - should respect date filtering (playlist has 1 video from 2022-07-29)
        (
            "playlist_excludes_out_of_range",
            TEST_PLAYLIST_URL,
            SourceType.PLAYLIST,
            None,
            datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
            0,
            "Playlist should exclude videos when sync date is after all uploads (2025 > 2022)",
        ),
        (
            "playlist_includes_in_range",
            TEST_PLAYLIST_URL,
            SourceType.PLAYLIST,
            None,
            datetime(2022, 1, 1, 12, 0, 0, tzinfo=UTC),
            1,
            "Playlist should include video uploaded 2022-07-29 when sync from 2022-01-01",
        ),
    ],
)
async def test_date_filtering_behavior(
    data_coordinator: DataCoordinator,
    feed_db: FeedDatabase,
    test_case: str,
    url: str,
    source_type: SourceType,
    resolved_url: str | None,
    sync_timestamp: datetime,
    expected_enqueue_count: int,
    description: str,
):
    """Test date filtering behavior across different URL types.

    This comprehensive test demonstrates the complete date filtering behavior:

    **Single videos:** Always ignore date filtering to prevent partial metadata issues.
    Both in-range and out-of-range cases should download the video (count=1).

    **Channels:** Respect date filtering and include/exclude videos based on upload dates.
    @coletdjnz channel has videos from 2024-07-10, 2024-03-08, and 2022-07-29.

    **Playlists:** Respect date filtering and include/exclude videos based on upload dates.
    Test playlist contains 1 video from 2022-07-29.
    """
    feed_id = f"test_date_filtering_{test_case}"

    feed_config = create_feed_config(url=url)

    # Create feed
    await create_test_feed(feed_db, feed_id, url, source_type, resolved_url)

    # Set sync timestamp - this determines the date filtering window
    await feed_db.mark_sync_success(feed_id, sync_time=sync_timestamp)

    # Process the feed
    results = await data_coordinator.process_feed(feed_id, feed_config)

    # Verify overall success
    assert results.overall_success is True, f"Processing should succeed for {test_case}"
    assert results.fatal_error is None, f"No fatal errors for {test_case}"

    # Verify all phases succeed
    assert results.enqueue_result.success is True, (
        f"Enqueue should succeed for {test_case}"
    )
    assert results.download_result.success is True, (
        f"Download should succeed for {test_case}"
    )
    assert results.prune_result.success is True, f"Prune should succeed for {test_case}"
    assert results.rss_generation_result.success is True, (
        f"RSS generation should succeed for {test_case}"
    )

    # The key assertion: verify expected behavior based on URL type and date range
    assert results.enqueue_result.count == expected_enqueue_count, description
    assert results.download_result.count == expected_enqueue_count, description
