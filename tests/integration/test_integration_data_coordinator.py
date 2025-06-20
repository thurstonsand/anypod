# pyright: reportPrivateUsage=false

"""Integration tests for DataCoordinator process_feed orchestration method.

Tests focus on happy path scenarios and verify the complete end-to-end
feed processing pipeline including enqueue, download, prune, and RSS generation.
"""

from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from anypod.config import FeedConfig
from anypod.data_coordinator import DataCoordinator
from anypod.data_coordinator.downloader import Downloader
from anypod.data_coordinator.enqueuer import Enqueuer
from anypod.data_coordinator.pruner import Pruner
from anypod.db import DownloadDatabase
from anypod.db.feed_db import FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.file_manager import FileManager
from anypod.path_manager import PathManager
from anypod.rss import RSSFeedGenerator
from anypod.ytdlp_wrapper import YtdlpWrapper

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
def path_manager(tmp_path_factory: pytest.TempPathFactory) -> Generator[PathManager]:
    """Provides a PathManager instance with a temporary data directory."""
    yield PathManager(
        base_data_dir=tmp_path_factory.mktemp("data"),
        base_url="http://localhost",
    )


@pytest.fixture
def feed_db() -> Generator[FeedDatabase]:
    """Provides a FeedDatabase instance with a temporary database."""
    feed_db = FeedDatabase(db_path=None, memory_name="integration_test")
    yield feed_db
    feed_db.close()


@pytest.fixture
def download_db() -> Generator[DownloadDatabase]:
    """Provides a DownloadDatabase instance with a temporary database."""
    download_db = DownloadDatabase(db_path=None, memory_name="integration_test")
    yield download_db
    download_db.close()


@pytest.fixture
def file_manager(paths: PathManager) -> Generator[FileManager]:
    """Provides a FileManager instance with shared data directory."""
    file_manager = FileManager(paths)
    yield file_manager


@pytest.fixture
def ytdlp_wrapper(paths: PathManager) -> Generator[YtdlpWrapper]:
    """Provides a YtdlpWrapper instance with shared directories."""
    yield YtdlpWrapper(paths)


@pytest.fixture
def enqueuer(
    feed_db: FeedDatabase, download_db: DownloadDatabase, ytdlp_wrapper: YtdlpWrapper
) -> Generator[Enqueuer]:
    """Provides an Enqueuer instance for the coordinator."""
    yield Enqueuer(feed_db, download_db, ytdlp_wrapper)


@pytest.fixture
def downloader(
    download_db: DownloadDatabase,
    file_manager: FileManager,
    ytdlp_wrapper: YtdlpWrapper,
) -> Generator[Downloader]:
    """Provides a Downloader instance for the coordinator."""
    yield Downloader(download_db, file_manager, ytdlp_wrapper)


@pytest.fixture
def pruner(
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    file_manager: FileManager,
) -> Generator[Pruner]:
    """Provides a Pruner instance for the coordinator."""
    yield Pruner(feed_db, download_db, file_manager)


@pytest.fixture
def rss_generator(
    download_db: DownloadDatabase,
    paths: PathManager,
) -> Generator[RSSFeedGenerator]:
    """Provides an RSSFeedGenerator instance for the coordinator."""
    yield RSSFeedGenerator(download_db, paths)


@pytest.fixture
def data_coordinator(
    enqueuer: Enqueuer,
    downloader: Downloader,
    pruner: Pruner,
    rss_generator: RSSFeedGenerator,
    feed_db: FeedDatabase,
) -> Generator[DataCoordinator]:
    """Provides a DataCoordinator instance combining all services."""
    yield DataCoordinator(enqueuer, downloader, pruner, rss_generator, feed_db)


def create_test_feed(feed_db: FeedDatabase, feed_id: str) -> Feed:
    """Create a test feed in the database."""
    feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.UNKNOWN,  # Will be determined by ytdlp
        source_url="https://example.com/test",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title=f"Test Feed {feed_id}",
    )
    feed_db.upsert_feed(feed)
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


def setup_feed_with_initial_sync(feed_db: FeedDatabase, feed_id: str) -> Feed:
    """Create and setup a test feed with initial sync timestamp."""
    _ = create_test_feed(feed_db, feed_id)

    # Set an initial last_successful_sync timestamp to enable process_feed
    # Use November 10, 2014 at 12:00 to ensure Big Buck Bunny (published 2014-11-10 14:05:55) is in range
    # With hourly cron, this creates a 2-hour window from 12:00-14:00 that includes the video
    initial_sync_time = datetime(2014, 11, 10, 12, 0, 0, tzinfo=UTC)
    feed_db.mark_sync_success(feed_id, sync_time=initial_sync_time)

    return feed_db.get_feed_by_id(feed_id)


def setup_feed_with_channel_sync(feed_db: FeedDatabase, feed_id: str) -> Feed:
    """Create and setup a test feed for channel testing with sync timestamp."""
    _ = create_test_feed(feed_db, feed_id)

    # Set sync timestamp for July 9, 2024 to include newest coletdjnz videos (July 10, 2024)
    # With hourly cron, this creates a 2-hour window that includes the videos
    initial_sync_time = datetime(2024, 7, 9, 22, 0, 0, tzinfo=UTC)
    feed_db.mark_sync_success(feed_id, sync_time=initial_sync_time)

    return feed_db.get_feed_by_id(feed_id)


@pytest.mark.integration
def test_process_feed_complete_success(
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

    # Setup feed with initial sync timestamp
    setup_feed_with_initial_sync(feed_db, feed_id)

    # Process the feed
    results = data_coordinator.process_feed(feed_id, feed_config)

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
    downloaded_items = download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    assert len(downloaded_items) >= 1, "Should have downloaded items in database"
    assert len(downloaded_items) == results.total_downloaded

    # Verify downloaded files exist
    for downloaded_item in downloaded_items:
        assert file_manager.download_exists(
            feed_id, downloaded_item.id, downloaded_item.ext
        ), f"Downloaded file should exist for {downloaded_item.id}"
        assert downloaded_item.filesize > 0, "Downloaded item should have filesize"

    # Verify no items left in QUEUED status
    queued_items = download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    assert len(queued_items) == 0, (
        "Should have no queued items after successful processing"
    )

    # Verify RSS feed generation
    feed_xml = rss_generator.get_feed_xml(feed_id)
    assert feed_xml is not None, "RSS feed should be generated"
    assert len(feed_xml) > 0, "RSS feed should have content"
    assert b"<?xml" in feed_xml, "RSS feed should be valid XML"

    # Verify feed sync status updated
    updated_feed = feed_db.get_feed_by_id(feed_id)
    assert updated_feed.last_successful_sync is not None
    assert updated_feed.last_successful_sync > datetime(2024, 1, 1, tzinfo=UTC)


@pytest.mark.integration
def test_process_feed_incremental_processing(
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

    # Setup feed with initial sync timestamp
    setup_feed_with_initial_sync(feed_db, feed_id)

    # Manually insert an existing downloaded item
    existing_download = Download(
        feed=feed_id,
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
    download_db.upsert_download(existing_download)

    # Create a dummy file for the existing download
    feed_data_dir = Path(file_manager._paths.base_data_dir) / feed_id
    feed_data_dir.mkdir(parents=True, exist_ok=True)
    dummy_file = feed_data_dir / f"{BIG_BUCK_BUNNY_VIDEO_ID}.mp4"
    dummy_file.write_bytes(b"dummy content")

    # Verify initial state
    initial_downloaded = download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    assert len(initial_downloaded) == 1
    initial_download_id = initial_downloaded[0].id

    # Process the feed
    results = data_coordinator.process_feed(feed_id, feed_config)

    # Verify overall success
    assert results.overall_success is True

    # Verify enqueue phase found no new items (since Big Buck Bunny already exists)
    assert results.enqueue_result.success is True
    assert results.enqueue_result.count == 0, "Should not enqueue existing items"

    # Verify download phase had nothing to download
    assert results.download_result.success is True
    assert results.download_result.count == 0, "Should not download existing items"

    # Verify existing download is untouched
    final_downloaded = download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    assert len(final_downloaded) == 1
    assert final_downloaded[0].id == initial_download_id
    assert final_downloaded[0].status == DownloadStatus.DOWNLOADED

    # Verify file still exists
    assert dummy_file.exists(), "Existing file should be preserved"

    # RSS generation should still succeed
    assert results.rss_generation_result.success is True


@pytest.mark.integration
def test_process_feed_idempotency(
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

    # Setup feed with initial sync timestamp
    setup_feed_with_initial_sync(feed_db, feed_id)

    # First run - should process normally
    first_results = data_coordinator.process_feed(feed_id, feed_config)
    assert first_results.overall_success is True
    assert first_results.total_enqueued >= 1
    assert first_results.total_downloaded >= 1

    # Capture state after first run
    first_downloaded = download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    first_downloaded_count = len(first_downloaded)
    first_download_ids = {dl.id for dl in first_downloaded}

    # Verify files exist
    for dl in first_downloaded:
        assert file_manager.download_exists(feed_id, dl.id, dl.ext)

    # Second run - should be minimal processing
    second_results = data_coordinator.process_feed(feed_id, feed_config)

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
    second_downloaded = download_db.get_downloads_by_status(
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
        assert file_manager.download_exists(feed_id, dl.id, dl.ext)


@pytest.mark.integration
def test_process_feed_with_retention_pruning(
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

    # Setup feed with channel sync timestamp (for 2024 videos)
    setup_feed_with_channel_sync(feed_db, feed_id)

    # Manually insert several old downloaded items to test pruning
    base_time = datetime(2020, 1, 1, tzinfo=UTC)
    old_downloads: list[Download] = []
    for i in range(3):
        old_download = Download(
            feed=feed_id,
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
        download_db.upsert_download(old_download)
        old_downloads.append(old_download)

        # Create dummy files for old downloads
        feed_data_dir = Path(file_manager._paths.base_data_dir) / feed_id
        feed_data_dir.mkdir(parents=True, exist_ok=True)
        dummy_file = feed_data_dir / f"old_video_{i}.mp4"
        dummy_file.write_bytes(b"old dummy content")

    # Verify initial state - 3 old downloads
    initial_downloaded = download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    assert len(initial_downloaded) == 3

    # Process the feed
    results = data_coordinator.process_feed(feed_id, feed_config)

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
    final_downloaded = download_db.get_downloads_by_status(
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
    assert file_manager.download_exists(
        feed_id, remaining_download.id, remaining_download.ext
    ), "File should exist for remaining download"

    # Verify some old files were deleted
    old_files_remaining = sum(
        1
        for old_dl in old_downloads
        if file_manager.download_exists(feed_id, old_dl.id, old_dl.ext)
    )
    assert old_files_remaining < 3, "Some old files should have been deleted"

    # Verify archived items exist in database
    archived_downloads = download_db.get_downloads_by_status(
        DownloadStatus.ARCHIVED, feed_id=feed_id
    )
    assert len(archived_downloads) >= 2, "Should have archived old downloads"


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_case, url, sync_timestamp, expected_enqueue_count, description",
    [
        # Single video tests - should always ignore date filtering
        (
            "single_video_ignores_date_filter_out_of_range",
            BIG_BUCK_BUNNY_SHORT_URL,
            datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
            1,
            "Single video should be downloaded regardless of out-of-range date (ignores date filtering)",
        ),
        (
            "single_video_ignores_date_filter_in_range",
            BIG_BUCK_BUNNY_SHORT_URL,
            datetime(2014, 11, 10, 12, 0, 0, tzinfo=UTC),
            1,
            "Single video should be downloaded when in date range (but still ignores date filtering)",
        ),
        # Channel tests - should respect date filtering (@coletdjnz has videos from 2024-07-10, 2024-03-08, 2022-07-29)
        (
            "channel_excludes_out_of_range",
            COLETDJNZ_CHANNEL_VIDEOS,
            datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
            0,
            "Channel should exclude all videos when sync date is after all uploads (2025 > 2024)",
        ),
        (
            "channel_includes_recent_videos",
            COLETDJNZ_CHANNEL_VIDEOS,
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            2,
            "Channel should include 2024 videos (uploaded 2024-07-10 and 2024-03-08) when sync from 2024-01-01",
        ),
        # Playlist tests - should respect date filtering (playlist has 1 video from 2022-07-29)
        (
            "playlist_excludes_out_of_range",
            TEST_PLAYLIST_URL,
            datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
            0,
            "Playlist should exclude videos when sync date is after all uploads (2025 > 2022)",
        ),
        (
            "playlist_includes_in_range",
            TEST_PLAYLIST_URL,
            datetime(2022, 1, 1, 12, 0, 0, tzinfo=UTC),
            1,
            "Playlist should include video uploaded 2022-07-29 when sync from 2022-01-01",
        ),
    ],
)
def test_date_filtering_behavior(
    data_coordinator: DataCoordinator,
    feed_db: FeedDatabase,
    test_case: str,
    url: str,
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
    create_test_feed(feed_db, feed_id)

    # Set sync timestamp - this determines the date filtering window
    feed_db.mark_sync_success(feed_id, sync_time=sync_timestamp)

    # Process the feed
    results = data_coordinator.process_feed(feed_id, feed_config)

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
