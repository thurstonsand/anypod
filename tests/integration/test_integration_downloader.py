# pyright: reportPrivateUsage=false

"""Integration tests for Downloader with real YouTube URLs and file operations."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from anypod.config import FeedConfig
from anypod.data_coordinator.downloader import Downloader
from anypod.data_coordinator.enqueuer import Enqueuer
from anypod.db import DownloadDatabase
from anypod.db.feed_db import FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.file_manager import FileManager

# Test constants - same as other integration tests for consistency
BIG_BUCK_BUNNY_VIDEO_ID = "aqz-KE-bpKQ"
BIG_BUCK_BUNNY_URL = f"https://www.youtube.com/watch?v={BIG_BUCK_BUNNY_VIDEO_ID}"
BIG_BUCK_BUNNY_SHORT_URL = f"https://youtu.be/{BIG_BUCK_BUNNY_VIDEO_ID}"
BIG_BUCK_BUNNY_TITLE = (
    "Big Buck Bunny 60fps 4K - Official Blender Foundation Short Film"
)
BIG_BUCK_BUNNY_PUBLISHED = datetime(2014, 11, 10, 14, 5, 55, tzinfo=UTC)
BIG_BUCK_BUNNY_DURATION = 635.0

COLETDJNZ_CHANNEL_VIDEOS = "https://www.youtube.com/@coletdjnz/videos"
TWITTER_SINGLE_URL = "https://x.com/ActuallyNPH/status/560049149836808192"
INVALID_VIDEO_URL = "https://www.youtube.com/watch?v=thisvideodoesnotexistxyz"

# CLI args for minimal quality downloads as a string
YT_DLP_MINIMAL_ARGS_STR = "--format worst*[ext=mp4]/worst[ext=mp4]/best[ext=mp4]"

# --- Tests for Downloader.download_queued ---
# Test schedule and config
TEST_CRON_SCHEDULE = "0 * * * *"
MAX_ERRORS = 3

# Sample feed configurations for testing
SAMPLE_FEED_CONFIG = FeedConfig(
    url=BIG_BUCK_BUNNY_SHORT_URL,
    yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
    schedule=TEST_CRON_SCHEDULE,
    keep_last=None,
    since=None,
    max_errors=MAX_ERRORS,
)

CHANNEL_FEED_CONFIG = FeedConfig(
    url=COLETDJNZ_CHANNEL_VIDEOS,
    yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
    schedule=TEST_CRON_SCHEDULE,
    keep_last=2,
    since=None,
    max_errors=MAX_ERRORS,
)

TWITTER_FEED_CONFIG = FeedConfig(
    url=TWITTER_SINGLE_URL,
    yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
    schedule=TEST_CRON_SCHEDULE,
    keep_last=None,
    since=None,
    max_errors=MAX_ERRORS,
)

TEST_SINGLE_VIDEO_CASES: list[tuple[str, FeedConfig, SourceType, str | None]] = [
    ("test_single_video", SAMPLE_FEED_CONFIG, SourceType.SINGLE_VIDEO, None),
    (
        "test_twitter_single_video",
        TWITTER_FEED_CONFIG,
        SourceType.SINGLE_VIDEO,
        TWITTER_SINGLE_URL,
    ),
]

INVALID_FEED_CONFIG = FeedConfig(
    url=INVALID_VIDEO_URL,
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
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title=f"Test Feed {feed_id}",
    )
    await feed_db.upsert_feed(feed)
    return feed


async def enqueue_test_items(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    feed_id: str,
    feed_config: FeedConfig,
    source_type: SourceType,
    resolved_url: str | None = None,
    fetch_since_date: datetime | None = None,
    cookies_path: Path | None = None,
) -> int:
    """Helper function to enqueue test items for a feed."""
    if fetch_since_date is None:
        fetch_since_date = datetime.min.replace(tzinfo=UTC)

    # Create feed in database first
    assert feed_config.url is not None, (
        "Scheduled feed configuration must define 'url'."
    )
    source_url = feed_config.url
    await create_test_feed(feed_db, feed_id, source_url, source_type, resolved_url)

    newly_queued_count, _ = await enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
        cookies_path=cookies_path,
    )
    return newly_queued_count


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "feed_id, feed_config, source_type, resolved_url", TEST_SINGLE_VIDEO_CASES
)
async def test_download_queued_single_video_success(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    downloader: Downloader,
    download_db: DownloadDatabase,
    file_manager: FileManager,
    cookies_path: Path | None,
    feed_id: str,
    feed_config: FeedConfig,
    source_type: SourceType,
    resolved_url: str | None,
) -> None:
    """Tests successful download of a single queued video."""
    # First, use enqueuer to populate database with a real entry
    queued_count = await enqueue_test_items(
        enqueuer,
        feed_db,
        feed_id,
        feed_config,
        source_type,
        resolved_url,
        cookies_path=cookies_path,
    )
    assert queued_count >= 1, "Expected at least 1 item to be queued by enqueuer"

    # Verify item is in QUEUED status
    queued_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    assert len(queued_downloads) >= 1
    original_download = queued_downloads[0]
    assert original_download.status == DownloadStatus.QUEUED

    # Now test the downloader
    success_count, failure_count = await downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        cookies_path=cookies_path,
        limit=-1,
    )

    # Verify download results
    assert success_count >= 1, (
        f"Expected at least 1 successful download, got {success_count}"
    )
    assert failure_count == 0, f"Expected 0 failures, got {failure_count}"

    # Verify database was updated
    downloads = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    assert len(downloads) >= 1

    # Verify the specific download was updated
    download = next((dl for dl in downloads if dl.id == original_download.id), None)
    assert download is not None
    assert download.status == DownloadStatus.DOWNLOADED
    assert download.ext  # Should have extension set
    assert download.filesize and download.filesize > 0  # Should have filesize set
    assert download.retries == 0  # Should be reset
    assert download.last_error is None  # Should be cleared

    # Verify thumbnail saved and recorded
    assert download.thumbnail_ext == "jpg"
    assert await file_manager.image_exists(feed_id, download.id, "jpg")

    # Verify file was actually downloaded
    assert await file_manager.download_exists(feed_id, download.id, download.ext)

    # Verify no more queued items for this feed
    remaining_queued = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    assert len(remaining_queued) == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_queued_multiple_videos_success(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    downloader: Downloader,
    download_db: DownloadDatabase,
    file_manager: FileManager,
    cookies_path: Path | None,
):
    """Tests successful download of multiple queued videos from a channel."""
    feed_id = "test_multiple_videos"
    feed_config = CHANNEL_FEED_CONFIG

    # Use enqueuer to populate database with multiple entries
    queued_count = await enqueue_test_items(
        enqueuer,
        feed_db,
        feed_id,
        feed_config,
        SourceType.PLAYLIST,
        COLETDJNZ_CHANNEL_VIDEOS,
        cookies_path=cookies_path,
    )
    assert queued_count >= 1, "Expected at least 1 item to be queued by enqueuer"

    # Get original queued items
    original_queued = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    original_count = len(original_queued)
    assert original_count >= 1

    # Test the downloader
    success_count, failure_count = await downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        cookies_path=cookies_path,
        limit=-1,
    )

    # Verify results
    assert success_count + failure_count == original_count
    assert success_count >= 1

    # Verify database updates
    downloaded_items = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    assert len(downloaded_items) == success_count

    # Verify files were downloaded
    for downloaded_item in downloaded_items:
        assert await file_manager.download_exists(
            feed_id, downloaded_item.id, downloaded_item.ext
        )
        assert downloaded_item.filesize is not None and downloaded_item.filesize > 0
        # Verify thumbnails saved and recorded for each
        assert downloaded_item.thumbnail_ext == "jpg"
        assert await file_manager.image_exists(feed_id, downloaded_item.id, "jpg")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_queued_with_limit(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    downloader: Downloader,
    download_db: DownloadDatabase,
    file_manager: FileManager,
    cookies_path: Path | None,
):
    """Tests that the limit parameter properly restricts the number of downloads processed."""
    feed_id = "test_limit"
    feed_config = CHANNEL_FEED_CONFIG

    # Use enqueuer to populate database
    enqueued_count = await enqueue_test_items(
        enqueuer,
        feed_db,
        feed_id,
        feed_config,
        SourceType.PLAYLIST,
        COLETDJNZ_CHANNEL_VIDEOS,
        cookies_path=cookies_path,
    )
    assert enqueued_count >= 1

    # Test downloader with limit of 1
    success_count, failure_count = await downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        cookies_path=cookies_path,
        limit=1,
    )

    # Should process exactly 1 item (assuming it succeeds)
    total_processed = success_count + failure_count
    assert total_processed <= 1, (
        f"Expected at most 1 item processed with limit=1, got {total_processed}"
    )

    # If we got a success, verify it
    if success_count > 0:
        downloaded_items = await download_db.get_downloads_by_status(
            DownloadStatus.DOWNLOADED, feed_id=feed_id
        )
        assert len(downloaded_items) == success_count
        # Verify thumbnails for the downloaded subset
        for dl in downloaded_items:
            assert dl.thumbnail_ext == "jpg"
            assert await file_manager.image_exists(feed_id, dl.id, "jpg")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_queued_no_queued_items(
    downloader: Downloader,
    download_db: DownloadDatabase,
    cookies_path: Path | None,
):
    """Tests that downloader handles feeds with no queued items gracefully."""
    feed_id = "test_no_queued"
    feed_config = SAMPLE_FEED_CONFIG

    # Don't run enqueuer, so no queued items exist

    success_count, failure_count = await downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        cookies_path=cookies_path,
        limit=-1,
    )

    # Should return 0 for both counts
    assert success_count == 0
    assert failure_count == 0

    # Verify no downloads in database for this feed
    all_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    assert len(all_downloads) == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_queued_handles_invalid_urls(
    downloader: Downloader,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    cookies_path: Path | None,
):
    """Tests that downloader properly handles downloads with invalid URLs."""
    feed_id = "test_invalid_urls"
    feed_config = INVALID_FEED_CONFIG

    # Create the feed first
    assert feed_config.url is not None, (
        "Scheduled feed configuration must define 'url'."
    )
    feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.UNKNOWN,
        source_url=feed_config.url,
        resolved_url=feed_config.url,
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title=f"Test Feed {feed_id}",
    )
    await feed_db.upsert_feed(feed)

    # Manually insert an invalid download to test error handling
    published_time = datetime.now(UTC)
    invalid_download = Download(
        feed_id=feed_id,
        id="invalid_video_id",
        source_url=INVALID_VIDEO_URL,
        title="Invalid Video",
        published=published_time,
        ext="mp4",
        mime_type="video/mp4",
        filesize=12345,
        duration=10,
        status=DownloadStatus.QUEUED,
        retries=0,
        discovered_at=published_time,
        updated_at=published_time,
    )
    await download_db.upsert_download(invalid_download)

    # Verify it's queued
    queued_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    assert len(queued_downloads) == 1

    # Test downloader
    success_count, failure_count = await downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        cookies_path=cookies_path,
        limit=-1,
    )

    # Should have 1 failure
    assert success_count == 0
    assert failure_count == 1

    # Verify the download had its retry count bumped
    updated_download = await download_db.get_download_by_id(feed_id, "invalid_video_id")
    assert updated_download.retries > 0
    assert updated_download.last_error is not None
    # No thumbnail should be recorded on failure
    assert updated_download.thumbnail_ext is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_queued_retry_logic_max_errors(
    downloader: Downloader,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    cookies_path: Path | None,
):
    """Tests that downloads transition to ERROR status after max retries."""
    feed_id = "test_max_errors"
    feed_config = FeedConfig(
        url=INVALID_VIDEO_URL,
        yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
        schedule=TEST_CRON_SCHEDULE,
        keep_last=None,
        since=None,
        max_errors=1,  # Set to 1 for quick testing
    )

    # Create the feed first
    assert feed_config.url is not None, (
        "Scheduled feed configuration must define 'url'."
    )
    feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.UNKNOWN,
        source_url=feed_config.url,
        resolved_url=feed_config.url,
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title=f"Test Feed {feed_id}",
    )
    await feed_db.upsert_feed(feed)

    # Insert an invalid download
    published_time = datetime.now(UTC)
    invalid_download = Download(
        feed_id=feed_id,
        id="will_error_out",
        source_url=INVALID_VIDEO_URL,
        title="Will Error Out",
        published=published_time,
        ext="mp4",
        mime_type="video/mp4",
        filesize=12345,
        duration=10,
        status=DownloadStatus.QUEUED,
        retries=0,
        discovered_at=published_time,
        updated_at=published_time,
    )
    await download_db.upsert_download(invalid_download)

    # Run downloader - first failure, should bump retry
    success_count, failure_count = await downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        cookies_path=cookies_path,
        limit=-1,
    )
    assert success_count == 0
    assert failure_count == 1

    # Check download status - should now be ERROR since max_errors=1
    updated_download = await download_db.get_download_by_id(feed_id, "will_error_out")
    assert updated_download.retries == 1
    assert updated_download.status == DownloadStatus.ERROR
    assert updated_download.last_error is not None
    # No thumbnail should be recorded when download fails
    assert updated_download.thumbnail_ext is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_queued_mixed_success_and_failure(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    downloader: Downloader,
    download_db: DownloadDatabase,
    file_manager: FileManager,
    cookies_path: Path | None,
):
    """Tests handling of mixed successful and failed downloads."""
    feed_id = "test_mixed_results"
    feed_config = SAMPLE_FEED_CONFIG

    # First, enqueue a valid download
    queued_count = await enqueue_test_items(
        enqueuer,
        feed_db,
        feed_id,
        feed_config,
        SourceType.SINGLE_VIDEO,
        cookies_path=cookies_path,
    )
    assert queued_count >= 1

    # Then manually add an invalid download to the same feed
    published_time = datetime.now(UTC)
    invalid_download = Download(
        feed_id=feed_id,
        id="invalid_mixed_test",
        source_url=INVALID_VIDEO_URL,
        title="Invalid Mixed Test",
        published=published_time,
        ext="mp4",
        mime_type="video/mp4",
        filesize=12345,
        duration=10,
        status=DownloadStatus.QUEUED,
        retries=0,
        discovered_at=published_time,
        updated_at=published_time,
    )
    await download_db.upsert_download(invalid_download)

    # Verify we have at least 2 queued items
    queued_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    assert len(queued_downloads) >= 2

    # Run downloader
    success_count, failure_count = await downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        cookies_path=cookies_path,
        limit=-1,
    )

    # Should have both successes and failures
    assert success_count >= 1, "Expected at least 1 success"
    assert failure_count >= 1, "Expected at least 1 failure"
    assert success_count + failure_count == len(queued_downloads)

    # Verify successful downloads
    downloaded_items = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    assert len(downloaded_items) == success_count

    for downloaded_item in downloaded_items:
        assert await file_manager.download_exists(
            feed_id, downloaded_item.id, downloaded_item.ext
        )
        # Verify thumbnails saved and recorded for successful items
        assert downloaded_item.thumbnail_ext == "jpg"
        assert await file_manager.image_exists(feed_id, downloaded_item.id, "jpg")

    # Verify failed download had retry bumped
    failed_download = await download_db.get_download_by_id(
        feed_id, "invalid_mixed_test"
    )
    assert failed_download.retries > 0
    assert failed_download.last_error is not None
    # Failed item should not have a recorded thumbnail
    assert failed_download.thumbnail_ext is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_queued_file_properties(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    downloader: Downloader,
    download_db: DownloadDatabase,
    file_manager: FileManager,
    cookies_path: Path | None,
):
    """Tests that downloaded files have correct properties and metadata."""
    feed_id = "test_file_properties"
    feed_config = SAMPLE_FEED_CONFIG

    # Enqueue and download
    await enqueue_test_items(
        enqueuer,
        feed_db,
        feed_id,
        feed_config,
        SourceType.SINGLE_VIDEO,
        cookies_path=cookies_path,
    )

    success_count, _ = await downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        cookies_path=cookies_path,
        limit=1,  # Just test one to keep it fast
    )

    assert success_count >= 1

    # Get the downloaded item
    downloads = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    download = downloads[0]

    # File should exist
    assert await file_manager.download_exists(feed_id, download.id, download.ext)

    # Thumbnail should exist and be recorded
    assert download.thumbnail_ext == "jpg"
    assert await file_manager.image_exists(feed_id, download.id, "jpg")

    # File should be readable - get_download_stream returns an async iterator
    stream_data = b""
    download_stream = await file_manager.get_download_stream(
        feed_id, download.id, download.ext
    )
    async for chunk in download_stream:
        stream_data += chunk
        break  # Just check that we can read at least one chunk
    assert len(stream_data) > 0

    # Database should have correct metadata
    assert download.ext in ["mp4", "webm", "mkv"]  # Common video formats
    assert download.filesize is not None and download.filesize > 0
    assert download.retries == 0
    assert download.last_error is None

    # File size in database should match actual file size
    feed_data_dir = Path(file_manager._paths.base_data_dir) / feed_id
    actual_file = feed_data_dir / f"{download.id}.{download.ext}"
    actual_size = actual_file.stat().st_size
    assert download.filesize == actual_size


@pytest.mark.integration
@pytest.mark.asyncio
async def test_filesize_metadata_flow(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    downloader: Downloader,
    download_db: DownloadDatabase,
    file_manager: FileManager,
    cookies_path: Path | None,
):
    """Tests that filesize metadata flows correctly from enqueue through download.

    Verifies that:
    1. Initial metadata may have approximate filesize (not 0)
    2. After download, filesize is updated to exact file size
    3. Database filesize matches actual downloaded file size
    """
    feed_id = "test_filesize_flow"
    feed_config = SAMPLE_FEED_CONFIG

    # Enqueue items to get initial metadata
    queued_count = await enqueue_test_items(
        enqueuer,
        feed_db,
        feed_id,
        feed_config,
        SourceType.SINGLE_VIDEO,
        cookies_path=cookies_path,
    )
    assert queued_count >= 1, "Should have queued at least one item"

    # Get the queued item and check initial filesize
    queued_items = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    queued_item = queued_items[0]
    initial_filesize = queued_item.filesize

    # Initial filesize should be either 0 (no estimate) or > 0 (has estimate)
    assert initial_filesize >= 0, "Initial filesize should be non-negative"

    # Download the item
    success_count, _ = await downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        cookies_path=cookies_path,
        limit=1,
    )
    assert success_count >= 1, "Should have downloaded at least one item"

    # Get the downloaded item
    downloaded_items = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    downloaded_item = downloaded_items[0]
    final_filesize = downloaded_item.filesize

    # Final filesize should always be > 0 and represent actual file size
    assert final_filesize > 0, "Downloaded item should have actual filesize > 0"

    # Verify final filesize matches actual file
    expected_filename = f"{downloaded_item.id}.{downloaded_item.ext}"
    feed_data_dir = Path(file_manager._paths.base_data_dir) / feed_id
    actual_file = feed_data_dir / expected_filename
    actual_size = actual_file.stat().st_size

    assert final_filesize == actual_size, (
        "Database filesize should match actual file size"
    )

    # Verify thumbnail recorded and exists for the downloaded item
    assert downloaded_item.thumbnail_ext == "jpg"
    assert await file_manager.image_exists(feed_id, downloaded_item.id, "jpg")

    # If initial estimate was available and reasonable, it should be in the ballpark
    if initial_filesize > 0:
        # Allow for some variance between estimate and actual (up to 50% difference)
        size_ratio = min(initial_filesize, final_filesize) / max(
            initial_filesize, final_filesize
        )
        assert size_ratio > 0.5, (
            f"Initial estimate ({initial_filesize}) should be reasonably close to actual ({final_filesize})"
        )
