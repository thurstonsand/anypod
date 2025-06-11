# pyright: reportPrivateUsage=false

"""Integration tests for Downloader with real YouTube URLs and file operations."""

from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
import shutil

import pytest

from anypod.config import FeedConfig
from anypod.data_coordinator.downloader import Downloader
from anypod.data_coordinator.enqueuer import Enqueuer
from anypod.db import DownloadDatabase
from anypod.db.feed_db import FeedDatabase
from anypod.db.types import Download, DownloadStatus
from anypod.file_manager import FileManager
from anypod.path_manager import PathManager
from anypod.ytdlp_wrapper import YtdlpWrapper

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
INVALID_VIDEO_URL = "https://www.youtube.com/watch?v=thisvideodoesnotexistxyz"

# CLI args for minimal quality and limited playlist downloads as a string
YT_DLP_MINIMAL_ARGS_STR = (
    "--playlist-items 1 --format worst*[ext=mp4]/worst[ext=mp4]/best[ext=mp4]"
)

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
    keep_last=None,
    since=None,
    max_errors=MAX_ERRORS,
)

INVALID_FEED_CONFIG = FeedConfig(
    url=INVALID_VIDEO_URL,
    yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
    schedule=TEST_CRON_SCHEDULE,
    keep_last=None,
    since=None,
    max_errors=MAX_ERRORS,
)


@pytest.fixture
def shared_dirs(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[tuple[Path, Path]]:
    """Provides shared temporary directories for tests."""
    app_tmp_dir = tmp_path_factory.mktemp("tmp")
    app_data_dir = tmp_path_factory.mktemp("data")

    yield app_tmp_dir, app_data_dir

    # Cleanup
    shutil.rmtree(app_tmp_dir, ignore_errors=True)
    shutil.rmtree(app_data_dir, ignore_errors=True)


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
def file_manager(shared_dirs: tuple[Path, Path]) -> Generator[FileManager]:
    """Provides a FileManager instance with shared data directory."""
    _, app_data_dir = shared_dirs
    app_tmp_dir = shared_dirs[0]
    paths = PathManager(
        base_data_dir=app_data_dir,
        base_tmp_dir=app_tmp_dir,
        base_url="http://localhost",
    )
    file_manager = FileManager(paths)
    yield file_manager


@pytest.fixture
def ytdlp_wrapper(shared_dirs: tuple[Path, Path]) -> Generator[YtdlpWrapper]:
    """Provides a YtdlpWrapper instance with shared directories."""
    app_tmp_dir, app_data_dir = shared_dirs
    paths = PathManager(
        base_data_dir=app_data_dir,
        base_tmp_dir=app_tmp_dir,
        base_url="http://localhost",
    )
    yield YtdlpWrapper(paths)


@pytest.fixture
def enqueuer(
    feed_db: FeedDatabase, download_db: DownloadDatabase, ytdlp_wrapper: YtdlpWrapper
) -> Generator[Enqueuer]:
    """Provides an Enqueuer instance for populating the database."""
    yield Enqueuer(feed_db, download_db, ytdlp_wrapper)


@pytest.fixture
def downloader(
    download_db: DownloadDatabase,
    file_manager: FileManager,
    ytdlp_wrapper: YtdlpWrapper,
) -> Generator[Downloader]:
    """Provides a Downloader instance for the tests."""
    yield Downloader(download_db, file_manager, ytdlp_wrapper)


def enqueue_test_items(
    enqueuer: Enqueuer,
    feed_id: str,
    feed_config: FeedConfig,
    fetch_since_date: datetime | None = None,
) -> int:
    """Helper function to enqueue test items for a feed.

    Args:
        enqueuer: The Enqueuer instance to use.
        feed_id: The feed identifier.
        feed_config: The feed configuration.
        fetch_since_date: Optional date filter, defaults to datetime.min.

    Returns:
        Number of items queued.
    """
    if fetch_since_date is None:
        fetch_since_date = datetime.min.replace(tzinfo=UTC)

    return enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
    )


@pytest.mark.integration
def test_download_queued_single_video_success(
    enqueuer: Enqueuer,
    downloader: Downloader,
    download_db: DownloadDatabase,
    file_manager: FileManager,
):
    """Tests successful download of a single queued video."""
    feed_id = "test_single_video"
    feed_config = SAMPLE_FEED_CONFIG

    # First, use enqueuer to populate database with a real entry
    queued_count = enqueue_test_items(enqueuer, feed_id, feed_config)
    assert queued_count >= 1, "Expected at least 1 item to be queued by enqueuer"

    # Verify item is in QUEUED status
    queued_downloads = download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    assert len(queued_downloads) >= 1
    original_download = queued_downloads[0]
    assert original_download.status == DownloadStatus.QUEUED

    # Now test the downloader
    success_count, failure_count = downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        limit=-1,
    )

    # Verify download results
    assert success_count >= 1, (
        f"Expected at least 1 successful download, got {success_count}"
    )
    assert failure_count == 0, f"Expected 0 failures, got {failure_count}"

    # Verify database was updated
    downloaded_items = download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed=feed_id
    )
    assert len(downloaded_items) >= 1

    # Verify the specific download was updated
    downloaded_item = next(
        (dl for dl in downloaded_items if dl.id == original_download.id), None
    )
    assert downloaded_item is not None
    assert downloaded_item.status == DownloadStatus.DOWNLOADED
    assert downloaded_item.ext  # Should have extension set
    assert (
        downloaded_item.filesize and downloaded_item.filesize > 0
    )  # Should have filesize set
    assert downloaded_item.retries == 0  # Should be reset
    assert downloaded_item.last_error is None  # Should be cleared

    # Verify file was actually downloaded
    assert file_manager.download_exists(
        feed_id, downloaded_item.id, downloaded_item.ext
    )

    # Verify no more queued items for this feed
    remaining_queued = download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    assert len(remaining_queued) == 0


@pytest.mark.integration
def test_download_queued_multiple_videos_success(
    enqueuer: Enqueuer,
    downloader: Downloader,
    download_db: DownloadDatabase,
    file_manager: FileManager,
):
    """Tests successful download of multiple queued videos from a channel."""
    feed_id = "test_multiple_videos"
    feed_config = CHANNEL_FEED_CONFIG

    # Use enqueuer to populate database with multiple entries
    queued_count = enqueue_test_items(enqueuer, feed_id, feed_config)
    assert queued_count >= 1, "Expected at least 1 item to be queued by enqueuer"

    # Get original queued items
    original_queued = download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    original_count = len(original_queued)
    assert original_count >= 1

    # Test the downloader
    success_count, failure_count = downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        limit=-1,
    )

    # Verify results
    assert success_count + failure_count == original_count
    assert success_count >= 1

    # Verify database updates
    downloaded_items = download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed=feed_id
    )
    assert len(downloaded_items) == success_count

    # Verify files were downloaded
    for downloaded_item in downloaded_items:
        assert file_manager.download_exists(
            feed_id, downloaded_item.id, downloaded_item.ext
        )
        assert downloaded_item.filesize is not None and downloaded_item.filesize > 0


@pytest.mark.integration
def test_download_queued_with_limit(
    enqueuer: Enqueuer,
    downloader: Downloader,
    download_db: DownloadDatabase,
):
    """Tests that the limit parameter properly restricts the number of downloads processed."""
    feed_id = "test_limit"
    feed_config = CHANNEL_FEED_CONFIG

    # Use enqueuer to populate database
    queued_count = enqueue_test_items(enqueuer, feed_id, feed_config)
    assert queued_count >= 1

    # Test downloader with limit of 1
    success_count, failure_count = downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        limit=1,
    )

    # Should process exactly 1 item (assuming it succeeds)
    total_processed = success_count + failure_count
    assert total_processed <= 1, (
        f"Expected at most 1 item processed with limit=1, got {total_processed}"
    )

    # If we got a success, verify it
    if success_count > 0:
        downloaded_items = download_db.get_downloads_by_status(
            DownloadStatus.DOWNLOADED, feed=feed_id
        )
        assert len(downloaded_items) == success_count


@pytest.mark.integration
def test_download_queued_no_queued_items(
    downloader: Downloader,
    download_db: DownloadDatabase,
):
    """Tests that downloader handles feeds with no queued items gracefully."""
    feed_id = "test_no_queued"
    feed_config = SAMPLE_FEED_CONFIG

    # Don't run enqueuer, so no queued items exist

    success_count, failure_count = downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        limit=-1,
    )

    # Should return 0 for both counts
    assert success_count == 0
    assert failure_count == 0

    # Verify no downloads in database for this feed
    all_downloads = download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed=feed_id
    )
    assert len(all_downloads) == 0


@pytest.mark.integration
def test_download_queued_handles_invalid_urls(
    downloader: Downloader,
    download_db: DownloadDatabase,
):
    """Tests that downloader properly handles downloads with invalid URLs."""
    feed_id = "test_invalid_urls"
    feed_config = INVALID_FEED_CONFIG

    # Manually insert an invalid download to test error handling
    published_time = datetime.now(UTC)
    invalid_download = Download(
        feed=feed_id,
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
    download_db.upsert_download(invalid_download)

    # Verify it's queued
    queued_downloads = download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    assert len(queued_downloads) == 1

    # Test downloader
    success_count, failure_count = downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        limit=-1,
    )

    # Should have 1 failure
    assert success_count == 0
    assert failure_count == 1

    # Verify the download had its retry count bumped
    updated_download = download_db.get_download_by_id(feed_id, "invalid_video_id")
    assert updated_download.retries > 0
    assert updated_download.last_error is not None


@pytest.mark.integration
def test_download_queued_retry_logic_max_errors(
    downloader: Downloader,
    download_db: DownloadDatabase,
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

    # Insert an invalid download
    published_time = datetime.now(UTC)
    invalid_download = Download(
        feed=feed_id,
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
    download_db.upsert_download(invalid_download)

    # Run downloader - first failure, should bump retry
    success_count, failure_count = downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        limit=-1,
    )
    assert success_count == 0
    assert failure_count == 1

    # Check download status - should now be ERROR since max_errors=1
    updated_download = download_db.get_download_by_id(feed_id, "will_error_out")
    assert updated_download.retries == 1
    assert updated_download.status == DownloadStatus.ERROR
    assert updated_download.last_error is not None


@pytest.mark.integration
def test_download_queued_mixed_success_and_failure(
    enqueuer: Enqueuer,
    downloader: Downloader,
    download_db: DownloadDatabase,
    file_manager: FileManager,
):
    """Tests handling of mixed successful and failed downloads."""
    feed_id = "test_mixed_results"
    feed_config = SAMPLE_FEED_CONFIG

    # First, enqueue a valid download
    queued_count = enqueue_test_items(enqueuer, feed_id, feed_config)
    assert queued_count >= 1

    # Then manually add an invalid download to the same feed
    published_time = datetime.now(UTC)
    invalid_download = Download(
        feed=feed_id,
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
    download_db.upsert_download(invalid_download)

    # Verify we have at least 2 queued items
    queued_downloads = download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    assert len(queued_downloads) >= 2

    # Run downloader
    success_count, failure_count = downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        limit=-1,
    )

    # Should have both successes and failures
    assert success_count >= 1, "Expected at least 1 success"
    assert failure_count >= 1, "Expected at least 1 failure"
    assert success_count + failure_count == len(queued_downloads)

    # Verify successful downloads
    downloaded_items = download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed=feed_id
    )
    assert len(downloaded_items) == success_count

    for downloaded_item in downloaded_items:
        assert file_manager.download_exists(
            feed_id, downloaded_item.id, downloaded_item.ext
        )

    # Verify failed download had retry bumped
    failed_download = download_db.get_download_by_id(feed_id, "invalid_mixed_test")
    assert failed_download.retries > 0
    assert failed_download.last_error is not None


@pytest.mark.integration
def test_download_queued_file_properties(
    enqueuer: Enqueuer,
    downloader: Downloader,
    download_db: DownloadDatabase,
    file_manager: FileManager,
):
    """Tests that downloaded files have correct properties and metadata."""
    feed_id = "test_file_properties"
    feed_config = SAMPLE_FEED_CONFIG

    # Enqueue and download
    enqueue_test_items(enqueuer, feed_id, feed_config)

    success_count, _ = downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        limit=1,  # Just test one to keep it fast
    )

    assert success_count >= 1

    # Get the downloaded item
    downloaded_items = download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed=feed_id
    )
    downloaded_item = downloaded_items[0]

    # File should exist
    assert file_manager.download_exists(
        feed_id, downloaded_item.id, downloaded_item.ext
    )

    # File should be readable
    stream = file_manager.get_download_stream(
        feed_id, downloaded_item.id, downloaded_item.ext
    )
    assert stream.readable()
    stream.close()

    # Database should have correct metadata
    assert downloaded_item.ext in ["mp4", "webm", "mkv"]  # Common video formats
    assert downloaded_item.filesize is not None and downloaded_item.filesize > 0
    assert downloaded_item.retries == 0
    assert downloaded_item.last_error is None

    # File size in database should match actual file size
    feed_data_dir = Path(file_manager._paths.base_data_dir) / feed_id
    actual_file = feed_data_dir / f"{downloaded_item.id}.{downloaded_item.ext}"
    actual_size = actual_file.stat().st_size
    assert downloaded_item.filesize == actual_size


@pytest.mark.integration
def test_filesize_metadata_flow(
    enqueuer: Enqueuer,
    downloader: Downloader,
    download_db: DownloadDatabase,
    file_manager: FileManager,
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
    queued_count = enqueue_test_items(enqueuer, feed_id, feed_config)
    assert queued_count >= 1, "Should have queued at least one item"

    # Get the queued item and check initial filesize
    queued_items = download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    queued_item = queued_items[0]
    initial_filesize = queued_item.filesize

    # Initial filesize should be either 0 (no estimate) or > 0 (has estimate)
    assert initial_filesize >= 0, "Initial filesize should be non-negative"

    # Download the item
    success_count, _ = downloader.download_queued(
        feed_id=feed_id,
        feed_config=feed_config,
        limit=1,
    )
    assert success_count >= 1, "Should have downloaded at least one item"

    # Get the downloaded item
    downloaded_items = download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed=feed_id
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

    # If initial estimate was available and reasonable, it should be in the ballpark
    if initial_filesize > 0:
        # Allow for some variance between estimate and actual (up to 50% difference)
        size_ratio = min(initial_filesize, final_filesize) / max(
            initial_filesize, final_filesize
        )
        assert size_ratio > 0.5, (
            f"Initial estimate ({initial_filesize}) should be reasonably close to actual ({final_filesize})"
        )
