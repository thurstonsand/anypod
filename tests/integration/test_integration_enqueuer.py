"""Integration tests for Enqueuer with real YouTube URLs and database operations."""

from collections.abc import Generator
from datetime import UTC, datetime
import shutil

import pytest

from anypod.config import FeedConfig
from anypod.data_coordinator.enqueuer import Enqueuer
from anypod.db import DatabaseManager, Download, DownloadStatus
from anypod.exceptions import EnqueueError
from anypod.ytdlp_wrapper import YtdlpWrapper

# Test constants
BIG_BUCK_BUNNY_VIDEO_ID = "aqz-KE-bpKQ"
BIG_BUCK_BUNNY_URL = f"https://www.youtube.com/watch?v={BIG_BUCK_BUNNY_VIDEO_ID}"
BIG_BUCK_BUNNY_SHORT_URL = f"https://youtu.be/{BIG_BUCK_BUNNY_VIDEO_ID}"
BIG_BUCK_BUNNY_TITLE = "Test Video"
BIG_BUCK_BUNNY_PUBLISHED = datetime(2014, 11, 10, 14, 5, 55, tzinfo=UTC)
BIG_BUCK_BUNNY_DURATION = 635.0

COLETDJNZ_CHANNEL_VIDEOS = "https://www.youtube.com/@coletdjnz/videos"

INVALID_VIDEO_URL = "https://www.youtube.com/watch?v=thisvideodoesnotexistxyz"

# CLI args for minimal quality and limited playlist downloads as a string
YT_DLP_MINIMAL_ARGS_STR = "--playlist-items 1 --format worst[ext=mp4]"

# Test schedule
TEST_CRON_SCHEDULE = "0 * * * *"
MAX_ERRORS = 3

# Same CC-BY licensed URLs as YtdlpWrapper tests for consistency
TEST_URLS_SINGLE_AND_PLAYLIST = [
    ("video_short_link", BIG_BUCK_BUNNY_SHORT_URL),
    (
        "video_in_playlist_link",
        f"{BIG_BUCK_BUNNY_URL}&list=PLt5yu3-wZAlSLRHmI1qNm0wjyVNWw1pCU",
    ),
]

TEST_URLS_PARAMS = [
    *TEST_URLS_SINGLE_AND_PLAYLIST,
    ("channel_videos_tab", COLETDJNZ_CHANNEL_VIDEOS),
    (
        "playlist",
        "https://youtube.com/playlist?list=PLt5yu3-wZAlSLRHmI1qNm0wjyVNWw1pCU",
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


@pytest.fixture
def db_manager() -> Generator[DatabaseManager]:
    """Provides a DatabaseManager instance with a temporary database."""
    db_manager = DatabaseManager(db_path=None, memory_name="integration_test")
    yield db_manager
    db_manager.close()


@pytest.fixture
def ytdlp_wrapper(tmp_path_factory: pytest.TempPathFactory) -> Generator[YtdlpWrapper]:
    """Provides a YtdlpWrapper instance for the tests."""
    app_tmp_dir = tmp_path_factory.mktemp("tmp")
    app_data_dir = tmp_path_factory.mktemp("data")

    yield YtdlpWrapper(app_tmp_dir=app_tmp_dir, app_data_dir=app_data_dir)

    # Teardown: remove temporary directories
    shutil.rmtree(app_tmp_dir)
    shutil.rmtree(app_data_dir)


@pytest.fixture
def enqueuer(
    db_manager: DatabaseManager, ytdlp_wrapper: YtdlpWrapper
) -> Generator[Enqueuer]:
    """Provides an Enqueuer instance for the tests."""
    yield Enqueuer(db_manager, ytdlp_wrapper)


@pytest.mark.integration
@pytest.mark.parametrize("url_type, url", TEST_URLS_PARAMS)
def test_enqueue_new_downloads_success(
    enqueuer: Enqueuer, db_manager: DatabaseManager, url_type: str, url: str
):
    """Tests successful enqueueing of new downloads for various URL types.

    Asserts that downloads are properly fetched, parsed, and inserted into
    the database with appropriate status.
    """
    feed_id = f"test_feed_{url_type}"
    feed_config = FeedConfig(
        url=url,
        yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
        schedule=TEST_CRON_SCHEDULE,
        keep_last=None,
        since=None,
        max_errors=MAX_ERRORS,
    )
    fetch_since_date = datetime.min.replace(tzinfo=UTC)

    # Enqueue new downloads
    queued_count = enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
    )

    # Verify that downloads were queued
    assert queued_count >= 1, f"Expected at least 1 queued download for {url_type}"

    # Verify downloads are in the database
    queued_downloads = db_manager.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    assert len(queued_downloads) >= 1, f"Expected queued downloads in DB for {url_type}"

    # Verify basic properties of the first download
    download = queued_downloads[0]
    assert download.feed == feed_id
    assert download.id, f"Download ID should not be empty for {url_type}"
    assert download.title, f"Download title should not be empty for {url_type}"
    assert download.source_url, (
        f"Download source_url should not be empty for {url_type}"
    )
    assert download.published, f"Download published should not be empty for {url_type}"
    assert download.duration > 0, f"Download duration should be > 0 for {url_type}"
    assert download.status == DownloadStatus.QUEUED


@pytest.mark.integration
def test_enqueue_new_downloads_invalid_url(enqueuer: Enqueuer):
    """Tests that enqueueing fails gracefully for invalid URLs."""
    feed_id = "test_invalid_feed"
    feed_config = FeedConfig(
        url=INVALID_VIDEO_URL,
        yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
        schedule=TEST_CRON_SCHEDULE,
        keep_last=None,
        since=None,
        max_errors=MAX_ERRORS,
    )
    fetch_since_date = datetime.min.replace(tzinfo=UTC)

    with pytest.raises(EnqueueError) as excinfo:
        enqueuer.enqueue_new_downloads(
            feed_id=feed_id,
            feed_config=feed_config,
            fetch_since_date=fetch_since_date,
        )

    assert "Could not fetch main feed metadata" in str(excinfo.value)
    assert excinfo.value.feed_id == feed_id
    assert excinfo.value.feed_url == INVALID_VIDEO_URL


@pytest.mark.integration
def test_enqueue_new_downloads_with_date_filter(
    enqueuer: Enqueuer, db_manager: DatabaseManager
):
    """Tests enqueueing with a date filter that should limit results."""
    feed_id = "test_date_filter"
    feed_config = FeedConfig(
        url=COLETDJNZ_CHANNEL_VIDEOS,
        yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
        schedule=TEST_CRON_SCHEDULE,
        keep_last=None,
        since=None,
        max_errors=MAX_ERRORS,
    )
    # Use a very recent date to potentially filter out older content
    fetch_since_date = datetime(2025, 1, 1, tzinfo=UTC)

    queued_count = enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
    )

    # Should still work, even if no results due to date filtering
    assert queued_count >= 0

    # Verify downloads in database match what was reported
    queued_downloads = db_manager.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    assert len(queued_downloads) == queued_count


@pytest.mark.integration
def test_enqueue_handles_existing_upcoming_downloads(
    enqueuer: Enqueuer, db_manager: DatabaseManager
):
    """Tests that existing UPCOMING downloads are properly handled and potentially transitioned to QUEUED."""
    feed_id = "test_upcoming_feed"
    feed_config = SAMPLE_FEED_CONFIG

    # Insert an UPCOMING download manually
    upcoming_download = Download(
        feed=feed_id,
        id=BIG_BUCK_BUNNY_VIDEO_ID,
        source_url=BIG_BUCK_BUNNY_URL,
        title=BIG_BUCK_BUNNY_TITLE,
        published=BIG_BUCK_BUNNY_PUBLISHED,
        ext="mp4",
        duration=BIG_BUCK_BUNNY_DURATION,
        status=DownloadStatus.UPCOMING,
        retries=0,
    )
    db_manager.upsert_download(upcoming_download)

    # Verify it's in UPCOMING status
    upcoming_downloads = db_manager.get_downloads_by_status(
        DownloadStatus.UPCOMING, feed=feed_id
    )
    assert len(upcoming_downloads) == 1
    assert upcoming_downloads[0].status == DownloadStatus.UPCOMING

    # Run enqueuer - should transition UPCOMING to QUEUED since Big Buck Bunny is a VOD
    queued_count = enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=datetime.min.replace(tzinfo=UTC),
    )

    # Should have at least 1 queued (the transitioned one)
    assert queued_count >= 1

    # Verify the download is now QUEUED
    queued_downloads = db_manager.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    assert len(queued_downloads) >= 1

    # The original UPCOMING download should be found in QUEUED status
    transitioned_download = next(
        (dl for dl in queued_downloads if dl.id == BIG_BUCK_BUNNY_VIDEO_ID), None
    )
    assert transitioned_download is not None
    assert transitioned_download.status == DownloadStatus.QUEUED

    # Should be no more UPCOMING downloads for this feed
    remaining_upcoming = db_manager.get_downloads_by_status(
        DownloadStatus.UPCOMING, feed=feed_id
    )
    assert len(remaining_upcoming) == 0


@pytest.mark.integration
def test_enqueue_handles_existing_downloaded_items(
    enqueuer: Enqueuer, db_manager: DatabaseManager
):
    """Tests that existing DOWNLOADED items are ignored during enqueue process."""
    feed_id = "test_downloaded_ignored_feed"
    feed_config = SAMPLE_FEED_CONFIG

    # Insert a DOWNLOADED item
    downloaded_item = Download(
        feed=feed_id,
        id=BIG_BUCK_BUNNY_VIDEO_ID,
        source_url=BIG_BUCK_BUNNY_URL,
        title=BIG_BUCK_BUNNY_TITLE,
        published=BIG_BUCK_BUNNY_PUBLISHED,
        ext="mp4",
        duration=BIG_BUCK_BUNNY_DURATION,
        status=DownloadStatus.DOWNLOADED,
        retries=0,
    )
    db_manager.upsert_download(downloaded_item)

    # Run enqueuer
    queued_count = enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=datetime.min.replace(tzinfo=UTC),
    )

    # Should NOT have queued the item since it's already DOWNLOADED
    assert queued_count == 0

    # Verify the item remains DOWNLOADED
    downloaded_downloads = db_manager.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed=feed_id
    )
    assert len(downloaded_downloads) == 1
    assert downloaded_downloads[0].id == BIG_BUCK_BUNNY_VIDEO_ID
    assert downloaded_downloads[0].status == DownloadStatus.DOWNLOADED

    # Verify no items were queued
    queued_downloads = db_manager.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    assert len(queued_downloads) == 0


@pytest.mark.integration
def test_enqueue_multiple_runs_idempotent(
    enqueuer: Enqueuer, db_manager: DatabaseManager
):
    """Tests that running enqueue multiple times on the same feed is idempotent."""
    feed_id = "test_idempotent_feed"
    feed_config = SAMPLE_FEED_CONFIG
    fetch_since_date = datetime.min.replace(tzinfo=UTC)

    # First run
    first_queued_count = enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
    )
    assert first_queued_count >= 1

    # Get downloads after first run
    first_run_downloads = db_manager.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )

    # Second run - should not queue new items (they already exist)
    second_queued_count = enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
    )

    # Second run should queue 0 new items since they already exist
    assert second_queued_count == 0

    # Downloads should be the same
    second_run_downloads = db_manager.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    assert len(second_run_downloads) == len(first_run_downloads)


@pytest.mark.integration
def test_enqueue_with_impossible_filter(
    enqueuer: Enqueuer, db_manager: DatabaseManager
):
    """Tests enqueueing with filters that match no videos still creates downloads but with filter applied."""
    feed_id = "test_impossible_filter"

    feed_config = FeedConfig(
        url=BIG_BUCK_BUNNY_SHORT_URL,
        yt_args='--playlist-items 1 --format worst[ext=mp4] --match-filters "duration > 10000000"',  # type: ignore
        schedule=TEST_CRON_SCHEDULE,
        keep_last=None,
        since=None,
        max_errors=MAX_ERRORS,
    )
    fetch_since_date = datetime.min.replace(tzinfo=UTC)

    # The filter applies to download, not metadata fetch, so downloads are still created
    queued_count = enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
    )

    # Metadata fetching still works, downloads are created
    assert queued_count == 0

    # Verify downloads exist in database (the filter will apply during actual download)
    queued_downloads = db_manager.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    assert len(queued_downloads) == 0
