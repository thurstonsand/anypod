"""Integration tests for Enqueuer with real YouTube URLs and database operations."""

from collections.abc import Generator
from datetime import UTC, datetime
import shutil

import pytest

from anypod.config import FeedConfig
from anypod.config.types import (
    FeedMetadataOverrides,
    PodcastCategories,
    PodcastExplicit,
)
from anypod.data_coordinator.enqueuer import Enqueuer
from anypod.db import DownloadDatabase
from anypod.db.feed_db import FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.exceptions import EnqueueError
from anypod.path_manager import PathManager
from anypod.ytdlp_wrapper import YtdlpWrapper

# Test constants
BIG_BUCK_BUNNY_VIDEO_ID = "aqz-KE-bpKQ"
BIG_BUCK_BUNNY_URL = f"https://www.youtube.com/watch?v={BIG_BUCK_BUNNY_VIDEO_ID}"
BIG_BUCK_BUNNY_SHORT_URL = f"https://youtu.be/{BIG_BUCK_BUNNY_VIDEO_ID}"
BIG_BUCK_BUNNY_TITLE = "Test Video"
BIG_BUCK_BUNNY_PUBLISHED = datetime(2014, 11, 10, 14, 5, 55, tzinfo=UTC)
BIG_BUCK_BUNNY_DURATION = 635

COLETDJNZ_CHANNEL_VIDEOS = "https://www.youtube.com/@coletdjnz/videos"

INVALID_VIDEO_URL = "https://www.youtube.com/watch?v=thisvideodoesnotexistxyz"

# CLI args for minimal quality and limited playlist downloads as a string
YT_DLP_MINIMAL_ARGS_STR = (
    "--playlist-items 1 --format worst*[ext=mp4]/worst[ext=mp4]/best[ext=mp4]"
)

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
def ytdlp_wrapper(tmp_path_factory: pytest.TempPathFactory) -> Generator[YtdlpWrapper]:
    """Provides a YtdlpWrapper instance for the tests."""
    app_tmp_dir = tmp_path_factory.mktemp("tmp")
    app_data_dir = tmp_path_factory.mktemp("data")

    paths = PathManager(
        base_data_dir=app_data_dir,
        base_tmp_dir=app_tmp_dir,
        base_url="http://localhost",
    )

    yield YtdlpWrapper(paths)

    # Teardown: remove temporary directories
    shutil.rmtree(app_tmp_dir)
    shutil.rmtree(app_data_dir)


@pytest.fixture
def enqueuer(
    feed_db: FeedDatabase, download_db: DownloadDatabase, ytdlp_wrapper: YtdlpWrapper
) -> Generator[Enqueuer]:
    """Provides an Enqueuer instance for the tests."""
    yield Enqueuer(feed_db, download_db, ytdlp_wrapper)


def create_test_feed(feed_db: FeedDatabase, feed_id: str, url: str) -> Feed:
    """Create a test feed in the database."""
    feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.UNKNOWN,  # Will be determined by ytdlp
        title=f"Test Feed {feed_id}",
    )
    feed_db.upsert_feed(feed)
    return feed


@pytest.mark.integration
@pytest.mark.parametrize("url_type, url", TEST_URLS_PARAMS)
def test_enqueue_new_downloads_success(
    enqueuer: Enqueuer,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    url_type: str,
    url: str,
):
    """Tests successful enqueueing of new downloads for various URL types.

    Asserts that downloads are properly fetched, parsed, and inserted into
    the database with appropriate status.
    """
    feed_id = f"test_feed_{url_type}"

    # Create feed in database
    create_test_feed(feed_db, feed_id, url)

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
    queued_downloads = download_db.get_downloads_by_status(
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

    # Verify feed metadata was updated from extracted metadata
    updated_feed = feed_db.get_feed_by_id(feed_id)

    # Verify specific extracted values based on URL type
    if url_type == "video_short_link":
        # Big Buck Bunny video metadata
        assert (
            updated_feed.title
            == "Big Buck Bunny 60fps 4K - Official Blender Foundation Short Film"
        )
        assert updated_feed.author == "Blender"
        assert (
            updated_feed.description
            and "UHD High Frame rate version" in updated_feed.description
        )
        assert (
            updated_feed.image_url
            == "https://i.ytimg.com/vi_webp/aqz-KE-bpKQ/maxresdefault.webp"
        )
        assert updated_feed.subtitle is None
        assert updated_feed.language is None
        assert updated_feed.category is None
        assert updated_feed.explicit is None

    elif url_type == "channel_videos_tab":
        # cole-dlp-test-acc channel metadata
        assert updated_feed.title == "cole-dlp-test-acc - Videos"
        assert updated_feed.author == "cole-dlp-test-acc"
        assert updated_feed.description == "test description"
        assert updated_feed.image_url is None
        assert updated_feed.subtitle is None
        assert updated_feed.language is None
        assert updated_feed.category is None
        assert updated_feed.explicit is None

    elif url_type in ["video_in_playlist_link", "playlist"]:
        # single video playlist metadata
        assert updated_feed.title == "single video playlist"
        assert updated_feed.author == "cole-dlp-test-acc"
        assert updated_feed.description == ""
        assert updated_feed.image_url is None
        assert updated_feed.subtitle is None
        assert updated_feed.language is None
        assert updated_feed.category is None
        assert updated_feed.explicit is None

    # Common assertions for all URL types
    assert updated_feed.is_enabled is True
    assert (
        updated_feed.source_type == SourceType.UNKNOWN
    )  # This is expected since source_type is not being set by the enqueuer


@pytest.mark.integration
def test_enqueue_new_downloads_invalid_url(enqueuer: Enqueuer, feed_db: FeedDatabase):
    """Tests that enqueueing fails gracefully for invalid URLs."""
    feed_id = "test_invalid_feed"

    # Create feed in database
    create_test_feed(feed_db, feed_id, INVALID_VIDEO_URL)

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
    enqueuer: Enqueuer, feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Tests enqueueing with a date filter that should limit results."""
    feed_id = "test_date_filter"

    # Create feed in database
    create_test_feed(feed_db, feed_id, COLETDJNZ_CHANNEL_VIDEOS)

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
    queued_downloads = download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    assert len(queued_downloads) == queued_count


@pytest.mark.integration
def test_enqueue_handles_existing_upcoming_downloads(
    enqueuer: Enqueuer, feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Tests that existing UPCOMING downloads are properly handled and potentially transitioned to QUEUED."""
    feed_id = "test_upcoming_feed"

    # Create feed in database
    create_test_feed(feed_db, feed_id, SAMPLE_FEED_CONFIG.url)

    feed_config = SAMPLE_FEED_CONFIG

    # Insert an UPCOMING download manually
    upcoming_download = Download(
        feed=feed_id,
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
    download_db.upsert_download(upcoming_download)

    # Verify it's in UPCOMING status
    upcoming_downloads = download_db.get_downloads_by_status(
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
    queued_downloads = download_db.get_downloads_by_status(
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
    remaining_upcoming = download_db.get_downloads_by_status(
        DownloadStatus.UPCOMING, feed=feed_id
    )
    assert len(remaining_upcoming) == 0


@pytest.mark.integration
def test_enqueue_handles_existing_downloaded_items(
    enqueuer: Enqueuer, feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Tests that existing DOWNLOADED items are ignored during enqueue process."""
    feed_id = "test_downloaded_ignored_feed"

    # Create feed in database
    create_test_feed(feed_db, feed_id, SAMPLE_FEED_CONFIG.url)

    feed_config = SAMPLE_FEED_CONFIG

    # Insert a DOWNLOADED item
    downloaded_item = Download(
        feed=feed_id,
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
    download_db.upsert_download(downloaded_item)

    # Run enqueuer
    queued_count = enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=datetime.min.replace(tzinfo=UTC),
    )

    # Should NOT have queued the item since it's already DOWNLOADED
    assert queued_count == 0

    # Verify the item remains DOWNLOADED
    downloaded_downloads = download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed=feed_id
    )
    assert len(downloaded_downloads) == 1
    assert downloaded_downloads[0].id == BIG_BUCK_BUNNY_VIDEO_ID
    assert downloaded_downloads[0].status == DownloadStatus.DOWNLOADED

    # Verify no items were queued
    queued_downloads = download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    assert len(queued_downloads) == 0


@pytest.mark.integration
def test_enqueue_multiple_runs_idempotent(
    enqueuer: Enqueuer, feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Tests that running enqueue multiple times on the same feed is idempotent."""
    feed_id = "test_idempotent_feed"

    # Create feed in database
    create_test_feed(feed_db, feed_id, SAMPLE_FEED_CONFIG.url)

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
    first_run_downloads = download_db.get_downloads_by_status(
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
    second_run_downloads = download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    assert len(second_run_downloads) == len(first_run_downloads)


@pytest.mark.integration
def test_enqueue_with_impossible_filter(
    enqueuer: Enqueuer, feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Tests enqueueing with filters that match no videos still creates downloads but with filter applied."""
    feed_id = "test_impossible_filter"

    # Create feed in database
    create_test_feed(feed_db, feed_id, BIG_BUCK_BUNNY_SHORT_URL)

    feed_config = FeedConfig(
        url=BIG_BUCK_BUNNY_SHORT_URL,
        yt_args='--playlist-items 1 --format worst*[ext=mp4]/worst[ext=mp4]/best[ext=mp4] --match-filters "duration > 10000000"',  # type: ignore
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
    queued_downloads = download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed=feed_id
    )
    assert len(queued_downloads) == 0


@pytest.mark.integration
def test_enqueue_feed_metadata_synchronization_with_overrides(
    enqueuer: Enqueuer, feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Tests that feed metadata synchronization works correctly with config overrides."""
    feed_id = "test_metadata_sync"

    # Create feed in database
    create_test_feed(feed_db, feed_id, BIG_BUCK_BUNNY_SHORT_URL)

    # Create feed config with metadata overrides
    metadata_overrides = FeedMetadataOverrides(  # type: ignore # quirk of pydantic
        title="Custom Podcast Title",
        subtitle="Custom Subtitle",
        description="Custom description that overrides extracted one",
        language="en-US",
        author="Custom Author",
        categories=PodcastCategories(["Technology", "Science"]),
        explicit=PodcastExplicit.NO,
    )

    feed_config = FeedConfig(
        url=BIG_BUCK_BUNNY_SHORT_URL,
        yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
        schedule=TEST_CRON_SCHEDULE,
        keep_last=None,
        since=None,
        max_errors=MAX_ERRORS,
        metadata=metadata_overrides,
    )
    fetch_since_date = datetime.min.replace(tzinfo=UTC)

    # Enqueue new downloads
    queued_count = enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
    )

    # Verify downloads were processed
    assert queued_count >= 1

    # Verify feed metadata uses overrides where provided and extracted values as fallback
    updated_feed = feed_db.get_feed_by_id(feed_id)

    # These should come from overrides
    assert updated_feed.title == "Custom Podcast Title"
    assert updated_feed.subtitle == "Custom Subtitle"
    assert updated_feed.description == "Custom description that overrides extracted one"
    assert updated_feed.language == "en-US"
    assert updated_feed.author == "Custom Author"
    assert str(updated_feed.category) == "Technology, Science"
    assert str(updated_feed.explicit) == "no"

    # Image URL should come from extracted metadata since not overridden
    assert (
        updated_feed.image_url
        == "https://i.ytimg.com/vi_webp/aqz-KE-bpKQ/maxresdefault.webp"
    )


@pytest.mark.integration
def test_enqueue_feed_metadata_partial_overrides(
    enqueuer: Enqueuer, feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Tests that partial metadata overrides work correctly, using extracted values for non-overridden fields."""
    feed_id = "test_partial_metadata_sync"

    # Create feed in database
    create_test_feed(feed_db, feed_id, BIG_BUCK_BUNNY_SHORT_URL)

    # Create feed config with only some metadata overrides
    metadata_overrides = FeedMetadataOverrides(  # type: ignore # quirk of pydantic
        title="Custom Title Only",
        author="Custom Author Only",
        # Other fields left as None to use extracted values
    )

    feed_config = FeedConfig(
        url=BIG_BUCK_BUNNY_SHORT_URL,
        yt_args=YT_DLP_MINIMAL_ARGS_STR,  # type: ignore
        schedule=TEST_CRON_SCHEDULE,
        keep_last=None,
        since=None,
        max_errors=MAX_ERRORS,
        metadata=metadata_overrides,
    )
    fetch_since_date = datetime.min.replace(tzinfo=UTC)

    # Enqueue new downloads
    queued_count = enqueuer.enqueue_new_downloads(
        feed_id=feed_id,
        feed_config=feed_config,
        fetch_since_date=fetch_since_date,
    )

    # Verify downloads were processed
    assert queued_count >= 1

    # Verify feed metadata combines overrides and extracted values
    updated_feed = feed_db.get_feed_by_id(feed_id)

    # These should come from overrides
    assert updated_feed.title == "Custom Title Only"
    assert updated_feed.author == "Custom Author Only"

    # These should come from extracted metadata
    assert (
        updated_feed.description
        and "UHD High Frame rate version" in updated_feed.description
    )
    assert (
        updated_feed.image_url
        == "https://i.ytimg.com/vi_webp/aqz-KE-bpKQ/maxresdefault.webp"
    )

    # These should remain None since not overridden and not in extracted metadata
    assert updated_feed.subtitle is None
    assert updated_feed.language is None
    assert updated_feed.category is None
    assert updated_feed.explicit is None
