# pyright: reportPrivateUsage=false

"""Integration tests for StateReconciler startup reconciliation.

Tests focus on full end-to-end reconciliation including database operations,
feed state changes, and retention policy changes.
"""

from datetime import UTC, datetime

import pytest

from anypod.config import FeedConfig
from anypod.config.types import (
    FeedMetadataOverrides,
    PodcastCategories,
    PodcastExplicit,
    PodcastType,
)
from anypod.data_coordinator.pruner import Pruner
from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.ffmpeg import FFmpeg
from anypod.ffprobe import FFProbe
from anypod.file_manager import FileManager
from anypod.image_downloader import ImageDownloader
from anypod.path_manager import PathManager
from anypod.state_reconciler import StateReconciler
from anypod.ytdlp_wrapper import YtdlpWrapper

# Test constants
TEST_CRON_SCHEDULE = "0 * * * *"
BASE_TIME = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

# Real URLs for testing
BIG_BUCK_BUNNY_SHORT_URL = "https://youtu.be/aqz-KE-bpKQ"
COLETDJNZ_CHANNEL_VIDEOS = "https://www.youtube.com/@coletdjnz/videos"


@pytest.fixture
def state_reconciler(
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    ytdlp_wrapper: YtdlpWrapper,
    pruner: Pruner,
    file_manager: FileManager,
    path_manager: PathManager,
    ffprobe: FFProbe,
    ffmpeg: FFmpeg,
) -> StateReconciler:
    """Provides a StateReconciler instance with real dependencies."""
    image_downloader = ImageDownloader(
        path_manager, ytdlp_wrapper, ffprobe=ffprobe, ffmpeg=ffmpeg
    )
    return StateReconciler(
        file_manager,
        image_downloader,
        feed_db,
        download_db,
        ytdlp_wrapper,
        pruner,
    )


def create_test_download(
    feed_id: str,
    download_id: str,
    status: DownloadStatus,
    published: datetime | None = None,
) -> Download:
    """Create a test Download object."""
    if published is None:
        published = datetime.now(UTC)

    return Download(
        feed_id=feed_id,
        id=download_id,
        source_url=f"https://example.com/{download_id}",
        title=f"Test Download {download_id}",
        published=published,
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024000 if status == DownloadStatus.DOWNLOADED else 0,
        duration=120,
        status=status,
        discovered_at=published,
        updated_at=published,
    )


async def write_test_media_file(
    feed_id: str,
    download_id: str,
    ext: str,
    contents: str,
    path_manager: PathManager,
) -> None:
    """Write a test media file to the filesystem."""
    media_path = await path_manager.media_file_path(feed_id, download_id, ext)
    media_path.parent.mkdir(parents=True, exist_ok=True)
    media_path.write_text(contents)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_new_feed_addition(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
    path_manager: PathManager,
) -> None:
    """New feed is properly added to database with all fields."""
    # Setup config
    feed_id = "new_feed"
    config_feeds = {
        feed_id: FeedConfig(
            url=BIG_BUCK_BUNNY_SHORT_URL,
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=50,
            since=datetime(2023, 1, 1, tzinfo=UTC),
            metadata=FeedMetadataOverrides(
                title="New Feed Title",
                subtitle="New Feed Subtitle",
                description="A brand new feed",
                language="en-US",
                author="Test Author",
                author_email=None,
                image_url="https://example.com/image.jpg",
                category=PodcastCategories("Technology > Tech News"),
                podcast_type=PodcastType.EPISODIC,
                explicit=PodcastExplicit.CLEAN,
            ),
        )
    }

    # Execute reconciliation
    ready_feeds = await state_reconciler.reconcile_startup_state(config_feeds)

    # Verify results
    assert ready_feeds == [feed_id]

    # Check database state
    feed_config = config_feeds[feed_id]
    db_feed = await feed_db.get_feed_by_id(feed_id)
    assert db_feed.id == feed_id
    assert db_feed.source_url == feed_config.url
    assert db_feed.source_type == SourceType.SINGLE_VIDEO
    assert db_feed.is_enabled == feed_config.enabled
    assert db_feed.keep_last == feed_config.keep_last
    assert db_feed.since == feed_config.since
    if feed_config.since:
        assert db_feed.last_successful_sync >= feed_config.since

    # Check metadata
    metadata = feed_config.metadata
    assert metadata is not None
    assert db_feed.title == metadata.title
    assert db_feed.subtitle == metadata.subtitle
    assert db_feed.description == metadata.description
    assert db_feed.language == metadata.language
    assert db_feed.author == metadata.author
    assert db_feed.remote_image_url == metadata.image_url
    assert db_feed.category is not None
    assert db_feed.category == metadata.category
    assert db_feed.explicit == metadata.explicit

    # Verify image was downloaded for new feed with image URL override
    assert db_feed.image_ext == "jpg"
    image_path = await path_manager.image_path(feed_id, None, "jpg")
    assert image_path.exists()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reconciliation_is_idempotent(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
) -> None:
    """Running state reconciliation twice with same config is a no-op."""
    # Setup config
    config_feeds = {
        "test_feed": FeedConfig(
            url=BIG_BUCK_BUNNY_SHORT_URL,
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=10,
            since=datetime(2024, 1, 1, tzinfo=UTC),
            metadata=FeedMetadataOverrides(  # type: ignore # this is a quirk of Pydantic
                title="Test Feed",
                language="en",
                category=PodcastCategories("Technology"),
            ),
        )
    }

    # First reconciliation
    ready_feeds1 = await state_reconciler.reconcile_startup_state(config_feeds)
    db_feed1 = await feed_db.get_feed_by_id("test_feed")

    # Second reconciliation with same config
    ready_feeds2 = await state_reconciler.reconcile_startup_state(config_feeds)
    db_feed2 = await feed_db.get_feed_by_id("test_feed")

    # Results should be identical
    assert ready_feeds1 == ready_feeds2 == ["test_feed"]
    assert db_feed1.updated_at == db_feed2.updated_at  # No changes made
    assert db_feed1 == db_feed2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_feed_removal_archives_downloads(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    path_manager: PathManager,
) -> None:
    """Removed feed has all downloads archived and files deleted."""
    # Setup: Create feed with downloads
    feed = Feed(
        id="to_remove",
        title="Feed to Remove",
        is_enabled=True,
        source_type=SourceType.PLAYLIST,
        source_url=COLETDJNZ_CHANNEL_VIDEOS,
        resolved_url=COLETDJNZ_CHANNEL_VIDEOS,
        last_successful_sync=BASE_TIME,
    )
    await feed_db.upsert_feed(feed)

    # Add downloads in various states
    downloads = [
        create_test_download(
            "to_remove", "dl1", DownloadStatus.QUEUED
        ),  # Start as QUEUED to mark as downloaded
        create_test_download("to_remove", "dl2", DownloadStatus.QUEUED),
        create_test_download("to_remove", "dl3", DownloadStatus.ERROR),
    ]

    for dl in downloads:
        await download_db.upsert_download(dl)

    # Create file for downloaded item and mark as downloaded
    await write_test_media_file("to_remove", "dl1", "mp4", "test content", path_manager)
    media_path = await path_manager.media_file_path("to_remove", "dl1", "mp4")

    # Mark first download as downloaded
    await download_db.mark_as_downloaded(feed.id, "dl1", "mp4", 12)

    # Execute reconciliation with empty config (feed removed)
    ready_feeds = await state_reconciler.reconcile_startup_state({})

    # Verify results
    assert ready_feeds == []

    # Check feed is disabled
    db_feed = await feed_db.get_feed_by_id("to_remove")
    assert db_feed.is_enabled is False

    # Check all downloads are archived
    for status in [
        DownloadStatus.DOWNLOADED,
        DownloadStatus.QUEUED,
        DownloadStatus.ERROR,
    ]:
        downloads = await download_db.get_downloads_by_status(
            status, feed_id="to_remove"
        )
        assert len(downloads) == 0

    archived = await download_db.get_downloads_by_status(
        DownloadStatus.ARCHIVED, feed_id="to_remove"
    )
    assert len(archived) == 3

    # Check file was deleted
    assert not media_path.exists()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_feed_enable_disable_transitions(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
) -> None:
    """Feed enable/disable transitions work correctly."""
    # Setup: Create disabled feed with errors
    feed = Feed(
        id="toggle_feed",
        title="Toggle Feed",
        is_enabled=False,
        source_type=SourceType.PLAYLIST,
        source_url=COLETDJNZ_CHANNEL_VIDEOS,
        resolved_url=COLETDJNZ_CHANNEL_VIDEOS,
        last_successful_sync=BASE_TIME,
        consecutive_failures=5,
        last_failed_sync=datetime.now(UTC),
    )
    await feed_db.upsert_feed(feed)

    # Enable the feed
    config_feeds = {
        "toggle_feed": FeedConfig(
            url=COLETDJNZ_CHANNEL_VIDEOS,
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=None,
            since=None,
            yt_args=None,  # type: ignore
            metadata=None,
        )
    }
    ready_feeds = await state_reconciler.reconcile_startup_state(config_feeds)

    # Verify enabled and errors cleared
    assert ready_feeds == ["toggle_feed"]
    db_feed = await feed_db.get_feed_by_id("toggle_feed")
    assert db_feed.is_enabled is True
    assert db_feed.consecutive_failures == 0
    assert db_feed.last_failed_sync is None

    # Disable the feed again
    config_feeds["toggle_feed"].enabled = False
    ready_feeds = await state_reconciler.reconcile_startup_state(config_feeds)

    # Verify disabled but errors not modified
    assert ready_feeds == []
    db_feed = await feed_db.get_feed_by_id("toggle_feed")
    assert db_feed.is_enabled is False
    assert db_feed.consecutive_failures == 0  # Not changed


@pytest.mark.integration
@pytest.mark.asyncio
async def test_url_change_resets_error_state(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
) -> None:
    """Changing feed URL resets error state."""
    # Setup: Feed with errors
    feed = Feed(
        id="url_change",
        title="URL Change Feed",
        is_enabled=True,
        source_type=SourceType.PLAYLIST,
        source_url=COLETDJNZ_CHANNEL_VIDEOS,
        resolved_url=COLETDJNZ_CHANNEL_VIDEOS,
        last_successful_sync=BASE_TIME,
        consecutive_failures=3,
    )
    await feed_db.upsert_feed(feed)

    # Change URL
    config_feeds = {
        "url_change": FeedConfig(
            url=BIG_BUCK_BUNNY_SHORT_URL,
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=None,
            since=None,
            yt_args=None,  # type: ignore
            metadata=None,
        )
    }
    await state_reconciler.reconcile_startup_state(config_feeds)

    # Verify URL changed and errors cleared
    db_feed = await feed_db.get_feed_by_id("url_change")
    assert db_feed.source_url == BIG_BUCK_BUNNY_SHORT_URL
    assert db_feed.consecutive_failures == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_since_expansion_restores_downloads(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
) -> None:
    """Expanding 'since' date restores archived downloads."""
    # Setup: Feed with archived downloads
    feed = Feed(
        id="since_test",
        title="Since Test Feed",
        is_enabled=True,
        source_type=SourceType.PLAYLIST,
        source_url=COLETDJNZ_CHANNEL_VIDEOS,
        resolved_url=COLETDJNZ_CHANNEL_VIDEOS,
        last_successful_sync=BASE_TIME,
        since=datetime(2024, 8, 15, tzinfo=UTC),  # Original since
    )
    await feed_db.upsert_feed(feed)

    # Add archived downloads: both AFTER original since, but we'll only restore one
    downloads = [
        create_test_download(
            "since_test",
            "should_restore",
            DownloadStatus.ARCHIVED,
            datetime(2024, 8, 1, tzinfo=UTC),  # After original since AND new since
        ),
        create_test_download(
            "since_test",
            "should_stay_archived",
            DownloadStatus.ARCHIVED,
            # After original since but BEFORE new since
            datetime(2024, 6, 1, tzinfo=UTC),
        ),
    ]

    for dl in downloads:
        await download_db.upsert_download(dl)

    # Change since to middle date - should restore only one download
    config_feeds = {
        "since_test": FeedConfig(
            url=COLETDJNZ_CHANNEL_VIDEOS,
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            since=datetime(2024, 7, 15, tzinfo=UTC),  # Between the two downloads
            keep_last=None,
            yt_args=None,  # type: ignore
            metadata=None,
        )
    }
    await state_reconciler.reconcile_startup_state(config_feeds)

    # Verify only one download was restored (the one after new since date)
    restored = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id="since_test"
    )
    assert len(restored) == 1
    assert restored[0].id == "should_restore"

    # Verify one download remains archived (the one before new since date)
    archived = await download_db.get_downloads_by_status(
        DownloadStatus.ARCHIVED, feed_id="since_test"
    )
    assert len(archived) == 1
    assert archived[0].id == "should_stay_archived"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_keep_last_increase_restores_downloads(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
) -> None:
    """Increasing keep_last restores most recent archived downloads."""
    # Setup: Feed with current downloads and archived ones
    feed = Feed(
        id="keep_last_test",
        title="Keep Last Test Feed",
        is_enabled=True,
        source_type=SourceType.PLAYLIST,
        source_url=COLETDJNZ_CHANNEL_VIDEOS,
        resolved_url=COLETDJNZ_CHANNEL_VIDEOS,
        last_successful_sync=BASE_TIME,
        keep_last=2,
    )
    await feed_db.upsert_feed(feed)

    # Add 2 current downloads
    for i in range(2):
        dl = create_test_download(
            feed.id,
            f"downloaded_{i}",
            DownloadStatus.DOWNLOADED,
            datetime(2024, 10 - i, 1, tzinfo=UTC),
        )
        await download_db.upsert_download(dl)

    # Add 3 archived downloads (older)
    for i in range(3):
        dl = create_test_download(
            feed.id,
            f"archived_{i}",
            DownloadStatus.ARCHIVED,
            datetime(2024, 7 - i, 1, tzinfo=UTC),
        )
        await download_db.upsert_download(dl)

    # Increase keep_last from 2 to 4 (should restore 2 of the 3 archived downloads)
    config_feeds = {
        feed.id: FeedConfig(
            url=feed.source_url,
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=4,
            since=None,
            yt_args=None,  # type: ignore
            metadata=None,
        )
    }
    await state_reconciler.reconcile_startup_state(config_feeds)

    # Verify 2 downloads were restored (to reach keep_last=4 total)
    restored = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed.id
    )
    assert len(restored) == 2
    assert all(dl.id.startswith("archived_") for dl in restored)

    # Verify 1 download remains archived (the oldest one)
    archived = await download_db.get_downloads_by_status(
        DownloadStatus.ARCHIVED, feed_id=feed.id
    )
    assert len(archived) == 1
    assert archived[0].id == "archived_2"  # The oldest one stays archived


@pytest.mark.integration
@pytest.mark.asyncio
async def test_metadata_updates(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
    path_manager: PathManager,
) -> None:
    """Feed metadata updates are applied correctly."""
    feed_id = "meta_test"

    # Setup: Feed with initial metadata and existing image
    feed = Feed(
        id=feed_id,
        title="Original Title",
        subtitle="Original Subtitle",
        is_enabled=True,
        source_type=SourceType.PLAYLIST,
        source_url=COLETDJNZ_CHANNEL_VIDEOS,
        resolved_url=COLETDJNZ_CHANNEL_VIDEOS,
        last_successful_sync=BASE_TIME,
        language="en",
        explicit=PodcastExplicit.YES,
        remote_image_url="https://example.com/old_image.jpg",
        image_ext="jpg",
    )
    await feed_db.upsert_feed(feed)

    # Create existing image file to test replacement
    old_image_path = await path_manager.image_path(feed_id, None, "jpg")
    old_image_path.parent.mkdir(parents=True, exist_ok=True)
    old_image_path.write_bytes(b"old image content")
    assert old_image_path.exists()

    # Update metadata with new image URL
    config_feeds = {
        feed_id: FeedConfig(
            url=COLETDJNZ_CHANNEL_VIDEOS,
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=None,
            since=None,
            yt_args=None,  # type: ignore
            metadata=FeedMetadataOverrides(
                title="Updated Title",
                subtitle="Updated Subtitle",
                description="New Description",
                language="es",
                author="New Author",
                author_email=None,
                image_url="https://example.com/new_image.jpg",
                category=PodcastCategories("Arts > Design"),
                podcast_type=PodcastType.EPISODIC,
                explicit=PodcastExplicit.NO,
            ),
        )
    }
    await state_reconciler.reconcile_startup_state(config_feeds)

    # Verify all metadata updated
    feed_config = config_feeds[feed_id]
    metadata = feed_config.metadata
    assert metadata is not None
    db_feed = await feed_db.get_feed_by_id(feed_id)
    assert db_feed.title == metadata.title
    assert db_feed.subtitle == metadata.subtitle
    assert db_feed.description == metadata.description
    assert db_feed.language == metadata.language
    assert db_feed.author == metadata.author
    assert db_feed.remote_image_url == metadata.image_url
    assert db_feed.category is not None
    assert db_feed.category == metadata.category
    assert db_feed.explicit == metadata.explicit

    # Verify image was downloaded and replaced
    assert db_feed.image_ext == "jpg"
    new_image_path = await path_manager.image_path(feed_id, None, "jpg")
    assert new_image_path.exists()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_feeds_parallel_changes(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
) -> None:
    """Multiple feeds with different changes are handled correctly."""
    # Setup: Multiple existing feeds
    feeds = [
        Feed(
            id="feed1",
            title="Feed 1",
            is_enabled=True,
            source_type=SourceType.PLAYLIST,
            source_url=COLETDJNZ_CHANNEL_VIDEOS,
            resolved_url=COLETDJNZ_CHANNEL_VIDEOS,
            last_successful_sync=BASE_TIME,
        ),
        Feed(
            id="feed2",
            title="Feed 2",
            is_enabled=False,
            source_type=SourceType.SINGLE_VIDEO,
            source_url=BIG_BUCK_BUNNY_SHORT_URL,
            last_successful_sync=BASE_TIME,
        ),
        Feed(
            id="feed3",
            title="Feed 3 (to remove)",
            is_enabled=True,
            source_type=SourceType.PLAYLIST,
            source_url=COLETDJNZ_CHANNEL_VIDEOS,
            resolved_url=COLETDJNZ_CHANNEL_VIDEOS,
            last_successful_sync=BASE_TIME,
        ),
    ]

    for feed in feeds:
        await feed_db.upsert_feed(feed)

    # Config with various changes
    config_feeds = {
        "feed1": FeedConfig(  # No changes
            url=COLETDJNZ_CHANNEL_VIDEOS,
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=None,
            since=None,
            yt_args=None,  # type: ignore
            metadata=None,
        ),
        "feed2": FeedConfig(  # Enable
            url=BIG_BUCK_BUNNY_SHORT_URL,
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=None,
            since=None,
            yt_args=None,  # type: ignore
            metadata=None,
        ),
        # feed3 removed
        "feed4": FeedConfig(  # New feed
            url=BIG_BUCK_BUNNY_SHORT_URL,
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=None,
            since=None,
            yt_args=None,  # type: ignore
            metadata=None,
        ),
    }

    # Execute reconciliation
    ready_feeds = await state_reconciler.reconcile_startup_state(config_feeds)

    # Verify results
    assert set(ready_feeds) == {"feed1", "feed2", "feed4"}

    # Check individual feed states
    assert (await feed_db.get_feed_by_id("feed1")).is_enabled is True
    assert (await feed_db.get_feed_by_id("feed2")).is_enabled is True
    assert (await feed_db.get_feed_by_id("feed3")).is_enabled is False  # Disabled
    assert (await feed_db.get_feed_by_id("feed4")).is_enabled is True  # New
