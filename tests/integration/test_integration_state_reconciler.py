# pyright: reportPrivateUsage=false

"""Integration tests for StateReconciler startup reconciliation.

Tests focus on full end-to-end reconciliation including database operations,
feed state changes, and retention policy changes.
"""

from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
import shutil
import tempfile

import pytest

from anypod.config import FeedConfig
from anypod.config.types import (
    FeedMetadataOverrides,
    PodcastCategories,
    PodcastExplicit,
)
from anypod.data_coordinator.pruner import Pruner
from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.file_manager import FileManager
from anypod.path_manager import PathManager
from anypod.state_reconciler import StateReconciler

# Test constants
TEST_CRON_SCHEDULE = "0 * * * *"
BASE_TIME = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def temp_media_dir() -> Generator[Path]:
    """Provides a temporary directory for media files."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def temp_db_path() -> Generator[Path]:
    """Provides a temporary database file path."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)
    yield db_path
    db_path.unlink(missing_ok=True)


@pytest.fixture
def feed_db(temp_db_path: Path) -> Generator[FeedDatabase]:
    """Provides a FeedDatabase instance."""
    db = FeedDatabase(temp_db_path)
    yield db
    db.close()


@pytest.fixture
def download_db(temp_db_path: Path) -> Generator[DownloadDatabase]:
    """Provides a DownloadDatabase instance."""
    db = DownloadDatabase(temp_db_path)
    yield db
    db.close()


@pytest.fixture
def path_manager(temp_media_dir: Path) -> PathManager:
    """Provides a PathManager instance with test directories."""
    return PathManager(
        base_data_dir=temp_media_dir,
        base_tmp_dir=temp_media_dir / ".tmp",
        base_url="https://example.com",
    )


@pytest.fixture
def file_manager(path_manager: PathManager) -> FileManager:
    """Provides a FileManager instance."""
    return FileManager(path_manager)


@pytest.fixture
def pruner(
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    file_manager: FileManager,
) -> Pruner:
    """Provides a Pruner instance."""
    return Pruner(feed_db, download_db, file_manager)


@pytest.fixture
def state_reconciler(
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    pruner: Pruner,
) -> StateReconciler:
    """Provides a StateReconciler instance with real dependencies."""
    return StateReconciler(feed_db, download_db, pruner)


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
        feed=feed_id,
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


def write_test_media_file(
    feed_id: str,
    download_id: str,
    ext: str,
    contents: str,
    path_manager: PathManager,
) -> None:
    """Write a test media file to the filesystem."""
    media_path = path_manager.media_file_path(feed_id, download_id, ext)
    media_path.parent.mkdir(parents=True, exist_ok=True)
    media_path.write_text(contents)


@pytest.mark.integration
def test_new_feed_addition(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
) -> None:
    """New feed is properly added to database with all fields."""
    # Setup config
    config_feeds = {
        "new_feed": FeedConfig(
            url="https://example.com/new_feed",
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
                image_url="https://example.com/image.jpg",
                categories=PodcastCategories("Technology > Tech News"),
                explicit=PodcastExplicit.CLEAN,
            ),
        )
    }

    # Execute reconciliation
    ready_feeds = state_reconciler.reconcile_startup_state(config_feeds)

    # Verify results
    feed_id = next(iter(config_feeds))
    assert ready_feeds == [feed_id]

    # Check database state
    feed_config = config_feeds[feed_id]
    db_feed = feed_db.get_feed_by_id(feed_id)
    assert db_feed.id == feed_id
    assert db_feed.source_url == feed_config.url
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
    assert db_feed.image_url == metadata.image_url
    assert db_feed.category is not None
    assert db_feed.category == metadata.categories
    assert db_feed.explicit == metadata.explicit


@pytest.mark.integration
def test_reconciliation_is_idempotent(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
) -> None:
    """Running state reconciliation twice with same config is a no-op."""
    # Setup config
    config_feeds = {
        "test_feed": FeedConfig(
            url="https://example.com/test",
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=10,
            since=datetime(2024, 1, 1, tzinfo=UTC),
            metadata=FeedMetadataOverrides(  # type: ignore # this is a quirk of Pydantic
                title="Test Feed",
                language="en",
                categories=PodcastCategories("Technology"),
            ),
        )
    }

    # First reconciliation
    ready_feeds1 = state_reconciler.reconcile_startup_state(config_feeds)
    db_feed1 = feed_db.get_feed_by_id("test_feed")

    # Second reconciliation with same config
    ready_feeds2 = state_reconciler.reconcile_startup_state(config_feeds)
    db_feed2 = feed_db.get_feed_by_id("test_feed")

    # Results should be identical
    assert ready_feeds1 == ready_feeds2 == ["test_feed"]
    assert db_feed1.updated_at == db_feed2.updated_at  # No changes made
    assert db_feed1 == db_feed2


@pytest.mark.integration
def test_feed_removal_archives_downloads(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    file_manager: FileManager,
    path_manager: PathManager,
) -> None:
    """Removed feed has all downloads archived and files deleted."""
    # Setup: Create feed with downloads
    feed = Feed(
        id="to_remove",
        title="Feed to Remove",
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://example.com/remove",
        last_successful_sync=BASE_TIME,
    )
    feed_db.upsert_feed(feed)

    # Add downloads in various states
    downloads = [
        create_test_download(
            "to_remove", "dl1", DownloadStatus.QUEUED
        ),  # Start as QUEUED to mark as downloaded
        create_test_download("to_remove", "dl2", DownloadStatus.QUEUED),
        create_test_download("to_remove", "dl3", DownloadStatus.ERROR),
    ]

    for dl in downloads:
        download_db.upsert_download(dl)

    # Create file for downloaded item and mark as downloaded
    write_test_media_file("to_remove", "dl1", "mp4", "test content", path_manager)
    media_path = path_manager.media_file_path("to_remove", "dl1", "mp4")

    # Mark first download as downloaded
    download_db.mark_as_downloaded(feed.id, "dl1", "mp4", 12)

    # Execute reconciliation with empty config (feed removed)
    ready_feeds = state_reconciler.reconcile_startup_state({})

    # Verify results
    assert ready_feeds == []

    # Check feed is disabled
    db_feed = feed_db.get_feed_by_id("to_remove")
    assert db_feed.is_enabled is False

    # Check all downloads are archived
    for status in [
        DownloadStatus.DOWNLOADED,
        DownloadStatus.QUEUED,
        DownloadStatus.ERROR,
    ]:
        downloads = download_db.get_downloads_by_status(status, feed_id="to_remove")
        assert len(downloads) == 0

    archived = download_db.get_downloads_by_status(
        DownloadStatus.ARCHIVED, feed_id="to_remove"
    )
    assert len(archived) == 3

    # Check file was deleted
    assert not media_path.exists()


@pytest.mark.integration
def test_feed_enable_disable_transitions(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
) -> None:
    """Feed enable/disable transitions work correctly."""
    # Setup: Create disabled feed with errors
    feed = Feed(
        id="toggle_feed",
        title="Toggle Feed",
        is_enabled=False,
        source_type=SourceType.CHANNEL,
        source_url="https://example.com/toggle",
        last_successful_sync=BASE_TIME,
        consecutive_failures=5,
        last_failed_sync=datetime.now(UTC),
        last_error="Previous failure",
    )
    feed_db.upsert_feed(feed)

    # Enable the feed
    config_feeds = {
        "toggle_feed": FeedConfig(
            url="https://example.com/toggle",
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=None,
            since=None,
            yt_args=None,  # type: ignore
            metadata=None,
        )
    }
    ready_feeds = state_reconciler.reconcile_startup_state(config_feeds)

    # Verify enabled and errors cleared
    assert ready_feeds == ["toggle_feed"]
    db_feed = feed_db.get_feed_by_id("toggle_feed")
    assert db_feed.is_enabled is True
    assert db_feed.consecutive_failures == 0
    assert db_feed.last_failed_sync is None
    assert db_feed.last_error is None

    # Disable the feed again
    config_feeds["toggle_feed"].enabled = False
    ready_feeds = state_reconciler.reconcile_startup_state(config_feeds)

    # Verify disabled but errors not modified
    assert ready_feeds == []
    db_feed = feed_db.get_feed_by_id("toggle_feed")
    assert db_feed.is_enabled is False
    assert db_feed.consecutive_failures == 0  # Not changed


@pytest.mark.integration
def test_url_change_resets_error_state(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
) -> None:
    """Changing feed URL resets error state."""
    # Setup: Feed with errors
    feed = Feed(
        id="url_change",
        title="URL Change Feed",
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://old.example.com/feed",
        last_successful_sync=BASE_TIME,
        consecutive_failures=3,
        last_error="Old URL error",
    )
    feed_db.upsert_feed(feed)

    # Change URL
    config_feeds = {
        "url_change": FeedConfig(
            url="https://new.example.com/feed",
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=None,
            since=None,
            yt_args=None,  # type: ignore
            metadata=None,
        )
    }
    state_reconciler.reconcile_startup_state(config_feeds)

    # Verify URL changed and errors cleared
    db_feed = feed_db.get_feed_by_id("url_change")
    assert db_feed.source_url == "https://new.example.com/feed"
    assert db_feed.consecutive_failures == 0
    assert db_feed.last_error is None


@pytest.mark.integration
def test_since_expansion_restores_downloads(
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
        source_type=SourceType.CHANNEL,
        source_url="https://example.com/since",
        last_successful_sync=BASE_TIME,
        since=datetime(2024, 8, 15, tzinfo=UTC),  # Original since
    )
    feed_db.upsert_feed(feed)

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
        download_db.upsert_download(dl)

    # Change since to middle date - should restore only one download
    config_feeds = {
        "since_test": FeedConfig(
            url="https://example.com/since",
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            since=datetime(2024, 7, 15, tzinfo=UTC),  # Between the two downloads
            keep_last=None,
            yt_args=None,  # type: ignore
            metadata=None,
        )
    }
    state_reconciler.reconcile_startup_state(config_feeds)

    # Verify only one download was restored (the one after new since date)
    restored = download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id="since_test"
    )
    assert len(restored) == 1
    assert restored[0].id == "should_restore"

    # Verify one download remains archived (the one before new since date)
    archived = download_db.get_downloads_by_status(
        DownloadStatus.ARCHIVED, feed_id="since_test"
    )
    assert len(archived) == 1
    assert archived[0].id == "should_stay_archived"


@pytest.mark.integration
def test_keep_last_increase_restores_downloads(
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
        source_type=SourceType.CHANNEL,
        source_url="https://example.com/keep",
        last_successful_sync=BASE_TIME,
        keep_last=2,
        total_downloads=2,  # 2 currently downloaded
    )
    feed_db.upsert_feed(feed)

    # Add 2 current downloads
    for i in range(2):
        dl = create_test_download(
            "keep_last_test",
            f"downloaded_{i}",
            DownloadStatus.DOWNLOADED,
            datetime(2024, 10 - i, 1, tzinfo=UTC),
        )
        download_db.upsert_download(dl)

    # Add 3 archived downloads (older)
    for i in range(3):
        dl = create_test_download(
            "keep_last_test",
            f"archived_{i}",
            DownloadStatus.ARCHIVED,
            datetime(2024, 7 - i, 1, tzinfo=UTC),
        )
        download_db.upsert_download(dl)

    # Increase keep_last from 2 to 4 (should restore 2 of the 3 archived downloads)
    config_feeds = {
        "keep_last_test": FeedConfig(
            url="https://example.com/keep",
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=4,
            since=None,
            yt_args=None,  # type: ignore
            metadata=None,
        )
    }
    state_reconciler.reconcile_startup_state(config_feeds)

    # Verify 2 downloads were restored (to reach keep_last=4 total)
    restored = download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id="keep_last_test"
    )
    assert len(restored) == 2
    assert all(dl.id.startswith("archived_") for dl in restored)

    # Verify 1 download remains archived (the oldest one)
    archived = download_db.get_downloads_by_status(
        DownloadStatus.ARCHIVED, feed_id="keep_last_test"
    )
    assert len(archived) == 1
    assert archived[0].id == "archived_2"  # The oldest one stays archived


@pytest.mark.integration
def test_metadata_updates(
    state_reconciler: StateReconciler,
    feed_db: FeedDatabase,
) -> None:
    """Feed metadata updates are applied correctly."""
    # Setup: Feed with initial metadata
    feed = Feed(
        id="meta_test",
        title="Original Title",
        subtitle="Original Subtitle",
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://example.com/meta",
        last_successful_sync=BASE_TIME,
        language="en",
        explicit=PodcastExplicit.YES,
    )
    feed_db.upsert_feed(feed)

    # Update metadata
    config_feeds = {
        "meta_test": FeedConfig(
            url="https://example.com/meta",
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
                image_url="https://example.com/new_image.jpg",
                categories=PodcastCategories("Arts > Design"),
                explicit=PodcastExplicit.NO,
            ),
        )
    }
    state_reconciler.reconcile_startup_state(config_feeds)

    # Verify all metadata updated
    feed_config = config_feeds["meta_test"]
    metadata = feed_config.metadata
    assert metadata is not None
    db_feed = feed_db.get_feed_by_id("meta_test")
    assert db_feed.title == metadata.title
    assert db_feed.subtitle == metadata.subtitle
    assert db_feed.description == metadata.description
    assert db_feed.language == metadata.language
    assert db_feed.author == metadata.author
    assert db_feed.image_url == metadata.image_url
    assert db_feed.category is not None
    assert db_feed.category == metadata.categories
    assert db_feed.explicit == metadata.explicit


@pytest.mark.integration
def test_multiple_feeds_parallel_changes(
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
            source_type=SourceType.CHANNEL,
            source_url="https://example.com/feed1",
            last_successful_sync=BASE_TIME,
        ),
        Feed(
            id="feed2",
            title="Feed 2",
            is_enabled=False,
            source_type=SourceType.PLAYLIST,
            source_url="https://example.com/feed2",
            last_successful_sync=BASE_TIME,
        ),
        Feed(
            id="feed3",
            title="Feed 3 (to remove)",
            is_enabled=True,
            source_type=SourceType.CHANNEL,
            source_url="https://example.com/feed3",
            last_successful_sync=BASE_TIME,
        ),
    ]

    for feed in feeds:
        feed_db.upsert_feed(feed)

    # Config with various changes
    config_feeds = {
        "feed1": FeedConfig(  # No changes
            url="https://example.com/feed1",
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=None,
            since=None,
            yt_args=None,  # type: ignore
            metadata=None,
        ),
        "feed2": FeedConfig(  # Enable
            url="https://example.com/feed2",
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=None,
            since=None,
            yt_args=None,  # type: ignore
            metadata=None,
        ),
        # feed3 removed
        "feed4": FeedConfig(  # New feed
            url="https://example.com/feed4",
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=None,
            since=None,
            yt_args=None,  # type: ignore
            metadata=None,
        ),
    }

    # Execute reconciliation
    ready_feeds = state_reconciler.reconcile_startup_state(config_feeds)

    # Verify results
    assert set(ready_feeds) == {"feed1", "feed2", "feed4"}

    # Check individual feed states
    assert feed_db.get_feed_by_id("feed1").is_enabled is True
    assert feed_db.get_feed_by_id("feed2").is_enabled is True
    assert feed_db.get_feed_by_id("feed3").is_enabled is False  # Disabled
    assert feed_db.get_feed_by_id("feed4").is_enabled is True  # New
