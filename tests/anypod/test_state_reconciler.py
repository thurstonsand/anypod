# pyright: reportPrivateUsage=false

"""Tests for the StateReconciler class.

This module contains unit tests for the StateReconciler, which manages
synchronization between YAML configuration and database state during startup
and when configuration changes are detected.
"""

from copy import deepcopy
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from anypod.config import FeedConfig
from anypod.config.types import (
    FeedMetadataOverrides,
    PodcastCategories,
    PodcastExplicit,
    PodcastType,
)
from anypod.data_coordinator.pruner import Pruner
from anypod.db import DownloadDatabase
from anypod.db.feed_db import FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.exceptions import (
    DatabaseOperationError,
    ImageDownloadError,
    PruneError,
    StateReconciliationError,
)
from anypod.file_manager import FileManager
from anypod.image_downloader import ImageDownloader
from anypod.state_reconciler import MIN_SYNC_DATE, StateReconciler
from anypod.ytdlp_wrapper import YtdlpWrapper

# Test constants
FEED_ID = "test_feed"
FEED_URL = "https://example.com/feed"
NEW_FEED_ID = "new_feed"
NEW_FEED_URL = "https://example.com/new_feed"
REMOVED_FEED_ID = "removed_feed"
TEST_CRON_SCHEDULE = "0 * * * *"

# Mock Feed objects for testing
BASE_TIME = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
MOCK_FEED = Feed(
    id=FEED_ID,
    title="Test Feed",
    subtitle=None,
    description=None,
    language=None,
    author=None,
    author_email=None,
    remote_image_url=None,
    is_enabled=True,
    source_type=SourceType.UNKNOWN,
    source_url=FEED_URL,
    last_successful_sync=BASE_TIME,
    since=None,
    keep_last=None,
)

MOCK_DISABLED_FEED = Feed(
    id=REMOVED_FEED_ID,
    title="Removed Feed",
    subtitle=None,
    description=None,
    language=None,
    author=None,
    author_email=None,
    remote_image_url=None,
    is_enabled=True,
    source_type=SourceType.UNKNOWN,
    source_url="https://example.com/removed",
    last_successful_sync=BASE_TIME,
)

# Mock Downloads for testing
MOCK_ARCHIVED_DOWNLOAD_1 = Download(
    feed_id=FEED_ID,
    id="archived_1",
    source_url="https://example.com/video1",
    title="Archived Video 1",
    published=datetime(2024, 6, 1, tzinfo=UTC),
    ext="mp4",
    mime_type="video/mp4",
    filesize=1024000,
    duration=120,
    status=DownloadStatus.ARCHIVED,
    discovered_at=datetime(2024, 6, 1, tzinfo=UTC),
    updated_at=datetime(2024, 6, 1, tzinfo=UTC),
)

MOCK_ARCHIVED_DOWNLOAD_2 = Download(
    feed_id=FEED_ID,
    id="archived_2",
    source_url="https://example.com/video2",
    title="Archived Video 2",
    published=datetime(2024, 7, 1, tzinfo=UTC),
    ext="mp4",
    mime_type="video/mp4",
    filesize=1024000,
    duration=120,
    status=DownloadStatus.ARCHIVED,
    discovered_at=datetime(2024, 7, 1, tzinfo=UTC),
    updated_at=datetime(2024, 7, 1, tzinfo=UTC),
)


# --- Fixtures ---


@pytest.fixture
def mock_feed_db() -> MagicMock:
    """Provides a MagicMock for FeedDatabase."""
    mock = MagicMock(spec=FeedDatabase)
    # Configure async methods with AsyncMock
    mock.get_feeds = AsyncMock()
    mock.upsert_feed = AsyncMock()
    return mock


@pytest.fixture
def mock_download_db() -> MagicMock:
    """Provides a MagicMock for DownloadDatabase."""
    mock = MagicMock(spec=DownloadDatabase)
    # Configure async methods with AsyncMock
    mock.get_downloads_by_status = AsyncMock()
    mock.requeue_downloads = AsyncMock()
    mock.count_downloads_by_status = AsyncMock()
    return mock


@pytest.fixture
def mock_pruner() -> MagicMock:
    """Provides a MagicMock for Pruner."""
    pruner = MagicMock(spec=Pruner)
    # Make archive_feed an async mock since it's now async
    pruner.archive_feed = AsyncMock()
    return pruner


@pytest.fixture
def mock_ytdlp_wrapper() -> MagicMock:
    """Provides a MagicMock for YtdlpWrapper."""
    mock = MagicMock(spec=YtdlpWrapper)
    mock.fetch_playlist_metadata = AsyncMock(return_value=MOCK_FEED)
    mock.discover_feed_properties = AsyncMock(return_value=(SourceType.UNKNOWN, None))
    return mock


@pytest.fixture
def mock_file_manager() -> MagicMock:
    """Provides a MagicMock for FileManager."""
    return MagicMock(spec=FileManager)


@pytest.fixture
def mock_image_downloader() -> MagicMock:
    """Provides a MagicMock for ImageDownloader with async methods."""
    dl = MagicMock(spec=ImageDownloader)
    dl.download_feed_image_direct = AsyncMock(return_value="jpg")
    dl.download_feed_image_ytdlp = AsyncMock(return_value="jpg")
    return dl


@pytest.fixture
def state_reconciler(
    mock_file_manager: MagicMock,
    mock_image_downloader: MagicMock,
    mock_feed_db: MagicMock,
    mock_download_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    mock_pruner: MagicMock,
) -> StateReconciler:
    """Provides a StateReconciler instance with mocked dependencies."""
    return StateReconciler(
        mock_file_manager,
        mock_image_downloader,
        mock_feed_db,
        mock_download_db,
        mock_ytdlp_wrapper,
        mock_pruner,
    )


@pytest.fixture
def base_feed_config() -> FeedConfig:
    """Provides a basic FeedConfig for testing."""
    return FeedConfig(
        url=FEED_URL,
        schedule=TEST_CRON_SCHEDULE,  # type: ignore
        enabled=True,
        keep_last=None,
        since=None,
        yt_args=None,  # type: ignore
        metadata=None,
    )


@pytest.fixture
def feed_config_with_metadata() -> FeedConfig:
    """Provides a FeedConfig with metadata overrides."""
    return FeedConfig(
        url=FEED_URL,
        schedule=TEST_CRON_SCHEDULE,  # type: ignore
        enabled=True,
        keep_last=None,
        since=None,
        yt_args=None,  # type: ignore
        metadata=FeedMetadataOverrides(
            title="Custom Title",
            subtitle="Custom Subtitle",
            description="Custom Description",
            language="en",
            author="Test Author",
            author_email="test@example.com",
            image_url="https://example.com/image.jpg",
            category=PodcastCategories("Technology"),
            podcast_type=PodcastType.EPISODIC,
            explicit=PodcastExplicit.NO,
        ),
    )


# --- Tests for StateReconciler._handle_new_feed ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_new_feed_basic(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """New feed is inserted with correct values."""
    await state_reconciler._handle_new_feed(NEW_FEED_ID, base_feed_config)

    mock_feed_db.upsert_feed.assert_called_once()
    inserted_feed = mock_feed_db.upsert_feed.call_args[0][0]
    assert inserted_feed.id == NEW_FEED_ID
    assert inserted_feed.source_url == base_feed_config.url
    assert inserted_feed.is_enabled is True
    assert inserted_feed.source_type == SourceType.UNKNOWN
    assert inserted_feed.last_successful_sync is not None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_new_feed_with_metadata(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    feed_config_with_metadata: FeedConfig,
) -> None:
    """New feed with metadata is inserted correctly."""
    await state_reconciler._handle_new_feed(NEW_FEED_ID, feed_config_with_metadata)

    mock_feed_db.upsert_feed.assert_called_once()
    inserted_feed = mock_feed_db.upsert_feed.call_args[0][0]

    # For type safety - we know metadata is not None in this fixture
    assert feed_config_with_metadata.metadata is not None
    metadata = feed_config_with_metadata.metadata

    assert inserted_feed.title == metadata.title
    assert inserted_feed.subtitle == metadata.subtitle
    assert inserted_feed.description == metadata.description
    assert inserted_feed.language == metadata.language
    assert inserted_feed.author == metadata.author
    assert inserted_feed.remote_image_url == metadata.image_url
    assert inserted_feed.category == metadata.category
    assert inserted_feed.explicit == metadata.explicit


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_new_feed_with_since(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """New feed with 'since' date uses it as initial sync time."""
    since_date = datetime(2023, 1, 1, tzinfo=UTC)
    config = deepcopy(base_feed_config)
    config.since = since_date

    await state_reconciler._handle_new_feed(NEW_FEED_ID, config)

    mock_feed_db.upsert_feed.assert_called_once()
    inserted_feed = mock_feed_db.upsert_feed.call_args[0][0]
    assert inserted_feed.last_successful_sync == since_date
    assert inserted_feed.since == since_date


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_new_feed_database_error(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """Database error during new feed insertion is propagated."""
    mock_feed_db.upsert_feed.side_effect = DatabaseOperationError("DB error")

    with pytest.raises(StateReconciliationError):
        await state_reconciler._handle_new_feed(NEW_FEED_ID, base_feed_config)


# --- Tests for StateReconciler._handle_removed_feed ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_removed_feed(
    state_reconciler: StateReconciler, mock_pruner: MagicMock
) -> None:
    """Removed feed is archived via pruner."""
    await state_reconciler._handle_removed_feed(REMOVED_FEED_ID)

    mock_pruner.archive_feed.assert_called_once_with(REMOVED_FEED_ID)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_removed_feed_pruner_error(
    state_reconciler: StateReconciler, mock_pruner: MagicMock
) -> None:
    """Pruner error during feed removal is wrapped."""
    mock_pruner.archive_feed.side_effect = PruneError("Prune failed")

    with pytest.raises(StateReconciliationError) as exc_info:
        await state_reconciler._handle_removed_feed(REMOVED_FEED_ID)

    assert exc_info.value.feed_id == REMOVED_FEED_ID


# --- Tests for StateReconciler._handle_existing_feed ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_existing_feed_no_changes(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """No changes to existing feed results in no database update."""
    existing_feed = deepcopy(MOCK_FEED)

    result = await state_reconciler._handle_existing_feed(
        existing_feed.id, base_feed_config, existing_feed
    )

    assert result is False
    mock_feed_db.upsert_feed.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_existing_feed_enable(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """Enabling a disabled feed resets error state."""
    disabled_feed = deepcopy(MOCK_FEED)
    disabled_feed.is_enabled = False
    disabled_feed.consecutive_failures = 3
    disabled_feed.last_failed_sync = datetime.now(UTC)

    config = deepcopy(base_feed_config)
    config.enabled = True

    result = await state_reconciler._handle_existing_feed(
        FEED_ID, config, disabled_feed
    )

    assert result is True
    mock_feed_db.upsert_feed.assert_called_once()
    updated_feed = mock_feed_db.upsert_feed.call_args[0][0]
    assert updated_feed.is_enabled is True
    assert updated_feed.consecutive_failures == 0
    assert updated_feed.last_failed_sync is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_existing_feed_disable(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """Disabling an enabled feed only updates enabled status."""
    enabled_feed = deepcopy(MOCK_FEED)
    enabled_feed.is_enabled = True

    config = deepcopy(base_feed_config)
    config.enabled = False

    result = await state_reconciler._handle_existing_feed(FEED_ID, config, enabled_feed)

    assert result is True
    mock_feed_db.upsert_feed.assert_called_once()
    updated_feed = mock_feed_db.upsert_feed.call_args[0][0]
    assert updated_feed.is_enabled is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_existing_feed_url_change(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """URL change resets error state."""
    existing_feed = deepcopy(MOCK_FEED)
    existing_feed.source_url = "https://old.example.com/feed"
    existing_feed.consecutive_failures = 2

    config = deepcopy(base_feed_config)
    config.url = "https://new.example.com/feed"

    new_feed_metadata = deepcopy(MOCK_FEED)
    new_feed_metadata.source_url = config.url
    mock_ytdlp_wrapper.fetch_playlist_metadata.return_value = new_feed_metadata

    result = await state_reconciler._handle_existing_feed(
        FEED_ID, config, existing_feed
    )

    assert result is True
    mock_feed_db.upsert_feed.assert_called_once()
    updated_feed = mock_feed_db.upsert_feed.call_args[0][0]
    assert updated_feed.source_url == "https://new.example.com/feed"
    assert updated_feed.consecutive_failures == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_existing_feed_metadata_changes(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    feed_config_with_metadata: FeedConfig,
) -> None:
    """Metadata changes are applied correctly."""
    existing_feed = deepcopy(MOCK_FEED)
    existing_feed.title = "Old Title"
    existing_feed.description = "Old Description"

    result = await state_reconciler._handle_existing_feed(
        FEED_ID, feed_config_with_metadata, existing_feed
    )

    assert result is True
    mock_feed_db.upsert_feed.assert_called_once()
    updated_feed = mock_feed_db.upsert_feed.call_args[0][0]

    # For type safety - we know metadata is not None in this fixture
    assert feed_config_with_metadata.metadata is not None
    metadata = feed_config_with_metadata.metadata

    assert updated_feed.title == metadata.title
    assert updated_feed.subtitle == metadata.subtitle
    assert updated_feed.description == metadata.description
    assert updated_feed.language == metadata.language
    assert updated_feed.author == metadata.author
    assert updated_feed.remote_image_url == metadata.image_url


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_existing_feed_since_removal_resets_sync_timestamp(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    mock_download_db: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """Removing 'since' filter resets last_successful_sync to allow re-fetching all videos."""
    # Setup existing feed with a 'since' filter
    existing_feed = deepcopy(MOCK_FEED)
    existing_feed.since = datetime(2024, 6, 1, tzinfo=UTC)
    existing_feed.last_successful_sync = datetime(2024, 6, 1, tzinfo=UTC)

    # Config removes the 'since' filter
    config = deepcopy(base_feed_config)
    config.since = None

    # Mock no archived downloads to restore
    mock_download_db.get_downloads_by_status.return_value = []

    result = await state_reconciler._handle_existing_feed(
        FEED_ID, config, existing_feed
    )

    assert result is True
    mock_feed_db.upsert_feed.assert_called_once()
    updated_feed = mock_feed_db.upsert_feed.call_args[0][0]

    # Verify that last_successful_sync was reset to MIN_SYNC_DATE
    assert updated_feed.last_successful_sync == MIN_SYNC_DATE
    assert updated_feed.since is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_existing_feed_enable_with_since(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """Enabling a disabled feed with 'since' config respects that date for sync timestamp."""
    disabled_feed = deepcopy(MOCK_FEED)
    disabled_feed.is_enabled = False
    disabled_feed.consecutive_failures = 3
    disabled_feed.last_failed_sync = datetime.now(UTC)

    # Create config with 'since' date
    since_date = datetime(2023, 6, 1, tzinfo=UTC)
    config = deepcopy(base_feed_config)
    config.enabled = True
    config.since = since_date

    result = await state_reconciler._handle_existing_feed(
        FEED_ID, config, disabled_feed
    )

    assert result is True
    mock_feed_db.upsert_feed.assert_called_once()
    updated_feed = mock_feed_db.upsert_feed.call_args[0][0]

    # Verify that last_successful_sync was set to the 'since' date, not MIN_SYNC_DATE
    assert updated_feed.last_successful_sync == since_date
    assert updated_feed.since == since_date
    assert updated_feed.is_enabled is True
    assert updated_feed.consecutive_failures == 0
    assert updated_feed.last_failed_sync is None


# --- Tests for StateReconciler._handle_constraint_changes ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_constraint_changes_no_changes(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """No changes to retention policies returns None."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 6, 1, tzinfo=UTC)
    db_feed.keep_last = 10

    log_params = {"feed_id": FEED_ID}
    result = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=db_feed.since,
        config_keep_last=db_feed.keep_last,
        db_feed=db_feed,
        log_params=log_params,
    )

    assert result is None
    mock_download_db.get_downloads_by_status.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_constraint_changes_since_expansion_only(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """Expanding 'since' date restores archived downloads."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 6, 1, tzinfo=UTC)
    db_feed.keep_last = None

    mock_download_db.get_downloads_by_status.return_value = [
        MOCK_ARCHIVED_DOWNLOAD_1,
        MOCK_ARCHIVED_DOWNLOAD_2,
    ]
    mock_download_db.requeue_downloads.return_value = 2

    log_params = {"feed_id": FEED_ID}
    earlier_since = datetime(2024, 5, 1, tzinfo=UTC)
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=earlier_since,  # Earlier than DB since
        config_keep_last=None,
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync == earlier_since
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=earlier_since,
        limit=-1,  # No keep_last limit
    )
    mock_download_db.requeue_downloads.assert_awaited_once_with(
        FEED_ID, ["archived_1", "archived_2"], from_status=DownloadStatus.ARCHIVED
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_constraint_changes_since_removal_only(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """Removing 'since' filter restores all archived downloads."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 6, 1, tzinfo=UTC)
    db_feed.keep_last = None

    mock_download_db.get_downloads_by_status.return_value = [
        MOCK_ARCHIVED_DOWNLOAD_1,
        MOCK_ARCHIVED_DOWNLOAD_2,
    ]
    mock_download_db.requeue_downloads.return_value = 2

    log_params = {"feed_id": FEED_ID}
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=None,  # Removing since filter
        config_keep_last=None,
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync == MIN_SYNC_DATE
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=None,  # No date filter
        limit=-1,  # No keep_last limit
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_constraint_changes_since_stricter_no_restoration(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """Making 'since' filter stricter does not restore downloads."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 5, 1, tzinfo=UTC)
    db_feed.keep_last = None

    log_params = {"feed_id": FEED_ID}
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=datetime(2024, 7, 1, tzinfo=UTC),  # Later than DB since
        config_keep_last=None,
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync is None
    mock_download_db.get_downloads_by_status.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_constraint_changes_keep_last_increase_only(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """Increasing keep_last restores archived downloads."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = None
    db_feed.keep_last = 5
    # Mock feed has 5 downloads currently
    db_feed.total_downloads_internal = 5

    mock_download_db.get_downloads_by_status.return_value = [
        MOCK_ARCHIVED_DOWNLOAD_1,
        MOCK_ARCHIVED_DOWNLOAD_2,
    ]
    mock_download_db.requeue_downloads.return_value = 2

    log_params = {"feed_id": FEED_ID}
    new_keep_last = 10
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=None,
        config_keep_last=new_keep_last,  # Increase from 5 to 10
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync == MIN_SYNC_DATE
    expected_limit = new_keep_last - db_feed.total_downloads_internal
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=None,  # No since filter
        limit=expected_limit,  # available slots
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_constraint_changes_since_removal_with_keep_last_limit(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """Removing 'since' filter is limited by keep_last constraint."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 6, 1, tzinfo=UTC)
    db_feed.keep_last = 8
    # Mock feed has 5 downloads currently
    db_feed.total_downloads_internal = 5

    mock_download_db.get_downloads_by_status.return_value = [
        MOCK_ARCHIVED_DOWNLOAD_1,
        MOCK_ARCHIVED_DOWNLOAD_2,
    ]
    mock_download_db.requeue_downloads.return_value = 2

    log_params = {"feed_id": FEED_ID}
    new_keep_last = 8
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=None,  # Remove since filter
        config_keep_last=new_keep_last,  # But keep_last limits restoration
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync == MIN_SYNC_DATE
    expected_limit = new_keep_last - db_feed.total_downloads_internal
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=None,  # No date filter
        limit=expected_limit,  # available slots
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_constraint_changes_since_expansion_blocked_by_keep_last(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """Since expansion is blocked when keep_last limit is already reached."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 6, 1, tzinfo=UTC)
    db_feed.keep_last = 5
    # Mock feed has 5 downloads, which equals the keep_last limit
    db_feed.total_downloads_internal = 5  # Set the internal field directly

    log_params = {"feed_id": FEED_ID}
    earlier_since = datetime(2024, 5, 1, tzinfo=UTC)
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=earlier_since,  # Earlier than DB since
        config_keep_last=5,  # Already at limit
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync == earlier_since
    mock_download_db.get_downloads_by_status.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_constraint_changes_both_policies_change(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """Both since expansion and keep_last increase work together."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 6, 1, tzinfo=UTC)
    db_feed.keep_last = 5
    # Mock feed has 4 downloads currently
    db_feed.total_downloads_internal = 4

    mock_download_db.get_downloads_by_status.return_value = [
        MOCK_ARCHIVED_DOWNLOAD_1,
    ]
    mock_download_db.requeue_downloads.return_value = 1

    log_params = {"feed_id": FEED_ID}
    new_keep_last = 8
    earlier_since = datetime(2024, 5, 1, tzinfo=UTC)
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=earlier_since,  # Earlier than DB since
        config_keep_last=new_keep_last,  # Increase from 5 to 8
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync == earlier_since
    expected_limit = new_keep_last - db_feed.total_downloads_internal
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=earlier_since,  # Since filter applied
        limit=expected_limit,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_constraint_changes_since_expansion_limited_by_new_keep_last(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """Since expansion is limited by newly added keep_last constraint."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 8, 1, tzinfo=UTC)  # Original since date
    db_feed.keep_last = None  # No previous keep_last
    # Mock feed has 10 downloads currently
    db_feed.total_downloads_internal = 10

    # Mock 5 archived downloads that would be restored by since expansion
    mock_archived_downloads = [
        MOCK_ARCHIVED_DOWNLOAD_1,
        MOCK_ARCHIVED_DOWNLOAD_2,
    ]
    mock_download_db.get_downloads_by_status.return_value = mock_archived_downloads
    mock_download_db.requeue_downloads.return_value = 2  # Only 2 restored due to limit

    log_params = {"feed_id": FEED_ID}
    expanded_since = datetime(2024, 7, 1, tzinfo=UTC)
    new_keep_last = 12
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=expanded_since,  # Expand since further back
        config_keep_last=new_keep_last,  # Add keep_last allowing only 2 more
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync == expanded_since
    expected_limit = new_keep_last - db_feed.total_downloads_internal
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=expanded_since,  # Since filter applied
        limit=expected_limit,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_constraint_changes_no_archived_downloads(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """No restoration when no archived downloads are available."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 6, 1, tzinfo=UTC)
    db_feed.keep_last = None

    mock_download_db.get_downloads_by_status.return_value = []  # No archived downloads

    log_params = {"feed_id": FEED_ID}
    earlier_since = datetime(2024, 5, 1, tzinfo=UTC)
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=earlier_since,  # Earlier than DB since
        config_keep_last=None,
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync == earlier_since
    mock_download_db.requeue_downloads.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_constraint_changes_database_error(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """Database error during pruning changes is wrapped."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 6, 1, tzinfo=UTC)
    db_feed.keep_last = None

    mock_download_db.get_downloads_by_status.side_effect = DatabaseOperationError(
        "DB error"
    )

    log_params = {"feed_id": FEED_ID}
    with pytest.raises(StateReconciliationError) as exc_info:
        await state_reconciler._handle_constraint_changes(
            FEED_ID,
            config_since=datetime(2024, 5, 1, tzinfo=UTC),
            config_keep_last=None,
            db_feed=db_feed,
            log_params=log_params,
        )

    assert exc_info.value.feed_id == FEED_ID


@pytest.mark.unit
@pytest.mark.asyncio
async def test_keep_last_removed_with_since_sets_sync_to_since(
    state_reconciler: StateReconciler, mock_download_db: MagicMock
) -> None:
    """Removing keep_last sets sync to since."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 6, 1, tzinfo=UTC)
    db_feed.keep_last = 5
    db_feed.total_downloads_internal = 3

    mock_download_db.get_downloads_by_status.return_value = [
        MOCK_ARCHIVED_DOWNLOAD_1,
    ]
    mock_download_db.requeue_downloads.return_value = 1

    log_params = {"feed_id": FEED_ID}
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=datetime(2024, 6, 1, tzinfo=UTC),
        config_keep_last=None,  # removal
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync == datetime(2024, 6, 1, tzinfo=UTC)
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=datetime(2024, 6, 1, tzinfo=UTC),
        limit=-1,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_keep_last_increase_with_since_sets_sync_to_since(
    state_reconciler: StateReconciler, mock_download_db: MagicMock
) -> None:
    """Increasing keep_last sets sync to since."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 6, 1, tzinfo=UTC)
    db_feed.keep_last = 3
    db_feed.total_downloads_internal = 3

    mock_download_db.get_downloads_by_status.return_value = [
        MOCK_ARCHIVED_DOWNLOAD_1,
    ]
    mock_download_db.requeue_downloads.return_value = 1

    log_params = {"feed_id": FEED_ID}
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=datetime(2024, 6, 1, tzinfo=UTC),
        config_keep_last=5,  # increase
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync == datetime(2024, 6, 1, tzinfo=UTC)
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=datetime(2024, 6, 1, tzinfo=UTC),
        limit=2,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_constraint_changes_keep_last_decrease_no_restoration(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """Decreasing keep_last should not trigger restoration or change sync."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = None
    db_feed.keep_last = 10
    db_feed.total_downloads_internal = 8

    log_params = {"feed_id": FEED_ID}
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=None,  # unchanged
        config_keep_last=5,  # decrease from 10 to 5
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync is None
    mock_download_db.get_downloads_by_status.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_keep_last_removed_without_since_sets_sync_to_min(
    state_reconciler: StateReconciler, mock_download_db: MagicMock
) -> None:
    """Removing keep_last with no since set should baseline sync to MIN_SYNC_DATE."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = None
    db_feed.keep_last = 5
    db_feed.total_downloads_internal = 3

    mock_download_db.get_downloads_by_status.return_value = []

    log_params = {"feed_id": FEED_ID}
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=None,
        config_keep_last=None,  # removal
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync == MIN_SYNC_DATE
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=None,
        limit=-1,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_keep_last_increase_no_archived_no_since_sets_sync_to_min(
    state_reconciler: StateReconciler, mock_download_db: MagicMock
) -> None:
    """Increasing keep_last with no since and no archived items still baselines sync to MIN."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = None
    db_feed.keep_last = 3
    db_feed.total_downloads_internal = 3

    mock_download_db.get_downloads_by_status.return_value = []

    log_params = {"feed_id": FEED_ID}
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=None,
        config_keep_last=5,  # increase
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync == MIN_SYNC_DATE
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=None,
        limit=2,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_keep_last_increase_no_archived_with_since_sets_sync_to_since(
    state_reconciler: StateReconciler, mock_download_db: MagicMock
) -> None:
    """Increasing keep_last with since and no archived items baselines sync to since."""
    db_feed = deepcopy(MOCK_FEED)
    since_date = datetime(2024, 6, 1, tzinfo=UTC)
    db_feed.since = since_date
    db_feed.keep_last = 3
    db_feed.total_downloads_internal = 3

    mock_download_db.get_downloads_by_status.return_value = []

    log_params = {"feed_id": FEED_ID}
    new_sync = await state_reconciler._handle_constraint_changes(
        FEED_ID,
        config_since=since_date,
        config_keep_last=5,  # increase
        db_feed=db_feed,
        log_params=log_params,
    )

    assert new_sync == since_date
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=since_date,
        limit=2,
    )


# --- Tests for StateReconciler._handle_image_url_changes ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_image_url_changes_override_added(
    state_reconciler: StateReconciler,
    mock_image_downloader: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """Config provides new image URL override."""
    # Setup existing feed without image override
    db_feed = deepcopy(MOCK_FEED)
    db_feed.remote_image_url = None

    # Setup config with new image URL
    config = deepcopy(base_feed_config)
    config.metadata = FeedMetadataOverrides(  # type: ignore # fields default to None
        image_url="https://example.com/new-image.jpg"
    )

    # Setup updated feed with the new URL
    updated_feed = deepcopy(db_feed)
    updated_feed.remote_image_url = "https://example.com/new-image.jpg"

    mock_image_downloader.download_feed_image_direct.return_value = "jpg"

    result = await state_reconciler._handle_image_url_changes(
        FEED_ID, config, db_feed, updated_feed
    )

    assert result == "jpg"
    mock_image_downloader.download_feed_image_direct.assert_called_once_with(
        FEED_ID, "https://example.com/new-image.jpg"
    )
    mock_image_downloader.download_feed_image_ytdlp.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_image_url_changes_override_changed(
    state_reconciler: StateReconciler,
    mock_image_downloader: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """Config changes existing image URL override."""
    # Setup existing feed with old image override
    db_feed = deepcopy(MOCK_FEED)
    db_feed.remote_image_url = "https://example.com/old-image.jpg"

    # Setup config with different image URL
    config = deepcopy(base_feed_config)
    config.metadata = FeedMetadataOverrides(  # type: ignore # fields default to None
        image_url="https://example.com/new-image.jpg",
    )

    # Setup updated feed with the new URL
    updated_feed = deepcopy(db_feed)
    updated_feed.remote_image_url = "https://example.com/new-image.jpg"

    mock_image_downloader.download_feed_image_direct.return_value = "jpg"

    result = await state_reconciler._handle_image_url_changes(
        FEED_ID, config, db_feed, updated_feed
    )

    assert result == "jpg"
    mock_image_downloader.download_feed_image_direct.assert_called_once_with(
        FEED_ID, "https://example.com/new-image.jpg"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_image_url_changes_override_removed_with_natural_url(
    state_reconciler: StateReconciler,
    mock_image_downloader: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """Config removes image override, reverting to natural feed image."""
    # Setup existing feed with image override
    db_feed = deepcopy(MOCK_FEED)
    db_feed.remote_image_url = "https://example.com/override.jpg"

    # Setup config without image URL (None)
    config = deepcopy(base_feed_config)
    config.metadata = None

    # Setup updated feed with natural URL (different from DB override)
    updated_feed = deepcopy(db_feed)
    updated_feed.remote_image_url = "https://example.com/natural.jpg"

    mock_image_downloader.download_feed_image_ytdlp.return_value = "jpg"

    result = await state_reconciler._handle_image_url_changes(
        FEED_ID, config, db_feed, updated_feed
    )

    assert result == "jpg"
    mock_image_downloader.download_feed_image_ytdlp.assert_called_once_with(
        feed_id=FEED_ID,
        source_type=updated_feed.source_type,
        source_url=updated_feed.source_url,
        resolved_url=updated_feed.resolved_url,
        user_yt_cli_args=config.yt_args,
        cookies_path=None,
    )
    mock_image_downloader.download_feed_image_direct.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_image_url_changes_no_change_needed(
    state_reconciler: StateReconciler,
    mock_image_downloader: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """No image URL changes needed."""
    # Setup existing feed with image URL
    db_feed = deepcopy(MOCK_FEED)
    db_feed.remote_image_url = "https://example.com/image.jpg"

    # Setup config with same image URL
    config = deepcopy(base_feed_config)
    config.metadata = FeedMetadataOverrides(  # type: ignore # fields default to None
        image_url="https://example.com/image.jpg",
    )

    # Setup updated feed with same URL
    updated_feed = deepcopy(db_feed)
    updated_feed.remote_image_url = "https://example.com/image.jpg"

    result = await state_reconciler._handle_image_url_changes(
        FEED_ID, config, db_feed, updated_feed
    )

    assert result is None
    mock_image_downloader.download_feed_image_direct.assert_not_called()
    mock_image_downloader.download_feed_image_ytdlp.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_image_url_changes_override_removed_same_natural_url(
    state_reconciler: StateReconciler,
    mock_image_downloader: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """Config removes override but natural URL same as DB - no action needed."""
    # Setup existing feed with image override
    db_feed = deepcopy(MOCK_FEED)
    db_feed.remote_image_url = "https://example.com/natural.jpg"

    # Setup config without image URL (None)
    config = deepcopy(base_feed_config)
    config.metadata = None

    # Setup updated feed with same natural URL as DB
    updated_feed = deepcopy(db_feed)
    updated_feed.remote_image_url = "https://example.com/natural.jpg"

    result = await state_reconciler._handle_image_url_changes(
        FEED_ID, config, db_feed, updated_feed
    )

    assert result is None
    mock_image_downloader.download_feed_image_direct.assert_not_called()
    mock_image_downloader.download_feed_image_ytdlp.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_image_url_changes_direct_download_failure(
    state_reconciler: StateReconciler,
    mock_image_downloader: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """Direct image download fails gracefully."""
    # Setup config with new image URL
    config = deepcopy(base_feed_config)
    config.metadata = FeedMetadataOverrides(
        image_url="https://example.com/image.jpg",
        title=None,
        subtitle=None,
        description=None,
        language=None,
        category=None,
        podcast_type=None,
        explicit=None,
        author=None,
        author_email=None,
    )

    db_feed = deepcopy(MOCK_FEED)
    updated_feed = deepcopy(db_feed)
    updated_feed.remote_image_url = "https://example.com/image.jpg"

    mock_image_downloader.download_feed_image_direct.side_effect = ImageDownloadError(
        "Download failed"
    )

    result = await state_reconciler._handle_image_url_changes(
        FEED_ID, config, db_feed, updated_feed
    )

    assert result is None
    mock_image_downloader.download_feed_image_direct.assert_called_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_image_url_changes_ytdlp_download_failure(
    state_reconciler: StateReconciler,
    mock_image_downloader: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """yt-dlp image download fails gracefully."""
    # Setup existing feed with override
    db_feed = deepcopy(MOCK_FEED)
    db_feed.remote_image_url = "https://example.com/override.jpg"

    # Config removes override
    config = deepcopy(base_feed_config)
    config.metadata = None

    # Natural URL is different
    updated_feed = deepcopy(db_feed)
    updated_feed.remote_image_url = "https://example.com/natural.jpg"

    mock_image_downloader.download_feed_image_ytdlp.side_effect = ImageDownloadError(
        "yt-dlp failed"
    )

    result = await state_reconciler._handle_image_url_changes(
        FEED_ID, config, db_feed, updated_feed
    )

    assert result is None
    mock_image_downloader.download_feed_image_ytdlp.assert_called_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_image_url_changes_direct_download_error(
    state_reconciler: StateReconciler,
    mock_image_downloader: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """Direct image download failures bubble up as ImageDownloadError and are handled."""
    # Setup config with new image URL
    image_url = "https://example.com/image.jpg"
    config = deepcopy(base_feed_config)
    config.metadata = FeedMetadataOverrides(  # type: ignore # fields default to None
        image_url=image_url,
    )

    db_feed = deepcopy(MOCK_FEED)
    updated_feed = deepcopy(db_feed)
    updated_feed.remote_image_url = image_url

    mock_image_downloader.download_feed_image_direct.side_effect = ImageDownloadError(
        "download failed",
        feed_id=FEED_ID,
        url=image_url,
    )

    result = await state_reconciler._handle_image_url_changes(
        FEED_ID, config, db_feed, updated_feed
    )

    assert result is None
    mock_image_downloader.download_feed_image_direct.assert_called_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_image_url_changes_ytdlp_download_returns_none(
    state_reconciler: StateReconciler,
    mock_image_downloader: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """YT-DLP image download returns None (no image found)."""
    # Setup existing feed with override
    db_feed = deepcopy(MOCK_FEED)
    db_feed.remote_image_url = "https://example.com/override.jpg"

    # Config removes override
    config = deepcopy(base_feed_config)
    config.metadata = None

    # Natural URL is different
    updated_feed = deepcopy(db_feed)
    updated_feed.remote_image_url = "https://example.com/natural.jpg"

    mock_image_downloader.download_feed_image_ytdlp.return_value = None

    result = await state_reconciler._handle_image_url_changes(
        FEED_ID, config, db_feed, updated_feed
    )

    assert result is None
    mock_image_downloader.download_feed_image_ytdlp.assert_called_once()


# --- Tests for StateReconciler.reconcile_startup_state ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_reconcile_startup_state_all_scenarios(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    mock_download_db: MagicMock,
    mock_pruner: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """Full startup reconciliation with new, existing, and removed feeds."""
    # Setup: 1 existing feed, 1 removed feed in DB
    mock_feed_db.get_feeds.return_value = [MOCK_FEED, MOCK_DISABLED_FEED]

    # Setup config: 1 existing feed (updated), 1 new feed
    config_feeds = {
        FEED_ID: base_feed_config,  # Existing
        NEW_FEED_ID: FeedConfig(
            url=NEW_FEED_URL,
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=None,
            since=None,
            yt_args=None,  # type: ignore
            metadata=None,
        ),  # New
    }

    # Mock behaviors
    mock_download_db.count_downloads_by_status.return_value = 0
    mock_download_db.get_downloads_by_status.return_value = []

    def _mock_fetch_playlist_metadata(*, source_url: str, **_: object) -> Feed:
        feed = deepcopy(MOCK_FEED)
        feed.source_url = source_url
        return feed

    mock_ytdlp_wrapper.fetch_playlist_metadata.side_effect = (
        _mock_fetch_playlist_metadata
    )

    # Execute
    ready_feeds = await state_reconciler.reconcile_startup_state(config_feeds)

    # Verify results
    assert set(ready_feeds) == {FEED_ID, NEW_FEED_ID}

    # Verify new feed was inserted
    assert mock_feed_db.upsert_feed.call_count >= 1
    new_feed_call = None
    for call_args in mock_feed_db.upsert_feed.call_args_list:
        feed = call_args[0][0]
        if feed.id == NEW_FEED_ID:
            new_feed_call = feed
            break
    assert new_feed_call is not None
    assert new_feed_call.source_url == NEW_FEED_URL

    # Verify removed feed was archived
    mock_pruner.archive_feed.assert_called_once_with(REMOVED_FEED_ID)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_reconcile_startup_state_database_error(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
) -> None:
    """Database error during startup fetch is fatal."""
    mock_feed_db.get_feeds.side_effect = DatabaseOperationError("DB error")

    with pytest.raises(StateReconciliationError):
        await state_reconciler.reconcile_startup_state({})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_reconcile_startup_state_disabled_feeds_not_ready(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """Disabled feeds are not included in ready list."""
    mock_feed_db.get_feeds.return_value = [MOCK_FEED]

    disabled_config = deepcopy(base_feed_config)
    disabled_config.enabled = False

    config_feeds = {FEED_ID: disabled_config}

    ready_feeds = await state_reconciler.reconcile_startup_state(config_feeds)

    assert ready_feeds == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_reconcile_startup_state_continues_on_individual_errors(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    base_feed_config: FeedConfig,
) -> None:
    """Individual feed errors don't stop overall reconciliation."""
    mock_feed_db.get_feeds.return_value = []

    # First feed will fail, second should succeed
    config_feeds = {
        "failing_feed": base_feed_config,
        NEW_FEED_ID: FeedConfig(
            url=NEW_FEED_URL,
            schedule=TEST_CRON_SCHEDULE,  # type: ignore
            enabled=True,
            keep_last=None,
            since=None,
            yt_args=None,  # type: ignore
            metadata=None,
        ),
    }

    # Make first insert fail, second succeed
    mock_feed_db.upsert_feed.side_effect = [
        DatabaseOperationError("Insert failed"),
        None,
    ]

    ready_feeds = await state_reconciler.reconcile_startup_state(config_feeds)

    # Only the successful feed should be ready
    assert ready_feeds == [NEW_FEED_ID]
    assert mock_feed_db.upsert_feed.call_count == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_manual_feed_not_scheduled_but_inserted(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
) -> None:
    """Manual feeds skip discovery and are excluded from scheduler."""
    mock_feed_db.get_feeds.return_value = []
    manual_config = FeedConfig(
        url=None,
        schedule="manual",  # type: ignore[arg-type]
        metadata=FeedMetadataOverrides(title="Manual Feed"),
    )

    ready = await state_reconciler.reconcile_startup_state({"manual": manual_config})

    assert ready == []
    mock_ytdlp_wrapper.discover_feed_properties.assert_not_called()
    inserted_feed = mock_feed_db.upsert_feed.await_args[0][0]
    assert inserted_feed.id == "manual"
    assert inserted_feed.source_type == SourceType.MANUAL
    assert inserted_feed.source_url is None
    assert inserted_feed.title == "Manual Feed"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_feed_metadata_manual_skips_discovery(
    state_reconciler: StateReconciler,
    mock_ytdlp_wrapper: MagicMock,
) -> None:
    """Manual feeds synthesize metadata without yt-dlp discovery."""
    manual_config = FeedConfig(
        url=None,
        schedule="manual",  # type: ignore[arg-type]
        metadata=FeedMetadataOverrides(title="Manual Feed", description="Desc"),
    )

    result = await state_reconciler._fetch_feed_metadata(
        "manual", manual_config, None, cookies_path=None
    )

    assert result.source_type == SourceType.MANUAL
    assert result.title == "Manual Feed"
    mock_ytdlp_wrapper.discover_feed_properties.assert_not_called()
    mock_ytdlp_wrapper.fetch_playlist_metadata.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_existing_manual_feed_updates_metadata_and_image(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    mock_image_downloader: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
) -> None:
    """Existing manual feeds update metadata and image via overrides."""
    manual_config = FeedConfig(
        url=None,
        schedule="manual",  # type: ignore[arg-type]
        metadata=FeedMetadataOverrides(
            title="Updated Manual Feed",
            description="Fresh description",
            image_url="https://example.com/manual-new.jpg",
        ),
    )
    db_feed = Feed(
        id="manual",
        title="Old Manual Feed",
        subtitle=None,
        description="Old description",
        language=None,
        author=None,
        author_email=None,
        remote_image_url="https://example.com/manual-old.jpg",
        image_ext="png",
        is_enabled=True,
        source_type=SourceType.MANUAL,
        source_url=None,
        resolved_url=None,
        last_successful_sync=BASE_TIME,
        since=None,
        keep_last=None,
    )
    mock_image_downloader.download_feed_image_direct.return_value = "jpg"

    result = await state_reconciler._handle_existing_feed(
        "manual", manual_config, db_feed
    )

    assert result is True
    mock_image_downloader.download_feed_image_direct.assert_called_once_with(
        "manual", "https://example.com/manual-new.jpg"
    )
    updated_feed = mock_feed_db.upsert_feed.await_args[0][0]
    assert updated_feed.title == "Updated Manual Feed"
    assert updated_feed.description == "Fresh description"
    assert updated_feed.image_ext == "jpg"
    mock_ytdlp_wrapper.discover_feed_properties.assert_not_called()
    mock_ytdlp_wrapper.fetch_playlist_metadata.assert_not_called()
