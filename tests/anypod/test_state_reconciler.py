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
    PruneError,
    StateReconciliationError,
)
from anypod.state_reconciler import StateReconciler
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
    image_url=None,
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
    image_url=None,
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
    # Configure discover_feed_properties to return a valid tuple
    mock.discover_feed_properties = AsyncMock(return_value=(SourceType.UNKNOWN, None))
    return mock


@pytest.fixture
def state_reconciler(
    mock_feed_db: MagicMock,
    mock_download_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    mock_pruner: MagicMock,
) -> StateReconciler:
    """Provides a StateReconciler instance with mocked dependencies."""
    return StateReconciler(
        mock_feed_db, mock_download_db, mock_ytdlp_wrapper, mock_pruner
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
            categories=PodcastCategories("Technology"),
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
    assert inserted_feed.image_url == metadata.image_url
    assert inserted_feed.category == metadata.categories
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
    base_feed_config: FeedConfig,
) -> None:
    """URL change resets error state."""
    existing_feed = deepcopy(MOCK_FEED)
    existing_feed.source_url = "https://old.example.com/feed"
    existing_feed.consecutive_failures = 2

    config = deepcopy(base_feed_config)
    config.url = "https://new.example.com/feed"

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
    assert updated_feed.image_url == metadata.image_url


# --- Tests for StateReconciler._handle_pruning_changes ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_pruning_changes_no_changes(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """No changes to retention policies returns False."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 6, 1, tzinfo=UTC)
    db_feed.keep_last = 10

    log_params = {"feed_id": FEED_ID}
    result = await state_reconciler._handle_pruning_changes(
        FEED_ID,
        config_since=datetime(2024, 6, 1, tzinfo=UTC),  # Same as DB
        config_keep_last=10,  # Same as DB
        db_feed=db_feed,
        log_params=log_params,
    )

    assert result is False
    mock_download_db.get_downloads_by_status.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_pruning_changes_since_expansion_only(
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
    result = await state_reconciler._handle_pruning_changes(
        FEED_ID,
        config_since=datetime(2024, 5, 1, tzinfo=UTC),  # Earlier than DB since
        config_keep_last=None,
        db_feed=db_feed,
        log_params=log_params,
    )

    assert result is True
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=datetime(2024, 5, 1, tzinfo=UTC),
        limit=-1,  # No keep_last limit
    )
    mock_download_db.requeue_downloads.assert_awaited_once_with(
        FEED_ID, ["archived_1", "archived_2"], from_status=DownloadStatus.ARCHIVED
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_pruning_changes_since_removal_only(
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
    result = await state_reconciler._handle_pruning_changes(
        FEED_ID,
        config_since=None,  # Removing since filter
        config_keep_last=None,
        db_feed=db_feed,
        log_params=log_params,
    )

    assert result is True
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=None,  # No date filter
        limit=-1,  # No keep_last limit
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_pruning_changes_since_stricter_no_restoration(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """Making 'since' filter stricter does not restore downloads."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 5, 1, tzinfo=UTC)
    db_feed.keep_last = None

    log_params = {"feed_id": FEED_ID}
    result = await state_reconciler._handle_pruning_changes(
        FEED_ID,
        config_since=datetime(2024, 7, 1, tzinfo=UTC),  # Later than DB since
        config_keep_last=None,
        db_feed=db_feed,
        log_params=log_params,
    )

    assert result is False
    mock_download_db.get_downloads_by_status.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_pruning_changes_keep_last_increase_only(
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
    result = await state_reconciler._handle_pruning_changes(
        FEED_ID,
        config_since=None,
        config_keep_last=10,  # Increase from 5 to 10
        db_feed=db_feed,
        log_params=log_params,
    )

    assert result is True
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=None,  # No since filter
        limit=5,  # 10 - 5 available slots
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_pruning_changes_since_removal_with_keep_last_limit(
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
    result = await state_reconciler._handle_pruning_changes(
        FEED_ID,
        config_since=None,  # Remove since filter
        config_keep_last=8,  # But keep_last limits restoration
        db_feed=db_feed,
        log_params=log_params,
    )

    assert result is True
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=None,  # No date filter
        limit=3,  # 8 - 5 available slots
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_pruning_changes_since_expansion_blocked_by_keep_last(
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
    result = await state_reconciler._handle_pruning_changes(
        FEED_ID,
        config_since=datetime(2024, 5, 1, tzinfo=UTC),  # Earlier than DB since
        config_keep_last=5,  # Already at limit
        db_feed=db_feed,
        log_params=log_params,
    )

    assert result is False
    mock_download_db.get_downloads_by_status.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_pruning_changes_both_policies_change(
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
    result = await state_reconciler._handle_pruning_changes(
        FEED_ID,
        config_since=datetime(2024, 5, 1, tzinfo=UTC),  # Earlier than DB since
        config_keep_last=8,  # Increase from 5 to 8
        db_feed=db_feed,
        log_params=log_params,
    )

    assert result is True
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=datetime(2024, 5, 1, tzinfo=UTC),  # Since filter applied
        limit=4,  # 8 - 4 available slots
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_pruning_changes_since_expansion_limited_by_new_keep_last(
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
    result = await state_reconciler._handle_pruning_changes(
        FEED_ID,
        config_since=datetime(2024, 7, 1, tzinfo=UTC),  # Expand since further back
        config_keep_last=12,  # Add keep_last allowing only 2 more (12-10=2)
        db_feed=db_feed,
        log_params=log_params,
    )

    assert result is True
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.ARCHIVED,
        feed_id=FEED_ID,
        published_after=datetime(2024, 7, 1, tzinfo=UTC),  # Since filter applied
        limit=2,  # Limited by keep_last: 12 - 10 = 2
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_pruning_changes_no_archived_downloads(
    state_reconciler: StateReconciler,
    mock_download_db: MagicMock,
) -> None:
    """No restoration when no archived downloads are available."""
    db_feed = deepcopy(MOCK_FEED)
    db_feed.since = datetime(2024, 6, 1, tzinfo=UTC)
    db_feed.keep_last = None

    mock_download_db.get_downloads_by_status.return_value = []  # No archived downloads

    log_params = {"feed_id": FEED_ID}
    result = await state_reconciler._handle_pruning_changes(
        FEED_ID,
        config_since=datetime(2024, 5, 1, tzinfo=UTC),  # Earlier than DB since
        config_keep_last=None,
        db_feed=db_feed,
        log_params=log_params,
    )

    assert result is False
    mock_download_db.requeue_downloads.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_pruning_changes_database_error(
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
        await state_reconciler._handle_pruning_changes(
            FEED_ID,
            config_since=datetime(2024, 5, 1, tzinfo=UTC),
            config_keep_last=None,
            db_feed=db_feed,
            log_params=log_params,
        )

    assert exc_info.value.feed_id == FEED_ID


# --- Tests for StateReconciler.reconcile_startup_state ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_reconcile_startup_state_all_scenarios(
    state_reconciler: StateReconciler,
    mock_feed_db: MagicMock,
    mock_download_db: MagicMock,
    mock_pruner: MagicMock,
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
