# pyright: reportPrivateUsage=false

"""Tests for the Enqueuer service and its download queue management."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from anypod.config import FeedConfig
from anypod.config.types import (
    FeedMetadataOverrides,
    PodcastCategories,
    PodcastExplicit,
)
from anypod.data_coordinator.enqueuer import Enqueuer
from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.exceptions import (
    DatabaseOperationError,
    DownloadNotFoundError,
    EnqueueError,
    YtdlpApiError,
)
from anypod.ytdlp_wrapper.ytdlp_wrapper import YtdlpWrapper

FEED_ID = "test_feed"
FEED_URL = "https://example.com/feed"
DEFAULT_MAX_ERRORS = 3

# Mock Feed object for testing
MOCK_FEED = Feed(
    id=FEED_ID,
    title="Test Feed",
    subtitle=None,
    description=None,
    language=None,
    author=None,
    image_url=None,
    is_enabled=True,
    source_type=SourceType.UNKNOWN,
    source_url="https://example.com/test",
    last_successful_sync=datetime.min.replace(tzinfo=UTC),
)


@pytest.fixture
def mock_feed_db() -> MagicMock:
    """Provides a MagicMock for FeedDatabase."""
    mock = MagicMock(spec=FeedDatabase)
    # Mock async methods
    mock.get_feed_by_id = AsyncMock()
    mock.update_feed_metadata = AsyncMock()
    return mock


@pytest.fixture
def mock_download_db() -> MagicMock:
    """Provides a MagicMock for DownloadDatabase."""
    mock = MagicMock(spec=DownloadDatabase)
    # Mock async methods
    mock.get_downloads_by_status = AsyncMock()
    mock.get_download_by_id = AsyncMock()
    mock.upsert_download = AsyncMock()
    mock.mark_as_queued_from_upcoming = AsyncMock()
    mock.bump_retries = AsyncMock()
    return mock


@pytest.fixture
def mock_ytdlp_wrapper() -> MagicMock:
    """Provides a MagicMock for YtdlpWrapper."""
    mock_feed = Feed(
        id=FEED_ID,
        title="Extracted Feed Title",
        subtitle=None,
        description="Extracted description",
        language="en",
        author="Extracted Author",
        author_email=None,
        image_url=None,
        is_enabled=True,
        source_type=SourceType.UNKNOWN,
        source_url=FEED_URL,
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
    )

    mock = MagicMock(spec=YtdlpWrapper)
    mock.fetch_metadata = AsyncMock(return_value=(mock_feed, []))
    return mock


@pytest.fixture
def sample_feed_config() -> FeedConfig:
    """Provides a sample FeedConfig."""
    return FeedConfig(
        url=FEED_URL,
        schedule="* * * * *",
        yt_args="",  # type: ignore # this gets preprocessed into a dict
        max_errors=DEFAULT_MAX_ERRORS,
        keep_last=None,
        since=None,
    )


@pytest.fixture
def enqueuer(
    mock_feed_db: MagicMock,
    mock_download_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
) -> Enqueuer:
    """Provides an Enqueuer instance with mocked dependencies."""
    return Enqueuer(
        feed_db=mock_feed_db,
        download_db=mock_download_db,
        ytdlp_wrapper=mock_ytdlp_wrapper,
    )


def create_download(
    id: str,
    status: DownloadStatus,
    feed_id: str = FEED_ID,
    published_offset_days: int = 0,
    title: str | None = None,
    source_url: str | None = None,
    ext: str = "mp4",
    duration: int = 120,
    retries: int = 0,
    mime_type: str | None = None,
    filesize: int | None = None,
) -> Download:
    """Helper function to create Download objects for tests."""
    # Determine default mime_type based on ext if not provided
    if mime_type is None:
        match ext:
            case "live":
                mime_type = "application/octet-stream"
            case "mp4":
                mime_type = "video/mp4"
            case "mp3":
                mime_type = "audio/mpeg"
            case _:
                mime_type = "video/mp4"  # fallback

    # Determine default filesize based on status if not provided
    if filesize is None:
        filesize = 1024000 if status == DownloadStatus.DOWNLOADED else 0

    current_time = datetime.now(UTC)
    return Download(
        feed_id=feed_id,
        id=id,
        source_url=source_url or f"https://example.com/video/{id}",
        title=title or f"Test Video {id}",
        published=current_time - timedelta(days=published_offset_days),
        ext=ext,
        mime_type=mime_type,
        filesize=filesize,
        duration=duration,
        status=status,
        discovered_at=current_time,
        updated_at=current_time,
        retries=retries,
    )


FETCH_SINCE_DATE = datetime.now(UTC) - timedelta(days=1)
FETCH_UNTIL_DATE = datetime.now(UTC)


# --- Tests for Enqueuer._synchronize_feed_metadata ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_synchronize_feed_metadata_handles_removed_overrides(
    enqueuer: Enqueuer,
    mock_feed_db: MagicMock,
):
    """Test that feed metadata sync correctly handles when overrides are removed from config."""
    # Current feed in database has override values that were previously set
    current_feed_with_overrides = Feed(
        id=FEED_ID,
        title="Previously Override Title",
        subtitle="Previously Override Subtitle",
        description="Previously override description",
        language="en-US",
        author="Override Author",
        image_url="https://example.com/override.jpg",
        category=PodcastCategories(["Technology", "Science"]),
        explicit=PodcastExplicit.YES,
        is_enabled=True,
        source_type=SourceType.UNKNOWN,
        source_url="https://example.com/test",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
    )

    # Fetched feed metadata from ytdlp (what we'd get from the source)
    fetched_feed = Feed(
        id=FEED_ID,
        title="Source Title",
        subtitle=None,  # No subtitle in source
        description="Source description",
        language=None,  # No language in source
        author="Source Author",
        image_url="https://example.com/source.jpg",
        category=PodcastCategories("TV & Film"),  # No category in source, use default
        explicit=PodcastExplicit.NO,  # No explicit flag in source
        is_enabled=True,
        source_type=SourceType.UNKNOWN,
        source_url="https://example.com/test",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
    )

    # Feed config with NO metadata overrides (user removed them)
    feed_config_no_overrides = FeedConfig(
        url=FEED_URL,
        schedule="* * * * *",  # type: ignore
        yt_args="",  # type: ignore
        max_errors=3,
        keep_last=None,
        since=None,
        metadata=None,  # No overrides
    )

    await enqueuer._synchronize_feed_metadata(
        current_feed_with_overrides,
        fetched_feed,
        feed_config_no_overrides,
        {"feed_id": FEED_ID},
    )

    # Verify update_feed_metadata was called with the right changes
    mock_feed_db.update_feed_metadata.assert_awaited_once()
    call_args = mock_feed_db.update_feed_metadata.call_args

    # Should update to source values, clearing override fields that have no source equivalent
    expected_updates = {
        "title": "Source Title",  # Override -> Source value
        "description": "Source description",  # Override -> Source value
        "author": "Source Author",  # Override -> Source value
        "image_url": "https://example.com/source.jpg",  # Override -> Source value
        "category": PodcastCategories("TV & Film"),  # Override -> default (cleared)
        "explicit": PodcastExplicit.NO,  # Override -> default (cleared)
    }

    assert call_args[0][0] == FEED_ID  # First positional arg is feed_id
    actual_updates = call_args[1]  # Keyword arguments
    assert actual_updates == expected_updates


@pytest.mark.unit
@pytest.mark.asyncio
async def test_synchronize_feed_metadata_handles_partial_override_removal(
    enqueuer: Enqueuer,
    mock_feed_db: MagicMock,
):
    """Test metadata sync when some overrides are removed but others remain."""
    # Current feed in database has multiple override values
    current_feed = Feed(
        id=FEED_ID,
        title="Override Title",
        subtitle="Override Subtitle",
        description="Override description",
        language="en-US",
        author="Override Author",
        image_url="https://example.com/override.jpg",
        category=PodcastCategories(["Technology"]),
        explicit=PodcastExplicit.YES,
        is_enabled=True,
        source_type=SourceType.UNKNOWN,
        source_url="https://example.com/test",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
    )

    # Fetched metadata from source
    fetched_feed = Feed(
        id=FEED_ID,
        title="Source Title",
        subtitle=None,
        description="Source description",
        language=None,
        author="Source Author",
        image_url="https://example.com/source.jpg",
        category=PodcastCategories("TV & Film"),
        explicit=PodcastExplicit.NO,
        is_enabled=True,
        source_type=SourceType.UNKNOWN,
        source_url="https://example.com/test",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
    )

    # Feed config with PARTIAL overrides (user removed some but kept others)
    partial_overrides = FeedMetadataOverrides(  # type: ignore
        title="Keep Override Title",  # Still overridden
        author="Keep Override Author",  # Still overridden
        # subtitle, description, language, image_url, category, explicit removed
    )

    feed_config_partial = FeedConfig(
        url=FEED_URL,
        schedule="* * * * *",  # type: ignore
        yt_args="",  # type: ignore
        max_errors=3,
        keep_last=None,
        since=None,
        metadata=partial_overrides,
    )

    await enqueuer._synchronize_feed_metadata(
        current_feed, fetched_feed, feed_config_partial, {"feed_id": FEED_ID}
    )

    # Verify the right mix of overrides and source values
    mock_feed_db.update_feed_metadata.assert_awaited_once()
    call_args = mock_feed_db.update_feed_metadata.call_args

    expected_updates = {
        "title": "Keep Override Title",  # Still overridden
        "description": "Source description",  # Now from source
        "author": "Keep Override Author",  # Still overridden
        "image_url": "https://example.com/source.jpg",  # Now from source
        "category": PodcastCategories(
            "TV & Film"
        ),  # Cleared (was override, now source has default)
        "explicit": PodcastExplicit.NO,  # Cleared (was override, now source has value)
    }

    assert call_args[0][0] == FEED_ID
    actual_updates = call_args[1]
    assert actual_updates == expected_updates


@pytest.mark.unit
@pytest.mark.asyncio
async def test_synchronize_feed_metadata_preserves_source_type_from_fetched_feed(
    enqueuer: Enqueuer,
    mock_feed_db: MagicMock,
):
    """Test that feed metadata sync includes source_type from fetched feed metadata."""
    # Current feed in database has UNKNOWN source_type

    current_feed = Feed(
        id=FEED_ID,
        title="Test Feed",
        subtitle=None,
        description=None,
        language=None,
        author=None,
        image_url=None,
        category=PodcastCategories("TV & Film"),
        explicit=PodcastExplicit.NO,
        is_enabled=True,
        source_type=SourceType.UNKNOWN,
        source_url="https://example.com/test",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        since=None,
        keep_last=None,
    )

    # Fetched feed metadata has correct source_type from discovery process
    fetched_feed = Feed(
        id=FEED_ID,
        title="Channel Title from YouTube",
        subtitle=None,
        description="Channel description from YouTube",
        language=None,
        author="Channel Author from YouTube",
        image_url="https://yt3.googleusercontent.com/channel_image.jpg",
        category=PodcastCategories("TV & Film"),
        explicit=PodcastExplicit.NO,
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://example.com/test",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        since=None,
        keep_last=None,
    )

    # Feed config with no metadata overrides
    feed_config_no_overrides = FeedConfig(
        url=FEED_URL,
        schedule="* * * * *",  # type: ignore
        yt_args="",  # type: ignore
        max_errors=3,
        keep_last=None,
        since=None,
        metadata=None,  # No overrides
    )

    await enqueuer._synchronize_feed_metadata(
        current_feed, fetched_feed, feed_config_no_overrides, {"feed_id": FEED_ID}
    )

    # Verify update_feed_metadata was called with the right changes
    mock_feed_db.update_feed_metadata.assert_awaited_once()
    call_args = mock_feed_db.update_feed_metadata.call_args

    expected_updates = {
        "title": fetched_feed.title,
        "description": fetched_feed.description,
        "author": fetched_feed.author,
        "image_url": fetched_feed.image_url,
    }

    assert call_args[0][0] == FEED_ID
    actual_updates = call_args[1]
    assert actual_updates == expected_updates


@pytest.mark.unit
@pytest.mark.asyncio
async def test_synchronize_feed_metadata_preserves_source_type_with_metadata_overrides(
    enqueuer: Enqueuer,
    mock_feed_db: MagicMock,
):
    """Test that source_type is preserved even when metadata overrides are present."""
    # Current feed in database
    current_feed = Feed(
        id=FEED_ID,
        title="Override Title",
        subtitle="Override Subtitle",
        description="Override description",
        language="en-US",
        author="Override Author",
        image_url="https://example.com/override.jpg",
        category=PodcastCategories(["Technology"]),
        explicit=PodcastExplicit.YES,
        is_enabled=True,
        source_type=SourceType.UNKNOWN,  # Bug: should be updated to CHANNEL
        source_url="https://example.com/test",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        since=datetime(2024, 1, 1, tzinfo=UTC),
        keep_last=10,
    )

    # Fetched feed metadata with correct source_type
    fetched_feed = Feed(
        id=FEED_ID,
        title="Source Title",
        subtitle=None,
        description="Source description",
        language=None,
        author="Source Author",
        image_url="https://example.com/source.jpg",
        category=PodcastCategories("TV & Film"),
        explicit=PodcastExplicit.NO,
        is_enabled=True,
        source_type=SourceType.CHANNEL,  # Should be preserved
        source_url="https://example.com/test",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        since=datetime(2024, 1, 1, tzinfo=UTC),
        keep_last=15,  # Different keep_last to test config precedence
    )

    # Feed config with metadata overrides but different config values
    metadata_overrides = FeedMetadataOverrides(  # type: ignore
        title="Keep Override Title",
        description="Keep Override Description",
    )

    feed_config_with_overrides = FeedConfig(
        url=FEED_URL,
        schedule="* * * * *",  # type: ignore
        yt_args="",  # type: ignore
        max_errors=3,
        keep_last=20,  # Config keep_last should take precedence
        since=datetime(2024, 2, 1, tzinfo=UTC),  # Config since should take precedence
        metadata=metadata_overrides,
    )

    await enqueuer._synchronize_feed_metadata(
        current_feed, fetched_feed, feed_config_with_overrides, {"feed_id": FEED_ID}
    )

    # Verify the update includes source_type and respects override hierarchy
    mock_feed_db.update_feed_metadata.assert_awaited_once()
    call_args = mock_feed_db.update_feed_metadata.call_args

    # Only fields that changed from current state should be included
    expected_updates = {
        "title": metadata_overrides.title,
        "description": metadata_overrides.description,
        "author": fetched_feed.author,
        "image_url": fetched_feed.image_url,
        "category": PodcastCategories("TV & Film"),
        "explicit": PodcastExplicit.NO,
    }

    assert call_args[0][0] == FEED_ID
    actual_updates = call_args[1]
    assert actual_updates == expected_updates


# --- Tests for Enqueuer._handle_remaining_upcoming_downloads ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_existing_upcoming_downloads_none_found(
    enqueuer: Enqueuer,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test _handle_remaining_upcoming_downloads when no upcoming downloads are in DB."""
    mock_download_db.get_downloads_by_status.return_value = []

    count = await enqueuer._handle_remaining_upcoming_downloads(
        MOCK_FEED, sample_feed_config
    )
    assert count == 0
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.UPCOMING, feed_id=FEED_ID
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_existing_upcoming_download_transitions_to_queued(
    enqueuer: Enqueuer,
    mock_download_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test an upcoming download that transitions to QUEUED."""
    upcoming_dl = create_download("video1", DownloadStatus.UPCOMING)
    refetched_vod_dl = create_download("video1", DownloadStatus.QUEUED)

    mock_download_db.get_downloads_by_status.return_value = [upcoming_dl]
    mock_ytdlp_wrapper.fetch_metadata.return_value = (MOCK_FEED, [refetched_vod_dl])

    count = await enqueuer._handle_remaining_upcoming_downloads(
        MOCK_FEED, sample_feed_config
    )

    assert count == 1
    mock_ytdlp_wrapper.fetch_metadata.assert_awaited_once_with(
        FEED_ID,
        SourceType.SINGLE_VIDEO,
        upcoming_dl.source_url,
        None,
        sample_feed_config.yt_args,
        cookies_path=None,
    )
    mock_download_db.mark_as_queued_from_upcoming.assert_awaited_once_with(
        FEED_ID, "video1"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_existing_upcoming_download_remains_upcoming(
    enqueuer: Enqueuer,
    mock_download_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test an upcoming download that is still UPCOMING after refetch."""
    upcoming_dl = create_download("video1", DownloadStatus.UPCOMING)
    # Refetched data still shows it as upcoming
    refetched_upcoming_dl = create_download("video1", DownloadStatus.UPCOMING)

    mock_download_db.get_downloads_by_status.return_value = [upcoming_dl]
    mock_ytdlp_wrapper.fetch_metadata.return_value = (
        MOCK_FEED,
        [refetched_upcoming_dl],
    )

    count = await enqueuer._handle_remaining_upcoming_downloads(
        MOCK_FEED, sample_feed_config
    )

    assert count == 0
    mock_ytdlp_wrapper.fetch_metadata.assert_awaited_once_with(
        FEED_ID,
        SourceType.SINGLE_VIDEO,
        upcoming_dl.source_url,
        None,
        sample_feed_config.yt_args,
        cookies_path=None,
    )
    mock_download_db.mark_as_queued_from_upcoming.assert_not_called()
    mock_download_db.requeue_downloads.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_existing_upcoming_download_refetch_fails_bumps_retries(
    enqueuer: Enqueuer,
    mock_download_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test upcoming download refetch failure, leading to retry bump."""
    upcoming_dl = create_download("video1", DownloadStatus.UPCOMING)
    mock_download_db.get_downloads_by_status.return_value = [upcoming_dl]
    mock_ytdlp_wrapper.fetch_metadata.side_effect = YtdlpApiError(
        message="Fetch failed", feed_id=FEED_ID, url=upcoming_dl.source_url
    )
    # Simulate bump_retries not transitioning to ERROR
    mock_download_db.bump_retries.return_value = (1, DownloadStatus.UPCOMING, False)

    count = await enqueuer._handle_remaining_upcoming_downloads(
        MOCK_FEED, sample_feed_config
    )

    assert count == 0
    mock_download_db.bump_retries.assert_awaited_once_with(
        feed_id=FEED_ID,
        download_id="video1",
        error_message="Failed to re-fetch metadata for upcoming download.",
        max_allowed_errors=sample_feed_config.max_errors,
    )
    mock_download_db.mark_as_queued_from_upcoming.assert_not_called()
    mock_download_db.requeue_downloads.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_existing_upcoming_download_refetch_fails_transitions_to_error(
    enqueuer: Enqueuer,
    mock_download_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test upcoming download refetch failure that transitions to ERROR state."""
    upcoming_dl = create_download(
        "video1", DownloadStatus.UPCOMING, retries=sample_feed_config.max_errors - 1
    )
    mock_download_db.get_downloads_by_status.return_value = [upcoming_dl]
    mock_ytdlp_wrapper.fetch_metadata.side_effect = YtdlpApiError(
        message="Fetch failed", feed_id=FEED_ID, url=upcoming_dl.source_url
    )
    # Simulate bump_retries transitioning to ERROR
    mock_download_db.bump_retries.return_value = (
        sample_feed_config.max_errors,
        DownloadStatus.ERROR,
        True,
    )

    count = await enqueuer._handle_remaining_upcoming_downloads(
        MOCK_FEED, sample_feed_config
    )

    assert count == 0
    mock_download_db.bump_retries.assert_awaited_once_with(
        feed_id=FEED_ID,
        download_id="video1",
        error_message="Failed to re-fetch metadata for upcoming download.",
        max_allowed_errors=sample_feed_config.max_errors,
    )
    mock_download_db.mark_as_queued_from_upcoming.assert_not_called()
    mock_download_db.requeue_downloads.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_existing_upcoming_download_refetch_returns_no_match(
    enqueuer: Enqueuer,
    mock_download_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test upcoming download refetch returns no matching download or multiple."""
    upcoming_dl = create_download("video1", DownloadStatus.UPCOMING)
    mock_download_db.get_downloads_by_status.return_value = [upcoming_dl]
    # Simulate no matching download found in refetched results
    mock_ytdlp_wrapper.fetch_metadata.return_value = (
        MOCK_FEED,
        [create_download("video_other", DownloadStatus.QUEUED)],
    )
    mock_download_db.bump_retries.return_value = (1, DownloadStatus.UPCOMING, False)

    count = await enqueuer._handle_remaining_upcoming_downloads(
        MOCK_FEED, sample_feed_config
    )

    assert count == 0
    mock_download_db.bump_retries.assert_awaited_once_with(
        feed_id=FEED_ID,
        download_id="video1",
        error_message="Original ID not found in re-fetched metadata, or mismatched/multiple downloads found.",
        max_allowed_errors=sample_feed_config.max_errors,
    )


# --- Tests for Enqueuer._fetch_and_process_new_feed_downloads ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_and_process_new_feed_downloads_no_new_downloads(
    enqueuer: Enqueuer,
    mock_ytdlp_wrapper: MagicMock,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test _fetch_and_process_new_feed_downloads when no new downloads are fetched."""
    mock_ytdlp_wrapper.fetch_metadata.return_value = (MOCK_FEED, [])

    _, count = await enqueuer._fetch_and_process_feed_and_new_downloads(
        MOCK_FEED, sample_feed_config, FETCH_SINCE_DATE, FETCH_UNTIL_DATE
    )
    assert count == 0
    mock_ytdlp_wrapper.fetch_metadata.assert_awaited_once_with(
        FEED_ID,
        MOCK_FEED.source_type,
        MOCK_FEED.source_url,
        MOCK_FEED.resolved_url,
        sample_feed_config.yt_args,
        FETCH_SINCE_DATE,
        FETCH_UNTIL_DATE,
        sample_feed_config.keep_last,
        None,
    )
    mock_download_db.get_download_by_id.assert_not_called()
    mock_download_db.upsert_download.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_and_process_new_feed_downloads_new_vod_download(
    enqueuer: Enqueuer,
    mock_ytdlp_wrapper: MagicMock,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test processing a new VOD download."""
    new_vod = create_download("new_video1", DownloadStatus.QUEUED)
    mock_ytdlp_wrapper.fetch_metadata.return_value = (MOCK_FEED, [new_vod])
    mock_download_db.get_download_by_id.side_effect = DownloadNotFoundError(
        message="Not found", feed_id=FEED_ID, download_id="new_video1"
    )

    _, count = await enqueuer._fetch_and_process_feed_and_new_downloads(
        MOCK_FEED, sample_feed_config, FETCH_SINCE_DATE, FETCH_UNTIL_DATE
    )

    assert count == 1
    mock_download_db.get_download_by_id.assert_awaited_once_with(FEED_ID, "new_video1")
    mock_download_db.upsert_download.assert_awaited_once_with(new_vod)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_and_process_new_feed_downloads_new_upcoming_download(
    enqueuer: Enqueuer,
    mock_ytdlp_wrapper: MagicMock,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test processing a new UPCOMING download."""
    new_upcoming = create_download("new_video_live", DownloadStatus.UPCOMING)
    mock_ytdlp_wrapper.fetch_metadata.return_value = (MOCK_FEED, [new_upcoming])
    mock_download_db.get_download_by_id.side_effect = DownloadNotFoundError(
        message="Not found", feed_id=FEED_ID, download_id="new_video_live"
    )

    _, count = await enqueuer._fetch_and_process_feed_and_new_downloads(
        MOCK_FEED, sample_feed_config, FETCH_SINCE_DATE, FETCH_UNTIL_DATE
    )

    assert count == 0  # Not QUEUED yet
    mock_download_db.get_download_by_id.assert_awaited_once_with(
        FEED_ID, "new_video_live"
    )
    mock_download_db.upsert_download.assert_awaited_once_with(new_upcoming)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_and_process_new_feed_downloads_existing_upcoming_now_vod(
    enqueuer: Enqueuer,
    mock_ytdlp_wrapper: MagicMock,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test processing an existing UPCOMING download that is now a VOD."""
    existing_upcoming_in_db = create_download("video_live1", DownloadStatus.UPCOMING)
    fetched_as_vod = create_download("video_live1", DownloadStatus.QUEUED)

    mock_ytdlp_wrapper.fetch_metadata.return_value = (MOCK_FEED, [fetched_as_vod])
    mock_download_db.get_download_by_id.return_value = existing_upcoming_in_db

    _, count = await enqueuer._fetch_and_process_feed_and_new_downloads(
        MOCK_FEED, sample_feed_config, FETCH_SINCE_DATE, FETCH_UNTIL_DATE
    )

    assert count == 1
    mock_download_db.get_download_by_id.assert_awaited_once_with(FEED_ID, "video_live1")
    mock_download_db.upsert_download.assert_awaited_once()
    upserted_download = mock_download_db.upsert_download.call_args[0][0]
    assert upserted_download == fetched_as_vod


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_and_process_new_feed_downloads_existing_downloaded_ignored(
    enqueuer: Enqueuer,
    mock_ytdlp_wrapper: MagicMock,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test that an already DOWNLOADED item is ignored (not requeued) if fetched again as QUEUED."""
    existing_downloaded_in_db = create_download("video_done", DownloadStatus.DOWNLOADED)
    fetched_again_as_queued = create_download("video_done", DownloadStatus.QUEUED)

    mock_ytdlp_wrapper.fetch_metadata.return_value = (
        MOCK_FEED,
        [fetched_again_as_queued],
    )
    mock_download_db.get_download_by_id.return_value = existing_downloaded_in_db

    _, count = await enqueuer._fetch_and_process_feed_and_new_downloads(
        MOCK_FEED, sample_feed_config, FETCH_SINCE_DATE, FETCH_UNTIL_DATE
    )

    assert count == 0
    mock_download_db.get_download_by_id.assert_awaited_once_with(FEED_ID, "video_done")
    mock_download_db.requeue_downloads.assert_not_called()
    mock_download_db.upsert_download.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_and_process_new_feed_downloads_existing_error_requeued(
    enqueuer: Enqueuer,
    mock_ytdlp_wrapper: MagicMock,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test that if DB status is ERROR and fetched status is QUEUED, it calls requeue_downloads."""
    existing_error_in_db = create_download("video_err", DownloadStatus.ERROR, retries=1)
    fetched_as_queued = create_download("video_err", DownloadStatus.QUEUED)

    mock_ytdlp_wrapper.fetch_metadata.return_value = (MOCK_FEED, [fetched_as_queued])
    mock_download_db.get_download_by_id.return_value = existing_error_in_db

    _, count = await enqueuer._fetch_and_process_feed_and_new_downloads(
        MOCK_FEED, sample_feed_config, FETCH_SINCE_DATE, FETCH_UNTIL_DATE
    )

    assert count == 1  # Because it was re-queued
    mock_download_db.get_download_by_id.assert_awaited_once_with(FEED_ID, "video_err")
    mock_download_db.upsert_download.assert_awaited_once()
    upserted_download = mock_download_db.upsert_download.call_args[0][0]
    assert upserted_download == fetched_as_queued


# --- Tests for Enqueuer.enqueue_new_downloads ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enqueue_new_downloads_full_flow_mixed_scenarios(
    enqueuer: Enqueuer,
    mock_download_db: MagicMock,
    mock_feed_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test the main enqueue_new_downloads with a mix of scenarios."""
    mock_feed_db.get_feed_by_id.return_value = MOCK_FEED  # Return mock feed
    # --- Setup for main feed fetch ---
    # 1. New VOD from feed
    new_vod_feed = create_download("feed_new_vod", DownloadStatus.QUEUED)
    # 2. Existing UPCOMING in DB, now fetched as VOD from feed
    existing_up3_db = create_download("feed_up3_now_vod", DownloadStatus.UPCOMING)
    fetched_up3_as_vod = create_download("feed_up3_now_vod", DownloadStatus.QUEUED)
    # 3. New upcoming from feed
    new_upcoming_feed = create_download("feed_new_upcoming", DownloadStatus.UPCOMING)

    # Main feed fetch returns these 3 items
    main_feed_fetch_result = [new_vod_feed, fetched_up3_as_vod, new_upcoming_feed]

    # --- Setup for _handle_remaining_upcoming_downloads ---
    # 4. Upcoming that becomes VOD (not returned by main feed)
    upcoming1_db = create_download("up1", DownloadStatus.UPCOMING)
    upcoming1_refetched_vod = create_download("up1", DownloadStatus.QUEUED)
    # 5. Upcoming that stays upcoming (not returned by main feed)
    upcoming2_db = create_download("up2", DownloadStatus.UPCOMING)
    upcoming2_refetched_upcoming = create_download("up2", DownloadStatus.UPCOMING)

    # get_downloads_by_status is called after main feed processing
    # By then, existing_up3_db would have been updated to QUEUED, so only return truly remaining UPCOMING
    mock_download_db.get_downloads_by_status.return_value = [upcoming1_db, upcoming2_db]

    # Mock the main feed fetch first, then individual re-fetches for remaining
    mock_ytdlp_wrapper.fetch_metadata.side_effect = [
        (MOCK_FEED, main_feed_fetch_result),  # Main feed fetch
        (MOCK_FEED, [upcoming1_refetched_vod]),  # Re-fetch for up1
        (MOCK_FEED, [upcoming2_refetched_upcoming]),  # Re-fetch for up2
    ]

    # Mock get_download_by_id calls for main feed processing
    # This depends on the order of items in main_feed_fetch_result
    mock_download_db.get_download_by_id.side_effect = [
        DownloadNotFoundError(
            message="Not found", feed_id=FEED_ID, download_id="feed_new_vod"
        ),  # new_vod_feed
        existing_up3_db,  # fetched_up3_as_vod
        DownloadNotFoundError(
            message="Not found", feed_id=FEED_ID, download_id="feed_new_upcoming"
        ),  # new_upcoming_feed
    ]

    # --- Execute ---
    total_queued = await enqueuer.enqueue_new_downloads(
        FEED_ID, sample_feed_config, FETCH_SINCE_DATE, FETCH_UNTIL_DATE
    )

    # --- Assertions ---
    # Expected:
    # - upcoming1_db -> QUEUED (1)
    # - new_vod_feed -> QUEUED (1)
    # - existing_up3_db -> QUEUED (1)
    assert total_queued == 3

    # Assert calls for _handle_remaining_upcoming_downloads
    assert mock_download_db.get_downloads_by_status.call_count == 1
    assert mock_download_db.get_downloads_by_status.call_args_list[0] == call(
        DownloadStatus.UPCOMING, feed_id=FEED_ID
    )

    # Assert ytdlp_wrapper.fetch_metadata calls
    # Call 1: main feed fetch
    # Call 2: re-fetch for upcoming1_db
    # Call 3: re-fetch for upcoming2_db
    assert mock_ytdlp_wrapper.fetch_metadata.call_count == 3
    expected_calls = [
        call(
            FEED_ID,
            MOCK_FEED.source_type,
            MOCK_FEED.source_url,
            MOCK_FEED.resolved_url,
            sample_feed_config.yt_args,
            FETCH_SINCE_DATE,
            FETCH_UNTIL_DATE,
            sample_feed_config.keep_last,
            None,
        ),
        call(
            FEED_ID,
            SourceType.SINGLE_VIDEO,  # Always SINGLE_VIDEO for individual re-fetches
            "https://example.com/video/up1",  # Individual video URL
            None,  # No resolved URL for individual videos
            sample_feed_config.yt_args,
            cookies_path=None,
        ),
        call(
            FEED_ID,
            SourceType.SINGLE_VIDEO,  # Always SINGLE_VIDEO for individual re-fetches
            "https://example.com/video/up2",  # Individual video URL
            None,  # No resolved URL for individual videos
            sample_feed_config.yt_args,
            cookies_path=None,
        ),
    ]
    mock_ytdlp_wrapper.fetch_metadata.assert_has_calls(expected_calls)

    # Assert download_db calls
    # - mark_as_queued_from_upcoming called for upcoming1_db (from _handle_remaining_upcoming_downloads)
    # - upsert_download called for new_vod_feed, existing_up3_db (with updated status), and new_upcoming_feed
    mock_download_db.mark_as_queued_from_upcoming.assert_has_calls(
        [
            call(FEED_ID, upcoming1_db.id),
        ]
    )
    assert mock_download_db.mark_as_queued_from_upcoming.call_count == 1

    # Should include: new_vod_feed, updated existing_up3_db (with QUEUED status), new_upcoming_feed
    expected_upsert_calls = [
        call(new_vod_feed),
        call(new_upcoming_feed),
    ]
    # For existing_up3_db, we expect an upsert with updated status to QUEUED
    # We need to check that upsert was called with a download that has the right id and QUEUED status
    upsert_calls = mock_download_db.upsert_download.call_args_list
    # new_vod_feed, existing_up3_db (updated), new_upcoming_feed
    assert len(upsert_calls) == 3

    # Check that the calls include the expected new downloads
    for expected_call in expected_upsert_calls:
        assert expected_call in upsert_calls

    # Check that one of the upsert calls is for existing_up3_db with QUEUED status
    found_up3_update = False
    for call_args in upsert_calls:
        download = call_args[0][0]  # First positional argument is the Download
        if (
            download.id == existing_up3_db.id
            and download.status == DownloadStatus.QUEUED
        ):
            found_up3_update = True
            break
    assert found_up3_update, (
        "Expected upsert call for existing_up3_db with QUEUED status"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enqueue_new_downloads_db_error_on_get_upcoming(
    enqueuer: Enqueuer,
    mock_download_db: MagicMock,
    mock_feed_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test EnqueueError when DB fails during fetching upcoming downloads."""
    mock_feed_db.get_feed_by_id.return_value = MOCK_FEED  # Return mock feed
    # Mock main feed fetch to succeed but get_downloads_by_status fails
    mock_ytdlp_wrapper.fetch_metadata.return_value = (MOCK_FEED, [])
    mock_download_db.get_downloads_by_status.side_effect = DatabaseOperationError(
        "DB error"
    )
    with pytest.raises(EnqueueError) as exc_info:
        await enqueuer.enqueue_new_downloads(
            FEED_ID, sample_feed_config, FETCH_SINCE_DATE, FETCH_UNTIL_DATE
        )
    assert "Could not fetch upcoming downloads from DB" in str(exc_info.value)
    assert exc_info.value.feed_id == FEED_ID


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enqueue_new_downloads_ytdlp_error_on_main_feed_fetch(
    enqueuer: Enqueuer,
    mock_download_db: MagicMock,
    mock_feed_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test EnqueueError when YTDLP fails during main feed metadata fetch."""
    mock_feed_db.get_feed_by_id.return_value = MOCK_FEED  # Return mock feed
    mock_download_db.get_downloads_by_status.return_value = []  # No upcoming
    mock_ytdlp_wrapper.fetch_metadata.side_effect = YtdlpApiError(
        "YTDLP error", feed_id=FEED_ID, url=FEED_URL
    )

    with pytest.raises(EnqueueError) as exc_info:
        await enqueuer.enqueue_new_downloads(
            FEED_ID, sample_feed_config, FETCH_SINCE_DATE, FETCH_UNTIL_DATE
        )

    assert "Could not fetch main feed metadata" in str(exc_info.value)
    assert exc_info.value.feed_id == FEED_ID
    assert exc_info.value.feed_url == FEED_URL
    # Ensure ytdlp_wrapper.fetch_metadata was called for the main feed
    mock_ytdlp_wrapper.fetch_metadata.assert_awaited_once_with(
        FEED_ID,
        MOCK_FEED.source_type,
        MOCK_FEED.source_url,
        MOCK_FEED.resolved_url,
        sample_feed_config.yt_args,
        FETCH_SINCE_DATE,
        FETCH_UNTIL_DATE,
        sample_feed_config.keep_last,
        None,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enqueue_new_downloads_no_upcoming_no_new(
    enqueuer: Enqueuer,
    mock_download_db: MagicMock,
    mock_feed_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test enqueue_new_downloads when no upcoming downloads exist and no new downloads are found."""
    mock_feed_db.get_feed_by_id.return_value = MOCK_FEED  # Return mock feed
    mock_download_db.get_downloads_by_status.return_value = []  # No upcoming
    mock_ytdlp_wrapper.fetch_metadata.return_value = (MOCK_FEED, [])  # No new downloads

    queued_count = await enqueuer.enqueue_new_downloads(
        FEED_ID, sample_feed_config, FETCH_SINCE_DATE, FETCH_UNTIL_DATE
    )

    assert queued_count == 0
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.UPCOMING, feed_id=FEED_ID
    )
    mock_ytdlp_wrapper.fetch_metadata.assert_awaited_once_with(
        FEED_ID,
        MOCK_FEED.source_type,
        MOCK_FEED.source_url,
        MOCK_FEED.resolved_url,
        sample_feed_config.yt_args,
        FETCH_SINCE_DATE,
        FETCH_UNTIL_DATE,
        sample_feed_config.keep_last,
        None,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enqueue_deduplication_with_same_day_overlapping_windows(
    enqueuer: Enqueuer,
    mock_download_db: MagicMock,
    mock_feed_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test that deduplication works correctly when same video appears in overlapping day windows.

    This tests the scenario where yt-dlp's day-level date precision causes the same video
    to be found in multiple runs with overlapping date windows that fall on the same day.
    """
    mock_feed_db.get_feed_by_id.return_value = MOCK_FEED  # Return mock feed
    # Create a test download that will be "found" in both runs
    test_download = create_download(
        id="test_video_same_day",
        status=DownloadStatus.QUEUED,
        published_offset_days=-1,  # Published yesterday
    )

    # Mock ytdlp_wrapper to return the same video both times
    mock_ytdlp_wrapper.fetch_metadata.return_value = (MOCK_FEED, [test_download])

    # Mock database to simulate the same video being found in both runs
    call_count = {"value": 0}

    def mock_get_download_by_id(feed_id: str, download_id: str) -> Download:
        call_count["value"] += 1
        if download_id == "test_video_same_day" and call_count["value"] > 1:
            # Second call: return the existing download (simulates deduplication)
            existing = deepcopy(test_download)
            existing.updated_at = datetime(
                2025, 6, 17, 10, 0, 0, tzinfo=UTC
            )  # Fixed timestamp
            return existing
        else:
            # First call: no existing download, raise exception
            raise DownloadNotFoundError(
                "Download not found.", feed_id=feed_id, download_id=download_id
            )

    mock_download_db.get_download_by_id.side_effect = mock_get_download_by_id
    mock_download_db.get_downloads_by_status.return_value = []  # No upcoming downloads

    # First run: overlapping day window (e.g., 8am to 10am same day)
    first_since = datetime(2025, 6, 17, 8, 0, 0, tzinfo=UTC)
    first_until = datetime(2025, 6, 17, 10, 0, 0, tzinfo=UTC)

    first_count = await enqueuer.enqueue_new_downloads(
        FEED_ID, sample_feed_config, first_since, first_until
    )

    # Second run: overlapping day window (e.g., 9am to 11am same day)
    # Due to yt-dlp's day-level precision, both will use same YYYYMMDD date
    second_since = datetime(2025, 6, 17, 9, 0, 0, tzinfo=UTC)
    second_until = datetime(2025, 6, 17, 11, 0, 0, tzinfo=UTC)

    second_count = await enqueuer.enqueue_new_downloads(
        FEED_ID, sample_feed_config, second_since, second_until
    )

    # Deduplication should work correctly
    assert first_count == 1, "First run should queue the new download"
    assert second_count == 0, (
        "Second run should not queue the same download (deduplication)"
    )

    # Verify ytdlp_wrapper was called twice (once for each run)
    assert mock_ytdlp_wrapper.fetch_metadata.call_count == 2

    # Verify first call used first date window
    first_call = mock_ytdlp_wrapper.fetch_metadata.call_args_list[0]
    assert first_call[0][5] == first_since  # fetch_since_date
    assert first_call[0][6] == first_until  # fetch_until_date

    # Verify second call used second date window
    second_call = mock_ytdlp_wrapper.fetch_metadata.call_args_list[1]
    assert second_call[0][5] == second_since  # fetch_since_date
    assert second_call[0][6] == second_until  # fetch_until_date

    # Verify database operations for deduplication
    assert mock_download_db.get_download_by_id.call_count == 2

    # First run should insert the download
    mock_download_db.upsert_download.assert_called()
    upsert_calls = mock_download_db.upsert_download.call_args_list
    assert len(upsert_calls) == 1, "Only first run should insert the download"

    # The upserted download should be the test download
    upserted_download = upsert_calls[0][0][0]
    assert upserted_download.id == "test_video_same_day"
    assert upserted_download.status == DownloadStatus.QUEUED
