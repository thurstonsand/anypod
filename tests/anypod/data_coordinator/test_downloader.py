# pyright: reportPrivateUsage=false

"""Tests for the Downloader service.

This module contains unit tests for the Downloader class, which is responsible
for processing items in the download queue, interacting with YtdlpWrapper for
media fetching, FileManager for storage, and DownloadDatabase for status updates.
"""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from anypod.config import FeedConfig
from anypod.config.types import FeedMetadataOverrides
from anypod.data_coordinator.downloader import Downloader
from anypod.db import DownloadDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.exceptions import (
    DatabaseOperationError,
    DownloadError,
    YtdlpApiError,
)
from anypod.file_manager import FileManager
from anypod.ytdlp_wrapper import YtdlpWrapper

# Mock Feed object for testing
MOCK_FEED = Feed(
    id="test_feed",
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

# --- Fixtures ---


@pytest.fixture
def mock_download_db() -> MagicMock:
    """Provides a mock DownloadDatabase."""
    mock = MagicMock(spec=DownloadDatabase)
    # Mock async methods
    mock.get_downloads_by_status = AsyncMock()
    mock.get_download_by_id = AsyncMock()
    mock.upsert_download = AsyncMock()
    mock.mark_as_downloaded = AsyncMock()
    mock.bump_retries = AsyncMock()
    return mock


@pytest.fixture
def mock_file_manager() -> MagicMock:
    """Provides a mock FileManager with specific async method mocks."""
    mock = MagicMock(spec=FileManager)

    # Only mock the specific async methods that FileManager actually has
    mock.delete_download_file = AsyncMock()
    mock.download_exists = AsyncMock()
    mock.get_download_stream = AsyncMock()

    return mock


@pytest.fixture
def mock_ytdlp_wrapper() -> MagicMock:
    """Provides a mock YtdlpWrapper."""
    mock = MagicMock(spec=YtdlpWrapper)
    # Set async methods to AsyncMock
    mock.fetch_metadata = AsyncMock()
    mock.download_media_to_file = AsyncMock()
    return mock


@pytest.fixture
def downloader(
    mock_download_db: MagicMock,
    mock_file_manager: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
) -> Downloader:
    """Provides a Downloader instance with mocked dependencies."""
    return Downloader(mock_download_db, mock_file_manager, mock_ytdlp_wrapper)


@pytest.fixture
def sample_download() -> Download:
    """Provides a sample Download object."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    return Download(
        feed_id="test_feed",
        id="test_dl_id_1",
        source_url="http://example.com/video1",
        title="Test Video 1",
        published=base_time,
        ext="mp4",
        mime_type="video/mp4",
        duration=120,
        status=DownloadStatus.QUEUED,
        discovered_at=base_time,
        updated_at=base_time,
        description="Original description",
        filesize=0,
    )


@pytest.fixture
def sample_feed_config() -> FeedConfig:
    """Provides a sample FeedConfig object."""
    return FeedConfig(
        url="http://example.com/feed_url",
        yt_args="--format best",  # type: ignore # this gets preprocessed into a dict
        schedule="0 0 * * *",  # type: ignore
        keep_last=10,
        since=None,
        max_errors=3,
        metadata=FeedMetadataOverrides(title="Test Podcast"),  # type: ignore
    )


# --- Tests for _handle_download_success ---


@pytest.mark.unit
@pytest.mark.asyncio
@patch("aiofiles.os.stat", new_callable=AsyncMock, return_value=MagicMock(st_size=1024))
async def test_handle_download_success_updates_db(
    _mock_stat: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_download: Download,
):
    """Tests that _handle_download_success calls mark_as_downloaded on DB manager."""
    downloaded_file = Path("/path/to/downloaded_video.mp4")

    await downloader._handle_download_success(sample_download, downloaded_file)

    mock_download_db.mark_as_downloaded.assert_awaited_once_with(
        feed_id=sample_download.feed_id,
        download_id=sample_download.id,
        ext="mp4",
        filesize=1024,  # From our mocked stat result
    )


@pytest.mark.unit
@pytest.mark.asyncio
@patch("aiofiles.os.stat", new_callable=AsyncMock, return_value=MagicMock(st_size=1024))
async def test_handle_download_success_db_update_fails_raises_downloader_error(
    _mock_stat: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_download: Download,
):
    """Tests that DB operation failure in _handle_download_success raises DownloadError."""
    downloaded_file = Path("/path/to/downloaded_video.mp4")
    db_error = DatabaseOperationError("DB boom")
    mock_download_db.mark_as_downloaded.side_effect = db_error

    with pytest.raises(DownloadError) as exc_info:
        await downloader._handle_download_success(sample_download, downloaded_file)

    assert exc_info.value.__cause__ is db_error
    assert exc_info.value.feed_id == sample_download.feed_id
    assert exc_info.value.download_id == sample_download.id


# --- Tests for _handle_download_failure ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_download_failure_bumps_retries(
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests that _handle_download_failure calls bump_retries on DB manager."""
    error = ValueError("Download exploded")
    await downloader._handle_download_failure(
        sample_download, sample_feed_config, error
    )

    mock_download_db.bump_retries.assert_awaited_once_with(
        feed_id=sample_download.feed_id,
        download_id=sample_download.id,
        error_message=str(error),
        max_allowed_errors=sample_feed_config.max_errors,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_download_failure_db_error_logged(
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests that DB errors during bump_retries are logged and not re-raised."""
    error = ValueError("Download exploded")
    mock_download_db.bump_retries.side_effect = DatabaseOperationError(
        "DB boom for retries"
    )

    try:
        await downloader._handle_download_failure(
            sample_download, sample_feed_config, error
        )
    except Exception as e:
        pytest.fail(
            f"_handle_download_failure should not raise an exception, but got: {e}"
        )


# --- Tests for _check_and_update_metadata ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_check_and_update_metadata_detects_changes(
    downloader: Downloader,
    mock_download_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests that _check_and_update_metadata detects and updates changed metadata."""
    updated_download = sample_download.model_copy(
        update={
            "title": "Updated Title",
            "description": "Updated description",
            "thumbnail": "http://example.com/new_thumb.jpg",
            "duration": 180,
        }
    )

    mock_ytdlp_wrapper.fetch_metadata.return_value = (MOCK_FEED, [updated_download])

    result = await downloader._check_and_update_metadata(
        sample_download, sample_feed_config
    )

    mock_ytdlp_wrapper.fetch_metadata.assert_called_once_with(
        sample_download.feed_id,
        sample_download.source_url,
        sample_feed_config.yt_args,
        cookies_path=None,
    )

    mock_download_db.upsert_download.assert_awaited_once()
    updated_in_db = mock_download_db.upsert_download.call_args[0][0]
    assert updated_in_db.title == updated_download.title
    assert updated_in_db.description == updated_download.description
    assert updated_in_db.thumbnail == updated_download.thumbnail
    assert updated_in_db.duration == updated_download.duration

    # Verify the returned download has updated values
    assert result.title == updated_download.title
    assert result.description == updated_download.description


@pytest.mark.unit
@pytest.mark.asyncio
async def test_check_and_update_metadata_no_changes(
    downloader: Downloader,
    mock_download_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests that _check_and_update_metadata doesn't update DB when no changes detected."""
    # Mock the fetch to return the same download (no changes)
    mock_ytdlp_wrapper.fetch_metadata.return_value = (MOCK_FEED, [sample_download])

    result = await downloader._check_and_update_metadata(
        sample_download, sample_feed_config
    )

    # Verify metadata was fetched
    mock_ytdlp_wrapper.fetch_metadata.assert_called_once()

    # Verify the database was NOT updated since nothing changed
    mock_download_db.upsert_download.assert_not_called()

    # Verify the returned download is unchanged
    assert result == sample_download


@pytest.mark.unit
@pytest.mark.asyncio
async def test_check_and_update_metadata_fetch_fails_returns_original(
    downloader: Downloader,
    mock_download_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests that _check_and_update_metadata returns original on fetch failure."""
    # Mock the fetch to fail
    mock_ytdlp_wrapper.fetch_metadata.side_effect = YtdlpApiError("Fetch failed")

    result = await downloader._check_and_update_metadata(
        sample_download, sample_feed_config
    )

    # Verify the database was NOT updated
    mock_download_db.upsert_download.assert_not_called()

    # Verify the original download is returned
    assert result == sample_download


@pytest.mark.unit
@pytest.mark.asyncio
async def test_check_and_update_metadata_no_matching_download_returns_original(
    downloader: Downloader,
    mock_download_db: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests that _check_and_update_metadata returns original when no matching download found."""
    # Mock the fetch to return a different download ID
    different_download = sample_download.model_copy(update={"id": "different_id"})
    mock_ytdlp_wrapper.fetch_metadata.return_value = (MOCK_FEED, [different_download])

    result = await downloader._check_and_update_metadata(
        sample_download, sample_feed_config
    )

    # Verify the database was NOT updated
    mock_download_db.upsert_download.assert_not_called()

    # Verify the original download is returned
    assert result == sample_download


# --- Tests for _process_single_download ---


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(Downloader, "_handle_download_success", new_callable=AsyncMock)
@patch.object(Downloader, "_check_and_update_metadata", new_callable=AsyncMock)
async def test_process_single_download_success_flow(
    mock_check_metadata: AsyncMock,
    mock_handle_success: AsyncMock,
    downloader: Downloader,
    mock_ytdlp_wrapper: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests the success path of _process_single_download."""
    downloaded_path = Path("/final/video.mp4")
    mock_ytdlp_wrapper.download_media_to_file.return_value = downloaded_path
    mock_check_metadata.return_value = sample_download  # Return unchanged

    await downloader._process_single_download(sample_download, sample_feed_config)

    mock_check_metadata.assert_called_once_with(
        sample_download, sample_feed_config, None
    )

    mock_ytdlp_wrapper.download_media_to_file.assert_called_once_with(
        sample_download,
        sample_feed_config.yt_args,
        cookies_path=None,
    )
    mock_handle_success.assert_called_once_with(sample_download, downloaded_path)


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(Downloader, "_check_and_update_metadata", new_callable=AsyncMock)
async def test_process_single_download_ytdlp_failure_raises_downloader_error(
    mock_check_metadata: AsyncMock,
    downloader: Downloader,
    mock_ytdlp_wrapper: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests that YtdlpApiError during download raises DownloadError."""
    original_ytdlp_error = YtdlpApiError(
        "yt-dlp failed", feed_id="test_feed", download_id="test_dl_id_1"
    )
    mock_ytdlp_wrapper.download_media_to_file.side_effect = original_ytdlp_error
    mock_check_metadata.return_value = sample_download  # Return unchanged

    with pytest.raises(DownloadError) as exc_info:
        await downloader._process_single_download(sample_download, sample_feed_config)

    assert exc_info.value.feed_id == sample_download.feed_id
    assert exc_info.value.download_id == sample_download.id
    assert exc_info.value.__cause__ is original_ytdlp_error


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(Downloader, "_handle_download_success", new_callable=AsyncMock)
@patch.object(Downloader, "_check_and_update_metadata", new_callable=AsyncMock)
async def test_process_single_download_calls_check_metadata(
    mock_check_metadata: AsyncMock,
    mock_handle_success: AsyncMock,
    downloader: Downloader,
    mock_ytdlp_wrapper: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests that _process_single_download calls _check_and_update_metadata before downloading."""
    # Setup mocks
    updated_download = sample_download.model_copy(update={"title": "Updated Title"})
    downloaded_path = Path("/final/video.mp4")
    mock_ytdlp_wrapper.download_media_to_file.return_value = downloaded_path
    mock_check_metadata.return_value = updated_download

    await downloader._process_single_download(sample_download, sample_feed_config)

    # Verify metadata check was called first
    mock_check_metadata.assert_called_once_with(
        sample_download, sample_feed_config, None
    )

    # Verify download was called with the updated download
    mock_ytdlp_wrapper.download_media_to_file.assert_called_once_with(
        updated_download,
        sample_feed_config.yt_args,
        cookies_path=None,
    )
    mock_handle_success.assert_called_once_with(updated_download, downloaded_path)


# --- Tests for download_queued ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_queued_no_items_returns_zero_counts(
    downloader: Downloader, mock_download_db: MagicMock, sample_feed_config: FeedConfig
):
    """Tests download_queued returns (0,0) if no items are fetched from DB."""
    mock_download_db.get_downloads_by_status.return_value = []

    success, failure = await downloader.download_queued(
        "test_feed", sample_feed_config, limit=5
    )

    assert success == 0
    assert failure == 0
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.QUEUED, "test_feed", 5
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_queued_db_fetch_error_raises_downloader_error(
    downloader: Downloader, mock_download_db: MagicMock, sample_feed_config: FeedConfig
):
    """Tests that DB error when fetching queued items raises DownloadError."""
    db_error = DatabaseOperationError("DB fetch failed")
    mock_download_db.get_downloads_by_status.side_effect = db_error

    with pytest.raises(DownloadError) as exc_info:
        await downloader.download_queued("test_feed", sample_feed_config)

    assert exc_info.value.feed_id == "test_feed"
    assert exc_info.value.__cause__ is db_error


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(Downloader, "_process_single_download", new_callable=AsyncMock)
async def test_download_queued_processes_items_and_counts_success(
    mock_process_single: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
    sample_download: Download,  # Re-use for creating a list
):
    """Tests download_queued iterates and counts successful processing."""
    download2 = sample_download.model_copy(update={"id": "test_dl_id_2"})
    queued_items = [sample_download, download2]
    mock_download_db.get_downloads_by_status.return_value = queued_items

    # _process_single_download does not raise for success
    mock_process_single.return_value = None

    success, failure = await downloader.download_queued("test_feed", sample_feed_config)

    assert success == 2
    assert failure == 0
    mock_process_single.assert_has_calls(
        [
            call(sample_download, sample_feed_config, None),
            call(download2, sample_feed_config, None),
        ],
        any_order=False,
    )


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(Downloader, "_process_single_download", new_callable=AsyncMock)
async def test_download_queued_processes_items_and_counts_failures(
    mock_process_single: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
    sample_download: Download,
):
    """Tests download_queued iterates and counts failures from _process_single_download."""
    download2 = sample_download.model_copy(update={"id": "test_dl_id_2"})
    queued_items = [sample_download, download2]
    mock_download_db.get_downloads_by_status.return_value = queued_items

    # Simulate _process_single_download raising DownloadError for all items
    mock_process_single.side_effect = DownloadError("Processing failed")

    success, failure = await downloader.download_queued("test_feed", sample_feed_config)

    assert success == 0
    assert failure == 2
    mock_process_single.assert_has_calls(
        [
            call(sample_download, sample_feed_config, None),
            call(download2, sample_feed_config, None),
        ],
        any_order=False,
    )


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(Downloader, "_process_single_download", new_callable=AsyncMock)
async def test_download_queued_mixed_success_and_failure(
    mock_process_single: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
    sample_download: Download,
):
    """Tests download_queued handles a mix of success and failure."""
    dl1 = sample_download.model_copy(update={"id": "dl1_success"})
    dl2 = sample_download.model_copy(update={"id": "dl2_fail"})
    dl3 = sample_download.model_copy(update={"id": "dl3_success"})
    queued_items = [dl1, dl2, dl3]
    mock_download_db.get_downloads_by_status.return_value = queued_items

    # dl1 succeeds, dl2 fails, dl3 succeeds
    mock_process_single.side_effect = [
        None,  # dl1 success
        DownloadError("dl2 failed"),  # dl2 failure
        None,  # dl3 success
    ]

    success, failure = await downloader.download_queued("test_feed", sample_feed_config)

    assert success == 2
    assert failure == 1
    mock_process_single.assert_has_calls(
        [
            call(dl1, sample_feed_config, None),
            call(dl2, sample_feed_config, None),
            call(dl3, sample_feed_config, None),
        ],
        any_order=False,
    )
