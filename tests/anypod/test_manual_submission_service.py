# pyright: reportPrivateUsage=false

"""Unit tests for ManualSubmissionService."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from anypod.config import FeedConfig
from anypod.config.types import FeedMetadataOverrides
from anypod.db.types import Download, DownloadStatus, SourceType
from anypod.exceptions import (
    ManualSubmissionUnavailableError,
    ManualSubmissionUnsupportedURLError,
    YtdlpError,
)
from anypod.manual_submission_service import ManualSubmissionService
from anypod.ytdlp_wrapper import YtdlpWrapper

FEED_ID = "manual_feed"
TEST_URL = "https://www.youtube.com/watch?v=test123"


@pytest.fixture
def mock_ytdlp_wrapper() -> MagicMock:
    """Create a mock YtdlpWrapper for testing."""
    return MagicMock(spec=YtdlpWrapper)


@pytest.fixture
def manual_submission_service(mock_ytdlp_wrapper: MagicMock) -> ManualSubmissionService:
    """Create a ManualSubmissionService with mocked dependencies."""
    return ManualSubmissionService(mock_ytdlp_wrapper)


@pytest.fixture
def feed_config() -> FeedConfig:
    """Create a test feed configuration."""
    return FeedConfig(
        url=None,
        schedule="manual",  # type: ignore[arg-type]
        metadata=FeedMetadataOverrides(title="Manual Feed"),
        yt_args=["-f", "best"],
    )


@pytest.fixture
def sample_download() -> Download:
    """Create a sample download in QUEUED state."""
    return Download(
        feed_id=FEED_ID,
        id="test123",
        source_url=TEST_URL,
        title="Test Video",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000000,
        duration=120,
        status=DownloadStatus.QUEUED,
    )


# --- Tests for fetch_submission_download ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_submission_download_success(
    manual_submission_service: ManualSubmissionService,
    mock_ytdlp_wrapper: MagicMock,
    feed_config: FeedConfig,
    sample_download: Download,
) -> None:
    """Successful metadata fetch returns Download in QUEUED state."""
    mock_ytdlp_wrapper.fetch_new_downloads_metadata.return_value = [sample_download]

    cookies_path = Path("/cookies/cookies.txt")

    result = await manual_submission_service.fetch_submission_download(
        feed_id=FEED_ID,
        feed_config=feed_config,
        url=TEST_URL,
        cookies_path=cookies_path,
    )

    assert result == sample_download
    mock_ytdlp_wrapper.fetch_new_downloads_metadata.assert_awaited_once_with(
        feed_id=FEED_ID,
        source_type=SourceType.SINGLE_VIDEO,
        source_url=TEST_URL,
        resolved_url=TEST_URL,
        user_yt_cli_args=feed_config.yt_args,
        fetch_since_date=None,
        keep_last=None,
        cookies_path=cookies_path,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_submission_download_ytdlp_error_raises_unsupported(
    manual_submission_service: ManualSubmissionService,
    mock_ytdlp_wrapper: MagicMock,
    feed_config: FeedConfig,
) -> None:
    """YtdlpError from wrapper raises ManualSubmissionUnsupportedURLError."""
    mock_ytdlp_wrapper.fetch_new_downloads_metadata.side_effect = YtdlpError(
        "yt-dlp failed"
    )

    with pytest.raises(ManualSubmissionUnsupportedURLError) as exc_info:
        await manual_submission_service.fetch_submission_download(
            feed_id=FEED_ID,
            feed_config=feed_config,
            url=TEST_URL,
            cookies_path=None,
        )

    assert exc_info.value.feed_id == FEED_ID
    assert exc_info.value.url == TEST_URL


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_submission_download_empty_results_raises_unavailable(
    manual_submission_service: ManualSubmissionService,
    mock_ytdlp_wrapper: MagicMock,
    feed_config: FeedConfig,
) -> None:
    """Empty download list from wrapper raises ManualSubmissionUnavailableError."""
    mock_ytdlp_wrapper.fetch_new_downloads_metadata.return_value = []

    with pytest.raises(ManualSubmissionUnavailableError) as exc_info:
        await manual_submission_service.fetch_submission_download(
            feed_id=FEED_ID,
            feed_config=feed_config,
            url=TEST_URL,
            cookies_path=None,
        )

    assert exc_info.value.feed_id == FEED_ID
    assert exc_info.value.url == TEST_URL


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_submission_download_upcoming_status_raises_unavailable(
    manual_submission_service: ManualSubmissionService,
    mock_ytdlp_wrapper: MagicMock,
    feed_config: FeedConfig,
    sample_download: Download,
) -> None:
    """Download with UPCOMING status raises ManualSubmissionUnavailableError."""
    sample_download.status = DownloadStatus.UPCOMING
    mock_ytdlp_wrapper.fetch_new_downloads_metadata.return_value = [sample_download]

    with pytest.raises(ManualSubmissionUnavailableError) as exc_info:
        await manual_submission_service.fetch_submission_download(
            feed_id=FEED_ID,
            feed_config=feed_config,
            url=TEST_URL,
            cookies_path=None,
        )

    assert exc_info.value.feed_id == FEED_ID
    assert exc_info.value.url == TEST_URL


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_submission_download_allows_non_queued_status(
    manual_submission_service: ManualSubmissionService,
    mock_ytdlp_wrapper: MagicMock,
    feed_config: FeedConfig,
    sample_download: Download,
) -> None:
    """Non-upcoming statuses proceed with a warning instead of failing."""
    sample_download.status = DownloadStatus.DOWNLOADED
    mock_ytdlp_wrapper.fetch_new_downloads_metadata.return_value = [sample_download]

    result = await manual_submission_service.fetch_submission_download(
        feed_id=FEED_ID,
        feed_config=feed_config,
        url=TEST_URL,
        cookies_path=None,
    )

    assert result == sample_download


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_submission_download_uses_first_result_when_multiple(
    manual_submission_service: ManualSubmissionService,
    mock_ytdlp_wrapper: MagicMock,
    feed_config: FeedConfig,
    sample_download: Download,
) -> None:
    """Service returns first download when wrapper returns multiple results."""
    second_download = Download(
        feed_id=FEED_ID,
        id="test456",
        source_url="https://www.youtube.com/watch?v=test456",
        title="Second Video",
        published=datetime(2024, 1, 2, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=2000000,
        duration=180,
        status=DownloadStatus.QUEUED,
    )
    mock_ytdlp_wrapper.fetch_new_downloads_metadata.return_value = [
        sample_download,
        second_download,
    ]

    result = await manual_submission_service.fetch_submission_download(
        feed_id=FEED_ID,
        feed_config=feed_config,
        url=TEST_URL,
        cookies_path=None,
    )

    assert result == sample_download
