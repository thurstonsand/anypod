"""Tests for the Enqueuer service and its download queue management."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, call

import pytest

from anypod.config import FeedConfig
from anypod.data_coordinator.enqueuer import Enqueuer
from anypod.db.db import DatabaseManager, Download, DownloadStatus
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


@pytest.fixture
def mock_db_manager() -> MagicMock:
    """Provides a MagicMock for DatabaseManager."""
    return MagicMock(spec=DatabaseManager)


@pytest.fixture
def mock_ytdlp_wrapper() -> MagicMock:
    """Provides a MagicMock for YtdlpWrapper."""
    return MagicMock(spec=YtdlpWrapper)


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
def enqueuer(mock_db_manager: MagicMock, mock_ytdlp_wrapper: MagicMock) -> Enqueuer:
    """Provides an Enqueuer instance with mocked dependencies."""
    return Enqueuer(db_manager=mock_db_manager, ytdlp_wrapper=mock_ytdlp_wrapper)


def create_download(
    id: str,
    status: DownloadStatus,
    feed_id: str = FEED_ID,
    published_offset_days: int = 0,
    title: str | None = None,
    source_url: str | None = None,
    ext: str = "mp4",
    duration: float = 120.0,
    retries: int = 0,
) -> Download:
    """Helper function to create Download objects for tests."""
    return Download(
        feed=feed_id,
        id=id,
        source_url=source_url or f"https://example.com/video/{id}",
        title=title or f"Test Video {id}",
        published=datetime.now(UTC) - timedelta(days=published_offset_days),
        ext=ext,
        duration=duration,
        status=status,
        retries=retries,
    )


FETCH_SINCE_DATE = datetime.now(UTC) - timedelta(days=1)


# Basic test case: No upcoming downloads, no new downloads from feed
@pytest.mark.unit
def test_enqueue_new_downloads_no_upcoming_no_new(
    enqueuer: Enqueuer,
    mock_db_manager: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test enqueue_new_downloads when no upcoming downloads exist and no new downloads are found."""
    mock_db_manager.get_downloads_by_status.return_value = []  # No upcoming
    mock_ytdlp_wrapper.fetch_metadata.return_value = []  # No new downloads

    queued_count = enqueuer.enqueue_new_downloads(
        FEED_ID, sample_feed_config, FETCH_SINCE_DATE
    )

    assert queued_count == 0
    mock_db_manager.get_downloads_by_status.assert_called_once_with(
        DownloadStatus.UPCOMING, feed=FEED_ID
    )
    mock_ytdlp_wrapper.fetch_metadata.assert_called_once_with(
        FEED_ID,
        sample_feed_config.url,
        {
            "dateafter": FETCH_SINCE_DATE.strftime("%Y%m%d"),
            **sample_feed_config.yt_args,
        },
    )


@pytest.mark.unit
def test_handle_existing_upcoming_downloads_none_found(
    enqueuer: Enqueuer,
    mock_db_manager: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test _handle_existing_upcoming_downloads when no upcoming downloads are in DB."""
    mock_db_manager.get_downloads_by_status.return_value = []

    count = enqueuer._handle_existing_upcoming_downloads(FEED_ID, sample_feed_config)  # type: ignore
    assert count == 0
    mock_db_manager.get_downloads_by_status.assert_called_once_with(
        DownloadStatus.UPCOMING, feed=FEED_ID
    )


@pytest.mark.unit
def test_handle_existing_upcoming_download_transitions_to_queued(
    enqueuer: Enqueuer,
    mock_db_manager: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test an upcoming download that transitions to QUEUED."""
    upcoming_dl = create_download("video1", DownloadStatus.UPCOMING)
    refetched_vod_dl = create_download("video1", DownloadStatus.QUEUED)

    mock_db_manager.get_downloads_by_status.return_value = [upcoming_dl]
    mock_ytdlp_wrapper.fetch_metadata.return_value = [refetched_vod_dl]

    count = enqueuer._handle_existing_upcoming_downloads(FEED_ID, sample_feed_config)  # type: ignore

    assert count == 1
    mock_ytdlp_wrapper.fetch_metadata.assert_called_once_with(
        FEED_ID, upcoming_dl.source_url, sample_feed_config.yt_args
    )
    mock_db_manager.mark_as_queued_from_upcoming.assert_called_once_with(
        FEED_ID, "video1"
    )


@pytest.mark.unit
def test_handle_existing_upcoming_download_remains_upcoming(
    enqueuer: Enqueuer,
    mock_db_manager: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test an upcoming download that is still UPCOMING after refetch."""
    upcoming_dl = create_download("video1", DownloadStatus.UPCOMING)
    # Refetched data still shows it as upcoming
    refetched_upcoming_dl = create_download("video1", DownloadStatus.UPCOMING)

    mock_db_manager.get_downloads_by_status.return_value = [upcoming_dl]
    mock_ytdlp_wrapper.fetch_metadata.return_value = [refetched_upcoming_dl]

    count = enqueuer._handle_existing_upcoming_downloads(FEED_ID, sample_feed_config)  # type: ignore

    assert count == 0
    mock_ytdlp_wrapper.fetch_metadata.assert_called_once_with(
        FEED_ID, upcoming_dl.source_url, sample_feed_config.yt_args
    )
    mock_db_manager.mark_as_queued_from_upcoming.assert_not_called()
    mock_db_manager.requeue_download.assert_not_called()


@pytest.mark.unit
def test_handle_existing_upcoming_download_refetch_fails_bumps_retries(
    enqueuer: Enqueuer,
    mock_db_manager: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test upcoming download refetch failure, leading to retry bump."""
    upcoming_dl = create_download("video1", DownloadStatus.UPCOMING)
    mock_db_manager.get_downloads_by_status.return_value = [upcoming_dl]
    mock_ytdlp_wrapper.fetch_metadata.side_effect = YtdlpApiError(
        message="Fetch failed", feed_id=FEED_ID, url=upcoming_dl.source_url
    )
    # Simulate bump_retries not transitioning to ERROR
    mock_db_manager.bump_retries.return_value = (1, DownloadStatus.UPCOMING, False)

    count = enqueuer._handle_existing_upcoming_downloads(FEED_ID, sample_feed_config)  # type: ignore

    assert count == 0
    mock_db_manager.bump_retries.assert_called_once_with(
        feed_id=FEED_ID,
        download_id="video1",
        error_message="Failed to re-fetch metadata for upcoming download.",
        max_allowed_errors=sample_feed_config.max_errors,
    )
    mock_db_manager.mark_as_queued_from_upcoming.assert_not_called()
    mock_db_manager.requeue_download.assert_not_called()


@pytest.mark.unit
def test_handle_existing_upcoming_download_refetch_fails_transitions_to_error(
    enqueuer: Enqueuer,
    mock_db_manager: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test upcoming download refetch failure that transitions to ERROR state."""
    upcoming_dl = create_download(
        "video1", DownloadStatus.UPCOMING, retries=sample_feed_config.max_errors - 1
    )
    mock_db_manager.get_downloads_by_status.return_value = [upcoming_dl]
    mock_ytdlp_wrapper.fetch_metadata.side_effect = YtdlpApiError(
        message="Fetch failed", feed_id=FEED_ID, url=upcoming_dl.source_url
    )
    # Simulate bump_retries transitioning to ERROR
    mock_db_manager.bump_retries.return_value = (
        sample_feed_config.max_errors,
        DownloadStatus.ERROR,
        True,
    )

    count = enqueuer._handle_existing_upcoming_downloads(FEED_ID, sample_feed_config)  # type: ignore

    assert count == 0
    mock_db_manager.bump_retries.assert_called_once_with(
        feed_id=FEED_ID,
        download_id="video1",
        error_message="Failed to re-fetch metadata for upcoming download.",
        max_allowed_errors=sample_feed_config.max_errors,
    )
    mock_db_manager.mark_as_queued_from_upcoming.assert_not_called()
    mock_db_manager.requeue_download.assert_not_called()


@pytest.mark.unit
def test_handle_existing_upcoming_download_refetch_returns_no_match(
    enqueuer: Enqueuer,
    mock_db_manager: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test upcoming download refetch returns no matching download or multiple."""
    upcoming_dl = create_download("video1", DownloadStatus.UPCOMING)
    mock_db_manager.get_downloads_by_status.return_value = [upcoming_dl]
    # Simulate no matching download found in refetched results
    mock_ytdlp_wrapper.fetch_metadata.return_value = [
        create_download("video_other", DownloadStatus.QUEUED)
    ]
    mock_db_manager.bump_retries.return_value = (1, DownloadStatus.UPCOMING, False)

    count = enqueuer._handle_existing_upcoming_downloads(FEED_ID, sample_feed_config)  # type: ignore

    assert count == 0
    mock_db_manager.bump_retries.assert_called_once_with(
        feed_id=FEED_ID,
        download_id="video1",
        error_message="Original ID not found in re-fetched metadata, or mismatched/multiple downloads found.",
        max_allowed_errors=sample_feed_config.max_errors,
    )


@pytest.mark.unit
def test_fetch_and_process_new_feed_downloads_no_new_downloads(
    enqueuer: Enqueuer,
    mock_ytdlp_wrapper: MagicMock,
    mock_db_manager: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test _fetch_and_process_new_feed_downloads when no new downloads are fetched."""
    mock_ytdlp_wrapper.fetch_metadata.return_value = []

    count = enqueuer._fetch_and_process_new_feed_downloads(  # type: ignore
        FEED_ID, sample_feed_config, FETCH_SINCE_DATE
    )
    assert count == 0
    mock_ytdlp_wrapper.fetch_metadata.assert_called_once_with(
        FEED_ID,
        sample_feed_config.url,
        {
            "dateafter": FETCH_SINCE_DATE.strftime("%Y%m%d"),
            **sample_feed_config.yt_args,
        },
    )
    mock_db_manager.get_download_by_id.assert_not_called()
    mock_db_manager.upsert_download.assert_not_called()


@pytest.mark.unit
def test_fetch_and_process_new_feed_downloads_new_vod_download(
    enqueuer: Enqueuer,
    mock_ytdlp_wrapper: MagicMock,
    mock_db_manager: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test processing a new VOD download."""
    new_vod = create_download("new_video1", DownloadStatus.QUEUED)
    mock_ytdlp_wrapper.fetch_metadata.return_value = [new_vod]
    mock_db_manager.get_download_by_id.side_effect = DownloadNotFoundError(
        message="Not found", feed_id=FEED_ID, download_id="new_video1"
    )

    count = enqueuer._fetch_and_process_new_feed_downloads(  # type: ignore
        FEED_ID, sample_feed_config, FETCH_SINCE_DATE
    )

    assert count == 1
    mock_db_manager.get_download_by_id.assert_called_once_with(FEED_ID, "new_video1")
    mock_db_manager.upsert_download.assert_called_once_with(new_vod)


@pytest.mark.unit
def test_fetch_and_process_new_feed_downloads_new_upcoming_download(
    enqueuer: Enqueuer,
    mock_ytdlp_wrapper: MagicMock,
    mock_db_manager: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test processing a new UPCOMING download."""
    new_upcoming = create_download("new_video_live", DownloadStatus.UPCOMING)
    mock_ytdlp_wrapper.fetch_metadata.return_value = [new_upcoming]
    mock_db_manager.get_download_by_id.side_effect = DownloadNotFoundError(
        message="Not found", feed_id=FEED_ID, download_id="new_video_live"
    )

    count = enqueuer._fetch_and_process_new_feed_downloads(  # type: ignore
        FEED_ID, sample_feed_config, FETCH_SINCE_DATE
    )

    assert count == 0  # Not QUEUED yet
    mock_db_manager.get_download_by_id.assert_called_once_with(
        FEED_ID, "new_video_live"
    )
    mock_db_manager.upsert_download.assert_called_once_with(new_upcoming)


@pytest.mark.unit
def test_fetch_and_process_new_feed_downloads_existing_upcoming_now_vod(
    enqueuer: Enqueuer,
    mock_ytdlp_wrapper: MagicMock,
    mock_db_manager: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test processing an existing UPCOMING download that is now a VOD."""
    existing_upcoming_in_db = create_download("video_live1", DownloadStatus.UPCOMING)
    fetched_as_vod = create_download("video_live1", DownloadStatus.QUEUED)

    mock_ytdlp_wrapper.fetch_metadata.return_value = [fetched_as_vod]
    mock_db_manager.get_download_by_id.return_value = existing_upcoming_in_db

    count = enqueuer._fetch_and_process_new_feed_downloads(  # type: ignore
        FEED_ID, sample_feed_config, FETCH_SINCE_DATE
    )

    assert count == 1
    mock_db_manager.get_download_by_id.assert_called_once_with(FEED_ID, "video_live1")
    mock_db_manager.mark_as_queued_from_upcoming.assert_called_once_with(
        FEED_ID, "video_live1"
    )
    mock_db_manager.upsert_download.assert_not_called()


@pytest.mark.unit
def test_fetch_and_process_new_feed_downloads_existing_downloaded_requeued(
    enqueuer: Enqueuer,
    mock_ytdlp_wrapper: MagicMock,
    mock_db_manager: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test that an already DOWNLOADED item is re-queued if fetched again as QUEUED."""
    existing_downloaded_in_db = create_download("video_done", DownloadStatus.DOWNLOADED)
    fetched_again_as_queued = create_download("video_done", DownloadStatus.QUEUED)

    mock_ytdlp_wrapper.fetch_metadata.return_value = [fetched_again_as_queued]
    mock_db_manager.get_download_by_id.return_value = existing_downloaded_in_db

    count = enqueuer._fetch_and_process_new_feed_downloads(  # type: ignore
        FEED_ID, sample_feed_config, FETCH_SINCE_DATE
    )

    assert count == 1
    mock_db_manager.get_download_by_id.assert_called_once_with(FEED_ID, "video_done")
    mock_db_manager.requeue_download.assert_called_once_with(FEED_ID, "video_done")
    mock_db_manager.upsert_download.assert_not_called()


@pytest.mark.unit
def test_fetch_and_process_new_feed_downloads_existing_error_requeued(
    enqueuer: Enqueuer,
    mock_ytdlp_wrapper: MagicMock,
    mock_db_manager: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test that if DB status is ERROR and fetched status is QUEUED, it calls requeue_download."""
    existing_error_in_db = create_download("video_err", DownloadStatus.ERROR, retries=1)
    fetched_as_queued = create_download("video_err", DownloadStatus.QUEUED)

    mock_ytdlp_wrapper.fetch_metadata.return_value = [fetched_as_queued]
    mock_db_manager.get_download_by_id.return_value = existing_error_in_db

    count = enqueuer._fetch_and_process_new_feed_downloads(  # type: ignore
        FEED_ID, sample_feed_config, FETCH_SINCE_DATE
    )

    assert count == 1  # Because it was re-queued
    mock_db_manager.get_download_by_id.assert_called_once_with(FEED_ID, "video_err")
    mock_db_manager.requeue_download.assert_called_once_with(FEED_ID, "video_err")
    mock_db_manager.upsert_download.assert_not_called()


@pytest.mark.unit
def test_enqueue_new_downloads_full_flow_mixed_scenarios(
    enqueuer: Enqueuer,
    mock_db_manager: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test the main enqueue_new_downloads with a mix of scenarios."""
    # --- Setup for _handle_existing_upcoming_downloads ---
    # 1. Upcoming that becomes VOD
    upcoming1_db = create_download("up1", DownloadStatus.UPCOMING)
    upcoming1_refetched_vod = create_download("up1", DownloadStatus.QUEUED)
    # 2. Upcoming that stays upcoming
    upcoming2_db = create_download("up2", DownloadStatus.UPCOMING)
    upcoming2_refetched_upcoming = create_download("up2", DownloadStatus.UPCOMING)

    mock_db_manager.get_downloads_by_status.return_value = [upcoming1_db, upcoming2_db]

    # --- Setup for _fetch_and_process_new_feed_downloads ---
    # 3. New VOD from feed
    new_vod_feed = create_download("feed_new_vod", DownloadStatus.QUEUED)
    # 4. Existing UPCOMING in DB, now fetched as VOD from feed
    existing_up3_db = create_download("feed_up3_now_vod", DownloadStatus.UPCOMING)
    fetched_up3_as_vod = create_download("feed_up3_now_vod", DownloadStatus.QUEUED)
    # 5. New upcoming from feed
    new_upcoming_feed = create_download("feed_new_upcoming", DownloadStatus.UPCOMING)

    # The third call to ytdlp_wrapper.fetch_metadata (for the main feed)
    main_feed_fetch_result = [new_vod_feed, fetched_up3_as_vod, new_upcoming_feed]
    mock_ytdlp_wrapper.fetch_metadata.side_effect = [
        [upcoming1_refetched_vod],
        [upcoming2_refetched_upcoming],
        main_feed_fetch_result,
    ]

    # Mock get_download_by_id calls for main feed processing
    # This depends on the order of items in main_feed_fetch_result
    mock_db_manager.get_download_by_id.side_effect = [
        DownloadNotFoundError(
            message="Not found", feed_id=FEED_ID, download_id="feed_new_vod"
        ),  # new_vod_feed
        existing_up3_db,  # fetched_up3_as_vod
        DownloadNotFoundError(
            message="Not found", feed_id=FEED_ID, download_id="feed_new_upcoming"
        ),  # new_upcoming_feed
    ]

    # --- Execute ---
    total_queued = enqueuer.enqueue_new_downloads(
        FEED_ID, sample_feed_config, FETCH_SINCE_DATE
    )

    # --- Assertions ---
    # Expected:
    # - upcoming1_db -> QUEUED (1)
    # - new_vod_feed -> QUEUED (1)
    # - existing_up3_db -> QUEUED (1)
    assert total_queued == 3

    # Assert calls for _handle_existing_upcoming_downloads
    assert mock_db_manager.get_downloads_by_status.call_count == 1
    assert mock_db_manager.get_downloads_by_status.call_args_list[0] == call(
        DownloadStatus.UPCOMING, feed=FEED_ID
    )

    # Assert ytdlp_wrapper.fetch_metadata calls
    # Call 1 (upcoming1_db)
    # Call 2 (upcoming2_db)
    # Call 3 (main feed fetch)
    assert mock_ytdlp_wrapper.fetch_metadata.call_count == 3
    mock_ytdlp_wrapper.fetch_metadata.assert_has_calls(
        [
            call(FEED_ID, upcoming1_db.source_url, sample_feed_config.yt_args),
            call(FEED_ID, upcoming2_db.source_url, sample_feed_config.yt_args),
            call(
                FEED_ID,
                sample_feed_config.url,
                {
                    "dateafter": FETCH_SINCE_DATE.strftime("%Y%m%d"),
                    **sample_feed_config.yt_args,
                },
            ),
        ]
    )

    # Assert db_manager.update_status calls
    # Called for upcoming1_db (True) and for existing_up3_db (True)
    # Reset side_effect for update_status for clarity in this specific test's assertions
    mock_db_manager.mark_as_queued_from_upcoming.assert_has_calls(
        [
            call(FEED_ID, upcoming1_db.id),
            call(FEED_ID, existing_up3_db.id),
        ]
    )
    assert mock_db_manager.mark_as_queued_from_upcoming.call_count == 2

    mock_db_manager.upsert_download.assert_has_calls(
        [
            call(new_vod_feed),
            call(new_upcoming_feed),
        ]
    )
    assert mock_db_manager.upsert_download.call_count == 2


@pytest.mark.unit
def test_enqueue_new_downloads_db_error_on_get_upcoming(
    enqueuer: Enqueuer,
    mock_db_manager: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test EnqueueError when DB fails during fetching upcoming downloads."""
    mock_db_manager.get_downloads_by_status.side_effect = DatabaseOperationError(
        "DB error"
    )
    with pytest.raises(EnqueueError) as exc_info:
        enqueuer.enqueue_new_downloads(FEED_ID, sample_feed_config, FETCH_SINCE_DATE)
    assert "Could not fetch upcoming downloads from DB" in str(exc_info.value)
    assert exc_info.value.feed_id == FEED_ID


@pytest.mark.unit
def test_enqueue_new_downloads_ytdlp_error_on_main_feed_fetch(
    enqueuer: Enqueuer,
    mock_db_manager: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test EnqueueError when YTDLP fails during main feed metadata fetch."""
    mock_db_manager.get_downloads_by_status.return_value = []  # No upcoming
    mock_ytdlp_wrapper.fetch_metadata.side_effect = YtdlpApiError(
        "YTDLP error", feed_id=FEED_ID, url=FEED_URL
    )

    with pytest.raises(EnqueueError) as exc_info:
        enqueuer.enqueue_new_downloads(FEED_ID, sample_feed_config, FETCH_SINCE_DATE)

    assert "Could not fetch main feed metadata" in str(exc_info.value)
    assert exc_info.value.feed_id == FEED_ID
    assert exc_info.value.feed_url == FEED_URL
    # Ensure ytdlp_wrapper.fetch_metadata was called for the main feed
    mock_ytdlp_wrapper.fetch_metadata.assert_called_once_with(
        FEED_ID,
        sample_feed_config.url,
        {
            "dateafter": FETCH_SINCE_DATE.strftime("%Y%m%d"),
            **sample_feed_config.yt_args,
        },
    )
