# pyright: reportPrivateUsage=false

"""Integration tests for FeedScheduler with real APScheduler.

Tests focus on scheduler functionality with real APScheduler backend,
verifying job scheduling, execution, lifecycle management, and integration
with DataCoordinator for actual feed processing jobs.
"""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from anypod.config import FeedConfig
from anypod.data_coordinator import DataCoordinator, Downloader, Enqueuer, Pruner
from anypod.data_coordinator.types import PhaseResult, ProcessingResults
from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.types import DownloadStatus, Feed, SourceType
from anypod.file_manager import FileManager
from anypod.rss import RSSFeedGenerator
from anypod.schedule import FeedScheduler

# Test constants
CRON_EVERY_SECOND = "* * * * * *"  # Every second for fast testing
CRON_EVERY_2_SECONDS = "* * * * * */2"  # Every 2 seconds for fast testing
CRON_DAILY = "@midnight"  # Daily at midnight
BIG_BUCK_BUNNY_VIDEO_ID = "aqz-KE-bpKQ"
BIG_BUCK_BUNNY_SHORT_URL = f"https://youtu.be/{BIG_BUCK_BUNNY_VIDEO_ID}"
YT_DLP_MINIMAL_ARGS_STR = "--format worst*[ext=mp4]/worst[ext=mp4]/best[ext=mp4]"

# Test configuration constants
BASE_URL = "http://localhost"
EXAMPLE_URL = "https://example.com/test"
FEED_TITLE_PREFIX = "Test Feed"
MOCK_START_TIME = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

# Test timing constants
MAX_WAIT_TIME = 10
CHECK_INTERVAL = 0.5
SHORT_WAIT = 2
STARTUP_WAIT = 1.5
SHUTDOWN_WAIT = 0.1
DOWNLOAD_MAX_WAIT = 30
DOWNLOAD_CHECK_INTERVAL = 1
FAST_SHUTDOWN_THRESHOLD = 1.0


async def wait_for_condition(
    condition: Callable[[], bool | Awaitable[bool]],
    max_wait_time: float = MAX_WAIT_TIME,
    check_interval: float = CHECK_INTERVAL,
    timeout_message: str = "Condition not met within timeout",
) -> None:
    """Wait for a condition to become true, checking periodically.

    Args:
        condition: Function that returns True when condition is met.
                  Can be sync or async.
        max_wait_time: Maximum time to wait in seconds.
        check_interval: Time between checks in seconds.
        timeout_message: Message to include in timeout assertion.

    Raises:
        AssertionError: If condition is not met within max_wait_time.
    """
    elapsed = 0.0

    while elapsed < max_wait_time:
        result = condition()
        # Handle both sync and async conditions
        if asyncio.iscoroutine(result):
            result = await result

        if result:
            return

        await asyncio.sleep(check_interval)
        elapsed += check_interval

    # Condition not met within timeout
    raise AssertionError(f"{timeout_message} (waited {elapsed:.1f}s)")


@pytest.fixture
def data_coordinator(
    enqueuer: Enqueuer,
    downloader: Downloader,
    pruner: Pruner,
    rss_generator: RSSFeedGenerator,
    feed_db: FeedDatabase,
    cookies_path: Path | None,
) -> DataCoordinator:
    """Provides a DataCoordinator instance combining all services."""
    return DataCoordinator(
        enqueuer, downloader, pruner, rss_generator, feed_db, cookies_path
    )


@pytest.fixture
def mock_data_coordinator() -> MagicMock:
    """Provides a mock DataCoordinator for faster tests."""
    mock = MagicMock()
    mock.process_feed = AsyncMock(
        return_value=ProcessingResults(
            feed_id="test_feed",
            start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            total_duration_seconds=1.0,
            overall_success=True,
            enqueue_result=PhaseResult(success=True, count=1),
            download_result=PhaseResult(success=True, count=1),
            prune_result=PhaseResult(success=True, count=0),
            rss_generation_result=PhaseResult(success=True, count=1),
            feed_sync_updated=True,
        )
    )
    return mock


def create_test_feed(feed_db: FeedDatabase, feed_id: str) -> Feed:
    """Create a test feed in the database."""
    feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.UNKNOWN,
        source_url="https://example.com/test",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title=f"Test Feed {feed_id}",
    )
    feed_db.upsert_feed(feed)
    return feed


def create_feed_config(
    url: str = BIG_BUCK_BUNNY_SHORT_URL,
    schedule: str = CRON_EVERY_SECOND,
    yt_args: str = YT_DLP_MINIMAL_ARGS_STR,
) -> FeedConfig:
    """Create a FeedConfig instance for testing."""
    return FeedConfig(
        url=url,
        yt_args=yt_args,  # type: ignore
        schedule=schedule,
        keep_last=None,
        since=None,
        max_errors=3,
    )


# --- Integration Tests ---


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scheduler_lifecycle_basic(
    mock_data_coordinator: MagicMock,
):
    """Test basic scheduler lifecycle: create, start, stop."""
    feed_configs = {
        "test_feed": create_feed_config(schedule="0 0 * * *")  # Daily schedule
    }

    scheduler = FeedScheduler(
        ready_feed_ids=["test_feed"],
        feed_configs=feed_configs,
        data_coordinator=mock_data_coordinator,
    )

    # Verify initial state
    assert scheduler.running is False
    assert scheduler.get_scheduled_feed_ids() == ["test_feed"]

    # Start scheduler
    await scheduler.start()
    assert scheduler.running is True

    # Stop scheduler
    await scheduler.stop()
    # Give APScheduler a moment to fully shut down
    await asyncio.sleep(0.1)
    assert scheduler.running is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scheduler_job_execution_with_mock(
    mock_data_coordinator: MagicMock,
):
    """Test that scheduled jobs actually execute with mock coordinator."""
    # Use a very frequent schedule for testing
    feed_configs = {
        "frequent_feed": create_feed_config(schedule=CRON_EVERY_SECOND)  # Every second
    }

    scheduler = FeedScheduler(
        ready_feed_ids=["frequent_feed"],
        feed_configs=feed_configs,
        data_coordinator=mock_data_coordinator,
    )

    await scheduler.start()

    try:
        # Wait for job to execute
        await wait_for_condition(
            condition=lambda: mock_data_coordinator.process_feed.call_count > 0,
            timeout_message=f"Expected at least 1 call, got {mock_data_coordinator.process_feed.call_count}",
        )

        # Verify it was called with correct parameters
        call_args = mock_data_coordinator.process_feed.call_args_list[0]
        assert call_args[0][0] == "frequent_feed"  # feed_id
        assert call_args[0][1] == feed_configs["frequent_feed"]  # feed_config

    finally:
        await scheduler.stop()
        await asyncio.sleep(0.1)  # Give time to shut down


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scheduler_multiple_feeds(
    mock_data_coordinator: MagicMock,
):
    """Test scheduler with multiple feeds having different schedules."""
    feed_configs = {
        "feed1": create_feed_config(schedule=CRON_EVERY_SECOND),  # Every second
        "feed2": create_feed_config(schedule=CRON_EVERY_2_SECONDS),  # Every 2 seconds
    }

    scheduler = FeedScheduler(
        ready_feed_ids=["feed1", "feed2"],
        feed_configs=feed_configs,
        data_coordinator=mock_data_coordinator,
    )

    await scheduler.start()

    try:
        # Wait for both feeds to be called at least once
        def both_feeds_called() -> bool:
            call_args_list = mock_data_coordinator.process_feed.call_args_list
            feed_ids_called = {call[0][0] for call in call_args_list}
            return "feed1" in feed_ids_called and "feed2" in feed_ids_called

        await wait_for_condition(
            condition=both_feeds_called,
            max_wait_time=10,
            check_interval=0.5,
            timeout_message="Both feeds were not called within timeout",
        )

        # Verify both feeds were processed
        call_args_list = mock_data_coordinator.process_feed.call_args_list
        feed_ids_called = {call[0][0] for call in call_args_list}

        assert "feed1" in feed_ids_called, (
            f"feed1 not called. Called: {feed_ids_called}"
        )
        assert "feed2" in feed_ids_called, (
            f"feed2 not called. Called: {feed_ids_called}"
        )

        # feed1 should be called more frequently than feed2
        feed1_calls = sum(1 for call in call_args_list if call[0][0] == "feed1")
        feed2_calls = sum(1 for call in call_args_list if call[0][0] == "feed2")

        assert feed1_calls >= feed2_calls

    finally:
        await scheduler.stop()
        await asyncio.sleep(0.1)  # Give time to shut down


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scheduler_graceful_shutdown_wait_for_jobs(
    mock_data_coordinator: MagicMock,
):
    """Test graceful shutdown with wait_for_jobs parameter."""
    feed_configs = {
        "test_feed": create_feed_config(schedule=CRON_EVERY_SECOND)  # Every second
    }

    scheduler = FeedScheduler(
        ready_feed_ids=["test_feed"],
        feed_configs=feed_configs,
        data_coordinator=mock_data_coordinator,
    )

    await scheduler.start()

    # Wait for a job to potentially execute
    await asyncio.sleep(2)

    # Test that stop with wait_for_jobs=True completes
    await scheduler.stop(wait_for_jobs=True)

    # Give APScheduler a moment to fully shut down
    await asyncio.sleep(0.1)
    assert scheduler.running is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scheduler_shutdown_no_wait(
    mock_data_coordinator: MagicMock,
):
    """Test shutdown without waiting for jobs."""

    # Mock a slow-running job
    async def slow_process_feed(*args, **kwargs):  # type: ignore
        await asyncio.sleep(3)  # Simulate longer work
        return ProcessingResults(
            feed_id="slow_feed",
            start_time=datetime.now(UTC),
            total_duration_seconds=3.0,
            overall_success=True,
            enqueue_result=PhaseResult(success=True, count=1),
            download_result=PhaseResult(success=True, count=1),
            prune_result=PhaseResult(success=True, count=0),
            rss_generation_result=PhaseResult(success=True, count=1),
            feed_sync_updated=True,
        )

    mock_data_coordinator.process_feed.side_effect = slow_process_feed

    feed_configs = {
        "slow_feed": create_feed_config(schedule=CRON_EVERY_SECOND)  # Every second
    }

    scheduler = FeedScheduler(
        ready_feed_ids=["slow_feed"],
        feed_configs=feed_configs,
        data_coordinator=mock_data_coordinator,
    )

    await scheduler.start()

    # Wait for a job to start
    await asyncio.sleep(1.5)

    # Stop with wait_for_jobs=False (should not wait)
    start_time = time.time()
    await scheduler.stop(wait_for_jobs=False)
    stop_duration = time.time() - start_time

    # Should stop quickly without waiting
    assert stop_duration < 1.0
    # Give APScheduler a moment to fully shut down
    await asyncio.sleep(0.1)
    assert scheduler.running is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scheduler_job_failure_handling(
    mock_data_coordinator: MagicMock,
):
    """Test scheduler handles job failures gracefully."""

    # Mock a failing job
    async def failing_process_feed(*args, **kwargs):  # type: ignore
        raise Exception("Process feed failed")

    mock_data_coordinator.process_feed.side_effect = failing_process_feed

    feed_configs = {
        "failing_feed": create_feed_config(schedule=CRON_EVERY_SECOND)  # Every second
    }

    scheduler = FeedScheduler(
        ready_feed_ids=["failing_feed"],
        feed_configs=feed_configs,
        data_coordinator=mock_data_coordinator,
    )

    await scheduler.start()

    try:
        # Wait for job to execute and fail
        await wait_for_condition(
            condition=lambda: mock_data_coordinator.process_feed.call_count > 0,
            max_wait_time=10,
            check_interval=0.5,
            timeout_message="Failing job was not attempted within timeout",
        )

        # Scheduler should still be running despite job failures
        assert scheduler.running is True

        # Verify the failing job was attempted
        assert mock_data_coordinator.process_feed.call_count >= 1

    finally:
        await scheduler.stop()
        await asyncio.sleep(0.1)  # Give time to shut down


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scheduler_with_real_data_coordinator_and_file_download(
    data_coordinator: DataCoordinator,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    file_manager: FileManager,
):
    """Test scheduler with real DataCoordinator that actually downloads files.

    This test verifies that the scheduler triggers successful feed processing that results
    in actual file downloads and database records. Uses a sync timestamp from the same day
    as Big Buck Bunny to ensure the video is included in yt-dlp's day-level date filtering.
    """
    feed_id = "real_download_test_feed"

    # Create feed in database
    create_test_feed(feed_db, feed_id)

    initial_sync_time = datetime(2014, 11, 10, 12, 0, 0, tzinfo=UTC)
    feed_db.mark_sync_success(feed_id, sync_time=initial_sync_time)

    feed_configs = {
        feed_id: create_feed_config(
            url=BIG_BUCK_BUNNY_SHORT_URL,
            schedule=CRON_EVERY_SECOND,  # Every second
        )
    }

    scheduler = FeedScheduler(
        ready_feed_ids=[feed_id],
        feed_configs=feed_configs,
        data_coordinator=data_coordinator,
    )

    await scheduler.start()

    try:
        # Wait for successful download completion
        async def download_completed() -> bool:
            # Check if feed was processed successfully
            updated_feed = feed_db.get_feed_by_id(feed_id)
            if updated_feed.last_successful_sync > initial_sync_time:
                # Feed was processed, check for downloaded files
                downloads = download_db.get_downloads_by_status(
                    DownloadStatus.DOWNLOADED, feed_id=feed_id
                )
                if downloads:
                    # Check if any files were actually written to disk
                    for download in downloads:
                        # Check if the downloaded file exists using FileManager
                        file_path = await file_manager._paths.media_file_path(
                            feed_id, download.id, download.ext
                        )
                        if file_path.exists():
                            # Success! We found downloaded files
                            assert file_path.is_file()
                            assert updated_feed.last_successful_sync > initial_sync_time
                            return True
            return False

        await wait_for_condition(
            condition=download_completed,
            max_wait_time=DOWNLOAD_MAX_WAIT,
            check_interval=DOWNLOAD_CHECK_INTERVAL,
            timeout_message="Download was not completed within timeout",
        )
        # Test passed - download completed successfully

    finally:
        await scheduler.stop()
        await asyncio.sleep(0.1)  # Give time to shut down
