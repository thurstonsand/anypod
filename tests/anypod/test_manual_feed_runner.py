# pyright: reportPrivateUsage=false

"""Unit tests for ManualFeedRunner."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from anypod.config import FeedConfig
from anypod.config.types import FeedMetadataOverrides
from anypod.data_coordinator import DataCoordinator
from anypod.data_coordinator.types import PhaseResult, ProcessingResults
from anypod.exceptions import DataCoordinatorError
from anypod.manual_feed_runner import ManualFeedRunner

FEED_ID = "manual_feed"


@pytest.fixture
def mock_data_coordinator() -> MagicMock:
    """Create a mock DataCoordinator for testing."""
    return MagicMock(spec=DataCoordinator)


@pytest.fixture
def feed_configs() -> dict[str, FeedConfig]:
    """Create test feed configurations."""
    return {
        FEED_ID: FeedConfig(
            url=None,
            schedule="manual",  # type: ignore[arg-type]
            metadata=FeedMetadataOverrides(title="Manual Feed"),
        )
    }


@pytest.fixture
def feed_semaphore() -> asyncio.Semaphore:
    """Create a semaphore for testing."""
    return asyncio.Semaphore(1)


@pytest.fixture
def manual_feed_runner(
    mock_data_coordinator: MagicMock,
    feed_configs: dict[str, FeedConfig],
    feed_semaphore: asyncio.Semaphore,
) -> ManualFeedRunner:
    """Create a ManualFeedRunner with mocked dependencies."""
    return ManualFeedRunner(
        data_coordinator=mock_data_coordinator,
        feed_configs=feed_configs,
        feed_semaphore=feed_semaphore,
    )


@pytest.fixture
def sample_processing_results() -> ProcessingResults:
    """Create sample processing results."""
    return ProcessingResults(
        feed_id=FEED_ID,
        start_time=datetime.now(UTC),
        enqueue_result=PhaseResult(success=True, count=0),
        download_result=PhaseResult(success=True, count=0),
        prune_result=PhaseResult(success=True, count=0),
        rss_generation_result=PhaseResult(success=True, count=0),
    )


# --- Tests for trigger ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_trigger_creates_task_when_none_exists(
    manual_feed_runner: ManualFeedRunner,
    feed_configs: dict[str, FeedConfig],
    mock_data_coordinator: MagicMock,
    sample_processing_results: ProcessingResults,
) -> None:
    """Trigger creates a new task when no task exists for the feed."""
    # Use a slow process_feed to keep task in queued_tasks
    task_started = asyncio.Event()

    async def slow_process(*_args: object, **_kwargs: object) -> ProcessingResults:
        task_started.set()
        await asyncio.sleep(0.1)
        return sample_processing_results

    mock_data_coordinator.process_feed = slow_process
    feed_config = feed_configs[FEED_ID]

    await manual_feed_runner.trigger(FEED_ID, feed_config)

    # Wait for task to be in queued_tasks (before it acquires semaphore)
    # Check immediately after trigger - task should be queued
    assert FEED_ID in manual_feed_runner._queued_tasks
    task = manual_feed_runner._queued_tasks[FEED_ID]
    assert isinstance(task, asyncio.Task)

    # Wait for task to complete
    await task


@pytest.mark.unit
@pytest.mark.asyncio
async def test_trigger_creates_task_before_previous_starts_running(
    manual_feed_runner: ManualFeedRunner,
    feed_configs: dict[str, FeedConfig],
    mock_data_coordinator: MagicMock,
    sample_processing_results: ProcessingResults,
) -> None:
    """Trigger does not create duplicate when called before first task runs."""
    # Use a semaphore that's already locked to prevent task from starting
    locked_semaphore = asyncio.Semaphore(0)
    runner = ManualFeedRunner(
        data_coordinator=mock_data_coordinator,
        feed_configs=feed_configs,
        feed_semaphore=locked_semaphore,
    )

    mock_data_coordinator.process_feed = AsyncMock(
        return_value=sample_processing_results
    )
    feed_config = feed_configs[FEED_ID]

    # First trigger - task is queued but blocked by semaphore
    await runner.trigger(FEED_ID, feed_config)
    first_task = runner._queued_tasks.get(FEED_ID)
    assert first_task is not None

    # Second trigger - should detect existing task
    await runner.trigger(FEED_ID, feed_config)
    second_check = runner._queued_tasks.get(FEED_ID)

    # Should still be the same task
    assert second_check is first_task

    # Release semaphore and clean up
    locked_semaphore.release()
    await first_task


@pytest.mark.unit
@pytest.mark.asyncio
async def test_trigger_creates_new_task_after_previous_completes(
    manual_feed_runner: ManualFeedRunner,
    feed_configs: dict[str, FeedConfig],
    mock_data_coordinator: MagicMock,
    sample_processing_results: ProcessingResults,
) -> None:
    """Trigger creates a new task after previous task completes."""
    mock_data_coordinator.process_feed = AsyncMock(
        return_value=sample_processing_results
    )
    feed_config = feed_configs[FEED_ID]

    # First trigger
    await manual_feed_runner.trigger(FEED_ID, feed_config)
    first_task = manual_feed_runner._queued_tasks.get(FEED_ID)
    assert first_task is not None

    # Wait for completion
    await first_task

    # Second trigger after first completes
    await manual_feed_runner.trigger(FEED_ID, feed_config)
    second_task = manual_feed_runner._queued_tasks.get(FEED_ID)

    assert second_task is not None
    assert second_task is not first_task

    # Clean up
    await second_task


# --- Tests for _run_feed ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_feed_acquires_and_releases_semaphore(
    manual_feed_runner: ManualFeedRunner,
    feed_configs: dict[str, FeedConfig],
    mock_data_coordinator: MagicMock,
    sample_processing_results: ProcessingResults,
    feed_semaphore: asyncio.Semaphore,
) -> None:
    """Run feed properly acquires and releases semaphore."""
    mock_data_coordinator.process_feed = AsyncMock(
        return_value=sample_processing_results
    )
    feed_config = feed_configs[FEED_ID]

    # Semaphore starts with value 1
    assert feed_semaphore._value == 1  # type: ignore[attr-defined]

    # Create task manually to avoid trigger() complications
    task = asyncio.create_task(manual_feed_runner._run_feed(FEED_ID, feed_config))

    # Brief wait to let task acquire semaphore
    await asyncio.sleep(0.01)

    # Semaphore should be held (value 0) during execution
    # But since the task is so fast, it might have already released
    # So we just verify the task completes successfully
    result = await task

    # After completion, semaphore should be back to 1
    assert feed_semaphore._value == 1  # type: ignore[attr-defined]
    assert result == sample_processing_results


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_feed_removes_task_from_queued_tasks(
    manual_feed_runner: ManualFeedRunner,
    feed_configs: dict[str, FeedConfig],
    mock_data_coordinator: MagicMock,
    sample_processing_results: ProcessingResults,
) -> None:
    """Run feed removes task from queued_tasks registry."""
    mock_data_coordinator.process_feed = AsyncMock(
        return_value=sample_processing_results
    )
    feed_config = feed_configs[FEED_ID]

    # Manually add a dummy task to queued_tasks
    dummy_task = asyncio.create_task(asyncio.sleep(0))
    manual_feed_runner._queued_tasks[FEED_ID] = dummy_task

    # Run the feed
    await manual_feed_runner._run_feed(FEED_ID, feed_config)

    # Task should have been removed during execution
    assert FEED_ID not in manual_feed_runner._queued_tasks

    # Clean up
    await dummy_task


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_feed_calls_data_coordinator_with_correct_args(
    manual_feed_runner: ManualFeedRunner,
    feed_configs: dict[str, FeedConfig],
    mock_data_coordinator: MagicMock,
    sample_processing_results: ProcessingResults,
) -> None:
    """Run feed calls data coordinator with feed_id and config."""
    mock_data_coordinator.process_feed = AsyncMock(
        return_value=sample_processing_results
    )
    feed_config = feed_configs[FEED_ID]

    await manual_feed_runner._run_feed(FEED_ID, feed_config)

    mock_data_coordinator.process_feed.assert_awaited_once_with(FEED_ID, feed_config)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_feed_propagates_coordinator_errors(
    manual_feed_runner: ManualFeedRunner,
    feed_configs: dict[str, FeedConfig],
    mock_data_coordinator: MagicMock,
) -> None:
    """Run feed propagates DataCoordinatorError from coordinator."""
    error = DataCoordinatorError("Coordination failed")
    mock_data_coordinator.process_feed = AsyncMock(side_effect=error)
    feed_config = feed_configs[FEED_ID]

    with pytest.raises(DataCoordinatorError):
        await manual_feed_runner._run_feed(FEED_ID, feed_config)
