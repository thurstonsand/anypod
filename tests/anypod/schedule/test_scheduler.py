# pyright: reportPrivateUsage=false

"""Tests for the FeedScheduler class.

This module contains unit tests for the FeedScheduler class, which is responsible
for managing scheduled feed processing jobs using APScheduler with async support,
graceful error handling, and proper lifecycle management.
"""

from datetime import UTC, datetime
import time
from unittest.mock import MagicMock, patch

from pydantic import ValidationError
import pytest

from anypod.config import FeedConfig
from anypod.config.types import FeedMetadataOverrides
from anypod.data_coordinator.types import PhaseResult, ProcessingResults
from anypod.schedule import scheduler
from anypod.schedule.scheduler import FeedScheduler

# --- Fixtures ---


@pytest.fixture
def mock_data_coordinator() -> MagicMock:
    """Provides a mock DataCoordinator."""
    mock = MagicMock()
    mock.process_feed.return_value = ProcessingResults(
        feed_id="test_feed",
        start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
        enqueue_result=PhaseResult(success=True, count=1),
        download_result=PhaseResult(success=True, count=1),
        prune_result=PhaseResult(success=True, count=0),
        rss_generation_result=PhaseResult(success=True, count=1),
    )
    return mock


@pytest.fixture
def sample_feed_config() -> FeedConfig:
    """Provides a sample FeedConfig object."""
    return FeedConfig(
        url="http://example.com/feed_url",
        yt_args="--format best",  # type: ignore # this gets preprocessed into a dict
        schedule="0 3 * * *",  # type: ignore # this gets preprocessed into a CronExpression
        keep_last=10,
        since=None,
        max_errors=3,
        metadata=FeedMetadataOverrides(title="Test Podcast"),  # type: ignore
    )


@pytest.fixture
def sample_feed_configs(sample_feed_config: FeedConfig) -> dict[str, FeedConfig]:
    """Provides sample feed configurations."""
    return {
        "test_feed": sample_feed_config,
        "another_feed": FeedConfig(
            url="http://example.com/another_feed",
            schedule="*/30 * * * *",  # type: ignore # this gets preprocessed into a CronExpression
            yt_args=None,  # type: ignore
            metadata=None,
        ),
    }


# --- Tests for FeedScheduler.__init__ ---


@pytest.mark.unit
def test_init_with_valid_feeds_succeeds(
    mock_data_coordinator: MagicMock,
    sample_feed_configs: dict[str, FeedConfig],
):
    """Test that __init__ succeeds with valid feed configurations."""
    ready_feed_ids = ["test_feed", "another_feed"]

    scheduler = FeedScheduler(
        ready_feed_ids=ready_feed_ids,
        feed_configs=sample_feed_configs,
        data_coordinator=mock_data_coordinator,
    )

    # Verify scheduler was created successfully
    assert scheduler is not None
    assert scheduler.running is False  # Should not be running initially


@pytest.mark.unit
@pytest.mark.parametrize(
    "valid_cron",
    [
        "0 0 * * *",  # Daily at midnight
        "*/5 * * * *",  # Every 5 minutes
        "0 12 * * 1",  # Weekly on Monday at noon
        "30 2 1 * *",  # Monthly on 1st at 2:30 AM
        "0 0 1 1 *",  # Yearly on Jan 1st
    ],
)
def test_init_with_valid_cron_succeeds(
    mock_data_coordinator: MagicMock,
    valid_cron: str,
):
    """Test that __init__ succeeds with valid cron expressions."""
    valid_config = {
        "valid_feed": FeedConfig(
            url="http://example.com/feed",
            schedule=valid_cron,
            yt_args=None,  # type: ignore
            metadata=None,
        )
    }

    # Should not raise any exception
    scheduler = FeedScheduler(
        ready_feed_ids=["valid_feed"],
        feed_configs=valid_config,
        data_coordinator=mock_data_coordinator,
    )

    assert scheduler is not None
    assert scheduler.get_scheduled_feed_ids() == ["valid_feed"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "invalid_cron",
    [
        "invalid cron",
        "* * * *",  # Too few fields
        "* * * * * * *",  # Too many fields (7 fields)
        "99 * * * *",  # Invalid minute
        "* 25 * * *",  # Invalid hour
        "* * 32 * *",  # Invalid day
        "* * * 13 *",  # Invalid month
        "* * * * 8",  # Invalid day of week
    ],
)
def test_init_with_invalid_cron_raises_error(
    invalid_cron: str,
):
    """Test that FeedConfig raises ValidationError for invalid cron expressions."""
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig(
            url="http://example.com/feed",
            schedule=invalid_cron,
            yt_args=None,  # type: ignore
            metadata=None,
        )

    # Check that the error message contains the cron expression
    assert invalid_cron in str(exc_info.value)


@pytest.mark.unit
def test_init_with_empty_ready_feeds(
    mock_data_coordinator: MagicMock,
):
    """Test that __init__ works with empty ready feeds list."""
    scheduler = FeedScheduler(
        ready_feed_ids=[],
        feed_configs={},
        data_coordinator=mock_data_coordinator,
    )

    assert scheduler is not None
    assert scheduler.get_scheduled_feed_ids() == []


# --- Tests for get_scheduled_feed_ids ---


@pytest.mark.unit
def test_get_scheduled_feed_ids_returns_correct_feeds(
    mock_data_coordinator: MagicMock,
    sample_feed_configs: dict[str, FeedConfig],
):
    """Test that get_scheduled_feed_ids returns correct feed IDs."""
    ready_feed_ids = ["test_feed", "another_feed"]

    scheduler = FeedScheduler(
        ready_feed_ids=ready_feed_ids,
        feed_configs=sample_feed_configs,
        data_coordinator=mock_data_coordinator,
    )

    scheduled_feeds = scheduler.get_scheduled_feed_ids()
    assert set(scheduled_feeds) == set(ready_feed_ids)


# --- Tests for static helper methods ---


@pytest.mark.unit
@pytest.mark.parametrize(
    "feed_id,expected_job_id",
    [
        ("my_feed", "feed_my_feed"),
        ("test123", "feed_test123"),
        ("", "feed_"),
    ],
)
def test_feed_to_job_id(feed_id: str, expected_job_id: str):
    """Test feed_to_job_id conversion."""
    assert FeedScheduler._feed_to_job_id(feed_id) == expected_job_id


@pytest.mark.unit
@pytest.mark.parametrize(
    "job_id,expected_feed_id",
    [
        ("feed_my_feed", "my_feed"),
        ("feed_test123", "test123"),
        ("feed_", ""),
        ("invalid_job", None),
        ("not_a_feed_job", None),
    ],
)
def test_job_to_feed_id(job_id: str, expected_feed_id: str | None):
    """Test job_to_feed_id conversion."""
    assert FeedScheduler._job_to_feed_id(job_id) == expected_feed_id


# --- Tests for _process_feed_with_context ---


@pytest.mark.unit
@patch.object(scheduler, "set_context_id")
@patch.object(time, "time", return_value=1234567890)
def test_process_feed_with_context_sets_context_and_calls_coordinator(
    _mock_time: MagicMock,
    mock_set_context_id: MagicMock,
    mock_data_coordinator: MagicMock,
    sample_feed_config: FeedConfig,
):
    """Test that _process_feed_with_context sets context ID and calls data coordinator."""
    result = FeedScheduler._process_feed_with_context(
        mock_data_coordinator, "test_feed", sample_feed_config
    )

    # Verify context ID was set
    mock_set_context_id.assert_called_once_with("test_feed-1234567890")

    # Verify data coordinator was called
    mock_data_coordinator.process_feed.assert_called_once_with(
        "test_feed", sample_feed_config
    )

    # Verify result is returned
    assert result == mock_data_coordinator.process_feed.return_value
