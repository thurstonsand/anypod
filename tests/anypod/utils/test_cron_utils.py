# pyright: reportPrivateUsage=false

"""Tests for cron utilities."""

from datetime import UTC, datetime, timedelta

import pytest

from anypod.utils.cron_utils import calculate_fetch_until_date


@pytest.mark.unit
def test_calculate_fetch_until_date_basic_hourly():
    """Test calculate_fetch_until_date with hourly cron schedule."""
    cron_schedule = "0 * * * *"  # Every hour

    now = datetime.now(UTC)
    fetch_since_date = now - timedelta(hours=3)

    result = calculate_fetch_until_date(cron_schedule, fetch_since_date)

    # With hourly cron and fetch_since 3 hours ago:
    # calculated_until = fetch_since + 2 * 1 hour = now - 1 hour
    # fetch_until_date = min(now, now - 1 hour) = now - 1 hour
    expected_calculated = fetch_since_date + timedelta(hours=2)
    expected = min(now, expected_calculated)

    # Allow small time difference due to test execution time
    assert abs((result - expected).total_seconds()) < 1.0


@pytest.mark.unit
def test_calculate_fetch_until_date_daily():
    """Test calculate_fetch_until_date with daily cron schedule."""
    cron_schedule = "0 3 * * *"  # Daily at 3 AM

    now = datetime.now(UTC)
    fetch_since_date = now - timedelta(days=3)

    result = calculate_fetch_until_date(cron_schedule, fetch_since_date)

    # With daily cron and fetch_since 3 days ago:
    # calculated_until = fetch_since + 2 * 1 day = now + 1 day
    # fetch_until_date = min(now, now + 1 day) = now
    expected_calculated = fetch_since_date + timedelta(days=2)
    expected = min(now, expected_calculated)

    # Allow small time difference due to test execution time
    assert abs((result - expected).total_seconds()) < 1.0


@pytest.mark.unit
def test_calculate_fetch_until_date_now_wins():
    """Test calculate_fetch_until_date when current time is less than calculated until."""
    cron_schedule = "0 * * * *"  # Every hour

    # Set fetch_since to be 30 minutes ago
    # This will make calculated_until be 1.5 hours from now
    # So now should win (be less than calculated_until)
    now = datetime.now(UTC)
    fetch_since_date = now - timedelta(minutes=30)

    result = calculate_fetch_until_date(cron_schedule, fetch_since_date)

    # calculated_until = fetch_since + 2 * 1 hour = now + 1.5 hours
    # fetch_until_date = min(now, now + 1.5 hours) = now
    expected_calculated = fetch_since_date + timedelta(hours=2)
    expected = min(now, expected_calculated)

    # Should be approximately now since now < calculated_until
    assert abs((result - expected).total_seconds()) < 1.0
    assert result <= now + timedelta(seconds=1)  # Account for test execution time


@pytest.mark.unit
def test_calculate_fetch_until_date_every_5_minutes():
    """Test calculate_fetch_until_date with 5-minute cron schedule."""
    cron_schedule = "*/5 * * * *"  # Every 5 minutes

    now = datetime.now(UTC)
    fetch_since_date = now - timedelta(minutes=15)

    result = calculate_fetch_until_date(cron_schedule, fetch_since_date)

    # With 5-minute cron and fetch_since 15 minutes ago:
    # calculated_until = fetch_since + 2 * 5 minutes = now - 5 minutes
    # fetch_until_date = min(now, now - 5 minutes) = now - 5 minutes
    expected_calculated = fetch_since_date + timedelta(minutes=10)
    expected = min(now, expected_calculated)

    # Allow small time difference due to test execution time
    assert abs((result - expected).total_seconds()) < 1.0


@pytest.mark.unit
def test_calculate_fetch_until_date_weekly():
    """Test calculate_fetch_until_date with weekly cron schedule."""
    cron_schedule = "0 3 * * 1"  # Weekly on Monday at 3 AM

    now = datetime.now(UTC)
    fetch_since_date = now - timedelta(days=10)

    result = calculate_fetch_until_date(cron_schedule, fetch_since_date)

    # With weekly cron and fetch_since 10 days ago:
    # calculated_until = fetch_since + 2 * 7 days = now + 4 days
    # fetch_until_date = min(now, now + 4 days) = now
    expected_calculated = fetch_since_date + timedelta(days=14)
    expected = min(now, expected_calculated)

    # Allow small time difference due to test execution time
    assert abs((result - expected).total_seconds()) < 1.0


@pytest.mark.unit
def test_calculate_fetch_until_date_calculated_until_wins():
    """Test calculate_fetch_until_date when calculated until is less than current time."""
    cron_schedule = "0 * * * *"  # Every hour

    now = datetime.now(UTC)
    fetch_since_date = now - timedelta(hours=5)

    result = calculate_fetch_until_date(cron_schedule, fetch_since_date)

    # calculated_until = fetch_since + 2 * 1 hour = now - 3 hours
    # fetch_until_date = min(now, now - 3 hours) = now - 3 hours
    expected_calculated = fetch_since_date + timedelta(hours=2)
    expected = min(now, expected_calculated)

    # Should be approximately calculated_until since calculated_until < now
    assert abs((result - expected).total_seconds()) < 1.0
    assert result < now  # Should be in the past


@pytest.mark.unit
def test_calculate_fetch_until_date_irregular_cron():
    """Test calculate_fetch_until_date with irregular cron schedule."""
    cron_schedule = "0 6,18 * * *"  # Twice daily at 6 AM and 6 PM

    now = datetime.now(UTC)
    fetch_since_date = now - timedelta(days=2)

    result = calculate_fetch_until_date(cron_schedule, fetch_since_date)

    # With twice-daily cron (12 hour intervals) and fetch_since 2 days ago:
    # calculated_until = fetch_since + 2 * 12 hours = now - 1 day
    # fetch_until_date = min(now, now - 1 day) = now - 1 day
    expected_calculated = fetch_since_date + timedelta(hours=24)  # 2 * 12 hours
    expected = min(now, expected_calculated)

    # Allow small time difference due to test execution time
    assert abs((result - expected).total_seconds()) < 1.0
