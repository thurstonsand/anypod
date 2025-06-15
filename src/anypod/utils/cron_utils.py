"""Cron-related utility functions.

This module provides utilities for working with cron schedules and calculating
intervals between cron execution times.
"""

from datetime import UTC, datetime

from croniter import croniter


def calculate_fetch_until_date(
    cron_schedule: str, fetch_since_date: datetime
) -> datetime:
    """Calculate the fetch_until_date based on cron schedule.

    The until date is calculated as the minimum of:
    1. Current time (to avoid querying future dates)
    2. fetch_since_date + 2 * cron_interval

    The cron interval is calculated by finding the two most recent cron ticks
    and using their difference, which ensures an accurate interval even when
    the current time is just milliseconds after a cron tick.

    Args:
        cron_schedule: The cron schedule string.
        fetch_since_date: The start date for fetching.

    Returns:
        The calculated until date for fetching.
    """
    now = datetime.now(UTC)

    # Calculate the cron interval by finding the 2 most recent ticks
    # This ensures we get an accurate interval even if current time is just
    # milliseconds after a cron tick
    iter = croniter(cron_schedule, now)
    most_recent_tick = iter.get_prev(datetime)
    previous_tick = iter.get_prev(datetime)
    cron_interval = most_recent_tick - previous_tick

    # Calculate fetch_until_date as min(now, fetch_since_date + 2 * interval)
    calculated_until = fetch_since_date + (2 * cron_interval)
    fetch_until_date = min(now, calculated_until)

    return fetch_until_date
