"""Cron expression data type for Anypod configuration.

This module provides the CronExpression dataclass for representing
cron schedules with validation.
"""

from dataclasses import dataclass, field
from datetime import datetime

from croniter import croniter


@dataclass
class CronExpression:
    """Data representation of a cron expression.

    Takes the cron expression string as input. The expression
    can be either 5 or 6 fields long, with the sixth field being
    an optional "second" field.

    Each position is:
        * * * * * *
        | | | | | |
        | | | | | second (0-59)
        | | | | day of the week (0-6) (Sunday to Saturday; 7 is also Sunday)
        | | | month (1-12)
        | | day of the month (1-31)
        | hour (0-23)
        minute (0-59)

    Also possible to use aliases:
        - @midnight: 0 0 * * *
        - @hourly: 0 * * * *
        - @daily: 0 0 * * *
        - @weekly: 0 0 * * 0
        - @monthly: 0 0 1 * *
        - @yearly: 0 0 1 1 *
        - @annually: 0 0 1 1 *

    Attributes:
        cron_str: Cron expression string
        minute: Minute (0-59)
        hour: Hour (0-23)
        day: Day of month (1-31)
        month: Month (1-12)
        day_of_week: Day of week (0-6, Sunday=0)
        second: Second (0-59)
    """

    cron_str: str = field(repr=False, hash=False, compare=False)
    _itr: croniter = field(init=False, repr=False, hash=False, compare=False)

    minute: int | str | None = field(init=False)
    hour: int | str | None = field(init=False)
    day: int | str | None = field(init=False)
    month: int | str | None = field(init=False)
    day_of_week: int | str | None = field(init=False)
    second: int | str | None = field(init=False)

    def __post_init__(self):
        self._itr = croniter(self.cron_str)
        match self._itr.expressions:
            case (minute, hour, day, month, day_of_week):
                self.minute = minute
                self.hour = hour
                self.day = day
                self.month = month
                self.day_of_week = day_of_week
                self.second = None  # Initialize second to None for 5-field expressions
            case (minute, hour, day, month, day_of_week, second):
                self.minute = minute
                self.hour = hour
                self.day = day
                self.month = month
                self.day_of_week = day_of_week
                self.second = second
            case (_, _, _, _, _, _, year):
                raise ValueError(
                    f"Invalid cron expression: year value not allowed (but used {year})"
                )
            case _:
                raise ValueError(f"Invalid cron expression: {self.cron_str}")

    def next(self, start_time: datetime) -> datetime:
        """Get the next datetime that matches the cron expression.

        Args:
            start_time: The datetime to start from

        Returns:
            The next datetime that matches the cron expression
        """
        return self._itr.get_next(datetime, start_time=start_time)  # type: ignore

    def prev(self, start_time: datetime) -> datetime:
        """Get the previous datetime that matches the cron expression.

        Args:
            start_time: The datetime to start from

        Returns:
            The previous datetime that matches the cron expression
        """
        return self._itr.get_prev(datetime, start_time=start_time)  # type: ignore

    def __str__(self) -> str:
        """Return the cron expression string."""
        return self.cron_str
