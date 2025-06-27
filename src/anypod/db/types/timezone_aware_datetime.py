"""Timezone-aware datetime type for SQLAlchemy."""

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import DateTime, TypeDecorator

SQLITE_DATETIME_NOW = "datetime('now', 'utc')"


class TimezoneAwareDatetime(TypeDecorator[datetime]):
    """SQLAlchemy type that ensures all datetimes are timezone-aware (UTC).

    SQLite doesn't natively support timezone information, so this decorator
    ensures that:
    1. All datetime values must be timezone-aware when inserted
    2. All datetime values are stored as UTC (without tzinfo) in the database
    3. All datetime values read from the database are timezone-aware (UTC)
    """

    impl = DateTime
    cache_ok = True

    def process_bind_param(
        self, value: datetime | None, dialect: Any
    ) -> datetime | None:
        """Convert timezone-aware datetime to UTC for storage.

        Args:
            value: The datetime value to store.
            dialect: The SQL dialect being used.

        Returns:
            UTC datetime without tzinfo for storage, or None.

        Raises:
            TypeError: If the datetime is not timezone-aware.
        """
        if value is None:
            return None

        # Enforce that the datetime is timezone-aware
        if not value.tzinfo or value.tzinfo.utcoffset(value) is None:
            raise TypeError("tzinfo is required")

        # Convert to UTC and remove tzinfo for SQLite storage
        return value.astimezone(UTC).replace(tzinfo=None)

    def process_result_value(
        self, value: datetime | None, dialect: Any
    ) -> datetime | None:
        """Convert datetime from database to timezone-aware UTC.

        Args:
            value: The datetime value from the database.
            dialect: The SQL dialect being used.

        Returns:
            Timezone-aware UTC datetime, or None.
        """
        if value is None:
            return None

        # SQLite returns timezone-naive datetimes, so add UTC tzinfo
        return value.replace(tzinfo=UTC)
