"""Shared database utilities and functions.

This module provides common functionality used across database classes,
including datetime parsing utilities.
"""

from datetime import datetime

from .sqlite_utils_core import register_adapter

register_adapter(datetime, lambda dt: dt.isoformat())


def parse_required_datetime(value: str) -> datetime:
    """Parse a required datetime string from database.

    Args:
        value: ISO format datetime string from database.

    Returns:
        Parsed datetime object.

    Raises:
        ValueError: If the date format is invalid.
    """
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid date format in DB row: {value}") from e


def parse_datetime(value: str | None) -> datetime | None:
    """Parse an optional datetime string from database.

    Args:
        value: ISO format datetime string from database, or None.

    Returns:
        Parsed datetime object, or None if input was None.

    Raises:
        ValueError: If the date format is invalid.
    """
    if value is None:
        return None
    return parse_required_datetime(value)
