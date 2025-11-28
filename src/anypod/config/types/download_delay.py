"""Download delay data type for Anypod configuration.

This module provides the DownloadDelay dataclass for representing
durations that delay downloads after video publication.
"""

from dataclasses import dataclass
from datetime import timedelta
import re


# Pattern for duration strings: number followed by unit (h=hours, d=days, w=weeks)
_DURATION_PATTERN = re.compile(r"^(\d+)\s*(h|d|w)$", re.IGNORECASE)

# Multipliers for converting units to hours
_UNIT_TO_HOURS: dict[str, int] = {
    "h": 1,
    "d": 24,
    "w": 24 * 7,
}


@dataclass(frozen=True)
class DownloadDelay:
    """Data representation of a download delay duration.

    Parses duration strings in the format "<number><unit>" where unit is
    one of: h (hours), d (days), w (weeks). Whitespace between number
    and unit is allowed.

    Examples:
        - "1h" or "1 h" -> 1 hour
        - "24h" -> 24 hours
        - "3d" -> 3 days (72 hours)
        - "1w" -> 1 week (168 hours)

    Attributes:
        duration_str: Original duration string.
        total_hours: Total duration in hours.
        timedelta: Duration as a timedelta object.
    """

    duration_str: str
    total_hours: int
    timedelta: timedelta

    def __init__(self, duration_str: str):
        """Initialize DownloadDelay from a duration string.

        Args:
            duration_str: Duration string in format "<number><unit>".

        Raises:
            ValueError: If the duration string format is invalid or value is zero.
        """
        stripped = duration_str.strip()
        match = _DURATION_PATTERN.match(stripped)
        if not match:
            raise ValueError(
                f"Invalid duration format: '{duration_str}'. "
                "Expected format: <number><unit> where unit is h (hours), d (days), or w (weeks). "
                "Examples: '1h', '24h', '3d', '1w'"
            )

        value = int(match.group(1))
        unit = match.group(2).lower()

        if value <= 0:
            raise ValueError(
                f"Duration value must be positive, got {value} in '{duration_str}'"
            )

        hours = value * _UNIT_TO_HOURS[unit]
        td = timedelta(hours=hours)

        # Use object.__setattr__ since dataclass is frozen
        object.__setattr__(self, "duration_str", stripped)
        object.__setattr__(self, "total_hours", hours)
        object.__setattr__(self, "timedelta", td)

    def __str__(self) -> str:
        """Return the original duration string."""
        return self.duration_str

    def __repr__(self) -> str:
        """Return a detailed representation."""
        return f"DownloadDelay(duration_str='{self.duration_str}', total_hours={self.total_hours})"
