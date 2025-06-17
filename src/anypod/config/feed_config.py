"""Feed configuration models for Anypod.

This module provides configuration models for podcast feeds, including
metadata definitions, category validation, and feed-specific settings
that control how content is fetched and processed.
"""

from datetime import UTC, datetime, tzinfo
import os
import shlex
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, field_validator

from .types import CronExpression, FeedMetadataOverrides


class FeedConfig(BaseModel):
    """Configuration for a single podcast feed.

    Attributes:
        url: Feed source URL (e.g., YouTube channel, playlist).
        yt_args: Parsed arguments for yt-dlp from user-provided string.
        schedule: Cron schedule expression for feed processing.
        keep_last: Number of latest downloads to keep (prune policy).
        since: Only download newer downloads since this ISO8601 timestamp (prune policy).
        max_errors: Max attempts for downloading before marking as ERROR.
        metadata: Podcast metadata overrides for RSS feed generation.
                  Any values not specified here will be extracted from
                  the source content where possible.
    """

    enabled: bool = Field(
        default=True,
        description="Whether the feed is enabled. If disabled, the feed will not be processed.",
    )
    url: str = Field(..., min_length=1, description="Feed source URL")
    yt_args: dict[str, Any] = Field(
        default_factory=dict[str, Any],
        description="Parsed arguments for yt-dlp, from user-provided string in config.",
    )
    schedule: CronExpression = Field(
        ..., description="Cron schedule expression (supports seconds)"
    )
    keep_last: int | None = Field(
        None, ge=1, description="Prune policy - number of latest downloads to keep"
    )
    since: datetime | None = Field(
        None, description="ISO8601 timestamp to ignore older downloads"
    )
    max_errors: int = Field(
        default=3,
        ge=1,
        description="Max attempts for downloading media before marking as ERROR.",
    )
    metadata: FeedMetadataOverrides | None = Field(
        None, description="Podcast metadata overrides"
    )

    @field_validator("yt_args", mode="before")
    @classmethod
    def parse_yt_args_string(cls, v: Any) -> dict[str, Any]:
        """Parse yt_args string into a dictionary of yt-dlp options.

        Args:
            v: Value to parse, can be string or None.

        Returns:
            Dictionary of parsed yt-dlp options.

        Raises:
            ValueError: If the string cannot be parsed.
            TypeError: If the value is not a string or None.
        """
        match v:
            case None:
                return {}
            case str() if not v.strip():  # Handle empty string
                return {}
            case str():
                try:
                    # lazy import to prevent circular import
                    from ..ytdlp_wrapper.ytdlp_core import YtdlpCore

                    args_list = shlex.split(v)
                    parsed_opts = YtdlpCore.parse_options(args_list)
                    return parsed_opts
                except Exception as e:
                    raise ValueError(
                        f"Invalid yt_args string '{v}'. Failed to parse."
                    ) from e
            case _:
                raise TypeError(f"yt_args must be a string, got {type(v).__name__}")

    @field_validator("schedule", mode="before")
    @classmethod
    def parse_schedule(cls, v: Any) -> CronExpression:
        """Parse schedule into a CronExpression.

        Args:
            v: Value to parse, can be string, CronExpression, or None.

        Returns:
            CronExpression instance.

        Raises:
            ValueError: If the schedule cannot be parsed.
            TypeError: If the value is not a string or CronExpression.
        """
        match v:
            case CronExpression():
                return v
            case str() if v.strip():
                return CronExpression(v.strip())
            case str():  # Empty string
                raise ValueError("Schedule cannot be empty")
            case None:
                raise ValueError("Schedule is required")
            case _:
                raise TypeError(
                    f"schedule must be a string or CronExpression, got {type(v).__name__}"
                )

    @classmethod
    def _get_local_timezone(cls) -> tzinfo:
        """Get local timezone using tiered lookup.

        Tiered timezone lookup:
        1. TZ environment variable
        2. System's local timezone (from /etc/localtime on Unix)

        Returns:
            Timezone info object for the local timezone.

        Raises:
            ValueError: If timezone could not be determined.
        """
        # 1. Check for the TZ environment variable
        tz_env = os.environ.get("TZ")
        if tz_env:
            try:
                return ZoneInfo(tz_env)
            except ZoneInfoNotFoundError as e:
                raise ValueError(
                    f"Invalid timezone specified in TZ environment variable: {tz_env}"
                ) from e

        # 2. Fallback to the system's local timezone
        # On Unix-like systems, this resolves /etc/localtime.
        # On Windows, it uses the Windows registry.
        return datetime.now().astimezone().tzinfo or ZoneInfo("UTC")

    @classmethod
    def _to_utc_datetime(cls, date_string: str) -> datetime:
        """Convert a date string to a UTC datetime object with robust timezone handling.

        The function determines the timezone based on the following precedence:
        1. Explicit timezone offset in the ISO 8601 string (e.g., '-04:00').
        2. The TZ environment variable (e.g., 'America/New_York').
        3. The system's local timezone (e.g., from /etc/localtime).

        Args:
            date_string: The date and time as a string.

        Returns:
            A timezone-aware datetime object in UTC.

        Raises:
            ValueError: If the date string or timezone is invalid.
        """
        # 1. Attempt to parse as ISO 8601 with an explicit timezone
        try:
            # fromisoformat handles Z, +HH:MM, and -HH:MM
            dt_aware = datetime.fromisoformat(date_string)
            # If the datetime object has timezone info, convert to UTC and return
            if dt_aware.tzinfo:
                return dt_aware.astimezone(UTC)
        except ValueError:
            # This error is caught to allow parsing as a naive datetime next.
            # If it fails again below, a new ValueError will be raised.
            pass

        # The input string does not have an explicit offset, so it's a naive datetime.
        # We must now determine the intended local timezone.
        local_tz = cls._get_local_timezone()
        return (
            datetime.fromisoformat(date_string).replace(tzinfo=local_tz).astimezone(UTC)
        )

    @field_validator("since", mode="before")
    @classmethod
    def parse_since_date(cls, v: Any) -> datetime | None:
        """Parse since date string into a UTC datetime object.

        Accepts date strings in various formats and converts them to UTC using
        a tiered timezone lookup system:
        1. Explicit timezone offset in the ISO 8601 string (e.g., '-04:00')
        2. TZ environment variable
        3. System's local timezone (from /etc/localtime on Unix)

        Args:
            v: Value to parse, can be string, datetime, or None.

        Returns:
            UTC datetime object, or None if not provided.

        Raises:
            ValueError: If the date string cannot be parsed.
            TypeError: If the value is not a string, datetime, or None.
        """
        match v:
            case None:
                return None
            case str() as s if not s.strip():  # Handle empty string
                return None
            case datetime() as dt:
                # Already a datetime, ensure it's UTC
                if dt.tzinfo is None:
                    # Naive datetime - apply tiered timezone lookup
                    local_tz = cls._get_local_timezone()
                    localized = dt.replace(tzinfo=local_tz)
                    return localized.astimezone(UTC)
                else:
                    # Already timezone-aware, convert to UTC
                    return dt.astimezone(UTC)
            case str() as s:
                return cls._to_utc_datetime(s.strip())
            case _:
                raise TypeError(
                    f"since must be a string, datetime, or None, got {type(v).__name__}"
                )
