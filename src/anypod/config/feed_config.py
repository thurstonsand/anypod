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

from pydantic import BaseModel, Field, field_validator, model_validator

from .types import CronExpression, DownloadDelay, FeedMetadataOverrides


class FeedConfig(BaseModel):
    """Configuration for a single podcast feed.

    Attributes:
        url: Feed source URL (e.g., YouTube channel, playlist).
        yt_args: Command-line arguments for yt-dlp from user-provided string.
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
    url: str | None = Field(
        default=None,
        description="Feed source URL (exclude for manual feeds)",
        min_length=1,
    )
    yt_args: list[str] = Field(
        default_factory=list[str],
        description="Command-line arguments for yt-dlp, parsed from user-provided string in config.",
    )
    schedule: CronExpression | None = Field(
        ...,
        description=(
            "Cron schedule expression (supports seconds) or the string 'manual' to disable scheduling."
        ),
    )
    keep_last: int | None = Field(
        None, ge=1, description="Prune policy - number of latest downloads to keep"
    )
    since: datetime | None = Field(
        None,
        description="Date in YYYYMMDD format; downloads older than this date are ignored",
    )
    max_errors: int = Field(
        default=3,
        ge=1,
        description="Max attempts for downloading media before marking as ERROR.",
    )
    download_delay: DownloadDelay | None = Field(
        default=None,
        description=(
            "Delay downloads after video publication. Accepts duration strings: "
            "'1h', '24h', '3d', '1w'. Allows metadata to stabilize before downloading."
        ),
    )
    metadata: FeedMetadataOverrides | None = Field(
        None, description="Podcast metadata overrides"
    )

    @field_validator("yt_args", mode="before")
    @classmethod
    def parse_yt_args_string(cls, v: Any) -> list[str]:
        """Parse yt_args string into a list of command-line arguments.

        Args:
            v: Value to parse, can be string, list of strings, or None.

        Returns:
            List of command-line arguments for yt-dlp.

        Raises:
            ValueError: If the string cannot be parsed.
            TypeError: If the value is not a string or list of strings.
        """
        match v:
            case None:
                return []
            case str() as s:
                return shlex.split(s.strip())
            case list() as l if all(isinstance(arg, str) for arg in l):  # type: ignore
                return l  # type: ignore # confirmed that it is a list of strings
            case other:
                raise TypeError(
                    f"yt_args must be a string or list of strings, got {type(other).__name__}"
                )

    @field_validator("schedule", mode="before")
    @classmethod
    def parse_schedule(cls, v: Any) -> CronExpression | None:
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
            case str() if v.strip() == "manual":
                return None
            case str() if v.strip():
                return CronExpression(v.strip())
            case str():  # Empty string
                raise ValueError("Schedule cannot be empty")
            case None:
                raise ValueError("Schedule is required")
            case _:
                raise TypeError(
                    f"schedule must be 'manual', a string cron expression, or CronExpression, got {type(v).__name__}"
                )

    @staticmethod
    def _get_local_timezone() -> tzinfo:
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

    @field_validator("since", mode="before")
    @classmethod
    def parse_since_date(cls, v: Any) -> datetime | None:
        """Parse since date string in YYYYMMDD format into a UTC datetime object.

        Accepts date strings in YYYYMMDD format only and converts them to UTC
        representing 00:00 local time using a tiered timezone lookup system:
        1. TZ environment variable
        2. System's local timezone (from /etc/localtime on Unix)

        Args:
            v: Value to parse, can be string or None.

        Returns:
            UTC datetime object representing 00:00 local time, or None if not provided.

        Raises:
            ValueError: If the date string format is invalid or date is invalid.
            TypeError: If the value is not a string or None.
        """
        match v:
            case None:
                return None
            case str() as s if not s.strip():  # Handle empty string
                return None
            case str() as s:
                date_string = s.strip()
                try:
                    naive_dt = datetime.strptime(date_string, "%Y%m%d")  # noqa: DTZ007 # we want the naive dt here
                except ValueError as e:
                    raise ValueError(
                        f"Invalid date '{date_string}' must be in YYYYMMDD format"
                    ) from e
                return naive_dt.replace(
                    tzinfo=FeedConfig._get_local_timezone()
                ).astimezone(UTC)
            case datetime():
                return v.astimezone(UTC)
            case _:
                raise TypeError(
                    f"since must be a string in YYYYMMDD format or None, got {type(v).__name__}"
                )

    @field_validator("download_delay", mode="before")
    @classmethod
    def parse_download_delay(cls, v: Any) -> DownloadDelay | None:
        """Parse download_delay string into a DownloadDelay object.

        Args:
            v: Value to parse, can be string, DownloadDelay, or None.

        Returns:
            DownloadDelay instance or None if not provided.

        Raises:
            ValueError: If the duration string format is invalid.
            TypeError: If the value is not a string, DownloadDelay, or None.
        """
        match v:
            case None:
                return None
            case str() as s if not s.strip():
                return None
            case str() as s:
                return DownloadDelay(s)
            case DownloadDelay():
                return v
            case _:
                raise TypeError(
                    f"download_delay must be a duration string (e.g., '24h', '3d', '1w') or None, "
                    f"got {type(v).__name__}"
                )

    @model_validator(mode="after")
    def validate_manual_feed(self) -> FeedConfig:
        """Ensure manual feeds provide required metadata overrides."""
        if self.schedule is None:
            title_override = (
                self.metadata.title if self.metadata and self.metadata.title else None
            )
            if not title_override:
                raise ValueError(
                    "Manual feeds require metadata.title when schedule is set to 'manual'."
                )
        elif not self.url:
            raise ValueError("Feed URL is required for scheduled feeds.")
        return self

    @property
    def is_manual(self) -> bool:
        """Return True when the feed runs via manual submissions only."""
        return self.schedule is None
