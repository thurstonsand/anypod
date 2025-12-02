"""Feed configuration models for Anypod.

This module provides configuration models for podcast feeds, including
metadata definitions, category validation, and feed-specific settings
that control how content is fetched and processed.
"""

from datetime import UTC, datetime, timedelta, tzinfo
import os
import shlex
from typing import Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pycountry
from pydantic import BaseModel, Field, field_validator, model_validator
import pytimeparse2  # pyright: ignore[reportMissingTypeStubs]

from ..db.types.transcript_source import TranscriptSource
from .types import CronExpression, FeedMetadataOverrides


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
    transcript_lang: str | None = Field(
        default=None,
        description="Language code for downloading subtitles/transcripts (e.g., 'en'). If None, transcripts are not downloaded.",
    )
    transcript_source_priority: list[TranscriptSource] | None = Field(
        default=None,
        description="Ordered list of transcript sources to try (e.g., ['creator', 'auto']). First available source wins.",
    )
    download_delay: timedelta | None = Field(
        default=None,
        description=(
            "Delay downloads after video publication. If None (default), downloads "
            "immediately. Accepts duration strings: '1h', '24h', '3d', '1w'. "
            "Allows metadata to stabilize before downloading."
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

    @field_validator("transcript_lang", mode="before")
    @classmethod
    def validate_transcript_lang(cls, v: Any) -> str | None:
        """Validate transcript language code as ISO 639-1.

        Accepts standard ISO 639-1 two-letter language codes (e.g., 'en', 'fr', 'de').
        The value is passed directly to yt-dlp's --sub-langs option.

        Args:
            v: Value to validate, can be string or None.

        Returns:
            Lowercase ISO 639-1 language code, or None if not provided.

        Raises:
            ValueError: If the language code is not a valid ISO 639-1 code.
            TypeError: If the value is not a string or None.
        """
        match v:
            case None:
                return None
            case str() as s if not s.strip():
                return None
            case str() as s:
                code = s.strip().lower()
                if not pycountry.languages.get(alpha_2=code):
                    raise ValueError(
                        f"Invalid ISO 639-1 language code '{s}'. "
                        "Use two-letter codes like 'en', 'fr', 'de'."
                    )
                return code
            case _:
                raise TypeError(
                    f"transcript_lang must be an ISO 639-1 language code string or None, got {type(v).__name__}"
                )

    @staticmethod
    def _validate_transcript_source(item: Any) -> TranscriptSource:
        """Validate and convert a single transcript source value.

        Args:
            item: Value to validate (string or TranscriptSource enum).

        Returns:
            Validated TranscriptSource enum value.

        Raises:
            ValueError: If the value is not a valid transcript source.
            TypeError: If the value is not a string or TranscriptSource.
        """
        valid_sources = {TranscriptSource.CREATOR, TranscriptSource.AUTO}

        match item:
            case TranscriptSource() as source:
                pass
            case str() as s:
                try:
                    source = TranscriptSource(s.lower())
                except ValueError as e:
                    raise ValueError(
                        f"Invalid transcript source '{s}'. "
                        f"Valid values: {[src.value for src in valid_sources]}"
                    ) from e
            case _:
                raise TypeError(
                    f"transcript_source_priority items must be strings or "
                    f"TranscriptSource, got {type(item).__name__}"
                )

        if source not in valid_sources:
            raise ValueError(
                f"Invalid transcript source '{source.value}'. "
                f"Valid values: {[src.value for src in valid_sources]}"
            )
        return source

    @field_validator("transcript_source_priority", mode="before")
    @classmethod
    def validate_transcript_source_priority(
        cls, v: Any
    ) -> list[TranscriptSource] | None:
        """Validate and convert transcript source priority list.

        Accepts a list of strings or TranscriptSource enum values.
        Only CREATOR and AUTO are valid choices (NOT_AVAILABLE is not a valid priority).

        Args:
            v: Value to validate, can be list of strings/enums or None.

        Returns:
            List of TranscriptSource enum values, or None if not provided.

        Raises:
            ValueError: If invalid source values are provided or duplicates exist.
            TypeError: If the value is not a list or None.
        """
        match v:
            case None:
                return None
            case list():
                items = cast(list[Any], v)
                if len(items) == 0:
                    return None
            case _:
                raise TypeError(
                    f"transcript_source_priority must be a list or None, "
                    f"got {type(v).__name__}"
                )

        result: list[TranscriptSource] = []
        seen: set[TranscriptSource] = set()
        for item in items:
            source = FeedConfig._validate_transcript_source(item)
            if source in seen:
                raise ValueError(
                    f"Duplicate transcript source '{source.value}' in priority list."
                )
            seen.add(source)
            result.append(source)
        return result

    @field_validator("download_delay", mode="before")
    @classmethod
    def parse_download_delay(cls, v: Any) -> timedelta | None:
        """Parse download_delay string into a timedelta object.

        Args:
            v: Value to parse, can be string, timedelta, or None.

        Returns:
            timedelta instance or None if not provided.

        Raises:
            ValueError: If the duration string format is invalid.
            TypeError: If the value is not a string, timedelta, or None.
        """
        match v:
            case None:
                return None
            case str() as s if not s.strip():
                return None
            case str() as s:
                seconds = cast(
                    int | float | None,
                    pytimeparse2.parse(s),  # pyright: ignore[reportUnknownMemberType]
                )
                if seconds is None:
                    raise ValueError(
                        f"Invalid duration format: '{s}'. "
                        "Expected format: '<number><unit>' where unit is h, d, w, etc. "
                        "Examples: '1h', '24h', '3d', '1w', '2 hours', '1 day'"
                    )
                if seconds < 0:
                    raise ValueError(f"download_delay must be non-negative, got '{s}'")
                return timedelta(seconds=seconds)
            case timedelta():
                return v
            case _:
                raise TypeError(
                    f"download_delay must be a duration string (e.g., '24h', '3d', '1w') or None, "
                    f"got {type(v).__name__}"
                )

    @model_validator(mode="after")
    def validate_feed_config(self) -> FeedConfig:
        """Validate feed configuration and apply defaults."""
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

        if self.transcript_lang and self.transcript_source_priority is None:
            self.transcript_source_priority = [
                TranscriptSource.CREATOR,
                TranscriptSource.AUTO,
            ]

        return self

    @property
    def is_manual(self) -> bool:
        """Return True when the feed runs via manual submissions only."""
        return self.schedule is None
