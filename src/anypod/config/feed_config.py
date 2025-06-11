"""Feed configuration models for Anypod.

This module provides configuration models for podcast feeds, including
metadata definitions, category validation, and feed-specific settings
that control how content is fetched and processed.
"""

from datetime import datetime
import shlex
from typing import Any

from pydantic import BaseModel, Field, field_validator

from .types import FeedMetadataOverrides


class FeedConfig(BaseModel):
    """Configuration for a single podcast feed.

    Attributes:
        url: Feed source URL (e.g., YouTube channel, playlist).
        yt_args: Parsed arguments for yt-dlp from user-provided string.
        schedule: Cron schedule string for feed processing.
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
    schedule: str = Field(..., min_length=1, description="Cron schedule string")
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
