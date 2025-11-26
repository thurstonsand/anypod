"""Pydantic model for overriding feed metadata in RSS generation."""

from typing import Any

from pydantic import BaseModel, Field, field_validator

from .podcast_categories import PodcastCategories
from .podcast_type import PodcastType


class FeedMetadataOverrides(BaseModel):
    """Podcast metadata overrides for RSS feed generation."""

    title: str | None = Field(default=None, description="Podcast title")
    subtitle: str | None = Field(default=None, description="Podcast subtitle")
    description: str | None = Field(default=None, description="Podcast description")
    language: str | None = Field(
        default=None,
        description="Podcast language (e.g., 'en', 'es')",
    )
    category: PodcastCategories | None = Field(
        default=None,
        description="Apple Podcasts category/categories (max 2)",
    )
    podcast_type: PodcastType | None = Field(
        default=None,
        description="Podcast type: 'episodic' or 'serial'",
    )
    explicit: bool | None = Field(
        default=None,
        description="Explicit content flag; true | false (accepts yes/no/clean)",
    )
    image_url: str | None = Field(
        default=None,
        description="Podcast image URL, must be at least 1400x1400px, ideally 3000x3000px",
        serialization_alias="remote_image_url",
    )
    author: str | None = Field(default=None, description="Podcast author")
    author_email: str | None = Field(
        default=None,
        description="Podcast author email",
    )

    @field_validator("explicit", mode="before")
    @classmethod
    def parse_explicit(cls, v: Any) -> bool | None:
        """Parse explicit flag from various formats."""
        match v:
            case None:
                return None
            case bool():
                return v
            case str() if v.lower().strip() in ("true", "false", "yes", "no", "clean"):
                return v.lower().strip() in ("true", "yes")
            case _:
                raise ValueError(f"Invalid value for explicit: {v!r}")
