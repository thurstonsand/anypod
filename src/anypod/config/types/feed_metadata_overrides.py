"""Pydantic model for overriding feed metadata in RSS generation."""

from pydantic import BaseModel, Field

from .podcast_categories import PodcastCategories
from .podcast_explicit import PodcastExplicit
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
    explicit: PodcastExplicit | None = Field(
        default=None,
        description="Explicit content flag; yes | no | clean",
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
