"""Pydantic model for overriding feed metadata in RSS generation."""

from pydantic import BaseModel, Field

from .podcast_categories import PodcastCategories
from .podcast_explicit import PodcastExplicit


class FeedMetadataOverrides(BaseModel):
    """Podcast metadata overrides for RSS feed generation."""

    title: str | None = Field(None, description="Podcast title")
    subtitle: str | None = Field(None, description="Podcast subtitle")
    description: str | None = Field(None, description="Podcast description")
    language: str | None = Field(
        None, description="Podcast language (e.g., 'en', 'es')"
    )
    categories: PodcastCategories = Field(
        default_factory=PodcastCategories,
        description="Apple Podcasts category/categories (max 2)",
    )
    explicit: PodcastExplicit | None = Field(None, description="Explicit content flag")
    image_url: str | None = Field(
        None,
        description="Podcast image URL, must be at least 1400x1400px, ideally 3000x3000px",
    )
    author: str | None = Field(None, description="Podcast author")
