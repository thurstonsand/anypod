"""Metadata utility functions for feed metadata processing.

This module provides standalone utility functions for merging feed metadata
from yt-dlp extraction with configuration overrides, eliminating the need
for a dedicated service class.
"""

from typing import Any

from .config import FeedConfig
from .db.types import Feed


def merge_feed_metadata(fetched_feed: Feed, feed_config: FeedConfig) -> dict[str, Any]:
    """Merge config overrides with fetched feed metadata.

    Configuration overrides take precedence over extracted values.
    Missing fields in config are filled from the fetched feed metadata.

    Args:
        fetched_feed: Feed metadata extracted from yt-dlp.
        feed_config: Feed configuration with potential overrides and retention policies.

    Returns:
        Dictionary containing merged metadata, excluding None values.
    """
    # Start with override metadata if present
    candidate_metadata: dict[str, Any] = (
        feed_config.metadata.model_dump(exclude_none=True)
        if feed_config.metadata
        else {}
    )

    # Fill in missing values from fetched feed (fallback for fields not overridden)
    candidate_metadata["title"] = candidate_metadata.get("title") or fetched_feed.title
    candidate_metadata["subtitle"] = (
        candidate_metadata.get("subtitle") or fetched_feed.subtitle
    )
    candidate_metadata["description"] = (
        candidate_metadata.get("description") or fetched_feed.description
    )
    candidate_metadata["language"] = (
        candidate_metadata.get("language") or fetched_feed.language
    )
    candidate_metadata["author"] = (
        candidate_metadata.get("author") or fetched_feed.author
    )
    candidate_metadata["author_email"] = (
        candidate_metadata.get("author_email") or fetched_feed.author_email
    )
    candidate_metadata["image_url"] = (
        candidate_metadata.get("image_url") or fetched_feed.image_url
    )
    candidate_metadata["category"] = (
        candidate_metadata.get("category") or fetched_feed.category
    )
    candidate_metadata["podcast_type"] = (
        candidate_metadata.get("podcast_type") or fetched_feed.podcast_type
    )
    candidate_metadata["explicit"] = (
        candidate_metadata.get("explicit") or fetched_feed.explicit
    )

    # Remove None values to avoid overwriting defaults
    return {k: v for k, v in candidate_metadata.items() if v is not None}
