"""Metadata utility functions for feed and download metadata processing.

This module provides standalone utility functions for merging and updating
metadata from yt-dlp extraction, serving as the central location for metadata
resolution logic.
"""

from typing import Any

from .config import FeedConfig
from .db.types import Download, Feed


def merge_feed_metadata(
    fetched_feed: Feed | None, feed_config: FeedConfig
) -> dict[str, Any]:
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
        feed_config.metadata.model_dump(exclude_none=True, by_alias=True)
        if feed_config.metadata
        else {}
    )

    if fetched_feed:
        candidate_metadata["title"] = (
            candidate_metadata.get("title") or fetched_feed.title
        )
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
        candidate_metadata["remote_image_url"] = (
            candidate_metadata.get("remote_image_url") or fetched_feed.remote_image_url
        )
        candidate_metadata["category"] = (
            candidate_metadata.get("category") or fetched_feed.category
        )
        candidate_metadata["podcast_type"] = (
            candidate_metadata.get("podcast_type") or fetched_feed.podcast_type
        )
        # Use `is not None` check for explicit since False is a valid override value
        explicit_override = candidate_metadata.get("explicit")
        candidate_metadata["explicit"] = (
            explicit_override
            if explicit_override is not None
            else fetched_feed.explicit
        )

    # Remove None values to avoid overwriting defaults
    return {k: v for k, v in candidate_metadata.items() if v is not None}


def merge_download_metadata(existing: Download, fetched: Download) -> Download:
    """Merge fetched metadata into a copy of existing download.

    Fetched values take precedence over existing values. Existing values are
    preserved when fetched values are None/falsy.

    Does not modify status, error tracking, retry state, timestamps, or
    file-related fields (ext, filesize, thumbnail_ext).

    Args:
        existing: The existing Download from the database.
        fetched: The newly fetched Download with fresh metadata.

    Returns:
        A copy of existing with merged metadata fields.
    """
    updated = existing.model_copy()

    # Core metadata
    updated.source_url = fetched.source_url or existing.source_url
    updated.title = fetched.title or existing.title
    updated.published = fetched.published or existing.published
    updated.description = (
        fetched.description if fetched.description is not None else existing.description
    )

    updated.quality_info = (
        fetched.quality_info
        if fetched.quality_info is not None
        else existing.quality_info
    )

    # Thumbnail (remote URL only; thumbnail_ext is set during download)
    updated.remote_thumbnail_url = (
        fetched.remote_thumbnail_url
        if fetched.remote_thumbnail_url is not None
        else existing.remote_thumbnail_url
    )

    # Transcript metadata
    updated.transcript_ext = (
        fetched.transcript_ext
        if fetched.transcript_ext is not None
        else existing.transcript_ext
    )
    updated.transcript_lang = (
        fetched.transcript_lang
        if fetched.transcript_lang is not None
        else existing.transcript_lang
    )
    updated.transcript_source = (
        fetched.transcript_source
        if fetched.transcript_source is not None
        else existing.transcript_source
    )

    return updated
