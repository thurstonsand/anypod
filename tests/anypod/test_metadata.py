# pyright: reportPrivateUsage=false
"""Unit tests for metadata utility functions."""

from datetime import UTC, datetime

import pytest

from anypod.config import FeedConfig
from anypod.config.types import (
    CronExpression,
    FeedMetadataOverrides,
    PodcastCategories,
    PodcastType,
)
from anypod.db.types import Feed, SourceType
from anypod.metadata import merge_feed_metadata


@pytest.mark.unit
def test_merge_feed_metadata_no_overrides():
    """Test merging when no metadata overrides are provided."""
    # Create a fetched feed with some metadata
    fetched_feed = Feed(
        id="test_feed",
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://example.com/feed",
        resolved_url="https://example.com/feed/resolved",
        last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        title="Original Title",
        subtitle="Original Subtitle",
        description="Original Description",
        language="en",
        author="Original Author",
        author_email="original@example.com",
        remote_image_url="https://example.com/image.jpg",
        category=PodcastCategories("Technology"),
        podcast_type=PodcastType.EPISODIC,
        explicit=False,
    )

    # Create feed config without metadata overrides
    feed_config = FeedConfig(
        url="https://example.com/feed",
        schedule=CronExpression("0 * * * *"),
        since=datetime(2023, 1, 1, tzinfo=UTC),
        keep_last=100,
        metadata=None,
    )

    result = merge_feed_metadata(fetched_feed, feed_config)

    # Should use values from fetched feed and config
    assert result["title"] == "Original Title"
    assert result["subtitle"] == "Original Subtitle"
    assert result["description"] == "Original Description"
    assert result["language"] == "en"
    assert result["author"] == "Original Author"
    assert result["author_email"] == "original@example.com"
    assert result["remote_image_url"] == "https://example.com/image.jpg"
    assert result["category"] == PodcastCategories("Technology")
    assert result["podcast_type"] == PodcastType.EPISODIC
    assert result["explicit"] is False


@pytest.mark.unit
def test_merge_feed_metadata_with_overrides():
    """Test merging when metadata overrides are provided."""
    # Create a fetched feed with some metadata
    fetched_feed = Feed(
        id="test_feed",
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://example.com/feed",
        resolved_url="https://example.com/feed/resolved",
        last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        title="Original Title",
        subtitle="Original Subtitle",
        description="Original Description",
        language="en",
        author="Original Author",
        author_email="original@example.com",
        remote_image_url="https://example.com/image.jpg",
        category=PodcastCategories("Technology"),
        podcast_type=PodcastType.EPISODIC,
        explicit=False,
    )

    # Create metadata overrides
    metadata_overrides = FeedMetadataOverrides(
        title="Override Title",
        subtitle=None,
        description="Override Description",
        language=None,
        category=PodcastCategories("Business"),
        podcast_type=PodcastType.SERIAL,
        explicit=True,
        image_url=None,
        author="Override Author",
        author_email=None,
    )

    # Create feed config with overrides
    feed_config = FeedConfig(
        url="https://example.com/feed",
        schedule=CronExpression("0 * * * *"),
        since=datetime(2023, 1, 1, tzinfo=UTC),
        keep_last=50,
        metadata=metadata_overrides,
    )

    result = merge_feed_metadata(fetched_feed, feed_config)

    # Should use overrides where provided, fallback to fetched values
    assert result["title"] == "Override Title"  # overridden
    assert result["subtitle"] == "Original Subtitle"  # not overridden, from fetched
    assert result["description"] == "Override Description"  # overridden
    assert result["language"] == "en"  # not overridden, from fetched
    assert result["author"] == "Override Author"  # overridden
    assert (
        result["author_email"] == "original@example.com"
    )  # not overridden, from fetched
    assert (
        result["remote_image_url"] == "https://example.com/image.jpg"
    )  # not overridden, from fetched
    assert result["category"] == PodcastCategories(
        "Business"
    )  # overridden (categories -> category mapping)
    assert result["podcast_type"] == PodcastType.SERIAL  # overridden
    assert result["explicit"] is True  # overridden


@pytest.mark.unit
def test_merge_feed_metadata_partial_overrides():
    """Test merging when only some metadata fields are overridden."""
    # Create a fetched feed with some metadata
    fetched_feed = Feed(
        id="test_feed",
        is_enabled=True,
        source_type=SourceType.PLAYLIST,
        source_url="https://example.com/playlist",
        resolved_url="https://example.com/playlist/resolved",
        last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        title="Original Title",
        subtitle="Original Subtitle",
        description="Original Description",
        language="fr",
        author="Original Author",
        author_email="original@example.com",
        remote_image_url="https://example.com/image.jpg",
        category=PodcastCategories("Music"),
        podcast_type=PodcastType.EPISODIC,
        explicit=False,
    )

    # Create partial metadata overrides (only override some fields)
    metadata_overrides = FeedMetadataOverrides(  # type: ignore
        title="New Title",
        language="es",
    )

    # Create feed config with partial overrides
    feed_config = FeedConfig(
        url="https://example.com/playlist",
        schedule=CronExpression("0 * * * *"),
        since=None,  # No since filter
        keep_last=None,  # No limit
        metadata=metadata_overrides,
    )

    result = merge_feed_metadata(fetched_feed, feed_config)

    # Should use overrides where provided, fallback to fetched values for others
    assert result["title"] == "New Title"  # overridden
    assert result["subtitle"] == "Original Subtitle"  # from fetched
    assert result["description"] == "Original Description"  # from fetched
    assert result["language"] == "es"  # overridden
    assert result["author"] == "Original Author"  # from fetched
    assert result["author_email"] == "original@example.com"  # from fetched
    assert result["remote_image_url"] == "https://example.com/image.jpg"  # from fetched
    assert result["category"] == PodcastCategories("Music")  # from fetched
    assert result["podcast_type"] == PodcastType.EPISODIC  # from fetched
    assert result["explicit"] is False  # from fetched


@pytest.mark.unit
def test_merge_feed_metadata_removes_none_values():
    """Test that None values are excluded from the result."""
    # Create a fetched feed with some None values
    fetched_feed = Feed(
        id="test_feed",
        is_enabled=True,
        source_type=SourceType.SINGLE_VIDEO,
        source_url="https://example.com/video",
        resolved_url="https://example.com/video/resolved",
        last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        title="Video Title",
        subtitle=None,  # None value
        description=None,  # None value
        language="en",
        author=None,  # None value
        # author_email uses default "notifications@thurstons.house"
        remote_image_url="https://example.com/image.jpg",
        category=PodcastCategories("Technology"),
        podcast_type=PodcastType.EPISODIC,
        explicit=False,
    )

    # Create feed config without metadata overrides
    feed_config = FeedConfig(
        url="https://example.com/video",
        schedule=CronExpression("0 * * * *"),
        since=datetime(2023, 6, 1, tzinfo=UTC),
        keep_last=10,
        metadata=None,
    )

    result = merge_feed_metadata(fetched_feed, feed_config)

    # None values should be excluded from result
    assert "subtitle" not in result
    assert "description" not in result
    assert "author" not in result

    # Non-None values should be included
    assert result["title"] == "Video Title"
    assert result["language"] == "en"
    assert result["author_email"] == "notifications@thurstons.house"  # default value
    assert result["remote_image_url"] == "https://example.com/image.jpg"
    assert result["category"] == PodcastCategories("Technology")


@pytest.mark.unit
def test_merge_feed_metadata_category_mapping():
    """Test that categories field is correctly mapped to category."""
    # Create a fetched feed
    fetched_feed = Feed(
        id="test_feed",
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://example.com/feed",
        resolved_url="https://example.com/feed/resolved",
        last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        title="Feed Title",
        category=PodcastCategories("Technology"),
        podcast_type=PodcastType.EPISODIC,
        explicit=False,
    )

    # Create metadata overrides with categories field
    metadata_overrides = FeedMetadataOverrides(  # type: ignore
        category=PodcastCategories("Arts"),
    )

    # Create feed config with overrides
    feed_config = FeedConfig(
        url="https://example.com/feed",
        schedule=CronExpression("0 * * * *"),
        metadata=metadata_overrides,
        keep_last=None,
        since=None,
    )

    result = merge_feed_metadata(fetched_feed, feed_config)

    # categories should be mapped to category
    assert result["category"] == PodcastCategories("Arts")
    assert "categories" not in result


@pytest.mark.unit
def test_merge_feed_metadata_authoritative_fields():
    """Test that source_type, since, and keep_last come from authoritative sources."""
    # Create a fetched feed
    fetched_feed = Feed(
        id="test_feed",
        is_enabled=True,
        source_type=SourceType.PLAYLIST,
        source_url="https://example.com/feed",
        resolved_url="https://example.com/feed/resolved",
        last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        title="Feed Title",
        since=datetime(2020, 1, 1, tzinfo=UTC),  # This should be ignored
        keep_last=999,  # This should be ignored
        category=PodcastCategories("Technology"),
        podcast_type=PodcastType.EPISODIC,
        explicit=False,
    )

    # Create feed config with different values
    feed_config = FeedConfig(
        url="https://example.com/feed",
        schedule=CronExpression("0 * * * *"),
        since=datetime(2023, 5, 1, tzinfo=UTC),
        keep_last=25,
        metadata=None,
    )

    result = merge_feed_metadata(fetched_feed, feed_config)

    # Feed metadata should not include source_type, since, or keep_last
    assert "source_type" not in result
    assert "since" not in result
    assert "keep_last" not in result

    # But should include other metadata fields
    assert result["title"] == "Feed Title"
    assert result["category"] == PodcastCategories("Technology")
