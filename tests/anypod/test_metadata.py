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
from anypod.db.types import Download, DownloadStatus, Feed, SourceType, TranscriptSource
from anypod.metadata import merge_download_metadata, merge_feed_metadata


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
def test_merge_feed_metadata_explicit_false_overrides_true():
    """Test that explicit=False override correctly overrides explicit=True from feed.

    This tests a subtle bug where using `or` instead of `is not None` would cause
    `False or True` to evaluate to `True`, ignoring the user's explicit=False override.
    """
    # Create a fetched feed with explicit=True
    fetched_feed = Feed(
        id="test_feed",
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://example.com/feed",
        resolved_url="https://example.com/feed/resolved",
        last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        title="Original Title",
        category=PodcastCategories("Technology"),
        podcast_type=PodcastType.EPISODIC,
        explicit=True,  # Feed is marked as explicit
    )

    # User wants to override explicit to False
    metadata_overrides = FeedMetadataOverrides(  # type: ignore
        explicit=False,
    )

    feed_config = FeedConfig(
        url="https://example.com/feed",
        schedule=CronExpression("0 * * * *"),
        since=None,
        keep_last=None,
        metadata=metadata_overrides,
    )

    result = merge_feed_metadata(fetched_feed, feed_config)

    # explicit=False should override the feed's explicit=True
    assert result["explicit"] is False


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


# --- merge_download_metadata tests ---


@pytest.mark.unit
def test_merge_download_metadata_or_pattern_fields():
    """Test fields using 'or' pattern preserve existing when fetched is empty string."""
    existing = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/original",
        title="Original Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
    )

    # Fetched has empty strings for 'or' pattern fields
    # Note: For datetime, we need an actual None-like value, not just an old date
    # The merge logic uses `or` which means we'd need a falsy value
    # In practice, published is always a valid datetime from yt-dlp
    fetched = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="",  # Empty string should preserve existing
        title="",  # Empty string should preserve existing
        published=datetime(2024, 1, 1, tzinfo=UTC),  # Same as existing for this test
        ext="",  # Empty string should preserve existing
        mime_type="",  # Empty string should preserve existing
        filesize=0,  # Zero should preserve existing
        duration=0,  # Zero should preserve existing
        status=DownloadStatus.UPCOMING,
    )

    result = merge_download_metadata(existing, fetched)

    # All 'or' pattern fields should preserve existing values when fetched is falsy
    assert result.source_url == "https://example.com/original"
    assert result.title == "Original Title"
    assert result.published == datetime(2024, 1, 1, tzinfo=UTC)
    assert result.ext == "mp4"
    assert result.mime_type == "video/mp4"
    assert result.filesize == 1000
    assert result.duration == 120


@pytest.mark.unit
def test_merge_download_metadata_or_pattern_fields_overwrites_when_truthy():
    """Test fields using 'or' pattern overwrite when fetched is truthy."""
    existing = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/original",
        title="Original Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
    )

    # Fetched has new truthy values
    fetched = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/new",
        title="New Title",
        published=datetime(2024, 2, 1, tzinfo=UTC),
        ext="webm",
        mime_type="video/webm",
        filesize=2000,
        duration=240,
        status=DownloadStatus.UPCOMING,
    )

    result = merge_download_metadata(existing, fetched)

    # Core 'or' pattern fields should use fetched values
    assert result.source_url == "https://example.com/new"
    assert result.title == "New Title"
    assert result.published == datetime(2024, 2, 1, tzinfo=UTC)

    # File-related fields never updated
    assert result.ext == "mp4"
    assert result.mime_type == "video/mp4"
    assert result.filesize == 1000
    assert result.duration == 120


@pytest.mark.unit
def test_merge_download_metadata_is_not_none_pattern_fields():
    """Test fields using 'is not None' pattern allow empty string to overwrite."""
    existing = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        description="Original description",
        quality_info="1080p",
    )

    # Fetched has empty strings for 'is not None' pattern fields
    fetched = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        description="",  # Empty string should overwrite
        quality_info="",  # Empty string should overwrite
    )

    result = merge_download_metadata(existing, fetched)

    # Empty strings should overwrite for 'is not None' pattern fields
    assert result.description == ""
    assert result.quality_info == ""


@pytest.mark.unit
def test_merge_download_metadata_is_not_none_preserves_on_none():
    """Test fields using 'is not None' pattern preserve when fetched is None."""
    existing = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        description="Original description",
        quality_info="1080p",
    )

    # Fetched has None for 'is not None' pattern fields
    fetched = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        description=None,  # None should preserve existing
        quality_info=None,  # None should preserve existing
    )

    result = merge_download_metadata(existing, fetched)

    # None should preserve existing values
    assert result.description == "Original description"
    assert result.quality_info == "1080p"


@pytest.mark.unit
def test_merge_download_metadata_thumbnail_fields():
    """Test thumbnail-related fields merge correctly."""
    existing = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        remote_thumbnail_url="https://example.com/old_thumb.jpg",
        thumbnail_ext="jpg",  # This should NOT be modified by merge
    )

    # Fetched has new remote thumbnail URL
    fetched = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        remote_thumbnail_url="https://example.com/new_thumb.jpg",
        thumbnail_ext="png",  # This should be ignored, not part of merge
    )

    result = merge_download_metadata(existing, fetched)

    # remote_thumbnail_url should be updated
    assert result.remote_thumbnail_url == "https://example.com/new_thumb.jpg"
    # thumbnail_ext should be preserved from existing (not part of merge logic)
    assert result.thumbnail_ext == "jpg"


@pytest.mark.unit
def test_merge_download_metadata_thumbnail_preserves_on_none():
    """Test thumbnail URL preserved when fetched is None."""
    existing = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        remote_thumbnail_url="https://example.com/thumb.jpg",
    )

    # Fetched has None for remote_thumbnail_url
    fetched = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        remote_thumbnail_url=None,
    )

    result = merge_download_metadata(existing, fetched)

    # Should preserve existing remote_thumbnail_url
    assert result.remote_thumbnail_url == "https://example.com/thumb.jpg"


@pytest.mark.unit
def test_merge_download_metadata_thumbnail_overwrites_with_empty_string():
    """Test thumbnail URL overwrites when fetched is empty string."""
    existing = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        remote_thumbnail_url="https://example.com/thumb.jpg",
    )

    # Fetched has empty string for remote_thumbnail_url
    fetched = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        remote_thumbnail_url="",
    )

    result = merge_download_metadata(existing, fetched)

    # Empty string should overwrite (uses 'is not None' pattern)
    assert result.remote_thumbnail_url == ""


@pytest.mark.unit
def test_merge_download_metadata_transcript_fields():
    """Test transcript-related fields merge correctly."""
    existing = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        transcript_ext="vtt",
        transcript_lang="en",
        transcript_source=TranscriptSource.CREATOR,
    )

    # Fetched has updated transcript fields
    fetched = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        transcript_ext="srt",
        transcript_lang="es",
        transcript_source=TranscriptSource.AUTO,
    )

    result = merge_download_metadata(existing, fetched)

    # All transcript fields should be updated
    assert result.transcript_ext == "srt"
    assert result.transcript_lang == "es"
    assert result.transcript_source == TranscriptSource.AUTO


@pytest.mark.unit
def test_merge_download_metadata_transcript_preserves_on_none():
    """Test transcript fields preserved when fetched is None."""
    existing = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        transcript_ext="vtt",
        transcript_lang="en",
        transcript_source=TranscriptSource.CREATOR,
    )

    # Fetched has None for all transcript fields
    fetched = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        transcript_ext=None,
        transcript_lang=None,
        transcript_source=None,
    )

    result = merge_download_metadata(existing, fetched)

    # All transcript fields should preserve existing values
    assert result.transcript_ext == "vtt"
    assert result.transcript_lang == "en"
    assert result.transcript_source == TranscriptSource.CREATOR


@pytest.mark.unit
def test_merge_download_metadata_transcript_overwrites_with_empty_string():
    """Test transcript fields overwrite when fetched is empty string."""
    existing = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        transcript_ext="vtt",
        transcript_lang="en",
    )

    # Fetched has empty strings for transcript fields (uses 'is not None' pattern)
    fetched = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        transcript_ext="",
        transcript_lang="",
    )

    result = merge_download_metadata(existing, fetched)

    # Empty strings should overwrite
    assert result.transcript_ext == ""
    assert result.transcript_lang == ""


@pytest.mark.unit
def test_merge_download_metadata_status_fields_not_modified():
    """Test status and error tracking fields are not modified by merge."""
    existing = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.ERROR,
        retries=3,
        last_error="Connection timeout",
        download_logs="Error logs here",
    )

    # Fetched has different status/error values (should be ignored)
    fetched = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Updated Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.DOWNLOADED,
        retries=0,
        last_error=None,
        download_logs=None,
    )

    result = merge_download_metadata(existing, fetched)

    # Status and error fields should be preserved from existing
    assert result.status == DownloadStatus.ERROR
    assert result.retries == 3
    assert result.last_error == "Connection timeout"
    assert result.download_logs == "Error logs here"
    # But title should be updated
    assert result.title == "Updated Title"


@pytest.mark.unit
def test_merge_download_metadata_timestamp_fields_not_modified():
    """Test timestamp fields are not modified by merge."""
    discovered_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    updated_time = datetime(2024, 1, 2, 12, 0, 0, tzinfo=UTC)
    downloaded_time = datetime(2024, 1, 3, 12, 0, 0, tzinfo=UTC)

    existing = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Video Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.DOWNLOADED,
        discovered_at=discovered_time,
        updated_at=updated_time,
        downloaded_at=downloaded_time,
    )

    # Fetched has different timestamp values (should be ignored)
    fetched = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Updated Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        discovered_at=datetime(2024, 12, 1, tzinfo=UTC),
        updated_at=datetime(2024, 12, 2, tzinfo=UTC),
        downloaded_at=datetime(2024, 12, 3, tzinfo=UTC),
    )

    result = merge_download_metadata(existing, fetched)

    # Timestamp fields should be preserved from existing
    assert result.discovered_at == discovered_time
    assert result.updated_at == updated_time
    assert result.downloaded_at == downloaded_time
    # But title should be updated
    assert result.title == "Updated Title"


@pytest.mark.unit
def test_merge_download_metadata_does_not_mutate_existing():
    """Test merge returns a new copy without mutating the existing download."""
    existing = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="Original Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        description="Original description",
    )

    fetched = Download(
        feed_id="test_feed",
        id="test_download",
        source_url="https://example.com/video",
        title="New Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        status=DownloadStatus.UPCOMING,
        description="New description",
    )

    result = merge_download_metadata(existing, fetched)

    # Existing should remain unchanged
    assert existing.title == "Original Title"
    assert existing.description == "Original description"
    # Result should have new values
    assert result.title == "New Title"
    assert result.description == "New description"
    # Result should be a different object
    assert result is not existing


@pytest.mark.unit
def test_merge_download_metadata_all_field_categories():
    """Test comprehensive merge covering all field categories."""
    existing = Download(
        feed_id="test_feed",
        id="test_download",
        # Core metadata
        source_url="https://example.com/old",
        title="Old Title",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        description="Old description",
        # Media details
        ext="mp4",
        mime_type="video/mp4",
        filesize=1000,
        duration=120,
        quality_info="720p",
        # Status (should not be modified)
        status=DownloadStatus.UPCOMING,
        # Thumbnail
        remote_thumbnail_url="https://example.com/old_thumb.jpg",
        # Transcript
        transcript_ext="vtt",
        transcript_lang="en",
        transcript_source=TranscriptSource.CREATOR,
    )

    fetched = Download(
        feed_id="test_feed",
        id="test_download",
        # Core metadata - all updated
        source_url="https://example.com/new",
        title="New Title",
        published=datetime(2024, 2, 1, tzinfo=UTC),
        description="New description",
        # Media details - all updated
        ext="webm",
        mime_type="video/webm",
        filesize=2000,
        duration=240,
        quality_info="1080p",
        # Status (should be ignored)
        status=DownloadStatus.DOWNLOADED,
        # Thumbnail - updated
        remote_thumbnail_url="https://example.com/new_thumb.jpg",
        # Transcript - all updated
        transcript_ext="srt",
        transcript_lang="es",
        transcript_source=TranscriptSource.AUTO,
    )

    result = merge_download_metadata(existing, fetched)

    # Core metadata - all updated
    assert result.source_url == "https://example.com/new"
    assert result.title == "New Title"
    assert result.published == datetime(2024, 2, 1, tzinfo=UTC)
    assert result.description == "New description"

    # File-related fields never updated
    assert result.ext == "mp4"
    assert result.mime_type == "video/mp4"
    assert result.filesize == 1000
    assert result.duration == 120

    # Non-file media metadata - updated
    assert result.quality_info == "1080p"

    # Status - preserved from existing
    assert result.status == DownloadStatus.UPCOMING

    # Thumbnail - updated
    assert result.remote_thumbnail_url == "https://example.com/new_thumb.jpg"

    # Transcript - all updated
    assert result.transcript_ext == "srt"
    assert result.transcript_lang == "es"
    assert result.transcript_source == TranscriptSource.AUTO
