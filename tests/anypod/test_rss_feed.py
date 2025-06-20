# pyright: reportPrivateUsage=false

"""Tests for RSS feed generation functionality."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from xml.etree import ElementTree as ET

import pytest

from anypod.config.types import (
    PodcastCategories,
    PodcastExplicit,
)
from anypod.db import DownloadDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.exceptions import DatabaseOperationError, RSSGenerationError
from anypod.path_manager import PathManager
from anypod.rss.rss_feed import RSSFeedGenerator

# Test constants
TEST_BASE_URL = "http://localhost:8024"
TEST_FEED_ID = "test_feed"
TEST_PODCAST_TITLE = "Test Podcast"
TEST_PODCAST_DESCRIPTION = "A test podcast description"
TEST_AUTHOR = "Test Author"
EXPECTED_GENERATOR = "AnyPod: https://github.com/thurstonsan/anypod"


@pytest.fixture
def mock_download_db() -> MagicMock:
    """Fixture to provide a mocked DownloadDatabase."""
    return MagicMock(spec=DownloadDatabase)


@pytest.fixture
def path_manager(tmp_path: Path) -> PathManager:
    """Fixture to provide a PathManager instance."""
    data_dir = tmp_path / "data"
    paths = PathManager(data_dir, TEST_BASE_URL)
    return paths


@pytest.fixture
def test_feed() -> Feed:
    """Fixture to provide a test Feed object."""
    return Feed(
        id=TEST_FEED_ID,
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://www.youtube.com/@testchannel",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title=TEST_PODCAST_TITLE,
        description=TEST_PODCAST_DESCRIPTION,
        language="en",
        author=TEST_AUTHOR,
        image_url="https://example.com/artwork.jpg",
        category=PodcastCategories("Technology"),
        explicit=PodcastExplicit.NO,
    )


@pytest.fixture
def sample_downloads() -> list[Download]:
    """Fixture to provide sample download data."""
    return [
        Download(
            feed=TEST_FEED_ID,
            id="video1",
            source_url="https://youtube.com/watch?v=video1",
            title="Test Video 1",
            published=datetime(2023, 1, 15, 12, 0, 0, tzinfo=UTC),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1048576,
            duration=300,
            status=DownloadStatus.DOWNLOADED,
            discovered_at=datetime(2023, 1, 16, 12, 0, 0, tzinfo=UTC),
            updated_at=datetime(2023, 1, 16, 12, 0, 0, tzinfo=UTC),
            thumbnail="https://example.com/thumb1.jpg",
            description="Description for video 1",
        ),
        Download(
            feed=TEST_FEED_ID,
            id="video2",
            source_url="https://youtube.com/watch?v=video2",
            title="Test Video 2",
            published=datetime(2023, 1, 10, 10, 30, 0, tzinfo=UTC),
            ext="m4a",
            mime_type="audio/mp4",
            filesize=524288,
            duration=180,
            status=DownloadStatus.DOWNLOADED,
            discovered_at=datetime(2023, 1, 11, 10, 30, 0, tzinfo=UTC),
            updated_at=datetime(2023, 1, 11, 10, 30, 0, tzinfo=UTC),
            thumbnail="https://example.com/thumb2.jpg",
            description="Description for video 2",
        ),
    ]


@pytest.fixture
def rss_generator(
    mock_download_db: MagicMock, path_manager: PathManager
) -> RSSFeedGenerator:
    """Fixture to provide RSSFeedGenerator instance."""
    return RSSFeedGenerator(mock_download_db, path_manager)


# --- Tests for RSSFeedGenerator.update_feed ---


@pytest.mark.unit
def test_update_feed_success(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    sample_downloads: list[Download],
):
    """Test successful feed generation and caching."""
    feed_id = TEST_FEED_ID
    mock_download_db.get_downloads_by_status.return_value = sample_downloads

    rss_generator.update_feed(feed_id, test_feed)

    mock_download_db.get_downloads_by_status.assert_called_once_with(
        status_to_filter=DownloadStatus.DOWNLOADED, feed_id=feed_id
    )

    # Verify feed is cached
    cached_xml = rss_generator.get_feed_xml(feed_id)
    assert isinstance(cached_xml, bytes)
    assert len(cached_xml) > 0


@pytest.mark.unit
def test_update_feed_database_error(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
):
    """Test feed generation with database error."""
    feed_id = TEST_FEED_ID
    mock_download_db.get_downloads_by_status.side_effect = DatabaseOperationError(
        "Database connection failed"
    )

    with pytest.raises(RSSGenerationError) as exc_info:
        rss_generator.update_feed(feed_id, test_feed)

    assert "Failed to retrieve downloads for feed" in str(exc_info.value)
    assert exc_info.value.feed_id == feed_id


# --- Tests for RSSFeedGenerator.get_feed_xml ---


@pytest.mark.unit
def test_get_feed_xml_not_found(rss_generator: RSSFeedGenerator):
    """Test retrieving XML for non-existent feed."""
    feed_id = "nonexistent_feed"

    with pytest.raises(RSSGenerationError) as exc_info:
        rss_generator.get_feed_xml(feed_id)

    assert "Feed not found in cache" in str(exc_info.value)
    assert exc_info.value.feed_id == feed_id


@pytest.mark.unit
def test_generated_xml_structure(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    sample_downloads: list[Download],
):
    """Test that generated XML has correct RSS structure and content."""
    feed_id = TEST_FEED_ID
    mock_download_db.get_downloads_by_status.return_value = sample_downloads

    rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = rss_generator.get_feed_xml(feed_id)

    # Parse XML and verify structure
    root = ET.fromstring(xml_bytes)

    # Verify RSS root element
    assert root.tag == "rss"
    assert root.get("version") == "2.0"

    # Find channel element
    channel = root.find("channel")
    assert channel is not None

    # Verify basic channel elements
    title_elem = channel.find("title")
    assert title_elem is not None and title_elem.text == TEST_PODCAST_TITLE

    desc_elem = channel.find("description")
    assert desc_elem is not None and desc_elem.text == TEST_PODCAST_DESCRIPTION

    lang_elem = channel.find("language")
    assert lang_elem is not None and lang_elem.text == "en"

    gen_elem = channel.find("generator")
    assert gen_elem is not None and gen_elem.text == EXPECTED_GENERATOR

    ttl_elem = channel.find("ttl")
    assert ttl_elem is not None and ttl_elem.text == "60"

    # Verify iTunes podcast extensions
    itunes_ns = {"itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd"}
    itunes_summary = channel.find("itunes:summary", itunes_ns)
    assert itunes_summary is not None
    assert itunes_summary.text == TEST_PODCAST_DESCRIPTION

    itunes_author = channel.find("itunes:author", itunes_ns)
    assert itunes_author is not None
    assert itunes_author.text == TEST_AUTHOR

    itunes_explicit = channel.find("itunes:explicit", itunes_ns)
    assert itunes_explicit is not None
    assert itunes_explicit.text == "no"

    itunes_image = channel.find("itunes:image", itunes_ns)
    assert itunes_image is not None
    assert itunes_image.get("href") == "https://example.com/artwork.jpg"

    # Verify category
    itunes_category = channel.find("itunes:category", itunes_ns)
    assert itunes_category is not None
    assert itunes_category.get("text") == "Technology"

    # Verify items (episodes)
    items = channel.findall("item")
    assert len(items) == 2

    # Check first item (should be newest - video1)
    first_item = items[0]
    first_title = first_item.find("title")
    assert first_title is not None and first_title.text == "Test Video 1"

    first_desc = first_item.find("description")
    assert first_desc is not None and first_desc.text == "Description for video 1"

    # Verify enclosure for first item
    enclosure = first_item.find("enclosure")
    assert enclosure is not None
    assert enclosure.get("url") == f"{TEST_BASE_URL}/media/{TEST_FEED_ID}/video1.mp4"
    assert enclosure.get("type") == "video/mp4"
    assert enclosure.get("length") == "1048576"

    # Verify iTunes duration
    itunes_duration = first_item.find("itunes:duration", itunes_ns)
    assert itunes_duration is not None
    assert itunes_duration.text == "300"


@pytest.mark.unit
def test_generated_xml_enclosure_urls(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    sample_downloads: list[Download],
):
    """Test that enclosure URLs are correctly formatted."""
    feed_id = TEST_FEED_ID
    mock_download_db.get_downloads_by_status.return_value = sample_downloads

    rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = rss_generator.get_feed_xml(feed_id)

    root = ET.fromstring(xml_bytes)
    channel = root.find("channel")
    assert channel is not None
    items = channel.findall("item")

    # Check video file enclosure
    video_item = items[0]  # Should be video1 (newest first)
    video_enclosure = video_item.find("enclosure")
    assert video_enclosure is not None
    assert (
        video_enclosure.get("url") == f"{TEST_BASE_URL}/media/{TEST_FEED_ID}/video1.mp4"
    )
    assert video_enclosure.get("type") == "video/mp4"

    # Check audio file enclosure
    audio_item = items[1]  # Should be video2 (older)
    audio_enclosure = audio_item.find("enclosure")
    assert audio_enclosure is not None
    assert (
        audio_enclosure.get("url") == f"{TEST_BASE_URL}/media/{TEST_FEED_ID}/video2.m4a"
    )
    assert audio_enclosure.get("type") == "audio/mp4"


@pytest.mark.unit
def test_generated_xml_mime_types(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
):
    """Test that MIME types are correctly preserved in enclosures."""
    feed_id = TEST_FEED_ID
    downloads_with_various_types = [
        Download(
            feed=TEST_FEED_ID,
            id="video_mp4",
            source_url="https://example.com/video",
            title="MP4 Video",
            published=datetime(2023, 1, 1, tzinfo=UTC),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1000000,
            duration=300,
            status=DownloadStatus.DOWNLOADED,
            discovered_at=datetime(2023, 1, 2, tzinfo=UTC),
            updated_at=datetime(2023, 1, 2, tzinfo=UTC),
        ),
        Download(
            feed=TEST_FEED_ID,
            id="audio_m4a",
            source_url="https://example.com/audio",
            title="M4A Audio",
            published=datetime(2023, 1, 2, tzinfo=UTC),
            ext="m4a",
            mime_type="audio/mp4",
            filesize=500000,
            duration=180,
            status=DownloadStatus.DOWNLOADED,
            discovered_at=datetime(2023, 1, 3, tzinfo=UTC),
            updated_at=datetime(2023, 1, 3, tzinfo=UTC),
        ),
    ]

    mock_download_db.get_downloads_by_status.return_value = downloads_with_various_types

    rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = rss_generator.get_feed_xml(feed_id)

    root = ET.fromstring(xml_bytes)
    channel = root.find("channel")
    assert channel is not None
    items = channel.findall("item")

    # Find the M4A item
    m4a_item = None
    mp4_item = None
    for item in items:
        enclosure = item.find("enclosure")
        if enclosure is not None:
            if enclosure.get("type") == "audio/mp4":
                m4a_item = item
            elif enclosure.get("type") == "video/mp4":
                mp4_item = item

    assert m4a_item is not None, "M4A item should be found"
    assert mp4_item is not None, "MP4 item should be found"

    # Verify the MIME types are correctly set
    m4a_enclosure = m4a_item.find("enclosure")
    assert m4a_enclosure is not None and m4a_enclosure.get("type") == "audio/mp4"

    mp4_enclosure = mp4_item.find("enclosure")
    assert mp4_enclosure is not None and mp4_enclosure.get("type") == "video/mp4"


@pytest.mark.unit
def test_empty_downloads_list(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
):
    """Test RSS generation with no downloads."""
    feed_id = "empty_feed"
    mock_download_db.get_downloads_by_status.return_value = []

    rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = rss_generator.get_feed_xml(feed_id)

    root = ET.fromstring(xml_bytes)
    channel = root.find("channel")
    assert channel is not None
    items = channel.findall("item")

    # Should have valid RSS structure but no items
    title_elem = channel.find("title")
    assert title_elem is not None and title_elem.text == TEST_PODCAST_TITLE
    assert len(items) == 0


# --- Tests for FeedgenCore feed metadata validation ---


@pytest.mark.unit
def test_feed_config_without_metadata_fails():
    """Test that FeedgenCore raises error when feed has no required metadata."""
    from anypod.rss.feedgen_core import FeedgenCore

    # Create a feed without required metadata
    feed_without_metadata = Feed(
        id=TEST_FEED_ID,
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://www.youtube.com/@testchannel",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title=None,  # Missing required title
        description=None,  # Missing required description
    )

    paths = PathManager(Path("/tmp/data"), TEST_BASE_URL)
    with pytest.raises(ValueError) as exc_info:
        FeedgenCore(paths, TEST_FEED_ID, feed_without_metadata)

    assert "Feed title is required when creating an RSS feed" in str(exc_info.value)
