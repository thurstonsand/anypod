# pyright: reportPrivateUsage=false

"""Tests for RSS feed generation functionality."""

from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from unittest.mock import AsyncMock, MagicMock
from xml.etree import ElementTree as ET

import pytest

from anypod.config.types import (
    PodcastCategories,
    PodcastType,
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
    mock = MagicMock(spec=DownloadDatabase)
    # Configure async methods with AsyncMock
    mock.get_downloads_by_status = AsyncMock()
    return mock


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
        subtitle="A test podcast subtitle",
        description=TEST_PODCAST_DESCRIPTION,
        language="en",
        author=TEST_AUTHOR,
        author_email="test@example.com",
        remote_image_url="https://example.com/artwork.jpg",
        category=PodcastCategories("Technology"),
        podcast_type=PodcastType.EPISODIC,
        explicit=False,
    )


@pytest.fixture
def sample_downloads() -> list[Download]:
    """Fixture to provide sample download data."""
    return [
        Download(
            feed_id=TEST_FEED_ID,
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
            remote_thumbnail_url="https://example.com/thumb1.jpg",
            description="Description for video 1",
        ),
        Download(
            feed_id=TEST_FEED_ID,
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
            remote_thumbnail_url="https://example.com/thumb2.jpg",
            description="Description for video 2",
        ),
    ]


@pytest.fixture
def rss_generator(
    mock_download_db: MagicMock, path_manager: PathManager
) -> RSSFeedGenerator:
    """Fixture to provide RSSFeedGenerator instance."""
    return RSSFeedGenerator(mock_download_db, path_manager)


@pytest.fixture
def capture_rss_write(monkeypatch: pytest.MonkeyPatch) -> dict[str, bytes]:
    """Capture bytes written by RSSFeedGenerator to avoid disk IO in unit tests.

    Patches aiofiles.open to return an async writer that buffers bytes in memory,
    and patches aiofiles.os.replace/makedirs as no-ops.
    """
    captured: dict[str, bytes] = {"data": b""}

    class _DummyWriter:
        def __init__(self) -> None:
            self._buf = bytearray()

        async def __aenter__(self) -> _DummyWriter:
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> bool:
            captured["data"] = bytes(self._buf)
            return False

        async def write(self, data: bytes) -> None:
            self._buf.extend(data)

    def _fake_open(path: Path | str, mode: str = "rb") -> _DummyWriter:
        return _DummyWriter()

    async def _fake_replace(src: Path | str, dst: Path | str) -> None:
        return None

    async def _fake_makedirs(path: Path | str, exist_ok: bool = True) -> None:
        return None

    monkeypatch.setattr("aiofiles.open", _fake_open)
    monkeypatch.setattr("aiofiles.os.replace", _fake_replace)
    monkeypatch.setattr("aiofiles.os.makedirs", _fake_makedirs)
    return captured


# --- Tests for RSSFeedGenerator.update_feed ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_feed_success(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    sample_downloads: list[Download],
    capture_rss_write: dict[str, bytes],
):
    """Test successful feed generation and file persistence."""
    feed_id = TEST_FEED_ID
    mock_download_db.get_downloads_by_status.return_value = sample_downloads

    await rss_generator.update_feed(feed_id, test_feed)

    mock_download_db.get_downloads_by_status.assert_called_once_with(
        status_to_filter=DownloadStatus.DOWNLOADED, feed_id=feed_id
    )

    # Verify feed XML was written (captured via monkeypatch)
    xml_bytes = capture_rss_write["data"]
    assert isinstance(xml_bytes, bytes)
    assert len(xml_bytes) > 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_feed_database_error(
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
        await rss_generator.update_feed(feed_id, test_feed)

    assert "Failed to retrieve downloads for feed" in str(exc_info.value)
    assert exc_info.value.feed_id == feed_id


# --- Tests for RSS XML content ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_generated_xml_structure(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    sample_downloads: list[Download],
    capture_rss_write: dict[str, bytes],
):
    """Test that generated XML has correct RSS structure and content."""
    feed_id = TEST_FEED_ID
    mock_download_db.get_downloads_by_status.return_value = sample_downloads

    await rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = capture_rss_write["data"]

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
    assert itunes_explicit.text == "false"

    itunes_image = channel.find("itunes:image", itunes_ns)
    assert itunes_image is not None
    assert itunes_image.get("href") == "https://example.com/artwork.jpg"

    # Verify category
    itunes_category = channel.find("itunes:category", itunes_ns)
    assert itunes_category is not None
    assert itunes_category.get("text") == "Technology"

    itunes_subtitle = channel.find("itunes:subtitle", itunes_ns)
    assert itunes_subtitle is not None
    assert itunes_subtitle.text == test_feed.subtitle

    itunes_type = channel.find("itunes:type", itunes_ns)
    assert itunes_type is not None
    assert itunes_type.text == test_feed.podcast_type.rss_str()

    itunes_owner = channel.find("itunes:owner", itunes_ns)
    assert itunes_owner is not None
    itunes_owner_name = itunes_owner.find("itunes:name", itunes_ns)
    assert itunes_owner_name is not None
    assert itunes_owner_name.text == TEST_AUTHOR
    itunes_owner_email = itunes_owner.find("itunes:email", itunes_ns)
    assert itunes_owner_email is not None
    assert itunes_owner_email.text == test_feed.author_email

    # Verify RSS image element exists alongside iTunes image
    rss_image = channel.find("image")
    assert rss_image is not None
    rss_image_url = rss_image.find("url")
    assert rss_image_url is not None
    assert rss_image_url.text == test_feed.remote_image_url
    rss_image_title = rss_image.find("title")
    assert rss_image_title is not None
    assert rss_image_title.text == test_feed.title
    rss_image_link = rss_image.find("link")
    assert rss_image_link is not None
    assert rss_image_link.text == test_feed.source_url

    # Verify RSS category exists alongside iTunes category
    rss_category = channel.find("category")
    assert rss_category is not None
    test_feed_category = test_feed.category
    assert test_feed_category is not None
    assert rss_category.text == str(test_feed_category)

    # Verify channel publication date is set to newest episode date
    pubdate = channel.find("pubDate")
    assert pubdate is not None and pubdate.text is not None
    # Should be the publication date of the newest episode (video1)
    assert "15 Jan 2023" in pubdate.text

    # Verify items (episodes)
    items = channel.findall("item")
    assert len(items) == 2

    # Transcript elements should be absent when downloads have no transcript metadata
    podcast_ns = {"podcast": "https://podcastindex.org/namespace/1.0"}
    for item in items:
        assert item.find("podcast:transcript", podcast_ns) is None

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
    assert itunes_duration.text == "00:05:00"  # 300 seconds = 5 minutes

    # Verify iTunes episode type
    itunes_episode_type = first_item.find("itunes:episodeType", itunes_ns)
    assert itunes_episode_type is not None
    assert itunes_episode_type.text == "full"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_channel_and_items_use_hosted_images_when_exts_present(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    sample_downloads: list[Download],
    capture_rss_write: dict[str, bytes],
):
    """When feed.image_ext and download.thumbnail_ext are set, hosted URLs are used."""
    feed_id = TEST_FEED_ID
    # Set feed image extension to use hosted channel artwork
    test_feed.image_ext = "jpg"
    # Set per-item thumbnail extensions to use hosted episode images
    for dl in sample_downloads:
        dl.thumbnail_ext = "jpg"

    mock_download_db.get_downloads_by_status.return_value = sample_downloads

    await rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = capture_rss_write["data"]

    root = ET.fromstring(xml_bytes)
    channel = root.find("channel")
    assert channel is not None

    itunes_ns = {"itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd"}

    # Channel image should point to hosted artwork
    itunes_image = channel.find("itunes:image", itunes_ns)
    assert itunes_image is not None
    assert itunes_image.get("href") == f"{TEST_BASE_URL}/images/{feed_id}.jpg"

    rss_image = channel.find("image")
    assert rss_image is not None
    rss_image_url = rss_image.find("url")
    assert rss_image_url is not None
    assert rss_image_url.text == f"{TEST_BASE_URL}/images/{feed_id}.jpg"

    # Item-level itunes:image should point to hosted per-download thumbnails
    items = channel.findall("item")
    assert len(items) == 2

    first_item = items[0]
    first_itunes_image = first_item.find("itunes:image", itunes_ns)
    assert first_itunes_image is not None
    assert (
        first_itunes_image.get("href") == f"{TEST_BASE_URL}/images/{feed_id}/video1.jpg"
    )

    second_item = items[1]
    second_itunes_image = second_item.find("itunes:image", itunes_ns)
    assert second_itunes_image is not None
    assert (
        second_itunes_image.get("href")
        == f"{TEST_BASE_URL}/images/{feed_id}/video2.jpg"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_items_use_original_thumbnail_when_no_ext(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    sample_downloads: list[Download],
    capture_rss_write: dict[str, bytes],
):
    """When thumbnail_ext is absent, original thumbnail URLs should be used."""
    feed_id = TEST_FEED_ID
    # Ensure no hosted image ext is set for items
    for dl in sample_downloads:
        dl.thumbnail_ext = None

    mock_download_db.get_downloads_by_status.return_value = sample_downloads

    await rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = capture_rss_write["data"]

    root = ET.fromstring(xml_bytes)
    channel = root.find("channel")
    assert channel is not None

    itunes_ns = {"itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd"}
    items = channel.findall("item")
    assert len(items) == 2

    first_item = items[0]
    first_itunes_image = first_item.find("itunes:image", itunes_ns)
    assert first_itunes_image is not None
    assert first_itunes_image.get("href") == "https://example.com/thumb1.jpg"

    second_item = items[1]
    second_itunes_image = second_item.find("itunes:image", itunes_ns)
    assert second_itunes_image is not None
    assert second_itunes_image.get("href") == "https://example.com/thumb2.jpg"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_generated_xml_enclosure_urls(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    sample_downloads: list[Download],
    capture_rss_write: dict[str, bytes],
):
    """Test that enclosure URLs are correctly formatted."""
    feed_id = TEST_FEED_ID
    mock_download_db.get_downloads_by_status.return_value = sample_downloads

    await rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = capture_rss_write["data"]

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
@pytest.mark.asyncio
async def test_generated_xml_mime_types(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    capture_rss_write: dict[str, bytes],
):
    """Test that MIME types are correctly preserved in enclosures."""
    feed_id = TEST_FEED_ID
    downloads_with_various_types = [
        Download(
            feed_id=TEST_FEED_ID,
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
            feed_id=TEST_FEED_ID,
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

    await rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = capture_rss_write["data"]

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
@pytest.mark.asyncio
async def test_empty_downloads_list(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    capture_rss_write: dict[str, bytes],
):
    """Test RSS generation with no downloads."""
    feed_id = "empty_feed"
    mock_download_db.get_downloads_by_status.return_value = []

    await rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = capture_rss_write["data"]

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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_channel_publication_date_set_to_newest_episode(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    sample_downloads: list[Download],
    capture_rss_write: dict[str, bytes],
):
    """Test that channel pubDate is set to the newest episode date."""
    feed_id = TEST_FEED_ID
    mock_download_db.get_downloads_by_status.return_value = sample_downloads

    await rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = capture_rss_write["data"]

    root = ET.fromstring(xml_bytes)
    channel = root.find("channel")
    assert channel is not None

    # Check that pubDate is set
    pubdate = channel.find("pubDate")
    assert pubdate is not None
    assert pubdate.text is not None
    # Should be the publication date of the newest episode (video1 - Jan 15, 2023)
    assert "15 Jan 2023" in pubdate.text


@pytest.mark.unit
@pytest.mark.asyncio
async def test_channel_publication_date_absent_when_no_episodes(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    capture_rss_write: dict[str, bytes],
):
    """Test that channel pubDate is not set when there are no episodes."""
    feed_id = TEST_FEED_ID
    mock_download_db.get_downloads_by_status.return_value = []  # No episodes

    await rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = capture_rss_write["data"]

    root = ET.fromstring(xml_bytes)
    channel = root.find("channel")
    assert channel is not None

    # Check that pubDate is not set when there are no episodes
    pubdate = channel.find("pubDate")
    # When no episodes, feedgen does not set pubDate at all
    assert pubdate is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_manual_feed_uses_discovered_at_for_pubdate(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    capture_rss_write: dict[str, bytes],
):
    """Test that manual feeds use discovered_at instead of published for pubDate."""
    feed_id = TEST_FEED_ID
    test_feed.source_type = SourceType.MANUAL

    older_published = datetime(2022, 6, 15, 12, 0, 0, tzinfo=UTC)
    newer_discovered = datetime(2024, 1, 20, 10, 0, 0, tzinfo=UTC)

    downloads = [
        Download(
            feed_id=TEST_FEED_ID,
            id="old_video",
            source_url="https://youtube.com/watch?v=old_video",
            title="Old Video Added Recently",
            published=older_published,
            ext="mp4",
            mime_type="video/mp4",
            filesize=1048576,
            duration=300,
            status=DownloadStatus.DOWNLOADED,
            discovered_at=newer_discovered,
            updated_at=newer_discovered,
        ),
    ]

    mock_download_db.get_downloads_by_status.return_value = downloads

    await rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = capture_rss_write["data"]

    root = ET.fromstring(xml_bytes)
    channel = root.find("channel")
    assert channel is not None

    channel_pubdate = channel.find("pubDate")
    assert channel_pubdate is not None and channel_pubdate.text is not None
    assert "20 Jan 2024" in channel_pubdate.text

    items = channel.findall("item")
    assert len(items) == 1
    item_pubdate = items[0].find("pubDate")
    assert item_pubdate is not None and item_pubdate.text is not None
    assert "20 Jan 2024" in item_pubdate.text


# --- Tests for podcast:transcript elements ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_transcript_element_included_when_metadata_present(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    sample_downloads: list[Download],
    capture_rss_write: dict[str, bytes],
):
    """Test that podcast:transcript element is generated when download has transcript metadata."""
    feed_id = TEST_FEED_ID
    # Add transcript metadata to first download
    sample_downloads[0].transcript_ext = "vtt"
    sample_downloads[0].transcript_lang = "en"

    mock_download_db.get_downloads_by_status.return_value = sample_downloads

    await rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = capture_rss_write["data"]

    root = ET.fromstring(xml_bytes)
    channel = root.find("channel")
    assert channel is not None

    podcast_ns = {"podcast": "https://podcastindex.org/namespace/1.0"}
    items = channel.findall("item")
    assert len(items) == 2

    # First item should have transcript element
    first_item = items[0]
    transcript_elem = first_item.find("podcast:transcript", podcast_ns)
    assert transcript_elem is not None

    # Verify attributes
    expected_url = f"{TEST_BASE_URL}/transcripts/{TEST_FEED_ID}/video1.en.vtt"
    assert transcript_elem.get("url") == expected_url
    assert transcript_elem.get("type") == "text/vtt"
    assert transcript_elem.get("language") == "en"
    assert transcript_elem.get("rel") == "captions"

    # Second item should not have transcript element
    second_item = items[1]
    second_transcript = second_item.find("podcast:transcript", podcast_ns)
    assert second_transcript is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_transcript_mime_type_for_srt_format(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    capture_rss_write: dict[str, bytes],
):
    """Test that SRT transcript files get correct MIME type."""
    feed_id = TEST_FEED_ID
    downloads_with_srt = [
        Download(
            feed_id=TEST_FEED_ID,
            id="video_with_srt",
            source_url="https://youtube.com/watch?v=video_with_srt",
            title="Video with SRT Transcript",
            published=datetime(2023, 1, 1, tzinfo=UTC),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1000000,
            duration=300,
            status=DownloadStatus.DOWNLOADED,
            discovered_at=datetime(2023, 1, 2, tzinfo=UTC),
            updated_at=datetime(2023, 1, 2, tzinfo=UTC),
            transcript_ext="srt",
            transcript_lang="en",
        ),
    ]

    mock_download_db.get_downloads_by_status.return_value = downloads_with_srt

    await rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = capture_rss_write["data"]

    root = ET.fromstring(xml_bytes)
    channel = root.find("channel")
    assert channel is not None

    podcast_ns = {"podcast": "https://podcastindex.org/namespace/1.0"}
    items = channel.findall("item")
    assert len(items) == 1

    item = items[0]
    transcript_elem = item.find("podcast:transcript", podcast_ns)
    assert transcript_elem is not None
    assert transcript_elem.get("type") == "application/x-subrip"
    assert (
        transcript_elem.get("url")
        == f"{TEST_BASE_URL}/transcripts/{TEST_FEED_ID}/video_with_srt.en.srt"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_transcript_with_different_language_codes(
    rss_generator: RSSFeedGenerator,
    mock_download_db: MagicMock,
    test_feed: Feed,
    capture_rss_write: dict[str, bytes],
):
    """Test that transcript language codes are correctly set in RSS."""
    feed_id = TEST_FEED_ID
    downloads_multilang = [
        Download(
            feed_id=TEST_FEED_ID,
            id="video_en",
            source_url="https://youtube.com/watch?v=video_en",
            title="English Video",
            published=datetime(2023, 1, 3, tzinfo=UTC),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1000000,
            duration=300,
            status=DownloadStatus.DOWNLOADED,
            discovered_at=datetime(2023, 1, 4, tzinfo=UTC),
            updated_at=datetime(2023, 1, 4, tzinfo=UTC),
            transcript_ext="vtt",
            transcript_lang="en",
        ),
        Download(
            feed_id=TEST_FEED_ID,
            id="video_es",
            source_url="https://youtube.com/watch?v=video_es",
            title="Spanish Video",
            published=datetime(2023, 1, 2, tzinfo=UTC),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1000000,
            duration=300,
            status=DownloadStatus.DOWNLOADED,
            discovered_at=datetime(2023, 1, 3, tzinfo=UTC),
            updated_at=datetime(2023, 1, 3, tzinfo=UTC),
            transcript_ext="vtt",
            transcript_lang="es",
        ),
        Download(
            feed_id=TEST_FEED_ID,
            id="video_ja",
            source_url="https://youtube.com/watch?v=video_ja",
            title="Japanese Video",
            published=datetime(2023, 1, 1, tzinfo=UTC),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1000000,
            duration=300,
            status=DownloadStatus.DOWNLOADED,
            discovered_at=datetime(2023, 1, 2, tzinfo=UTC),
            updated_at=datetime(2023, 1, 2, tzinfo=UTC),
            transcript_ext="vtt",
            transcript_lang="ja",
        ),
    ]

    mock_download_db.get_downloads_by_status.return_value = downloads_multilang

    await rss_generator.update_feed(feed_id, test_feed)
    xml_bytes = capture_rss_write["data"]

    root = ET.fromstring(xml_bytes)
    channel = root.find("channel")
    assert channel is not None

    podcast_ns = {"podcast": "https://podcastindex.org/namespace/1.0"}
    items = channel.findall("item")
    assert len(items) == 3

    # Verify each item has correct language code
    # Items are sorted by published date descending (video_en, video_es, video_ja)
    en_item = items[0]
    en_transcript = en_item.find("podcast:transcript", podcast_ns)
    assert en_transcript is not None
    assert en_transcript.get("language") == "en"
    assert (
        en_transcript.get("url")
        == f"{TEST_BASE_URL}/transcripts/{TEST_FEED_ID}/video_en.en.vtt"
    )

    es_item = items[1]
    es_transcript = es_item.find("podcast:transcript", podcast_ns)
    assert es_transcript is not None
    assert es_transcript.get("language") == "es"
    assert (
        es_transcript.get("url")
        == f"{TEST_BASE_URL}/transcripts/{TEST_FEED_ID}/video_es.es.vtt"
    )

    ja_item = items[2]
    ja_transcript = ja_item.find("podcast:transcript", podcast_ns)
    assert ja_transcript is not None
    assert ja_transcript.get("language") == "ja"
    assert (
        ja_transcript.get("url")
        == f"{TEST_BASE_URL}/transcripts/{TEST_FEED_ID}/video_ja.ja.vtt"
    )
