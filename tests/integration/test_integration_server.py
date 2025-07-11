"""Integration tests for HTTP server with real dependencies."""

from datetime import UTC, datetime
import xml.etree.ElementTree as ET

from fastapi.testclient import TestClient
import pytest

from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.file_manager import FileManager
from anypod.path_manager import PathManager
from anypod.rss import RSSFeedGenerator

# --- Tests for RSS feed serving integration ---


@pytest.mark.integration
@pytest.mark.asyncio
async def test_serve_feed_integration_with_cached_feed(
    test_app: TestClient,
    rss_generator: RSSFeedGenerator,
    feed_db: FeedDatabase,
):
    """Test RSS feed serving with a real cached feed."""
    feed_id = "test_feed"

    # Create a test feed in the database
    test_feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://example.com/channel",
        last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        title="Test Channel",
        description="A test channel for integration testing",
    )
    await feed_db.upsert_feed(test_feed)

    # Generate and cache feed XML
    await rss_generator.update_feed(feed_id, test_feed)

    # Test serving the cached feed
    response = test_app.get(f"/feeds/{feed_id}.xml")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/rss+xml"
    assert "Cache-Control" in response.headers

    # Parse and verify XML content structure
    xml_content = response.content.decode()
    assert "<?xml" in xml_content
    assert "<rss" in xml_content

    # Parse XML and verify specific elements
    root = ET.fromstring(xml_content)
    assert root.tag == "rss"

    # Find the channel element and verify title
    channel = root.find("channel")
    assert channel is not None

    title_elem = channel.find("title")
    assert title_elem is not None
    assert title_elem.text == "Test Channel"

    description_elem = channel.find("description")
    assert description_elem is not None
    assert description_elem.text == "A test channel for integration testing"


@pytest.mark.integration
def test_serve_feed_integration_not_cached(test_app: TestClient):
    """Test RSS feed serving when feed is not in cache."""
    response = test_app.get("/feeds/nonexistent_feed.xml")

    assert response.status_code == 404
    assert response.json()["detail"] == "Feed not found"


# --- Tests for media file serving integration ---


@pytest.mark.integration
@pytest.mark.asyncio
async def test_serve_media_integration_with_real_file(
    test_app: TestClient,
    path_manager: PathManager,
):
    """Test media file serving with a real file on disk."""
    feed_id = "test_feed"
    filename = "test_video"
    ext = "mp4"

    # Create a test media file
    media_path = await path_manager.media_file_path(feed_id, filename, ext)

    test_content = b"fake video content for testing"
    media_path.write_bytes(test_content)

    # Test serving the media file
    response = test_app.get(f"/media/{feed_id}/{filename}.{ext}")

    assert response.status_code == 200
    assert response.headers["content-type"] == "video/mp4"
    assert response.content == test_content


@pytest.mark.integration
def test_serve_media_integration_file_not_found(test_app: TestClient):
    """Test media file serving when file doesn't exist."""
    response = test_app.get("/media/test_feed/nonexistent.mp4")

    assert response.status_code == 404
    assert response.json()["detail"] == "File not found"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_serve_media_integration_with_downloads(
    test_app: TestClient,
    path_manager: PathManager,
    download_db: DownloadDatabase,
    feed_db: FeedDatabase,
):
    """Test media file serving for files created via download process."""
    feed_id = "integration_feed"
    download_id = "test_download"
    ext = "m4a"

    # Create a feed first (required for foreign key constraint)
    test_feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://example.com/channel",
        last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        title="Integration Test Feed",
    )
    await feed_db.upsert_feed(test_feed)

    # Create a download record
    test_download = Download(
        feed_id=feed_id,
        id=download_id,
        source_url="https://example.com/audio",
        title="Test Audio",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext=ext,
        mime_type="audio/mp4",
        filesize=1024,
        duration=60,
        status=DownloadStatus.DOWNLOADED,
        discovered_at=datetime(2024, 1, 1, tzinfo=UTC),
        updated_at=datetime(2024, 1, 1, tzinfo=UTC),
    )
    await download_db.upsert_download(test_download)

    # Create the corresponding media file
    media_path = await path_manager.media_file_path(feed_id, download_id, ext)

    test_audio_content = b"fake audio content"
    media_path.write_bytes(test_audio_content)

    # Test serving the media file
    response = test_app.get(f"/media/{feed_id}/{download_id}.{ext}")

    assert response.status_code == 200
    assert response.headers["content-type"] == "audio/mp4"  # Our custom mapping
    assert response.content == test_audio_content


# --- Tests for dependency injection integration ---


@pytest.mark.integration
def test_dependency_injection_integration(
    test_app: TestClient,
    file_manager: FileManager,
    rss_generator: RSSFeedGenerator,
):
    """Test that dependency injection works correctly in integrated environment."""
    # Access the app's state to verify dependencies are properly attached
    app = test_app.app

    assert hasattr(app.state, "file_manager")  # type: ignore
    assert hasattr(app.state, "rss_generator")  # type: ignore
    assert app.state.file_manager is file_manager  # type: ignore
    assert app.state.rss_generator is rss_generator  # type: ignore


# --- Tests for CORS and middleware integration ---


@pytest.mark.integration
def test_cors_middleware_integration(test_app: TestClient):
    """Test that CORS middleware is working correctly."""
    # Make a request with Origin header to trigger CORS
    response = test_app.get("/api/health", headers={"Origin": "https://example.com"})

    # Should have CORS headers
    assert response.status_code == 200
    assert "access-control-allow-origin" in response.headers
    assert response.headers["access-control-allow-origin"] == "*"
