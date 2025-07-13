# pyright: reportPrivateUsage=false

"""Tests for the static file serving router."""

from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.exceptions import (
    DatabaseOperationError,
    FileOperationError,
    RSSGenerationError,
)
from anypod.file_manager import FileManager
from anypod.rss import RSSFeedGenerator
from anypod.server.routers.static import router


class DirectoryListingParser(HTMLParser):
    """HTML parser to extract links from directory listing pages."""

    def __init__(self):
        super().__init__()
        self.links: list[tuple[str, str]] = []  # (href, text) pairs
        self.title = ""
        self._current_tag = ""
        self._current_href = ""
        self._collecting_text = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Handle opening HTML tags."""
        self._current_tag = tag
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value is not None:
                    self._current_href = value
                    self._collecting_text = True
        elif tag == "title":
            self._collecting_text = True

    def handle_endtag(self, tag: str) -> None:
        """Handle closing HTML tags."""
        if tag == "a" and self._current_href:
            self._collecting_text = False
            self._current_href = ""
        elif tag == "title":
            self._collecting_text = False
        self._current_tag = ""

    def handle_data(self, data: str) -> None:
        """Handle text content within HTML tags."""
        if self._collecting_text:
            if self._current_tag == "a" and self._current_href:
                self.links.append((self._current_href, data.strip()))
            elif self._current_tag == "title":
                self.title = data.strip()


def parse_directory_listing(html_content: str) -> DirectoryListingParser:
    """Parse HTML directory listing and return structured data."""
    parser = DirectoryListingParser()
    parser.feed(html_content)
    return parser


@pytest.fixture
def mock_file_manager() -> Mock:
    """Create a mock FileManager for testing."""
    return Mock(spec=FileManager)


@pytest.fixture
def mock_rss_generator() -> Mock:
    """Create a mock RSSFeedGenerator for testing."""
    return Mock(spec=RSSFeedGenerator)


@pytest.fixture
def mock_feed_database() -> Mock:
    """Create a mock FeedDatabase for testing."""
    return Mock(spec=FeedDatabase)


@pytest.fixture
def mock_download_database() -> Mock:
    """Create a mock DownloadDatabase for testing."""
    return Mock(spec=DownloadDatabase)


@pytest.fixture
def app(
    mock_file_manager: Mock,
    mock_rss_generator: Mock,
    mock_feed_database: Mock,
    mock_download_database: Mock,
) -> FastAPI:
    """Create a FastAPI app with the static router and mocked dependencies."""
    app = FastAPI()
    app.include_router(router)

    # Attach mocked dependencies to app state
    app.state.file_manager = mock_file_manager
    app.state.rss_generator = mock_rss_generator
    app.state.feed_database = mock_feed_database
    app.state.download_database = mock_download_database

    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    """Create a test client for the static router."""
    return TestClient(app)


# --- Tests for RSS feed endpoint ---


@pytest.mark.unit
def test_serve_feed_success(client: TestClient, mock_rss_generator: Mock):
    """Test successful RSS feed serving."""
    # Mock RSS generator to return XML bytes
    mock_rss_generator.get_feed_xml.return_value = b'<?xml version="1.0"?><rss></rss>'

    response = client.get("/feeds/test_feed.xml")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/rss+xml"
    assert "Cache-Control" in response.headers
    assert response.content == b'<?xml version="1.0"?><rss></rss>'

    mock_rss_generator.get_feed_xml.assert_called_once_with("test_feed")


@pytest.mark.unit
def test_serve_feed_not_found(client: TestClient, mock_rss_generator: Mock):
    """Test RSS feed serving when RSS generation fails."""
    mock_rss_generator.get_feed_xml.side_effect = RSSGenerationError("Feed not found")

    response = client.get("/feeds/nonexistent_feed.xml")

    assert response.status_code == 404
    assert response.json()["detail"] == "Feed not found"


# --- Tests for media file endpoint ---


@pytest.mark.unit
def test_serve_media_success(
    client: TestClient, mock_file_manager: Mock, tmp_path: Path
):
    """Test successful media file serving."""
    # Create a real test file
    test_content = b"fake video content"
    test_file_path = tmp_path / "test_video.mp4"
    test_file_path.write_bytes(test_content)
    mock_file_manager.get_download_file_path.return_value = test_file_path

    response = client.get("/media/test_feed/test_video.mp4")

    assert response.status_code == 200
    assert response.headers["content-type"] == "video/mp4"
    assert "cache-control" in response.headers
    assert response.headers["cache-control"] == "public, max-age=86400"
    assert response.content == test_content

    mock_file_manager.get_download_file_path.assert_called_once_with(
        "test_feed", "test_video", "mp4"
    )


@pytest.mark.unit
def test_serve_media_file_not_found(client: TestClient, mock_file_manager: Mock):
    """Test media file serving when file doesn't exist."""
    mock_file_manager.get_download_file_path.side_effect = FileNotFoundError(
        "File not found"
    )

    response = client.get("/media/test_feed/nonexistent.mp4")

    assert response.status_code == 404
    assert response.json()["detail"] == "File not found"


@pytest.mark.unit
def test_serve_media_file_operation_error(client: TestClient, mock_file_manager: Mock):
    """Test media file serving when file operation fails."""
    mock_file_manager.get_download_file_path.side_effect = FileOperationError(
        "File operation failed"
    )

    response = client.get("/media/test_feed/error_file.mp4")

    assert response.status_code == 500
    assert response.json()["detail"] == "Internal server error"


@pytest.mark.unit
@pytest.mark.parametrize(
    "filename,ext,expected_content_type",
    [
        ("audio", "m4a", "audio/mp4"),  # Common podcast audio format
        ("video", "mp4", "video/mp4"),  # Common podcast video format
        ("audio", "mp3", "audio/mpeg"),  # Legacy podcast audio format
    ],
)
def test_serve_media_content_type_guessing(
    client: TestClient,
    mock_file_manager: Mock,
    tmp_path: Path,
    filename: str,
    ext: str,
    expected_content_type: str,
):
    """Test that media content type is correctly guessed from extension."""
    # Create a real test file
    test_content = b"fake content"
    test_file_path = tmp_path / f"{filename}.{ext}"
    test_file_path.write_bytes(test_content)
    mock_file_manager.get_download_file_path.return_value = test_file_path

    response = client.get(f"/media/test_feed/{filename}.{ext}")

    assert response.status_code == 200
    assert response.headers["content-type"] == expected_content_type
    assert response.content == test_content

    # Verify the correct parameters were passed
    mock_file_manager.get_download_file_path.assert_called_once_with(
        "test_feed", filename, ext
    )


# --- Tests for feed browser endpoint ---


@pytest.mark.unit
def test_browse_feeds_success(client: TestClient, mock_feed_database: Mock):
    """Test successful feed directory browsing."""
    # Mock feed database to return test feeds
    mock_feeds = [
        Feed(
            id="feed1",
            is_enabled=True,
            source_type=SourceType.CHANNEL,
            source_url="https://example.com/feed1",
            last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        ),
        Feed(
            id="feed2",
            is_enabled=True,
            source_type=SourceType.CHANNEL,
            source_url="https://example.com/feed2",
            last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        ),
    ]
    mock_feed_database.get_feeds.return_value = mock_feeds

    response = client.get("/feeds")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/html; charset=utf-8"

    # Parse HTML semantically
    parsed = parse_directory_listing(response.text)

    # Check page title
    assert parsed.title == "Index of /feeds"

    # Extract all links
    links_by_href = dict(parsed.links)

    # Should NOT have parent directory link for top-level directory
    assert "../" not in links_by_href

    # Should have feed links
    assert "/feeds/feed1.xml" in links_by_href
    assert "/feeds/feed2.xml" in links_by_href
    assert links_by_href["/feeds/feed1.xml"] == "feed1.xml"
    assert links_by_href["/feeds/feed2.xml"] == "feed2.xml"

    # Should have exactly 2 links (2 feeds, no parent)
    assert len(parsed.links) == 2

    mock_feed_database.get_feeds.assert_called_once_with(enabled=True)


@pytest.mark.unit
def test_browse_feeds_empty(client: TestClient, mock_feed_database: Mock):
    """Test feed directory browsing with no feeds."""
    mock_feed_database.get_feeds.return_value = []

    response = client.get("/feeds")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/html; charset=utf-8"

    # Parse HTML semantically
    parsed = parse_directory_listing(response.text)

    # Check page title
    assert parsed.title == "Index of /feeds"

    # Should have no links (no parent link for top-level, no feeds)
    assert len(parsed.links) == 0


@pytest.mark.unit
def test_browse_feeds_database_error(client: TestClient, mock_feed_database: Mock):
    """Test feed directory browsing when database fails."""
    mock_feed_database.get_feeds.side_effect = DatabaseOperationError("Database error")

    response = client.get("/feeds")

    assert response.status_code == 500
    assert response.json()["detail"] == "Internal server error"


# --- Tests for media browser endpoint ---


@pytest.mark.unit
def test_browse_media_success(client: TestClient, mock_feed_database: Mock):
    """Test successful media directory browsing."""
    from datetime import UTC, datetime

    # Mock feed database to return test feeds
    mock_feeds = [
        Feed(
            id="podcast1",
            is_enabled=True,
            source_type=SourceType.CHANNEL,
            source_url="https://example.com/podcast1",
            last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        ),
        Feed(
            id="podcast2",
            is_enabled=True,
            source_type=SourceType.CHANNEL,
            source_url="https://example.com/podcast2",
            last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        ),
    ]
    mock_feed_database.get_feeds.return_value = mock_feeds

    response = client.get("/media")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/html; charset=utf-8"

    # Parse HTML semantically
    parsed = parse_directory_listing(response.text)

    # Check page title
    assert parsed.title == "Index of /media"

    # Extract all links
    links_by_href = dict(parsed.links)

    # Should NOT have parent directory link for top-level directory
    assert "../" not in links_by_href

    # Should have feed directory links
    assert "/media/podcast1/" in links_by_href
    assert "/media/podcast2/" in links_by_href
    assert links_by_href["/media/podcast1/"] == "podcast1/"
    assert links_by_href["/media/podcast2/"] == "podcast2/"

    # Should have exactly 2 links (2 feeds, no parent)
    assert len(parsed.links) == 2

    mock_feed_database.get_feeds.assert_called_once_with(enabled=True)


@pytest.mark.unit
def test_browse_media_empty(client: TestClient, mock_feed_database: Mock):
    """Test media directory browsing with no feeds."""
    mock_feed_database.get_feeds.return_value = []

    response = client.get("/media")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/html; charset=utf-8"

    # Parse HTML semantically
    parsed = parse_directory_listing(response.text)

    # Check page title
    assert parsed.title == "Index of /media"

    # Should have no links (no parent link for top-level, no feeds)
    assert len(parsed.links) == 0


# --- Tests for media feed browser endpoint ---


@pytest.mark.unit
def test_browse_media_feed_success(client: TestClient, mock_download_database: Mock):
    """Test successful media feed directory browsing."""
    from datetime import UTC, datetime

    # Mock download database to return test downloads
    mock_downloads = [
        Download(
            feed_id="test_feed",
            id="video1",
            source_url="https://example.com/video1",
            title="Test Video 1",
            published=datetime(2024, 1, 1, tzinfo=UTC),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1000000,
            duration=600,
            status=DownloadStatus.DOWNLOADED,
            discovered_at=datetime(2024, 1, 1, tzinfo=UTC),
            updated_at=datetime(2024, 1, 1, tzinfo=UTC),
        ),
        Download(
            feed_id="test_feed",
            id="audio1",
            source_url="https://example.com/audio1",
            title="Test Audio 1",
            published=datetime(2024, 1, 2, tzinfo=UTC),
            ext="m4a",
            mime_type="audio/mp4",
            filesize=500000,
            duration=300,
            status=DownloadStatus.DOWNLOADED,
            discovered_at=datetime(2024, 1, 2, tzinfo=UTC),
            updated_at=datetime(2024, 1, 2, tzinfo=UTC),
        ),
    ]
    mock_download_database.get_downloads_by_status.return_value = mock_downloads

    response = client.get("/media/test_feed")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/html; charset=utf-8"

    # Parse HTML semantically
    parsed = parse_directory_listing(response.text)

    # Check page title
    assert parsed.title == "Index of /media/test_feed"

    # Extract all links
    links_by_href = dict(parsed.links)

    # Should have parent directory link pointing to /media
    assert "/media" in links_by_href
    assert links_by_href["/media"] == "../"

    # Should have media file links
    assert "/media/test_feed/video1.mp4" in links_by_href
    assert "/media/test_feed/audio1.m4a" in links_by_href
    assert links_by_href["/media/test_feed/video1.mp4"] == "video1.mp4"
    assert links_by_href["/media/test_feed/audio1.m4a"] == "audio1.m4a"

    # Should have exactly 3 links (parent + 2 media files)
    assert len(parsed.links) == 3

    mock_download_database.get_downloads_by_status.assert_called_once_with(
        DownloadStatus.DOWNLOADED, feed_id="test_feed"
    )


@pytest.mark.unit
def test_browse_media_feed_empty(client: TestClient, mock_download_database: Mock):
    """Test media feed directory browsing with no downloads."""
    mock_download_database.get_downloads_by_status.return_value = []

    response = client.get("/media/empty_feed")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/html; charset=utf-8"

    # Parse HTML semantically
    parsed = parse_directory_listing(response.text)

    # Check page title
    assert parsed.title == "Index of /media/empty_feed"

    # Extract all links
    links_by_href = dict(parsed.links)

    # Should have parent directory link pointing to /media
    assert "/media" in links_by_href
    assert links_by_href["/media"] == "../"

    # Should have exactly 1 link (just parent, no media files)
    assert len(parsed.links) == 1


@pytest.mark.unit
def test_browse_media_feed_database_error(
    client: TestClient, mock_download_database: Mock
):
    """Test media feed directory browsing when database fails."""
    mock_download_database.get_downloads_by_status.side_effect = DatabaseOperationError(
        "Database error"
    )

    response = client.get("/media/error_feed")

    assert response.status_code == 500
    assert response.json()["detail"] == "Internal server error"


@pytest.mark.unit
def test_browse_media_feed_html_escaping(
    client: TestClient, mock_download_database: Mock
):
    """Test that HTML content is properly escaped in directory listings."""
    from datetime import UTC, datetime

    # Create download with special characters that need escaping
    mock_downloads = [
        Download(
            feed_id="test_feed",
            id="video<script>alert('xss')</script>",
            source_url="https://example.com/video1",
            title="Test Video with <special> chars & symbols",
            published=datetime(2024, 1, 1, tzinfo=UTC),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1000000,
            duration=600,
            status=DownloadStatus.DOWNLOADED,
            discovered_at=datetime(2024, 1, 1, tzinfo=UTC),
            updated_at=datetime(2024, 1, 1, tzinfo=UTC),
        ),
    ]
    mock_download_database.get_downloads_by_status.return_value = mock_downloads

    response = client.get("/media/test_feed")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/html; charset=utf-8"

    # Parse HTML semantically
    parsed = parse_directory_listing(response.text)

    # Check page title
    assert parsed.title == "Index of /media/test_feed"

    # Verify HTML escaping in the raw HTML (before parsing)
    raw_html = response.text
    assert "&lt;script&gt;" in raw_html  # Script tags should be escaped in raw HTML
    assert "alert(&#x27;xss&#x27;)" in raw_html  # Single quotes should be escaped

    # The parser correctly unescapes HTML entities, so parsed text contains original characters
    link_texts = [text for _, text in parsed.links]
    assert "video<script>alert('xss')</script>.mp4" in link_texts


# --- Security Tests ---


@pytest.mark.unit
@pytest.mark.parametrize(
    "feed_id",
    [
        "valid_feed",  # Valid but feed not found
        "feed123",
        "feed_name",
        "feed-name",
        "a",  # Minimum length
        "a" * 255,  # Maximum length
    ],
)
def test_serve_feed_valid_ids_reach_handler(
    client: TestClient, mock_rss_generator: Mock, feed_id: str
):
    """Test that valid feed IDs pass validation and reach the RSS generator."""
    # Valid IDs should reach the RSS generator
    mock_rss_generator.get_feed_xml.side_effect = RSSGenerationError("Feed not found")

    response = client.get(f"/feeds/{feed_id}.xml")
    assert response.status_code == 404  # Feed not found

    # Should have called the RSS generator
    mock_rss_generator.get_feed_xml.assert_called_once_with(feed_id)


@pytest.mark.unit
@pytest.mark.parametrize(
    "feed_id,expected_status",
    [
        # Invalid feed IDs (FastAPI returns 422 for validation errors)
        ("feed$", 422),
        ("feed<script>", 422),
        ("feed with spaces", 422),
        ("feed.with.dots", 422),
        ("a" * 256, 422),  # Too long
        # Path traversal attempts (routing returns 404)
        ("../../../etc/passwd", 404),  # Contains slashes, routing returns 404
        (
            "feed/../../../etc",
            404,
        ),  # Contains slashes, routing resolves to different path
        ("feed%2e%2e%2f", 404),  # URL encoded ../, routing sees as different path
    ],
)
def test_serve_feed_invalid_ids_rejected(
    client: TestClient, mock_rss_generator: Mock, feed_id: str, expected_status: int
):
    """Test that invalid feed IDs are rejected before reaching the RSS generator."""
    # Invalid IDs should not reach the RSS generator
    mock_rss_generator.get_feed_xml.side_effect = Exception("Should not be called")

    response = client.get(f"/feeds/{feed_id}.xml")
    assert response.status_code == expected_status

    # Should not have called the RSS generator
    mock_rss_generator.get_feed_xml.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize(
    "feed_id,expected_status",
    [
        # Valid feed IDs
        ("valid_feed", 200),
        ("feed123", 200),
        ("feed-name", 200),
        # Basic security cases
        ("../../../etc/passwd", 404),  # Contains slashes, routing returns 404
        ("feed$", 422),  # Invalid characters
        ("feed with spaces", 422),  # Spaces not allowed
    ],
)
def test_browse_media_feed_validation(
    client: TestClient, mock_download_database: Mock, feed_id: str, expected_status: int
):
    """Test media browse endpoint validation for feed IDs."""
    mock_download_database.get_downloads_by_status.return_value = []
    response = client.get(f"/media/{feed_id}")
    assert response.status_code == expected_status


@pytest.mark.unit
@pytest.mark.parametrize(
    "filename,expected_status",
    [
        # Valid filenames
        ("valid_file", 404),  # Valid but file not found
        ("file.with.dots", 404),  # Dots allowed in filename
        # Basic security cases
        ("../../../etc/passwd", 404),  # Contains slashes, routing returns 404
        ("file$", 422),  # Invalid characters
        (".", 400),  # Path traversal attempt
        ("..", 400),  # Path traversal attempt
    ],
)
def test_serve_media_filename_validation(
    client: TestClient, mock_file_manager: Mock, filename: str, expected_status: int
):
    """Test media serve endpoint filename validation."""
    if expected_status == 404:
        mock_file_manager.get_download_file_path.side_effect = FileNotFoundError(
            "File not found"
        )

    response = client.get(f"/media/valid_feed/{filename}.mp4")
    assert response.status_code == expected_status


@pytest.mark.unit
@pytest.mark.parametrize(
    "ext,expected_status",
    [
        # Valid extensions
        ("mp4", 404),  # Valid but file not found
        ("m4a", 404),
        # Basic security cases
        ("../../../etc", 200),  # Path traversal - routing redirects to browse route
        ("mp4$", 422),  # Invalid characters
    ],
)
def test_serve_media_extension_validation(
    client: TestClient,
    mock_file_manager: Mock,
    mock_download_database: Mock,
    ext: str,
    expected_status: int,
):
    """Test media serve endpoint extension validation."""
    if expected_status in (404, 200):
        # For file not found (normal case) and path traversal that gets routed to browse
        mock_file_manager.get_download_file_path.side_effect = FileNotFoundError(
            "File not found"
        )
        mock_download_database.get_downloads_by_status.return_value = []

    response = client.get(f"/media/valid_feed/valid_filename.{ext}")
    assert response.status_code == expected_status


@pytest.mark.unit
def test_serve_media_path_traversal_basic(client: TestClient):
    """Test basic path traversal protection."""
    # FastAPI routing should reject these before they reach our handlers
    response = client.get("/media/../../../etc/passwd/file.mp4")
    assert response.status_code in [422, 404]


@pytest.mark.unit
def test_serve_media_security_validation(client: TestClient, mock_file_manager: Mock):
    """Test that security validation prevents path traversal."""
    # Mock should never be called for malicious input
    mock_file_manager.get_download_file_path.side_effect = Exception(
        "Should not be called"
    )

    # Test the dependency validation catches . and ..
    response = client.get("/media/valid_feed/...mp4")
    assert response.status_code == 400
    mock_file_manager.get_download_file_path.assert_not_called()
