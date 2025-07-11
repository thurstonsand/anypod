# pyright: reportPrivateUsage=false

"""Tests for the static file serving router."""

from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from anypod.exceptions import FileOperationError, RSSGenerationError
from anypod.file_manager import FileManager
from anypod.rss import RSSFeedGenerator
from anypod.server.routers.static import router


@pytest.fixture
def mock_file_manager() -> Mock:
    """Create a mock FileManager for testing."""
    return Mock(spec=FileManager)


@pytest.fixture
def mock_rss_generator() -> Mock:
    """Create a mock RSSFeedGenerator for testing."""
    return Mock(spec=RSSFeedGenerator)


@pytest.fixture
def app(mock_file_manager: Mock, mock_rss_generator: Mock) -> FastAPI:
    """Create a FastAPI app with the static router and mocked dependencies."""
    app = FastAPI()
    app.include_router(router)

    # Attach mocked dependencies to app state
    app.state.file_manager = mock_file_manager
    app.state.rss_generator = mock_rss_generator

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
def test_serve_media_success(client: TestClient, mock_file_manager: Mock):
    """Test successful media file serving."""

    # Mock file manager to return an async iterator
    async def mock_stream():
        yield b"chunk1"
        yield b"chunk2"

    mock_file_manager.get_download_stream.return_value = mock_stream()

    response = client.get("/media/test_feed/test_video.mp4")

    assert response.status_code == 200
    assert response.headers["content-type"] == "video/mp4"
    assert response.content == b"chunk1chunk2"

    mock_file_manager.get_download_stream.assert_called_once_with(
        "test_feed", "test_video", "mp4"
    )


@pytest.mark.unit
def test_serve_media_file_not_found(client: TestClient, mock_file_manager: Mock):
    """Test media file serving when file doesn't exist."""
    mock_file_manager.get_download_stream.side_effect = FileNotFoundError(
        "File not found"
    )

    response = client.get("/media/test_feed/nonexistent.mp4")

    assert response.status_code == 404
    assert response.json()["detail"] == "File not found"


@pytest.mark.unit
def test_serve_media_file_operation_error(client: TestClient, mock_file_manager: Mock):
    """Test media file serving when file operation fails."""
    mock_file_manager.get_download_stream.side_effect = FileOperationError(
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
    filename: str,
    ext: str,
    expected_content_type: str,
):
    """Test that media content type is correctly guessed from extension."""

    async def mock_stream():
        yield b"fake content"

    mock_file_manager.get_download_stream.return_value = mock_stream()

    response = client.get(f"/media/test_feed/{filename}.{ext}")

    assert response.status_code == 200
    assert response.headers["content-type"] == expected_content_type

    # Verify the correct parameters were passed
    mock_file_manager.get_download_stream.assert_called_once_with(
        "test_feed", filename, ext
    )
