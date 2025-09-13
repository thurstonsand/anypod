# pyright: reportPrivateUsage=false

"""Tests for the FastAPI application factory."""

from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from anypod.db.download_db import DownloadDatabase
from anypod.db.feed_db import FeedDatabase
from anypod.file_manager import FileManager
from anypod.server.app import create_app


@pytest.fixture
def mock_file_manager() -> Mock:
    """Create a mock FileManager for testing."""
    return Mock(spec=FileManager)


@pytest.fixture
def mock_feed_database() -> Mock:
    """Create a mock FeedDatabase for testing."""
    return Mock(spec=FeedDatabase)


@pytest.fixture
def mock_download_database() -> Mock:
    """Create a mock DownloadDatabase for testing."""
    return Mock(spec=DownloadDatabase)


@pytest.fixture
def app_with_mocks(
    mock_file_manager: Mock,
    mock_feed_database: Mock,
    mock_download_database: Mock,
) -> FastAPI:
    """Create a FastAPI app with mocked dependencies."""
    return create_app(
        file_manager=mock_file_manager,
        feed_database=mock_feed_database,
        download_database=mock_download_database,
    )


@pytest.fixture
def client(app_with_mocks: FastAPI) -> TestClient:
    """Create a test client for the FastAPI app."""
    return TestClient(app_with_mocks)


# --- Tests for create_app ---


@pytest.mark.unit
def test_create_app_basic_creation(
    mock_file_manager: Mock,
    mock_feed_database: Mock,
    mock_download_database: Mock,
):
    """Test that create_app returns a properly configured FastAPI instance."""
    app = create_app(
        file_manager=mock_file_manager,
        feed_database=mock_feed_database,
        download_database=mock_download_database,
    )

    assert app.title == "Anypod"
    assert app.description == "Thin yt-dlp -> podcast solution"
    assert app.version == "0.1.0"


@pytest.mark.unit
def test_create_app_dependencies_attached(
    mock_file_manager: Mock,
    mock_feed_database: Mock,
    mock_download_database: Mock,
):
    """Test that dependencies are properly attached to app state."""
    app = create_app(
        file_manager=mock_file_manager,
        feed_database=mock_feed_database,
        download_database=mock_download_database,
    )

    assert app.state.file_manager is mock_file_manager
    assert app.state.feed_database is mock_feed_database
    assert app.state.download_database is mock_download_database


@pytest.mark.unit
def test_create_app_middleware_configured(
    mock_file_manager: Mock,
    mock_feed_database: Mock,
    mock_download_database: Mock,
):
    """Test that middleware is properly configured."""
    app = create_app(
        file_manager=mock_file_manager,
        feed_database=mock_feed_database,
        download_database=mock_download_database,
    )

    # Check that custom logging middleware is configured
    logging_middleware = None
    for middleware in app.user_middleware:
        if "LoggingMiddleware" in str(middleware.cls):
            logging_middleware = middleware
            break

    assert logging_middleware is not None, (
        "Custom logging middleware should be configured"
    )
