# pyright: reportPrivateUsage=false

"""Tests for the admin router endpoints."""

from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.types import DownloadStatus
from anypod.exceptions import DatabaseOperationError, FeedNotFoundError
from anypod.server.routers.admin import router

# Shared test constants
ADMIN_PREFIX = "/admin"
FEED_ID = "test_feed"
RESET_COUNT = 3


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
    mock_feed_database: Mock,
    mock_download_database: Mock,
) -> FastAPI:
    """Create a FastAPI app with the admin router and mocked dependencies."""
    app = FastAPI()
    app.include_router(router)

    # Attach mocked dependencies to app state
    app.state.feed_database = mock_feed_database
    app.state.download_database = mock_download_database

    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    """Create a test client for the admin router."""
    return TestClient(app)


@pytest.mark.unit
def test_reset_errors_success(
    client: TestClient,
    mock_feed_database: Mock,
    mock_download_database: Mock,
) -> None:
    """Returns feed_id + reset_count; calls bulk requeue for ERROR items only."""
    mock_feed_database.get_feed_by_id.return_value = object()
    mock_download_database.requeue_downloads.return_value = RESET_COUNT

    response = client.post(f"{ADMIN_PREFIX}/feeds/{FEED_ID}/reset-errors")

    assert response.status_code == 200
    data = response.json()
    assert data == {"feed_id": FEED_ID, "reset_count": RESET_COUNT}
    mock_feed_database.get_feed_by_id.assert_called_once_with(FEED_ID)
    mock_download_database.requeue_downloads.assert_called_once_with(
        feed_id=FEED_ID, download_ids=None, from_status=DownloadStatus.ERROR
    )


@pytest.mark.unit
def test_reset_errors_feed_not_found(
    client: TestClient,
    mock_feed_database: Mock,
) -> None:
    """404 when feed does not exist."""
    missing_feed_id = "missing"
    mock_feed_database.get_feed_by_id.side_effect = FeedNotFoundError(
        "Feed not found.", feed_id=missing_feed_id
    )

    response = client.post(f"{ADMIN_PREFIX}/feeds/{missing_feed_id}/reset-errors")

    assert response.status_code == 404
    assert response.json()["detail"] == "Feed not found"


@pytest.mark.unit
def test_reset_errors_db_error_on_requeue(
    client: TestClient,
    mock_feed_database: Mock,
    mock_download_database: Mock,
) -> None:
    """500 when DB error occurs during bulk requeue."""
    mock_feed_database.get_feed_by_id.return_value = object()
    mock_download_database.requeue_downloads.side_effect = DatabaseOperationError(
        "DB error"
    )

    response = client.post(f"{ADMIN_PREFIX}/feeds/{FEED_ID}/reset-errors")

    assert response.status_code == 500
    assert response.json()["detail"] == "Database error"
