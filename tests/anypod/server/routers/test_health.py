# pyright: reportPrivateUsage=false

"""Tests for the health check router."""

from unittest.mock import AsyncMock, Mock

from fastapi import FastAPI
from helpers.test_client import ClientProtocol, create_test_client
import pytest

from anypod.db import FeedDatabase
from anypod.exceptions import DatabaseOperationError
from anypod.server.routers.health import router


@pytest.fixture
def mock_feed_db() -> Mock:
    """Create a healthy mock FeedDatabase."""
    db = Mock(spec=FeedDatabase)
    db.get_feeds = AsyncMock(return_value=[])
    return db


@pytest.fixture
def app(mock_feed_db: Mock) -> FastAPI:
    """Create a FastAPI app with the health router and attached FeedDatabase."""
    app = FastAPI()
    app.include_router(router)
    app.state.feed_database = mock_feed_db
    return app


@pytest.fixture
def client(app: FastAPI) -> ClientProtocol:
    """Create a test client for the health router."""
    return create_test_client(app)


# --- Tests for health endpoint ---


def test_health_check_success(client: ClientProtocol):
    """Test that health check returns 200 with correct structure."""
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"

    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "anypod"
    assert data["version"] == "0.1.0"
    assert "timestamp" in data


def test_health_check_returns_503_when_db_unhealthy(
    app: FastAPI, mock_feed_db: Mock
) -> None:
    """Health check verifies DB connectivity and returns 503 when DB fails."""
    mock_feed_db.get_feeds = AsyncMock(side_effect=DatabaseOperationError("DB down"))

    client = create_test_client(app)
    response = client.get("/api/health")

    assert response.status_code == 503
    assert response.json()["status"] == "unhealthy"
