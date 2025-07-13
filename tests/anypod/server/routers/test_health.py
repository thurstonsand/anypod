# pyright: reportPrivateUsage=false

"""Tests for the health check router."""

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from anypod.server.routers.health import router


@pytest.fixture
def app() -> FastAPI:
    """Create a minimal FastAPI app with just the health router."""
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    """Create a test client for the health router."""
    return TestClient(app)


# --- Tests for health endpoint ---


@pytest.mark.unit
def test_health_check_success(client: TestClient):
    """Test that health check returns 200 with correct structure."""
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"

    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "anypod"
    assert data["version"] == "0.1.0"
    assert "timestamp" in data
