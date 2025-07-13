"""Health check router for Anypod HTTP server.

This module provides health check endpoints for monitoring
the status of the Anypod service.
"""

from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(prefix="/api")


class HealthResponse(BaseModel):
    """Response model for health check endpoint.

    Attributes:
        status: Health status of the service.
        timestamp: Current server timestamp.
        service: Name of the service.
        version: Version of the service.
    """

    status: Literal["healthy", "degraded", "unhealthy"]
    timestamp: datetime
    service: str
    version: str


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Check the health status of the Anypod service.

    Returns basic health information including status and timestamp.

    Returns:
        Health status response.
    """
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(UTC),
        service="anypod",
        version="0.1.0",
    )
