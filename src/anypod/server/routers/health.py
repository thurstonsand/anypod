"""Health check router for Anypod HTTP server.

This module provides health check endpoints for monitoring
the status of the Anypod service.
"""

from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version as pkg_version
import logging
from typing import Literal

from fastapi import APIRouter, Response
from pydantic import BaseModel

from ...exceptions import DatabaseOperationError
from ..dependencies import FeedDatabaseDep

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api")


def _get_service_version() -> str:
    """Resolve the installed Anypod package version with a safe fallback."""
    try:
        return pkg_version("anypod")
    except PackageNotFoundError:
        return "0.1.0"


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
async def health_check(
    feed_db: FeedDatabaseDep,
    response: Response,
) -> HealthResponse:
    """Check the health status of the Anypod service.

    Verifies database reachability and returns HTTP 503 if the database check
    fails.

    Args:
        feed_db: Feed database dependency.
        response: Outgoing FastAPI response (used to set 503 on failure).

    Returns:
        Health status response.
    """
    status: Literal["healthy", "degraded", "unhealthy"] = "healthy"
    try:
        await feed_db.get_feeds(enabled=True)
    except DatabaseOperationError as e:
        logger.error("Health check database query failed.", exc_info=e)
        status = "unhealthy"
        response.status_code = 503

    return HealthResponse(
        status=status,
        timestamp=datetime.now(UTC),
        service="anypod",
        version=_get_service_version(),
    )
