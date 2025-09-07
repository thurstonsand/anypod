"""Admin endpoints for maintenance operations (private/local-only).

This router exposes administration endpoints intended for trusted access only.
It should be served from a separate FastAPI app bound to a private interface
or port, and not exposed on the public internet.
"""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ...db.types import DownloadStatus
from ...exceptions import DatabaseOperationError, FeedNotFoundError
from ..dependencies import DownloadDatabaseDep, FeedDatabaseDep
from ..validation import ValidatedFeedId

logger = logging.getLogger(__name__)


router = APIRouter(prefix="/admin")


class ResetErrorsResponse(BaseModel):
    """Response model for resetting ERROR downloads for a feed.

    Attributes:
        feed_id: The feed identifier.
        reset_count: Number of downloads transitioned from ERROR to QUEUED.
    """

    feed_id: str
    reset_count: int


@router.post("/feeds/{feed_id}/reset-errors", response_model=ResetErrorsResponse)
async def reset_error_downloads(
    feed_id: ValidatedFeedId,
    feed_db: FeedDatabaseDep,
    download_db: DownloadDatabaseDep,
) -> ResetErrorsResponse:
    """Reset all downloads in ERROR status for the specified feed.

    Validates the feed exists. Re-queues all downloads currently in ERROR
    for that feed. The operation is idempotent.

    Args:
        feed_id: The feed identifier (validated and sanitized).
        feed_db: Feed database dependency.
        download_db: Download database dependency.

    Returns:
        ResetErrorsResponse containing the feed_id and number of items reset.

    Raises:
        HTTPException: 404 if feed not found; 500 on database errors.
    """
    log_params = {"feed_id": feed_id}
    logger.debug("Admin reset-errors request received.", extra=log_params)

    try:
        # Validate feed exists (discard returned value; we only need existence)
        await feed_db.get_feed_by_id(feed_id)
    except FeedNotFoundError as e:
        raise HTTPException(status_code=404, detail="Feed not found") from e
    except DatabaseOperationError as e:
        raise HTTPException(status_code=500, detail="Database error") from e

    try:
        count = await download_db.requeue_downloads(
            feed_id=feed_id,
            download_ids=None,
            from_status=DownloadStatus.ERROR,
        )
    except DatabaseOperationError as e:
        raise HTTPException(status_code=500, detail="Database error") from e

    logger.info("Reset errors for feed.", extra={**log_params, "reset_count": count})
    return ResetErrorsResponse(feed_id=feed_id, reset_count=count)
