"""Admin endpoints for maintenance operations (private/local-only).

This router exposes administration endpoints intended for trusted access only.
It should be served from a separate FastAPI app bound to a private interface
or port, and not exposed on the public internet.
"""

import logging
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ...db.types import DownloadStatus
from ...exceptions import (
    DatabaseOperationError,
    DownloadNotFoundError,
    FeedNotFoundError,
    ManualSubmissionError,
    ManualSubmissionUnavailableError,
)
from ..dependencies import (
    CookiesPathDep,
    DownloadDatabaseDep,
    FeedConfigsDep,
    FeedDatabaseDep,
    ManualFeedRunnerDep,
    ManualSubmissionServiceDep,
)
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


class ManualDownloadRequest(BaseModel):
    """Request schema for manual video submissions."""

    url: str = Field(
        ...,
        min_length=1,
        description="Video URL supported by yt-dlp.",
    )


class ManualDownloadResponse(BaseModel):
    """Response schema for manual video submissions."""

    feed_id: str
    download_id: str
    status: DownloadStatus
    new: bool
    message: str


@router.post("/feeds/{feed_id}/downloads", response_model=ManualDownloadResponse)
async def submit_manual_download(
    feed_id: ValidatedFeedId,
    payload: ManualDownloadRequest,
    feed_db: FeedDatabaseDep,
    download_db: DownloadDatabaseDep,
    feed_configs: FeedConfigsDep,
    manual_submission_service: ManualSubmissionServiceDep,
    manual_feed_runner: ManualFeedRunnerDep,
    cookies_path: CookiesPathDep,
) -> ManualDownloadResponse:
    """Accept a single video URL for manual processing."""
    log_params: dict[str, Any] = {"feed_id": feed_id, "url": payload.url}
    feed_config = feed_configs.get(feed_id)
    if feed_config is None:
        raise HTTPException(status_code=404, detail="Feed not configured")
    if not feed_config.enabled:
        raise HTTPException(status_code=400, detail="Feed is disabled")
    if not feed_config.is_manual:
        raise HTTPException(
            status_code=400, detail="Feed does not accept manual submissions"
        )

    try:
        await feed_db.get_feed_by_id(feed_id)
    except FeedNotFoundError as e:
        raise HTTPException(status_code=404, detail="Feed not found") from e
    except DatabaseOperationError as e:
        raise HTTPException(status_code=500, detail="Database error") from e

    try:
        download = await manual_submission_service.fetch_submission_download(
            feed_id=feed_id,
            feed_config=feed_config,
            url=payload.url,
            cookies_path=cookies_path,
        )
    except ManualSubmissionUnavailableError as e:
        raise HTTPException(
            status_code=422,
            detail="The provided URL is not yet available as downloadable media. This may be a scheduled premiere or live stream that hasn't started yet.",
        ) from e
    except ManualSubmissionError as e:
        raise HTTPException(
            status_code=400,
            detail="Unable to process URL. Please verify it's a valid URL supported by yt-dlp and that the content is accessible.",
        ) from e
    except DatabaseOperationError as e:
        raise HTTPException(status_code=500, detail="Database error") from e

    try:
        existing_download = await download_db.get_download_by_id(feed_id, download.id)
    except DownloadNotFoundError:
        is_new = True
        existing_status = None
    except DatabaseOperationError as e:
        raise HTTPException(status_code=500, detail="Database error") from e
    else:
        is_new = False
        existing_status = existing_download.status
        download.status = existing_status

    try:
        await download_db.upsert_download(download)
    except DatabaseOperationError as e:
        raise HTTPException(status_code=500, detail="Database error") from e

    match existing_status:
        case None:
            final_status = DownloadStatus.QUEUED
            message = "Download queued"
            should_trigger = True
            logger.info(
                "Manual submission queued.",
                extra={**log_params, "download_id": download.id},
            )
        case DownloadStatus.DOWNLOADED:
            final_status = DownloadStatus.DOWNLOADED
            message = "Download already completed"
            should_trigger = False
            logger.info(
                "Manual submission already downloaded.",
                extra={**log_params, "download_id": download.id},
            )
        case _:
            try:
                await download_db.requeue_downloads(
                    feed_id=feed_id,
                    download_ids=download.id,
                )
            except DatabaseOperationError as e:
                raise HTTPException(status_code=500, detail="Database error") from e
            else:
                final_status = DownloadStatus.QUEUED
                message = "Existing download requeued"
                should_trigger = True
                logger.info(
                    "Manual submission requeued existing download.",
                    extra={
                        **log_params,
                        "download_id": download.id,
                        "from_status": existing_status.value,
                    },
                )

    if should_trigger:
        await manual_feed_runner.trigger(feed_id, feed_config)

    return ManualDownloadResponse(
        feed_id=feed_id,
        download_id=download.id,
        status=final_status,
        new=is_new,
        message=message,
    )
