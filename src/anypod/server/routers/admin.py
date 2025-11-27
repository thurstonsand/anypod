"""Admin endpoints for maintenance operations (private/local-only).

This router exposes administration endpoints intended for trusted access only.
It should be served from a separate FastAPI app bound to a private interface
or port, and not exposed on the public internet.
"""

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel, Field

from ...db.types import Download, DownloadStatus
from ...exceptions import (
    DatabaseOperationError,
    DownloadNotFoundError,
    FeedNotFoundError,
    FileOperationError,
    ManualSubmissionError,
    ManualSubmissionUnavailableError,
    RSSGenerationError,
)
from ..dependencies import (
    CookiesPathDep,
    DataCoordinatorDep,
    DownloadDatabaseDep,
    FeedConfigsDep,
    FeedDatabaseDep,
    FileManagerDep,
    ManualFeedRunnerDep,
    ManualSubmissionServiceDep,
)
from ..validation import ValidatedFeedId

logger = logging.getLogger(__name__)


router = APIRouter(prefix="/admin")


class RefreshFeedResponse(BaseModel):
    """Response model for manually triggering feed processing.

    Attributes:
        feed_id: The feed identifier.
        message: Human-readable status message.
    """

    feed_id: str
    message: str


@router.post(
    "/feeds/{feed_id}/refresh",
    response_model=RefreshFeedResponse,
    status_code=202,
)
async def refresh_feed(
    feed_id: ValidatedFeedId,
    feed_db: FeedDatabaseDep,
    feed_configs: FeedConfigsDep,
    manual_feed_runner: ManualFeedRunnerDep,
) -> RefreshFeedResponse:
    """Manually trigger feed processing outside of its normal schedule.

    Immediately enqueues the feed for metadata fetching, downloading,
    pruning, and RSS generation. The processing runs asynchronously
    in the background. Works for both scheduled (cron) and manual feeds.

    Args:
        feed_id: The feed identifier (validated and sanitized).
        feed_db: Feed database dependency.
        feed_configs: Configured feeds keyed by identifier.
        manual_feed_runner: Background runner for triggering feed processing.

    Returns:
        RefreshFeedResponse with the feed_id and status message.

    Raises:
        HTTPException: 404 if feed not found or not configured;
            400 if feed is disabled; 500 on database errors.
    """
    log_params = {"feed_id": feed_id}
    logger.debug("Admin refresh request received.", extra=log_params)

    feed_config = feed_configs.get(feed_id)
    if feed_config is None:
        raise HTTPException(status_code=404, detail="Feed not configured")
    if not feed_config.enabled:
        raise HTTPException(status_code=400, detail="Feed is disabled")

    try:
        await feed_db.get_feed_by_id(feed_id)
    except FeedNotFoundError as e:
        raise HTTPException(status_code=404, detail="Feed not found") from e
    except DatabaseOperationError as e:
        raise HTTPException(status_code=500, detail="Database error") from e

    await manual_feed_runner.trigger(feed_id, feed_config)

    logger.info("Feed refresh triggered.", extra=log_params)
    return RefreshFeedResponse(
        feed_id=feed_id,
        message="Feed processing triggered",
    )


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


EXCLUDED_FIELD_NAMES = frozenset({"feed", "id"})
DOWNLOAD_FIELD_NAMES = frozenset(
    field_name
    for field_name in Download.model_fields
    if field_name not in EXCLUDED_FIELD_NAMES
)


class DownloadFieldsResponse(BaseModel):
    """Provide the requested fields for a download record."""

    feed_id: str
    download_id: str
    download: dict[str, Any]


@router.get(
    "/feeds/{feed_id}/downloads/{download_id}",
    response_model=DownloadFieldsResponse,
)
async def get_download_fields(
    feed_id: ValidatedFeedId,
    download_id: str,
    download_db: DownloadDatabaseDep,
    fields: str | None = Query(
        default=None,
        description=(
            "Comma-separated list of download fields to include. "
            "Defaults to all columns."
        ),
    ),
) -> DownloadFieldsResponse:
    """Retrieve selected fields for a download record."""
    if fields:
        requested_fields = [
            field.strip() for field in fields.split(",") if field.strip()
        ]
        if not requested_fields:
            raise HTTPException(status_code=400, detail="No fields specified")
    else:
        requested_fields = sorted(DOWNLOAD_FIELD_NAMES)

    invalid_fields = [
        field for field in requested_fields if field not in DOWNLOAD_FIELD_NAMES
    ]
    if invalid_fields:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported field(s) requested: {', '.join(sorted(invalid_fields))}",
        )

    try:
        download = await download_db.get_download_by_id(feed_id, download_id)
    except DownloadNotFoundError as e:
        raise HTTPException(status_code=404, detail="Download not found") from e
    except DatabaseOperationError as e:
        raise HTTPException(status_code=500, detail="Database error") from e

    filtered_data = download.model_dump(
        mode="json", include=set(requested_fields), exclude=set(EXCLUDED_FIELD_NAMES)
    )

    return DownloadFieldsResponse(
        feed_id=feed_id,
        download_id=download_id,
        download=filtered_data,
    )


@router.delete(
    "/feeds/{feed_id}/downloads/{download_id}",
    status_code=204,
)
async def delete_download(
    feed_id: ValidatedFeedId,
    download_id: str,
    feed_configs: FeedConfigsDep,
    feed_db: FeedDatabaseDep,
    download_db: DownloadDatabaseDep,
    file_manager: FileManagerDep,
    data_coordinator: DataCoordinatorDep,
) -> Response:
    """Delete a download from a manual feed with full cleanup.

    Args:
        feed_id: Feed identifier for the manual feed.
        download_id: Identifier of the download to remove.
        feed_configs: Configured feeds keyed by identifier.
        feed_db: Feed database dependency.
        download_db: Download database dependency.
        file_manager: File manager for deleting media and images.
        data_coordinator: Coordinator used to regenerate RSS after deletion.

    Returns:
        Empty 204 response on success.

    Raises:
        HTTPException: 404 for missing feed or download, 400 for non-manual feed,
            500 for filesystem or database failures, or RSS regeneration failures.
    """
    log_params = {"feed_id": feed_id, "download_id": download_id}

    feed_config = feed_configs.get(feed_id)
    if feed_config is None:
        raise HTTPException(status_code=404, detail="Feed not configured")
    if not feed_config.is_manual:
        raise HTTPException(
            status_code=400,
            detail="Download deletion is only supported for manual feeds",
        )

    try:
        await feed_db.get_feed_by_id(feed_id)
    except FeedNotFoundError as e:
        raise HTTPException(status_code=404, detail="Feed not found") from e
    except DatabaseOperationError as e:
        raise HTTPException(status_code=500, detail="Database error") from e

    try:
        download = await download_db.delete_download(feed_id, download_id)
    except DownloadNotFoundError as e:
        raise HTTPException(status_code=404, detail="Download not found") from e
    except DatabaseOperationError as e:
        raise HTTPException(status_code=500, detail="Database error") from e

    try:
        rss_result = await data_coordinator.regenerate_rss(feed_id)
    except (RSSGenerationError, FeedNotFoundError, DatabaseOperationError) as e:
        raise HTTPException(
            status_code=500, detail="Failed to regenerate RSS feed"
        ) from e
    else:
        if not rss_result.overall_success:
            raise HTTPException(status_code=500, detail="Failed to regenerate RSS feed")

    try:
        await file_manager.delete_download_file(feed_id, download.id, download.ext)
    except FileNotFoundError:
        logger.warning(
            "Download file missing during deletion; continuing.", extra=log_params
        )
    except FileOperationError as e:
        raise HTTPException(
            status_code=500, detail="Failed to delete download file"
        ) from e

    if download.thumbnail_ext:
        try:
            await file_manager.delete_image(
                feed_id, download.id, download.thumbnail_ext
            )
        except FileNotFoundError:
            logger.warning(
                "Thumbnail missing during deletion; continuing.", extra=log_params
            )
        except FileOperationError as e:
            raise HTTPException(
                status_code=500, detail="Failed to delete thumbnail"
            ) from e

    logger.info(
        "Download deleted for manual feed.",
        extra={
            **log_params,
            "thumbnail_deleted": bool(download.thumbnail_ext),
            "total_duration_seconds": rss_result.total_duration_seconds,
        },
    )
    return Response(status_code=204)
