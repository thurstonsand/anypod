"""Admin endpoints for maintenance operations (private/local-only).

This router exposes administration endpoints intended for trusted access only.
It should be served from a separate FastAPI app bound to a private interface
or port, and not exposed on the public internet.
"""

from datetime import datetime
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import AwareDatetime, BaseModel, Field

from ...db.types import Download, DownloadStatus
from ...exceptions import (
    DatabaseOperationError,
    DownloadNotFoundError,
    EnqueueError,
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


class RequeueResponse(BaseModel):
    """Response model for requeue operations.

    Attributes:
        feed_id: The feed identifier.
        download_id: The download identifier (for download-level requeue), or None.
        requeue_count: Number of downloads transitioned from ERROR to QUEUED.
    """

    feed_id: str
    download_id: str | None = None
    requeue_count: int


@router.post("/feeds/{feed_id}/requeue", response_model=RequeueResponse)
async def requeue_error_downloads(
    feed_id: ValidatedFeedId,
    feed_db: FeedDatabaseDep,
    download_db: DownloadDatabaseDep,
    feed_configs: FeedConfigsDep,
    manual_feed_runner: ManualFeedRunnerDep,
) -> RequeueResponse:
    """Requeue all downloads in ERROR status for the specified feed.

    Validates the feed exists. Re-queues all downloads currently in ERROR
    for that feed and triggers the processing pipeline. The operation is
    idempotent.

    Args:
        feed_id: The feed identifier (validated and sanitized).
        feed_db: Feed database dependency.
        download_db: Download database dependency.
        feed_configs: Configured feeds keyed by identifier.
        manual_feed_runner: Background runner for triggering feed processing.

    Returns:
        RequeueResponse containing the feed_id and number of items requeued.

    Raises:
        HTTPException: 404 if feed not found or not configured;
            400 if feed is disabled; 500 on database errors.
    """
    log_params = {"feed_id": feed_id}
    logger.debug("Admin requeue request received.", extra=log_params)

    feed_config = feed_configs.get(feed_id)
    if feed_config is None:
        raise HTTPException(status_code=404, detail="Feed not configured")
    if not feed_config.enabled:
        raise HTTPException(status_code=400, detail="Feed is disabled")

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

    # Trigger processing pipeline if any downloads were requeued
    if count > 0:
        await manual_feed_runner.trigger(feed_id, feed_config)

    logger.info(
        "Requeued errors for feed.", extra={**log_params, "requeue_count": count}
    )
    return RequeueResponse(feed_id=feed_id, requeue_count=count)


@router.post(
    "/feeds/{feed_id}/downloads/{download_id}/requeue", response_model=RequeueResponse
)
async def requeue_download(
    feed_id: ValidatedFeedId,
    download_id: str,
    feed_db: FeedDatabaseDep,
    download_db: DownloadDatabaseDep,
    feed_configs: FeedConfigsDep,
    manual_feed_runner: ManualFeedRunnerDep,
) -> RequeueResponse:
    """Requeue a single ERROR download and trigger processing.

    Validates the download exists and is in ERROR status. Re-queues the
    download and triggers the processing pipeline.

    Args:
        feed_id: The feed identifier (validated and sanitized).
        download_id: The download identifier to requeue.
        feed_db: Feed database dependency.
        download_db: Download database dependency.
        feed_configs: Configured feeds keyed by identifier.
        manual_feed_runner: Background runner for triggering feed processing.

    Returns:
        RequeueResponse with requeue_count of 1 if successful.

    Raises:
        HTTPException: 404 if feed or download not found or not configured;
            400 if feed is disabled or download is not in ERROR status;
            500 on database errors.
    """
    log_params = {"feed_id": feed_id, "download_id": download_id}
    logger.debug("Admin download requeue request received.", extra=log_params)

    feed_config = feed_configs.get(feed_id)
    if feed_config is None:
        raise HTTPException(status_code=404, detail="Feed not configured")
    if not feed_config.enabled:
        raise HTTPException(status_code=400, detail="Feed is disabled")

    try:
        # Validate feed exists
        await feed_db.get_feed_by_id(feed_id)
    except FeedNotFoundError as e:
        raise HTTPException(status_code=404, detail="Feed not found") from e
    except DatabaseOperationError as e:
        raise HTTPException(status_code=500, detail="Database error") from e

    # Validate download exists and is in ERROR status
    try:
        download = await download_db.get_download_by_id(feed_id, download_id)
    except DownloadNotFoundError as e:
        raise HTTPException(status_code=404, detail="Download not found") from e
    except DatabaseOperationError as e:
        raise HTTPException(status_code=500, detail="Database error") from e

    if download.status != DownloadStatus.ERROR:
        raise HTTPException(
            status_code=400,
            detail=f"Download is not in ERROR status (current: {download.status.value})",
        )

    try:
        count = await download_db.requeue_downloads(
            feed_id=feed_id,
            download_ids=download_id,
            from_status=DownloadStatus.ERROR,
        )
    except DatabaseOperationError as e:
        raise HTTPException(status_code=500, detail="Database error") from e

    # Trigger processing pipeline if download was requeued
    if count > 0:
        await manual_feed_runner.trigger(feed_id, feed_config)

    logger.info("Requeued download.", extra={**log_params, "requeue_count": count})
    return RequeueResponse(
        feed_id=feed_id, download_id=download_id, requeue_count=count
    )


class ResetSyncRequest(BaseModel):
    """Request model for resetting a feed's last_successful_sync timestamp.

    Attributes:
        sync_time: The ISO 8601 timestamp to set as the last successful sync time.
    """

    sync_time: AwareDatetime = Field(
        ...,
        description="ISO 8601 timestamp with timezone to set as last_successful_sync.",
    )


class ResetSyncResponse(BaseModel):
    """Response model for resetting a feed's last_successful_sync timestamp.

    Attributes:
        feed_id: The feed identifier.
        sync_time: The timestamp that was set.
    """

    feed_id: str
    sync_time: datetime


@router.post("/feeds/{feed_id}/reset-sync", response_model=ResetSyncResponse)
async def reset_sync_timestamp(
    feed_id: ValidatedFeedId,
    payload: ResetSyncRequest,
    feed_db: FeedDatabaseDep,
) -> ResetSyncResponse:
    """Reset the last_successful_sync timestamp for a feed.

    Sets the feed's last_successful_sync to the provided timestamp, allowing
    the enqueuer to rediscover content published after that time. This also
    resets consecutive_failures to 0.

    Args:
        feed_id: The feed identifier (validated and sanitized).
        payload: Request body containing the sync_time timestamp.
        feed_db: Feed database dependency.

    Returns:
        ResetSyncResponse containing the feed_id and the timestamp that was set.

    Raises:
        HTTPException: 404 if feed not found; 500 on database errors.
    """
    log_params = {"feed_id": feed_id, "sync_time": payload.sync_time.isoformat()}
    logger.debug("Admin reset-sync request received.", extra=log_params)

    try:
        await feed_db.mark_sync_success(feed_id, sync_time=payload.sync_time)
    except FeedNotFoundError as e:
        raise HTTPException(status_code=404, detail="Feed not found") from e
    except DatabaseOperationError as e:
        raise HTTPException(status_code=500, detail="Database error") from e

    logger.info("Reset sync timestamp for feed.", extra=log_params)
    return ResetSyncResponse(feed_id=feed_id, sync_time=payload.sync_time)


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


class RefreshMetadataRequest(BaseModel):
    """Request model for metadata refresh operations.

    Attributes:
        refresh_transcript: Force re-download transcript even if metadata unchanged.
    """

    refresh_transcript: bool = False


class RefreshMetadataResponse(BaseModel):
    """Response model for metadata refresh operations.

    Attributes:
        feed_id: The feed identifier.
        download_id: The download identifier.
        metadata_changed: Whether any metadata fields were updated.
        updated_fields: List of field names that were updated.
        thumbnail_refreshed: Thumbnail refresh result (True=success, False=failed,
            None=not needed).
        transcript_refreshed: Transcript refresh result (True=success, False=failed,
            None=not needed).
    """

    feed_id: str
    download_id: str
    metadata_changed: bool
    updated_fields: list[str]
    thumbnail_refreshed: bool | None
    transcript_refreshed: bool | None


@router.post(
    "/feeds/{feed_id}/downloads/{download_id}/refresh-metadata",
    response_model=RefreshMetadataResponse,
)
async def refresh_download_metadata(
    feed_id: ValidatedFeedId,
    download_id: str,
    feed_db: FeedDatabaseDep,
    feed_configs: FeedConfigsDep,
    data_coordinator: DataCoordinatorDep,
    payload: RefreshMetadataRequest,
) -> RefreshMetadataResponse:
    """Re-fetch metadata for a specific download from yt-dlp.

    Updates database metadata fields. Preserves the download's status
    and file paths.
    Can be called on any download status (QUEUED, DOWNLOADED, etc.).

    Args:
        feed_id: The feed identifier (validated and sanitized).
        download_id: The download identifier.
        feed_db: Feed database dependency.
        feed_configs: Configured feeds keyed by identifier.
        data_coordinator: Coordinator for refresh operations.
        payload: request body with refresh options.

    Returns:
        RefreshMetadataResponse with details of updated fields.

    Raises:
        HTTPException: 404 if feed or download not found; 400 if feed is disabled
            or not configured; 500 on database or yt-dlp errors.
    """
    log_params = {"feed_id": feed_id, "download_id": download_id}
    logger.debug("Admin refresh-metadata request received.", extra=log_params)

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

    try:
        (
            _,
            updated_fields,
            thumbnail_refreshed,
            transcript_refreshed,
        ) = await data_coordinator.refresh_download_metadata(
            feed_id=feed_id,
            download_id=download_id,
            feed_config=feed_config,
            refresh_transcript=payload.refresh_transcript,
        )
    except EnqueueError as e:
        # Map specific error causes to appropriate HTTP status codes
        if isinstance(e.__cause__, DownloadNotFoundError):
            raise HTTPException(status_code=404, detail="Download not found") from e
        raise HTTPException(status_code=500, detail="Failed to refresh metadata") from e

    logger.info(
        "Download metadata refreshed.",
        extra={**log_params, "updated_fields": updated_fields},
    )

    return RefreshMetadataResponse(
        feed_id=feed_id,
        download_id=download_id,
        metadata_changed=len(updated_fields) > 0,
        updated_fields=updated_fields,
        thumbnail_refreshed=thumbnail_refreshed,
        transcript_refreshed=transcript_refreshed,
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
