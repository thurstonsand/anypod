"""Static file serving endpoints for RSS feeds and media files."""

import logging

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response, StreamingResponse

from ...exceptions import FileOperationError, RSSGenerationError
from ...mimetypes import mimetypes
from ..dependencies import FileManagerDep, RSSFeedGeneratorDep

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/feeds/{feed_id}.xml")
async def serve_feed(
    feed_id: str,
    request: Request,
    rss_generator: RSSFeedGeneratorDep,
) -> Response:
    """Serve RSS feed XML for a specific feed.

    Args:
        feed_id: The unique identifier for the feed.
        request: The FastAPI request object.
        rss_generator: The RSS generator dependency.

    Returns:
        RSS XML response.

    Raises:
        HTTPException: If feed not found or cannot be generated.
    """
    logger.debug("Serving RSS feed", extra={"feed_id": feed_id})

    try:
        feed_xml = rss_generator.get_feed_xml(feed_id)
    except RSSGenerationError as e:
        raise HTTPException(status_code=404, detail="Feed not found") from e

    return Response(
        content=feed_xml,
        media_type="application/rss+xml",
        headers={
            "Cache-Control": "public, max-age=300",  # 5 minutes # TODO: do we need this?
        },
    )


@router.get("/media/{feed_id}/{filename}.{ext}")
async def serve_media(
    feed_id: str,
    filename: str,
    ext: str,
    request: Request,
    file_manager: FileManagerDep,
) -> StreamingResponse:
    """Serve media file for a specific feed and filename.

    Args:
        feed_id: The unique identifier for the feed.
        filename: The media filename to serve.
        ext: The file extension of the media file.
        request: The FastAPI request object.
        file_manager: The file manager dependency.

    Returns:
        File response with media content.

    Raises:
        HTTPException: If file not found or cannot be served.
    """
    logger.debug(
        "Serving media file",
        extra={"feed_id": feed_id, "filename": filename, "ext": ext},
    )
    try:
        download_stream = await file_manager.get_download_stream(feed_id, filename, ext)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="File not found") from e
    except FileOperationError as e:
        raise HTTPException(status_code=500, detail="Internal server error") from e

    return StreamingResponse(
        download_stream,
        status_code=200,
        media_type=mimetypes.guess_type(f"file.{ext}")[0],
    )
