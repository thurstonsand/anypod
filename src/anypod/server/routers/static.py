"""Static file serving endpoints for RSS feeds and media files."""

from collections.abc import AsyncIterator
import html
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response, StreamingResponse

from ...clip_streamer import (
    generate_clip_filename,
    get_clip_content_type,
    parse_timestamp,
    stream_clip,
    validate_clip_range,
)
from ...db.types import DownloadStatus
from ...exceptions import DatabaseOperationError, FFmpegError, FileOperationError
from ...mimetypes import mimetypes
from ..dependencies import (
    DownloadDatabaseDep,
    FeedDatabaseDep,
    FileManagerDep,
)
from ..validation import ValidatedExtension, ValidatedFeedId, ValidatedFilename

logger = logging.getLogger(__name__)

router = APIRouter()


def _generate_directory_listing(
    title: str, links: list[tuple[str, str]], parent_path: str | None = None
) -> str:
    """Generate HTML directory listing.

    Args:
        title: Page title and directory name.
        links: List of (href, display_text) tuples for directory entries.
        parent_path: Optional parent directory path. If None, no parent link is shown.

    Returns:
        Complete HTML page as string.
    """
    escaped_title = html.escape(title)
    link_items: list[str] = []

    # Add parent directory link only if parent_path is provided
    if parent_path is not None:
        escaped_parent_path = html.escape(parent_path)
        link_items.append(f'<a href="{escaped_parent_path}">../</a>')

    # Add directory/file links
    for href, display_text in links:
        escaped_href = html.escape(href)
        escaped_text = html.escape(display_text)
        link_items.append(f'<a href="{escaped_href}">{escaped_text}</a>')

    links_html = "<br>".join(link_items)

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>Index of {escaped_title}</title>
    <style>
        body {{ font-family: monospace; margin: 20px; }}
        h1 {{ font-size: 18px; margin-bottom: 10px; }}
        hr {{ margin: 10px 0; }}
        a {{
            display: block;
            text-decoration: none;
            padding: 2px 4px;
            color: #0066cc;
        }}
        a:hover {{
            background-color: #f0f0f0;
            text-decoration: underline;
        }}
    </style>
</head>
<body>
    <h1>Index of {escaped_title}</h1>
    <hr>
    {links_html}
    <hr>
</body>
</html>"""


@router.api_route("/feeds/{feed_id}.xml", methods=["GET", "HEAD"])
async def serve_feed(
    feed_id: ValidatedFeedId,
    _request: Request,
    file_manager: FileManagerDep,
) -> Response:
    """Serve RSS feed XML for a specific feed.

    Args:
        feed_id: The unique identifier for the feed.
        request: The FastAPI request object.
        file_manager: FileManager used to resolve and validate the feed XML path.

    Returns:
        RSS XML response.

    Raises:
        HTTPException: If feed not found or cannot be generated.
    """
    logger.debug("Serving RSS feed", extra={"feed_id": feed_id})

    try:
        file_path = await file_manager.get_feed_xml_path(feed_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="Feed not found") from e
    except FileOperationError as e:
        raise HTTPException(status_code=500, detail="Internal server error") from e

    return FileResponse(
        path=file_path,
        media_type="application/rss+xml",
        headers={
            "Cache-Control": "public, max-age=300",
        },
    )


@router.get("/feeds")
async def browse_feeds(
    feed_db: FeedDatabaseDep,
) -> Response:
    """Browse available feeds as a file system directory listing.

    Args:
        feed_db: The feed database dependency.

    Returns:
        HTML response with directory listing of available feeds.

    Raises:
        HTTPException: If feeds cannot be retrieved.
    """
    logger.debug("Browsing feeds directory")

    try:
        feeds = await feed_db.get_feeds(enabled=True)
    except DatabaseOperationError as e:
        logger.error("Database error while retrieving feeds", exc_info=e)
        raise HTTPException(status_code=500, detail="Internal server error") from e

    # Generate links for each feed
    links: list[tuple[str, str]] = []
    for feed in feeds:
        href = f"/feeds/{feed.id}.xml"
        display_text = f"{feed.id}.xml"
        links.append((href, display_text))

    # No parent path for top-level /feeds directory
    html_content = _generate_directory_listing("/feeds", links, parent_path=None)
    return Response(content=html_content, media_type="text/html")


@router.get("/media")
async def browse_media(
    feed_db: FeedDatabaseDep,
) -> Response:
    """Browse available feed directories in media as a file system directory listing.

    Args:
        feed_db: The feed database dependency.

    Returns:
        HTML response with directory listing of feed directories.

    Raises:
        HTTPException: If feeds cannot be retrieved.
    """
    logger.debug("Browsing media directory")

    try:
        feeds = await feed_db.get_feeds(enabled=True)
    except DatabaseOperationError as e:
        logger.error(
            "Database error while retrieving feeds for media browsing", exc_info=e
        )
        raise HTTPException(status_code=500, detail="Internal server error") from e

    # Generate links for each feed directory
    links: list[tuple[str, str]] = []
    for feed in feeds:
        href = f"/media/{feed.id}/"
        display_text = f"{feed.id}/"
        links.append((href, display_text))

    # No parent path for top-level /media directory
    html_content = _generate_directory_listing("/media", links, parent_path=None)
    return Response(content=html_content, media_type="text/html")


@router.get("/media/{feed_id}")
async def browse_media_feed(
    feed_id: ValidatedFeedId,
    download_db: DownloadDatabaseDep,
) -> Response:
    """Browse media files for a specific feed as a file system directory listing.

    Args:
        feed_id: The unique identifier for the feed.
        download_db: The download database dependency.

    Returns:
        HTML response with directory listing of media files for the feed.

    Raises:
        HTTPException: If downloads cannot be retrieved.
    """
    logger.debug("Browsing media files for feed", extra={"feed_id": feed_id})

    try:
        downloads = await download_db.get_downloads_by_status(
            DownloadStatus.DOWNLOADED, feed_id=feed_id
        )
    except DatabaseOperationError as e:
        logger.error(
            "Database error while retrieving downloads for feed",
            extra={"feed_id": feed_id},
            exc_info=e,
        )
        raise HTTPException(status_code=500, detail="Internal server error") from e

    # Generate links for each media file
    links: list[tuple[str, str]] = []
    for download in downloads:
        filename = f"{download.id}.{download.ext}"
        href = f"/media/{feed_id}/{filename}"
        links.append((href, filename))

    # Parent path should go back to /media directory
    html_content = _generate_directory_listing(
        f"/media/{feed_id}", links, parent_path="/media"
    )
    return Response(content=html_content, media_type="text/html")


@router.api_route("/media/{feed_id}/{filename}.{ext}", methods=["GET", "HEAD"])
async def serve_media(
    feed_id: ValidatedFeedId,
    filename: ValidatedFilename,
    ext: ValidatedExtension,
    file_manager: FileManagerDep,
    start: str | None = Query(
        default=None,
        description="Clip start time (seconds, MM:SS, or HH:MM:SS)",
        examples=["30", "1:30", "0:01:30"],
    ),
    end: str | None = Query(
        default=None,
        description="Clip end time (seconds, MM:SS, or HH:MM:SS)",
        examples=["60", "2:00", "0:02:00"],
    ),
) -> FileResponse | StreamingResponse:
    """Serve media file or clip for a specific feed and filename.

    When both ``start`` and ``end`` query parameters are provided, returns a
    transcoded clip of the specified time range. The clip is streamed directly
    from FFmpeg without writing to disk.

    Args:
        feed_id: The unique identifier for the feed.
        filename: The media filename to serve.
        ext: The file extension of the media file.
        file_manager: The file manager dependency.
        start: Clip start time (optional). Accepts seconds, MM:SS, or HH:MM:SS.
        end: Clip end time (optional). Accepts seconds, MM:SS, or HH:MM:SS.

    Returns:
        File response with full media content, or streaming response with clip.

    Raises:
        HTTPException: If file not found, cannot be served, or clip parameters invalid.
    """
    logger.debug(
        "Serving media file",
        extra={"feed_id": feed_id, "filename": filename, "ext": ext},
    )
    try:
        file_path = await file_manager.get_download_file_path(feed_id, filename, ext)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="File not found") from e
    except FileOperationError as e:
        raise HTTPException(status_code=500, detail="Internal server error") from e

    # Check if clip parameters are provided
    if start is not None and end is not None:
        return await _serve_media_clip(file_path, filename, ext, start, end)

    # If only one clip parameter is provided, that's an error
    if start is not None or end is not None:
        raise HTTPException(
            status_code=400,
            detail="Both 'start' and 'end' parameters are required for clips",
        )

    return FileResponse(
        path=file_path,
        media_type=mimetypes.guess_type(f"file.{ext}")[0],
        headers={
            "Cache-Control": "public, max-age=86400",  # Cache for 24 hours
        },
    )


async def _serve_media_clip(
    file_path: Path,
    filename: str,
    ext: str,
    start: str,
    end: str,
) -> StreamingResponse:
    """Serve a clip of a media file using FFmpeg streaming.

    Args:
        file_path: Path to the source media file.
        filename: Original filename (for generating clip filename).
        ext: File extension.
        start: Start time string.
        end: End time string.

    Returns:
        StreamingResponse with the transcoded clip.

    Raises:
        HTTPException: If clip parameters are invalid or FFmpeg fails.
    """
    # Parse and validate timestamps
    try:
        start_seconds = parse_timestamp(start)
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid start time: {e}",
        ) from e

    try:
        end_seconds = parse_timestamp(end)
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid end time: {e}",
        ) from e

    # Validate the clip range
    try:
        clip_range = validate_clip_range(start_seconds, end_seconds)
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e),
        ) from e

    logger.info(
        "Serving media clip",
        extra={
            "file_path": str(file_path),
            "start": start_seconds,
            "end": end_seconds,
            "duration": clip_range.duration_seconds,
        },
    )

    # Generate clip filename for Content-Disposition header
    original_filename = f"{filename}.{ext}"
    clip_filename = generate_clip_filename(original_filename, clip_range)

    # Get content type for the clip
    content_type = get_clip_content_type(ext)

    async def clip_generator() -> AsyncIterator[bytes]:
        """Wrap stream_clip to handle FFmpegError."""
        try:
            async for chunk in stream_clip(file_path, clip_range, ext):
                yield chunk
        except FFmpegError as e:
            # Log the error; at this point we've already started streaming
            # so we can't return an HTTP error. The stream will just end.
            logger.error(
                "FFmpeg error during clip streaming",
                extra={"error": str(e), "stderr": e.stderr},
            )

    return StreamingResponse(
        clip_generator(),
        media_type=content_type,
        headers={
            # Short cache since clip URLs are unique per start/end
            "Cache-Control": "public, max-age=3600",
            # Suggest filename for downloads
            "Content-Disposition": f'inline; filename="{clip_filename}"',
        },
    )


@router.api_route("/images/{feed_id}.{ext}", methods=["GET", "HEAD"])
async def serve_feed_image(
    feed_id: ValidatedFeedId,
    ext: ValidatedExtension,
    file_manager: FileManagerDep,
) -> FileResponse:
    """Serve feed-level image file.

    Args:
        feed_id: The unique identifier for the feed.
        ext: The file extension of the image file.
        file_manager: The file manager dependency.

    Returns:
        File response with image content.

    Raises:
        HTTPException: If image not found or cannot be served.
    """
    logger.debug("Serving feed image", extra={"feed_id": feed_id})

    try:
        file_path = await file_manager.get_image_path(feed_id, None, ext)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="Feed image not found") from e
    except FileOperationError as e:
        raise HTTPException(status_code=500, detail="Internal server error") from e

    return FileResponse(
        path=file_path,
        media_type=mimetypes.guess_type(f"file.{ext}")[0],
        headers={"Cache-Control": "public, max-age=86400"},  # 24 hours
    )


@router.api_route("/images/{feed_id}/{filename}.{ext}", methods=["GET", "HEAD"])
async def serve_download_image(
    feed_id: ValidatedFeedId,
    filename: ValidatedFilename,
    ext: ValidatedExtension,
    file_manager: FileManagerDep,
) -> FileResponse:
    """Serve download-level image file.

    Args:
        feed_id: The unique identifier for the feed.
        filename: The download identifier (filename without extension).
        ext: The file extension of the image file.
        file_manager: The file manager dependency.

    Returns:
        File response with image content.

    Raises:
        HTTPException: If image not found or cannot be served.
    """
    logger.debug(
        "Serving download image", extra={"feed_id": feed_id, "download_id": filename}
    )

    try:
        file_path = await file_manager.get_image_path(feed_id, filename, ext)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="Download image not found") from e
    except FileOperationError as e:
        raise HTTPException(status_code=500, detail="Internal server error") from e

    return FileResponse(
        path=file_path,
        media_type=mimetypes.guess_type(f"file.{ext}")[0],
        headers={"Cache-Control": "public, max-age=86400"},  # 24 hours
    )


@router.api_route(
    "/transcripts/{feed_id}/{filename}.{lang}.{ext}", methods=["GET", "HEAD"]
)
async def serve_download_transcript(
    feed_id: ValidatedFeedId,
    filename: ValidatedFilename,
    lang: ValidatedFilename,
    ext: ValidatedExtension,
    file_manager: FileManagerDep,
) -> FileResponse:
    """Serve transcript file for a specific download.

    Args:
        feed_id: The unique identifier for the feed.
        filename: The download identifier (filename without extension).
        lang: The language code of the transcript (e.g., "en").
        ext: The file extension of the transcript file (e.g., "vtt").
        file_manager: The file manager dependency.

    Returns:
        File response with transcript content.

    Raises:
        HTTPException: If transcript not found or cannot be served.
    """
    logger.debug(
        "Serving transcript",
        extra={"feed_id": feed_id, "download_id": filename, "lang": lang, "ext": ext},
    )

    try:
        file_path = await file_manager.get_transcript_path(feed_id, filename, lang, ext)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="Transcript not found") from e
    except FileOperationError as e:
        raise HTTPException(status_code=500, detail="Internal server error") from e

    media_type = mimetypes.guess_type(f"file.{ext}")[0]

    return FileResponse(
        path=file_path,
        media_type=media_type,
        headers={"Cache-Control": "public, max-age=86400"},  # 24 hours
    )
