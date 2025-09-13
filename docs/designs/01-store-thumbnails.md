# Thumbnail Hosting Implementation Plan

## Overview

This document outlines the implementation plan for adding thumbnail image hosting to Anypod alongside the existing video hosting. The goal is to download and host thumbnail images locally, serving them through the same HTTP server that handles media files and RSS feeds.

## Current State Analysis

### Media Storage Structure
- **Media files**: Stored in `/data/media/{feed_id}/{download_id}.{ext}`
- **Path resolution**: `PathManager` handles URL construction and file path resolution
- **File operations**: `FileManager` handles file operations (read, delete, etc.)
- **HTTP serving**: Static routes serve media from `/media/{feed_id}/{filename}.{ext}`

### Current Thumbnail Handling
- **Storage**: Thumbnails are currently just URLs stored in the database (`download.thumbnail` field)
- **RSS inclusion**: `feedgen_core.py` includes thumbnail URLs in podcast feeds
- **Metadata extraction**: yt-dlp already extracts thumbnail metadata via `YtdlpThumbnails`

## Implementation Plan

### 1. (COMPLETE) Thumbnail Storage Structure

Create a parallel structure to media files for thumbnails:

```
/data/images/{feed_id}/
├── downloads/
│   └── {download_id}.jpg    # Individual video thumbnails
└── {feed_id}.jpg            # Channel/playlist thumbnails
```

**Notes:**
- Only JPG format will be supported for maximum podcast player compatibility
- Feed-level thumbnails stored as `{feed_id}.jpg` to avoid conflicts with downloads named "feed"
- Individual video thumbnails stored in `downloads/` subdirectory
- Feed-level thumbnails will be extracted from playlist/channel metadata during state reconciliation

### 2. (COMPLETE) Database Schema Changes

**Download Model Changes:**
Rename existing field and add extension field:

```python
# In src/anypod/db/types/download.py
class Download(SQLModel, table=True):
    # ... existing fields ...

    # Rename existing thumbnail field to be more explicit
    remote_thumbnail_url: str | None = None

    # Thumbnail hosting fields
    thumbnail_ext: str | None = None       # Thumbnail file extension - future extensibility, always "jpg" initially
```

**Feed Model Changes:**
Rename existing field to be more explicit:

```python
# In src/anypod/db/types/feed.py
class Feed(SQLModel, table=True):
    # ... existing fields ...

    # Rename existing image_url field to be more explicit
    remote_image_url: str | None = None
```

#### (COMPLETE) Alembic Migration

Create a seamless upgrade migration:

```python
# In alembic/versions/XXXX_add_thumbnail_fields.py
"""Add thumbnail fields and rename existing fields."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlmodel import SQLModel

# revision identifiers, used by Alembic.
revision: str = "XXXX_add_thumbnail_fields"
down_revision: Union[str, None] = "previous_revision"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    """Add thumbnail fields and rename existing fields."""
    # Rename thumbnail column to remote_thumbnail_url in downloads table
    op.execute("ALTER TABLE download RENAME COLUMN thumbnail TO remote_thumbnail_url")

    # Add new thumbnail extension column to downloads table
    op.add_column(
        "download",
        sa.Column("thumbnail_ext", sa.String(), nullable=True)
    )

    # Rename image_url column to remote_image_url in feeds table
    op.execute("ALTER TABLE feed RENAME COLUMN image_url TO remote_image_url")

def downgrade() -> None:
    """Reverse thumbnail fields and field renames."""
    # Remove thumbnail extension column from downloads table
    op.drop_column("download", "thumbnail_ext")

    # Rename remote_thumbnail_url back to thumbnail in downloads table
    op.execute("ALTER TABLE download RENAME COLUMN remote_thumbnail_url TO thumbnail")

    # Rename remote_image_url back to image_url in feeds table
    op.execute("ALTER TABLE feed RENAME COLUMN remote_image_url TO image_url")
```

**Migration Strategy:**
1. **Zero-downtime**: New columns are nullable, existing data remains unaffected
2. **Backward compatibility**: Code will handle NULL values gracefully
3. **Forward compatibility**: New thumbnail downloads will populate the fields
4. **Migration script**: Will be provided to download thumbnails for existing downloads

### 3. (COMPLETE) PathManager Extensions

Extend `PathManager` to handle image paths and URLs:

```python
# In src/anypod/path_manager.py
class PathManager:
    # ... existing methods ...

    @property
    def base_images_dir(self) -> Path:
        """Return the directory used for image files."""
        return self._base_data_dir / "images"

    async def feed_images_dir(self, feed_id: str) -> Path:
        """Return the directory for a feed's image files."""
        path = self.base_images_dir / feed_id
        await aiofiles.os.makedirs(path, exist_ok=True)
        return path

    def image_url(self, feed_id: str, download_id: str | None, ext: str) -> str:
        """Return the HTTP URL for an image file."""
        if download_id is None:
            # Feed-level image
            return urljoin(self._base_url, f"/images/{feed_id}.{ext}")
        else:
            # Download-level image
            return urljoin(self._base_url, f"/images/{feed_id}/{download_id}.{ext}")

    async def image_path(self, feed_id: str, download_id: str | None, ext: str) -> Path:
        """Return the full file system path for an image file."""
        feed_dir = await self.feed_images_dir(feed_id)
        if download_id is None:
            # Feed-level image
            return feed_dir / f"{feed_id}.{ext}"
        else:
            # Download-level image
            downloads_dir = feed_dir / "downloads"
            await aiofiles.os.makedirs(downloads_dir, exist_ok=True)
            return downloads_dir / f"{download_id}.{ext}"
```

### 4. (COMPLETE) YtdlpArgs Extensions

Add thumbnail download support to `YtdlpArgs`:

```python
# In src/anypod/ytdlp_wrapper/core/args.py
class YtdlpArgs:
    # ... existing fields ...

    def __init__(self, user_args: list[str] | None = None):
        # ... existing initialization ...
        self._write_thumbnails = False
        self._convert_thumbnails: str | None = None
        self._thumbnail_output: str | None = None
        self._paths_thumbnail: Path | None = None

    def write_thumbnails(self) -> "YtdlpArgs":
        """Enable thumbnail downloading."""
        self._write_thumbnails = True
        return self

    def convert_thumbnails(self, fmt: str) -> "YtdlpArgs":
        """Convert thumbnails to specified format (jpg, png, webp)."""
        self._convert_thumbnails = fmt
        return self

    def paths_thumbnail(self, path: Path) -> "YtdlpArgs":
        """Set the directory where thumbnails will be saved."""
        self._paths_thumbnail = path
        return self

    def output_thumbnail(self, template: str) -> "YtdlpArgs":
        """Set the output template for thumbnail files."""
        self._thumbnail_output = template
        return self

    def to_list(self) -> list[str]:
        # ... existing command building ...
        if self._write_thumbnails:
            cmd.append("--write-thumbnails")

        if self._convert_thumbnails is not None:
            cmd.extend(["--convert-thumbnails", self._convert_thumbnails])

        if self._paths_thumbnail is not None:
            cmd.extend(["--paths", f"thumbnail:{self._paths_thumbnail}"])

        if self._thumbnail_output is not None:
            cmd.extend(["-o", f"thumbnail:{self._thumbnail_output}"])

        return cmd
```

**Usage Example:**
```python
# Download thumbnails to specific directory with custom naming
args = (YtdlpArgs()
        .write_thumbnails()
        .convert_thumbnails("jpg")  # Always use jpg for podcast compatibility
        .paths_thumbnail(Path("/data/images/myfeed"))
        .output_thumbnail("%(id)s.%(ext)s"))  # Results in {download_id}.jpg

# For feed-level thumbnails
feed_args = (YtdlpArgs()
             .write_thumbnails()
             .convert_thumbnails("jpg")
             .paths_thumbnail(Path("/data/images/myfeed"))
             .output_thumbnail("feed.%(ext)s"))  # Results in feed.jpg
```

### 5. (COMPLETE) FileManager Extensions

Add generic image file operations:

```python
# In src/anypod/file_manager.py
class FileManager:
    # ... existing methods ...

    async def get_image_path(self, feed_id: str, download_id: str | None, ext: str) -> Path:
        """Get the file path for an image file."""
        try:
            file_path = await self._paths.image_path(feed_id, download_id, ext)
        except ValueError as e:
            raise FileOperationError(
                "Invalid feed or download identifier.",
                feed_id=feed_id,
                download_id=download_id,
            ) from e

        if not await aiofiles.os.path.isfile(file_path):
            raise FileNotFoundError(f"Image file not found: {file_path}")
        return file_path

    async def image_exists(self, feed_id: str, download_id: str | None, ext: str) -> bool:
        """Check if an image file exists."""
        try:
            await self.get_image_path(feed_id, download_id, ext)
            return True
        except FileNotFoundError:
            return False

    async def delete_image(self, feed_id: str, download_id: str | None, ext: str) -> None:
        """Delete an image file from the filesystem."""
        try:
            file_path = await self.get_image_path(feed_id, download_id, ext)
            await aiofiles.os.remove(file_path)
            logger.debug("Image file deleted", extra={"path": str(file_path)})
        except FileNotFoundError:
            # File already deleted or never existed - not an error
            logger.debug("Image file not found for deletion", extra={
                "feed_id": feed_id,
                "download_id": download_id,
                "ext": ext
            })
```

#### Image Cleanup Strategy

The existing pruner already handles media file cleanup. We need to extend it to also handle image cleanup:

**Pruner Integration:**
```python
# In src/anypod/data_coordinator/pruner.py
class Pruner:
    # ... existing methods ...

    async def _handle_image_deletion(self, download: Download, feed_id: str) -> None:
        """Handle image deletion for a DOWNLOADED item being pruned.

        Args:
            download: The Download object with DOWNLOADED status.
            feed_id: The feed identifier.

        Raises:
            PruneError: If image deletion fails.
        """
        if download.thumbnail_ext:
            log_params: dict[str, Any] = {
                "feed_id": feed_id,
                "download_id": download.id,
                "image_ext": download.thumbnail_ext,
            }
            logger.debug("Attempting to delete image for downloaded item being pruned.", extra=log_params)

            try:
                await self._file_manager.delete_image(feed_id, download.id, download.thumbnail_ext)
            except FileOperationError as e:
                raise PruneError(
                    message="Failed to delete image during pruning.",
                    feed_id=feed_id,
                    download_id=download.id,
                ) from e
            logger.debug("Image deleted successfully during pruning.", extra=log_params)

    async def _process_single_download_for_pruning(
        self, download: Download, feed_id: str
    ) -> bool:
        """Process a single download for pruning.

        Handles file deletion (if DOWNLOADED) and database archival.
        """
        # ... existing logic ...

        # Delete file if the download is DOWNLOADED
        if download.status == DownloadStatus.DOWNLOADED:
            try:
                await self._handle_file_deletion(download, feed_id)
            except FileNotFoundError:
                logger.warning("File not found during pruning, but DB record will still be archived.", ...)
            else:
                file_deleted = True

            # Add image deletion alongside file deletion
            try:
                await self._handle_image_deletion(download, feed_id)
            except PruneError:
                logger.warning("Image deletion failed during pruning, continuing with DB archival.", ...)

        # Always archive the download
        await self._archive_download(download, feed_id)

        return file_deleted

    async def archive_feed(self, feed_id: str) -> tuple[int, int]:
        """Archive an entire feed by disabling it and archiving all downloads."""
        # ... existing logic for archiving downloads ...

        # Add feed image cleanup at the end
        try:
            await self._file_manager.delete_image(feed_id, None, "jpg")
            logger.debug("Feed image deleted successfully during feed archival.", extra={"feed_id": feed_id})
        except (FileOperationError, FileNotFoundError):
            logger.debug("No feed image found to delete during feed archival.", extra={"feed_id": feed_id})

        # ... existing logic for disabling feed ...
```

### 6. Static Router Extensions

Add two separate image serving routes:

```python
# In src/anypod/server/routers/static.py

# Route for feed-level images: /images/{feed_id}.jpg
@router.api_route("/images/{feed_id}.{ext}", methods=["GET", "HEAD"])
async def serve_feed_image(
    feed_id: ValidatedFeedId,
    ext: ValidatedExtension,
    file_manager: FileManagerDep,
) -> FileResponse:
    """Serve feed-level image file."""
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

# Route for download-level images: /images/{feed_id}/{download_id}.jpg
@router.api_route("/images/{feed_id}/{download_id}.{ext}", methods=["GET", "HEAD"])
async def serve_download_image(
    feed_id: ValidatedFeedId,
    download_id: ValidatedFilename,
    ext: ValidatedExtension,
    file_manager: FileManagerDep,
) -> FileResponse:
    """Serve download-level image file."""
    logger.debug("Serving download image", extra={"feed_id": feed_id, "download_id": download_id})

    try:
        file_path = await file_manager.get_image_path(feed_id, download_id, ext)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="Download image not found") from e
    except FileOperationError as e:
        raise HTTPException(status_code=500, detail="Internal server error") from e

    return FileResponse(
        path=file_path,
        media_type=mimetypes.guess_type(f"file.{ext}")[0],
        headers={"Cache-Control": "public, max-age=86400"},  # 24 hours
    )
```

### 7. RSS Feed Generator Updates

Update `feedgen_core.py` to use hosted image URLs for both feed images and download thumbnails:

```python
# In src/anypod/rss/feedgen_core.py
def __init__(self, paths: PathManager, feed_id: str, feed: Feed):
    # ... existing initialization ...

    # Handle feed-level image (channel artwork)
    # Always try to use hosted feed image first
    try:
        hosted_feed_image_url = self._paths.image_url(feed_id, None, "jpg")
        fg.podcast.itunes_image(hosted_feed_image_url)  # type: ignore
        fg.image(  # type: ignore
            url=hosted_feed_image_url,
            title=feed.title,
            link=feed.source_url,
            description=f"Artwork for {feed.title}",
        )
    except ValueError:
        # Fallback to original image URL if hosted path is invalid
        if feed.remote_image_url:
            fg.podcast.itunes_image(feed.remote_image_url)  # type: ignore
            fg.image(  # type: ignore
                url=feed.remote_image_url,
                title=feed.title,
                link=feed.source_url,
                description=f"Artwork for {feed.title}",
            )

def with_downloads(self, downloads: list[Download]) -> "FeedgenCore":
    # ... existing feed processing ...

    for download in downloads:
        fe = self._fg.add_entry(order="append")

        # ... existing download processing ...

        # Use hosted thumbnail URL if available, fallback to original URL
        if download.thumbnail_ext:  # We have a hosted thumbnail
            try:
                # Use PathManager to construct the correct hosted URL
                thumbnail_url = self._paths.image_url(
                    download.feed_id, download.id, download.thumbnail_ext
                )
                fe.podcast.itunes_image(thumbnail_url)
            except ValueError:
                # Fallback to original thumbnail URL if hosted path is invalid
                if download.remote_thumbnail_url:
                    fe.podcast.itunes_image(download.remote_thumbnail_url)
        elif download.remote_thumbnail_url:
            # Use original thumbnail URL if no hosted version
            fe.podcast.itunes_image(download.remote_thumbnail_url)
```

**Feed Image Strategy:**
- **Primary**: Always try to use hosted feed image at `/images/{feed_id}.jpg` first
- **Fallback**: Use original `feed.remote_image_url` if hosted version is unavailable or invalid
- **Error Handling**: Graceful fallback ensures RSS generation never fails due to missing images

**Download Thumbnail Strategy:**
- **Primary**: Use hosted thumbnail URL via PathManager if `thumbnail_ext` exists
- **Fallback**: Use original `download.remote_thumbnail_url` if hosted version unavailable
- **PathManager Integration**: Use `image_url()` method for consistent URL construction

### 8. Data Coordinator Integration

Update the download workflow to handle thumbnails:

1. **Enqueuer**: Extract thumbnail URLs during metadata fetching
2. **Downloader**: Download thumbnails alongside media files using new YtdlpArgs methods
3. **Database**: Store thumbnail metadata (original URL and extension)

Key integration points:
- Update `ytdlp_wrapper.py` to handle thumbnail extraction
- Modify `downloader.py` to download thumbnails with proper output templates
- Update database operations to store thumbnail metadata

**Thumbnail Download Integration:**
```python
# In downloader.py - example usage for download thumbnails
feed_images_dir = await paths.feed_images_dir(feed_id)
downloads_dir = feed_images_dir / "downloads"

args = (YtdlpArgs()
        .write_thumbnails()
        .convert_thumbnails("jpg")  # Always use JPG for podcast compatibility
        .paths_thumbnail(downloads_dir)
        .output_thumbnail("%(id)s.%(ext)s"))  # Results in downloads/{download_id}.jpg

# For feed-level thumbnails (automatic during metadata collection)
# This happens in ytdlp_wrapper during fetch_playlist_metadata
feed_args = (YtdlpArgs()
             .write_thumbnails()
             .convert_thumbnails("jpg")
             .paths_thumbnail(feed_images_dir)
             .output_thumbnail("%(id)s.%(ext)s"))  # Results in {feed_id}.jpg for playlist metadata
```

### 9. State Reconciler Integration

The state reconciler (`state_reconciler.py`) must be updated to handle thumbnail cleanup and downloading:

#### When Downloads are Archived

Extend the existing pruner integration to clean up thumbnail images:

```python
# In src/anypod/data_coordinator/pruner.py - extend existing cleanup methods
class Pruner:
    # ... existing methods ...

    async def _cleanup_download_resources(self, download: Download) -> None:
        """Clean up all resources for an archived download."""
        # Existing media file cleanup
        await self._file_manager.delete_download_file(
            download.feed_id, download.id, download.ext
        )

        # Add thumbnail image cleanup
        if download.thumbnail_ext:
            await self._file_manager.delete_download_image(
                download.feed_id, download.id, download.thumbnail_ext
            )
```

#### When Feeds are Archived/Removed

Feed image cleanup should be handled by the pruner, not the state reconciler. The existing `_handle_removed_feed` method only needs to call the pruner's `archive_feed` method, which will handle all cleanup including feed images.



#### When Feeds are Added (New or Re-added)

Feed image downloading happens automatically during metadata collection. The existing `_handle_new_feed` method calls `fetch_playlist_metadata` which will download the feed image as part of the yt-dlp metadata extraction process.

```python
# In src/anypod/ytdlp_wrapper/ytdlp_wrapper.py - extend fetch_playlist_metadata
async def fetch_playlist_metadata(
    self,
    feed_id: str,
    source_type: SourceType,
    source_url: str,
    resolved_url: str,
    user_yt_cli_args: list[str] | None = None,
    yt_channel: str | None = None,
    cookies_path: Path | None = None,
) -> YtdlpWrapperResult:
    """Fetch playlist metadata and automatically download feed image."""
    # ... existing metadata fetching logic ...

    # Configure yt-dlp to download feed image during metadata extraction
    feed_images_dir = await self._paths.feed_images_dir(feed_id)

    args = (YtdlpArgs(user_yt_cli_args)
            .quiet()
            .no_warnings()
            .dump_single_json()
            .flat_playlist()
            .skip_download()
            .write_thumbnails()        # Enable thumbnail writing
            .convert_thumbnails("jpg") # Convert to JPG format
            .paths_thumbnail(feed_images_dir)
            .output_thumbnail("%(id)s.%(ext)s"))  # Results in {feed_id}.jpg

    if cookies_path:
        args = args.cookies(cookies_path)

    # ... existing subprocess execution and JSON parsing logic ...
    # The thumbnail will be downloaded as part of the yt-dlp execution
```

#### Thumbnail URL Change Detection

Extend the existing feed update logic to detect and handle thumbnail URL changes:

```python
# In src/anypod/state_reconciler.py - extend existing feed update logic
async def _handle_existing_feed(
    self,
    feed_id: str,
    feed_config: FeedConfig,
    db_feed: Feed,
    cookies_path: Path | None = None,
) -> None:
    """Handle an existing feed by applying configuration changes."""
    # ... existing feed update logic ...

    # After fetching fresh metadata, check for thumbnail URL changes in downloads
    try:
        downloads = await self._download_db.get_downloads_by_status(
            DownloadStatus.DOWNLOADED, feed_id=feed_id
        )

        for download in downloads:
            if (download.remote_thumbnail_url and
                download.thumbnail_ext and
                download.remote_thumbnail_url != download.fresh_metadata_thumbnail_url):

                # Thumbnail URL has changed, re-download it
                await self._download_fresh_thumbnail(download)

    except DatabaseOperationError as e:
        logger.warning("Failed to check for thumbnail URL changes", extra={
            "feed_id": feed_id,
            "error": str(e)
        })

async def _download_fresh_thumbnail(self, download: Download) -> None:
    """Download a fresh thumbnail for an existing download."""
    try:
        from .ytdlp_wrapper.core.args import YtdlpArgs
        from .ytdlp_wrapper.core.core import YtdlpCore

        if not download.remote_thumbnail_url:
            return

        feed_images_dir = await self._paths.feed_images_dir(download.feed_id)
        args = (YtdlpArgs()
                .quiet()
                .no_warnings()
                .skip_download()
                .write_thumbnails()
                .convert_thumbnails("jpg")
                .paths_thumbnail(feed_images_dir)
                .output_thumbnail("%(id)s.%(ext)s"))

        await YtdlpCore.download(args, download.remote_thumbnail_url)

        # Update download with new thumbnail metadata if needed
        # (The thumbnail_ext field should already be set)

        logger.debug("Fresh thumbnail downloaded", extra={
            "feed_id": download.feed_id,
            "download_id": download.id
        })

    except Exception as e:
        logger.warning("Failed to download fresh thumbnail", extra={
            "feed_id": download.feed_id,
            "download_id": download.id,
            "thumbnail_url": download.remote_thumbnail_url,
            "error": str(e)
        })
```

### 10. Migration Strategy

For existing installations with thumbnail URLs but no hosted thumbnails:

#### Database Migration
1. **Run alembic migration** to add new fields and rename existing ones
2. **Zero-downtime deployment** - migration is safe for existing data

#### Thumbnail Migration Script
```python
# scripts/migrate_thumbnails.py
async def migrate_existing_thumbnails():
    """Download thumbnails for existing downloads that have URLs but no hosted files."""
    from src.anypod.db import DatabaseManager
    from src.anypod.db.types import Download, DownloadStatus

    db = DatabaseManager()
    downloads = await db.download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED
    )

    for download in downloads:
        if download.remote_thumbnail_url and not download.thumbnail_ext:
            # Download and host the thumbnail
            await download_and_host_thumbnail(download)
            # Update database with thumbnail_ext = "jpg"

async def migrate_feed_images():
    """Download feed images for existing feeds."""
    from src.anypod.db import DatabaseManager

    db = DatabaseManager()
    feeds = await db.feed_db.get_feeds()

    for feed in feeds:
        if feed.remote_image_url:
            # Check if feed image already exists
            if not await file_manager.image_exists(feed.id, None, "jpg"):
                await download_and_host_feed_image(feed)
```

#### Implementation Steps
1. **Deploy code changes** with new database fields and logic
2. **Run alembic migration** to update database schema
3. **Run migration script** to download existing thumbnails
4. **Monitor logs** for any thumbnail download failures
5. **Verify RSS feeds** use hosted images where available

## Implementation Phases

### Phase 1: Core Infrastructure
- Create and run alembic migration for database schema changes (`thumbnail` → `remote_thumbnail_url`, `image_url` → `remote_image_url`, add `thumbnail_ext`)
- Extend PathManager with unified `image_url()` and `image_path()` methods using `download_id: str | None`
- Extend FileManager with generic image operations using `feed_id: str` and `download_id: str | None`
- Add static routes at `/images/{feed_id}.jpg` and `/images/{feed_id}/{download_id}.jpg` for image serving
- Extend existing Pruner methods to handle image cleanup alongside media file cleanup

### Phase 2: Download Integration
- Extend YtdlpArgs with separated `write_thumbnails()` and `convert_thumbnails("jpg")` methods
- Add `paths_thumbnail()` and `output_thumbnail()` methods for proper yt-dlp integration
- Update downloader to handle thumbnails alongside media files using proper output templates
- Update database operations to store `thumbnail_ext` metadata
- Extend ytdlp_wrapper `fetch_playlist_metadata` to automatically download feed images during metadata collection

### Phase 3: RSS Integration
- Update RSS generator to use hosted URLs for download thumbnails (prefer hosted, fallback to original)
- Update RSS generator to prefer hosted feed images over original URLs using unified PathManager methods
- Create migration script for existing downloads and feeds
- Test end-to-end functionality with real feeds

### Phase 4: State Reconciliation
- Add thumbnail URL change detection to existing feed update logic
- Ensure pruner handles complete feed cleanup including feed images

## Benefits

1. **Reduced External Dependencies**: No reliance on external image hosting
2. **Improved Performance**: Local image serving is faster and more reliable
3. **Better Reliability**: No external image URL failures or rate limiting
4. **Privacy**: All assets hosted locally, reducing privacy concerns
5. **Podcast Compatibility**: JPG-only format ensures maximum compatibility with podcast players
6. **Change Detection**: Can detect and re-download when original URLs change
7. **Storage Efficiency**: JPG format provides good compression for most use cases

## Considerations

1. **Storage Impact**: Images are small (typically < 100KB each) but will accumulate over time
2. **Download Time**: Minimal impact on download duration with proper yt-dlp configuration
3. **Format Limitation**: JPG-only restriction may limit artistic expression but maximizes compatibility
4. **Cleanup Requirements**: Must properly clean up images when downloads/feeds are archived
5. **Migration Effort**: Existing installations will need migration script to download thumbnails
6. **Error Handling**: Graceful fallback to original URLs if image download fails
