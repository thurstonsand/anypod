# Thumbnail Refresh Implementation Plan

## Overview

Add thumbnail refresh capability to the existing metadata refresh feature. When `remote_thumbnail_url` changes during metadata refresh, download the new thumbnail file. Thumbnail download failures won't block metadata updates (graceful degradation).

## User Requirements

- **When**: Only refresh if `remote_thumbnail_url` changed
- **Failure handling**: Keep old thumbnail on failure
- **Architecture**: Coordinator orchestrates enqueuer + downloader

## Implementation Strategy

Follow the existing `download_feed_thumbnail()` pattern (ytdlp_wrapper.py:325-386) which uses `.skip_download()` + `.write_thumbnail()` to download thumbnails without media.

## Changes Required

### 1. YtdlpWrapper: Add `download_media_thumbnail()` Method

**File**: `src/anypod/ytdlp_wrapper/ytdlp_wrapper.py`

**Location**: After `download_feed_thumbnail()` (around line 387)

**Method signature**:
```python
async def download_media_thumbnail(
    self,
    download: Download,
    user_yt_cli_args: list[str],
    cookies_path: Path | None = None,
) -> str:
    """Download only the thumbnail for a download (skip media)."""
```

**Implementation**:
- Prepare directories: `download_images_dir()` and `download_temp_dir()`
- Build ytdlp args following `download_feed_thumbnail()` pattern:
  - `.skip_download()` - Skip media
  - `.write_thumbnail()` + `.convert_thumbnails("jpg")`
  - `.paths_thumbnail()` + `.output_thumbnail(f"{download.id}.%(ext)s")`
  - `.paths_pl_thumbnail()` + `.output_pl_thumbnail()` for multi-attachment posts
  - Apply: `_update_to()`, `_pot_extractor_args()`, `handler.prepare_thumbnail_args()`
  - Add cookies if provided
- Execute: `await YtdlpCore.download(thumb_args, download.source_url)`
- Return download logs

**Pattern to follow**: `download_feed_thumbnail()` at lines 325-386

---

### 2. Downloader: Add `download_thumbnail_for_existing_download()` Method

**File**: `src/anypod/data_coordinator/downloader.py`

**Location**: After `_handle_download_failure()` (around line 202)

**Method signature**:
```python
async def download_thumbnail_for_existing_download(
    self,
    download: Download,
    yt_args: list[str],
    cookies_path: Path | None = None,
) -> bool:
    """Download thumbnail for an existing download."""
```

**Implementation**:
1. Call `await self.ytdlp_wrapper.download_media_thumbnail(download, yt_args, cookies_path)`
2. If successful (no exception), update DB:
   - Call `await self.download_db.set_thumbnail_extension(download.feed_id, download.id, "jpg")`
   - Return `True`
3. **Error handling**:
   - Catch `YtdlpApiError`: Log warning, return `False` (graceful degradation)
   - Catch `DatabaseOperationError`: Raise `DownloadError` (critical failure)

**Key insight**: Trust yt-dlp success. If it doesn't raise an exception, the thumbnail was downloaded. Don't use `image_exists()` check - the file might not have existed initially, but we're adding it now.

---

### 3. Coordinator: Orchestrate Thumbnail Refresh

**File**: `src/anypod/data_coordinator/coordinator.py`

**Method**: Modify `refresh_download_metadata()` at lines 399-435

**Changes**:

1. **Change return type** from `Download` to `tuple[Download, bool]`

2. **Before calling enqueuer**, get existing download to capture old thumbnail URL and avoid duplicate fetch:
```python
# Get existing download to track thumbnail URL changes
try:
    existing_download = await self._download_db.get_download_by_id(
        feed_id, download_id
    )
except (DownloadNotFoundError, DatabaseOperationError) as e:
    raise EnqueueError(...) from e

old_thumbnail_url = existing_download.remote_thumbnail_url
```

3. **Pass existing_download to enqueuer** to avoid duplicate DB query:
```python
updated_download = await self._enqueuer.refresh_download_metadata(
    feed_id=feed_id,
    download_id=download_id,
    existing_download=existing_download,  # NEW: pass in to avoid duplicate fetch
    yt_args=feed_config.yt_args,
    transcript_lang=feed_config.transcript_lang,
    transcript_source_priority=feed_config.transcript_source_priority,
    cookies_path=self._cookies_path,
)
```

4. **After enqueuer returns**, check if thumbnail URL changed and attempt refresh:
```python
# Check if thumbnail URL changed
thumbnail_refreshed = False

if (
    updated_download.remote_thumbnail_url != old_thumbnail_url
    and updated_download.remote_thumbnail_url is not None
):
    logger.info("Thumbnail URL changed, refreshing thumbnail.", ...)

    try:
        thumbnail_refreshed = await self._downloader.download_thumbnail_for_existing_download(
            updated_download,
            feed_config.yt_args,
            self._cookies_path,
        )

        if not thumbnail_refreshed:
            logger.warning("Thumbnail download failed during metadata refresh.", ...)
    except DownloadError as e:
        logger.warning(
            "Failed to download thumbnail during metadata refresh.",
            extra=log_params,
            exc_info=e,
        )

return updated_download, thumbnail_refreshed
```

5. **Add import**: `from ..exceptions import DownloadNotFoundError` (if not present)

---

### 4. Admin Endpoint: Update Response Model

**File**: `src/anypod/server/routers/admin.py`

**Changes**:

1. **Enhance `RefreshMetadataResponse`** (lines 437-450):
```python
class RefreshMetadataResponse(BaseModel):
    feed_id: str
    download_id: str
    metadata_changed: bool
    updated_fields: list[str]
    thumbnail_refreshed: bool              # NEW
```

2. **Update endpoint handler** (line 511):
```python
updated_download, thumbnail_refreshed = (
    await data_coordinator.refresh_download_metadata(...)
)
```

3. **Update return statement** (line 546):
```python
return RefreshMetadataResponse(
    feed_id=feed_id,
    download_id=download_id,
    metadata_changed=len(updated_fields) > 0,
    updated_fields=updated_fields,
    thumbnail_refreshed=thumbnail_refreshed,      # NEW
)
```

---

### 5. Enqueuer: Accept existing_download Parameter

**File**: `src/anypod/data_coordinator/enqueuer.py`

**Method**: Modify `refresh_download_metadata()` at lines 765-873

**Changes**:

1. **Add `existing_download` parameter** to avoid duplicate DB fetch:
```python
async def refresh_download_metadata(
    self,
    feed_id: str,
    download_id: str,
    existing_download: Download,  # NEW: passed from coordinator
    yt_args: list[str],
    transcript_lang: str | None = None,
    transcript_source_priority: list[TranscriptSource] | None = None,
    cookies_path: Path | None = None,
) -> Download:
```

2. **Remove lines 798-814** (the existing DB fetch logic - now passed in as parameter)

3. **Use the passed-in `existing_download`** directly in line 818+ where it fetches metadata

---

## Edge Cases Handled

1. **Thumbnail URL changed, download fails**: Keep old thumbnail file, set `thumbnail_error`, return success for metadata refresh
2. **Old thumbnail deletion**: Don't delete old thumbnail before downloading new one (yt-dlp overwrites atomically on success)
3. **Same URL, different content**: No re-download (URL comparison sufficient)
4. **Thumbnail URL removed (becomes None)**: Don't delete existing thumbnail file
5. **Thumbnail URL added (was None, now has URL)**: Download new thumbnail
6. **Database update failure**: Raise `DownloadError` (critical failure)

## Implementation Order

1. **Phase 1**: Add `download_media_thumbnail()` to YtdlpWrapper (standalone, testable in isolation)
2. **Phase 2**: Add `download_thumbnail_for_existing_download()` to Downloader (depends on Phase 1)
3. **Phase 3**: Update Enqueuer to accept `existing_download` parameter
4. **Phase 4**: Update Coordinator orchestration (depends on Phases 1-3)
5. **Phase 5**: Update Admin endpoint response (depends on Phase 4)
6. **Phase 6**: Add comprehensive unit tests for each component

## Testing Strategy

### Unit Tests to Add

**File**: `tests/anypod/data_coordinator/test_downloader.py`
- `test_download_thumbnail_for_existing_download_success`
- `test_download_thumbnail_for_existing_download_ytdlp_failure`
- `test_download_thumbnail_for_existing_download_db_failure`
- `test_download_thumbnail_for_existing_download_file_not_found`

**File**: `tests/anypod/ytdlp_wrapper/test_ytdlp_wrapper.py`
- `test_download_media_thumbnail_success`
- `test_download_media_thumbnail_failure`

**File**: `tests/anypod/data_coordinator/test_coordinator.py` (create if needed)
- `test_refresh_download_metadata_with_thumbnail_refresh`
- `test_refresh_download_metadata_thumbnail_url_unchanged`
- `test_refresh_download_metadata_thumbnail_download_fails`
- `test_refresh_download_metadata_thumbnail_infrastructure_error`

**File**: `tests/anypod/server/routers/test_admin.py`
- `test_refresh_metadata_response_includes_thumbnail_refreshed`
- `test_refresh_metadata_endpoint_with_thumbnail_refresh`

**File**: `tests/anypod/data_coordinator/test_enqueuer.py`
- Update existing `test_refresh_download_metadata_*` tests to pass `existing_download` parameter

## Critical Files

- `src/anypod/ytdlp_wrapper/ytdlp_wrapper.py` - Add thumbnail-only download method
- `src/anypod/data_coordinator/downloader.py` - Add download orchestration method
- `src/anypod/data_coordinator/coordinator.py` - Orchestrate thumbnail refresh
- `src/anypod/server/routers/admin.py` - Enhance API response
- `src/anypod/metadata.py` - Reference (no changes needed)

## Code Style Compliance

- Google-style docstrings with Args/Returns/Raises
- Type hints: `str | None` not `Optional[str]`
- Structured logging with `extra=log_params`
- Exception chaining with `raise ... from e`
- Tight `try` blocks
- Functions under 50 lines where possible
- Follow existing patterns
