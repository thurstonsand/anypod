# Design: Refresh Consolidation & Flexible Downloader

## Problem Statement

Two related issues with the current refresh implementation:

### 1. Duplicated Refresh Logic

- **Feed-level refresh** (`POST /admin/feeds/{feed_id}/refresh`) triggers `manual_feed_runner.trigger()` which runs the full `process_feed` flow (enqueue → download → prune → RSS)
- **Download-level refresh** (`POST /admin/feeds/{feed_id}/downloads/{download_id}/refresh-metadata`) calls `data_coordinator.refresh_download_metadata()` with a hacky flow: enqueuer fetches metadata → downloader potentially downloads thumbnail → enqueuer persists metadata
- The download-level refresh only refreshes metadata but doesn't trigger re-download if the download is in ERROR state
- The two refresh paths share similar metadata fetch/merge logic but implement it separately

### 2. Inflexible Downloader

- The downloader is all-or-nothing: `_process_single_download` downloads media + thumbnail + transcript together
- No way to selectively download:
  - Just a transcript for existing media (backfill scenario)
  - Just a thumbnail without re-downloading media (URL changed or local file missing)
  - Just media if transcript/thumbnail already exist

## Design Decisions

### Decision 1: Two Separate Operations with Aligned Naming

Two distinct use cases require two endpoints:

1. **Metadata-only refresh** (`refresh-metadata`) - Update title/description/thumbnail for DOWNLOADED items without re-downloading media
2. **Error retry** (`requeue`) - Reset ERROR→QUEUED and re-attempt download

**Endpoints:**

| Level    | Endpoint                    | Purpose                                                                  |
| -------- | --------------------------- | ------------------------------------------------------------------------ |
| Feed     | `POST .../requeue`          | Requeue all ERROR→QUEUED, trigger pipeline *(renamed from reset-errors)* |
| Download | `POST .../requeue`          | Requeue one ERROR→QUEUED, trigger pipeline *(new)*                       |
| Download | `POST .../refresh-metadata` | Sync metadata refresh, no status change *(existing, simplified)*         |

**requeue implementation:**

```python
# In admin router
@router.post("/feeds/{feed_id}/downloads/{download_id}/requeue")
async def requeue_download(...) -> RequeueResponse:
    """Requeue a single ERROR download and trigger processing."""
    # 1. Validate download exists and is in ERROR status
    # 2. Call download_db.requeue_downloads(feed_id, download_id, from_status=ERROR)
    # 3. Trigger manual_feed_runner.trigger(feed_id, feed_config)
    # 4. Return 202 Accepted with was_requeued indicator
```

**Rationale:**
- Clear separation: metadata refresh vs retry failed download
- Naming aligned: `requeue` at both feed and download level
- Both requeue endpoints trigger pipeline via `manual_feed_runner.trigger()`

### Decision 2: Unidirectional Flow with Component-Owned DB Writes

Each component owns DB writes for its domain:
- **Enqueuer**: metadata fields (title, description, duration, remote_thumbnail_url, transcript metadata, etc.)
- **Downloader**: artifact fields (ext, filesize, thumbnail_ext, transcript_ext, status)

**refresh-metadata flow (simplified):**

```text
1. Enqueuer.refresh_metadata(download, config) → fetches from yt-dlp, merges, persists to DB, returns updated Download
2. IF thumbnail URL changed OR thumbnail_ext is None with remote_thumbnail_url set:
   Downloader.download_artifacts(dl, config, THUMBNAIL) → writes thumbnail_ext to DB
3. IF refresh_transcript flag OR transcript metadata changed (ext/lang/source):
   Downloader.download_artifacts(dl, config, TRANSCRIPT) → writes transcript fields to DB
```

**Rationale:**
- Single `refresh_metadata` method in Enqueuer (consolidates fetch + persist)
- Unidirectional data flow (no enqueuer→downloader→enqueuer bounce)
- Each component owns its DB writes
- Same `download_artifacts` code path for all artifact downloads

### Decision 2a: Transcript Refresh Behavior

**Default behavior (`refresh_transcript: false`):**
- Refresh metadata fields only
- Auto-download transcript IF transcript metadata changed (new track available, language changed, etc.)

**Opt-in behavior (`refresh_transcript: true`):**
- Force re-download transcript even if metadata unchanged
- Use case: creator fixed typos in existing transcript

**Rationale:**
- Default is cheap (no unnecessary transcript downloads)
- Auto-refresh when metadata indicates new/changed transcript (analogous to thumbnail URL change)
- Explicit flag for "I know the content changed, re-fetch it"
- No content hashing or Last-Modified checks (unreliable across platforms, not worth complexity)

### Decision 3: DownloadArtifact Flags for Selective Downloads

Introduce a `Flag` enum to control which artifacts are downloaded:

```python
from enum import Flag, auto

class DownloadArtifact(Flag):
    NONE = 0
    MEDIA = auto()
    THUMBNAIL = auto()
    TRANSCRIPT = auto()
    ALL = MEDIA | THUMBNAIL | TRANSCRIPT
```

**Rationale:**
- Type-safe artifact selection
- Combinable via bitwise operations
- Self-documenting code

### Decision 4: Replace `_process_single_download` with Public `download_artifacts`

Remove the private `_process_single_download` and replace with a single public method that serves both internal batch processing and external selective downloads:

```python
async def download_artifacts(
    self,
    download: Download,
    feed_config: FeedConfig,
    artifacts: DownloadArtifact = DownloadArtifact.ALL,
    cookies_path: Path | None = None,
) -> ArtifactDownloadResult:
    """
    Download selected artifacts for a download.

    This is the single implementation for all download operations:
    - Called by download_queued with ALL for batch processing
    - Called by coordinator with THUMBNAIL for thumbnail-only refresh
    - Called by future endpoints with TRANSCRIPT for backfill

    Returns:
        ArtifactDownloadResult with success/failure for each requested artifact.
    """
```

**Rationale:**
- One method instead of two (`_process_single_download` + `download_artifacts`)
- `download_queued` calls `download_artifacts(dl, config, DownloadArtifact.ALL)`
- External callers use same method with specific flags
- Removes `download_thumbnail_for_existing_download` entirely

## Edge Cases

### 1. Media Exists but Transcript Missing

**Scenario:** Feed config now specifies `transcript_lang` but existing downloads have no transcript.

**Solution:** Call `download_transcript_for_existing_download` on DOWNLOADED items. Status remains DOWNLOADED—we're enriching metadata.

### 2. Failed Thumbnail Re-download

**Scenario:** `remote_thumbnail_url` unchanged but local file missing or `thumbnail_ext` is None.

**Solutions:**
- **On metadata refresh:** If URL unchanged but `thumbnail_ext` is None and `remote_thumbnail_url` exists, attempt thumbnail download
- **Future:** Optional admin endpoint `POST /admin/feeds/{feed_id}/downloads/{download_id}/refresh-thumbnail`

### 3. ARCHIVED Downloads

**Scenario:** Admin tries to refresh an ARCHIVED download.

**Solution:** Treat ARCHIVED as terminal. Return `400 Bad Request` or `409 Conflict` explaining that archived downloads cannot be refreshed.

### 4. Concurrent Refresh Requests

**Scenario:** Multiple refresh requests for the same download arrive simultaneously.

**Solution:**
- `requeue_downloads` is idempotent—calling on already-QUEUED is a no-op
- Metadata refresh + persist is safe—last write wins with no corruption
- Add appropriate logging for concurrent access visibility

### 5. ERROR Download Operations

**Scenario:** Admin wants to retry a failed download.

**Solution:** Use `requeue` endpoint (not `refresh-metadata`):
1. `requeue` validates download is in ERROR status
2. Resets ERROR → QUEUED via `requeue_downloads`
3. Triggers `manual_feed_runner.trigger()` to run full pipeline
4. Enqueue phase refreshes metadata and clears `retries`/`last_error`
5. Download phase attempts media download

**Note:** `refresh-metadata` on an ERROR download will update metadata but NOT change status or trigger re-download. Use `requeue` to retry.

### 6. Transcript-Only Download Fails

**Scenario:** Media exists, transcript download attempted but fails.

**Solution:**
- Log warning but don't change download status
- Return failure indicator to caller
- Leave transcript fields as-is (don't corrupt with partial data)

## Rejected Alternatives

### Alternative 1: Separate Download Task Queue

**Description:** Create a separate task queue/table for selective download operations (e.g., "download thumbnail for X", "download transcript for Y").

**Why Rejected:**
- Adds significant complexity (new table, new scheduler, new worker)
- Current problem can be solved with simpler refactoring
- Overkill for the scale of this application (self-hosted, small-scale)

### Alternative 2: Status-Based Artifact Tracking

**Description:** Add fields like `thumbnail_status`, `transcript_status` to track individual artifact states.

**Why Rejected:**
- Complicates the already sufficient `Download` model
- Current boolean fields (`thumbnail_ext`, `transcript_ext`) adequately indicate presence
- Would require migration and additional state machine complexity

### Alternative 3: Separate Transcript/Thumbnail Download Queues

**Description:** Introduce `QUEUED_THUMBNAIL`, `QUEUED_TRANSCRIPT` status values.

**Why Rejected:**
- Explodes the state machine
- Current approach (selective download methods) achieves the same goal more simply
- Status represents the download as a whole, not individual artifacts

## Integration Points

### DataCoordinator

- **Simplified:** `refresh_download_metadata`
  - Calls `Enqueuer.refresh_metadata()` (single method, handles fetch + persist)
  - Calls `Downloader.download_artifacts(THUMBNAIL)` if thumbnail needs refresh
  - Calls `Downloader.download_artifacts(TRANSCRIPT)` if transcript needs refresh or `refresh_transcript` flag set
  - Each component owns its DB writes (unidirectional flow)
- **Unchanged:** `process_feed`, `_execute_download_phase`

### Downloader

- **New type:** `DownloadArtifact` (Flag enum)
- **New type:** `ArtifactDownloadResult` (result dataclass)
- **Removed:** `_process_single_download` (replaced by `download_artifacts`)
- **Removed:** `download_thumbnail_for_existing_download` (replaced by `download_artifacts`)
- **New method:** `download_artifacts` (single public method for all download operations)
  - Handles its own DB writes for artifact fields (thumbnail_ext, transcript_ext, etc.)
- **Modified:** `download_queued` (calls `download_artifacts` with `ALL`)

### Admin Router

- **Renamed endpoint:** `POST .../requeue` (was `reset-errors`)
  - Requeues all ERROR downloads for feed, triggers pipeline
- **New endpoint:** `POST .../downloads/{download_id}/requeue`
  - Validates download is in ERROR status
  - Calls `download_db.requeue_downloads(feed_id, download_id, from_status=ERROR)`
  - Triggers `manual_feed_runner.trigger()`
  - Returns 202 with `was_requeued` indicator
- **Modified:** `refresh-metadata` endpoint
  - Adds optional `refresh_transcript: bool` query parameter (default false)
  - Calls `data_coordinator.refresh_download_metadata` (now with cleaner flow)
  - Response adds `transcript_refreshed: bool | None` field

### Enqueuer

- **New method:** `refresh_metadata` (replaces `fetch_refreshed_metadata` + `persist_refreshed_metadata`)
  - Fetches fresh metadata from yt-dlp
  - Merges with existing download
  - Persists to DB
  - Returns updated Download and change indicators (thumbnail_url_changed, transcript_metadata_changed)
- **Removed:** `fetch_refreshed_metadata`, `persist_refreshed_metadata` (consolidated)

### YtdlpWrapper

- **Potentially new:** `download_transcript_only` method (if needed for transcript-only path)
- **Unchanged:** `download_media_to_file`, `download_media_thumbnail`

## Implementation Plan

### Phase 1: Flexible Downloader

- [ ] Add `DownloadArtifact` flag enum and `ArtifactDownloadResult` dataclass to `src/anypod/data_coordinator/types/`
  - Define NONE, MEDIA, THUMBNAIL, TRANSCRIPT, ALL flags
  - Result tracks success/failure per artifact type

- [ ] Create `download_artifacts` method in `Downloader` (replaces `_process_single_download`)
  - Public method with `artifacts: DownloadArtifact = DownloadArtifact.ALL` parameter
  - Branch logic based on which artifacts are requested:
    - MEDIA path: existing `download_media_to_file` logic + success handling
    - THUMBNAIL path: inline existing `download_thumbnail_for_existing_download` logic
    - TRANSCRIPT path: new logic (may need `YtdlpWrapper.download_transcript_only`)
  - Handles its own DB writes for artifact fields (thumbnail_ext, transcript_ext, status, etc.)
  - Returns `ArtifactDownloadResult` with per-artifact success/failure
  - Default `ALL` preserves existing behavior

- [ ] Update `download_queued` to call `download_artifacts`
  - Replace `_process_single_download` call with `download_artifacts(dl, config, DownloadArtifact.ALL)`
  - Verify all existing tests pass unchanged

- [ ] Remove `_process_single_download` method
  - Logic moved to `download_artifacts`

- [ ] Remove `download_thumbnail_for_existing_download` method
  - Update coordinator to use `download_artifacts(download, config, DownloadArtifact.THUMBNAIL)`

- [ ] Add/extend `YtdlpWrapper` if transcript-only download requires new method
  - Research yt-dlp options for transcript-only download
  - Implement `download_transcript_only` if needed

- [ ] Add tests for flexible downloader
  - Test media-only, thumbnail-only, transcript-only downloads
  - Test combined artifact downloads
  - Test failure scenarios for each artifact type
  - Verify existing `download_queued` tests still pass

### Phase 2: Simplify refresh-metadata Flow

- [ ] Create `Enqueuer.refresh_metadata` method (consolidates fetch + persist)
  - Fetch fresh metadata from yt-dlp (existing logic from `fetch_refreshed_metadata`)
  - Merge with existing download (existing logic)
  - Persist to DB (existing logic from `persist_refreshed_metadata`)
  - Return `RefreshMetadataResult(download, thumbnail_url_changed, transcript_metadata_changed)`
  - `transcript_metadata_changed` = True if ext/lang/source differs from existing

- [ ] Remove `fetch_refreshed_metadata` and `persist_refreshed_metadata`
  - Consolidated into `refresh_metadata`

- [ ] Simplify `DataCoordinator.refresh_download_metadata`
  - Accept `refresh_transcript: bool` parameter
  - Call `Enqueuer.refresh_metadata()` → returns Download + change indicators
  - If `thumbnail_url_changed` or missing thumbnail, call `Downloader.download_artifacts(THUMBNAIL)`
  - If `refresh_transcript` flag or `transcript_metadata_changed`, call `Downloader.download_artifacts(TRANSCRIPT)`
  - Return result with `thumbnail_refreshed` and `transcript_refreshed` indicators

- [ ] Update `refresh-metadata` endpoint
  - Add optional `refresh_transcript: bool` query parameter (default false)
  - Pass to coordinator
  - Add `transcript_refreshed: bool | None` to response

- [ ] Update tests for simplified refresh-metadata flow
  - Verify metadata fields written by enqueuer
  - Verify thumbnail download triggered on URL change
  - Verify transcript download triggered on metadata change
  - Verify transcript download triggered on explicit flag
  - Verify existing behavior preserved

### Phase 3: Add requeue Endpoints

- [ ] Rename `POST /admin/feeds/{feed_id}/reset-errors` to `POST /admin/feeds/{feed_id}/requeue`
  - Update endpoint path
  - Update response model name to `RequeueResponse`
  - Behavior unchanged (requeue all ERROR downloads, trigger pipeline)

- [ ] Add `POST /admin/feeds/{feed_id}/downloads/{download_id}/requeue` endpoint
  - Validate download exists
  - Validate download is in ERROR status (return 400 if not)
  - Call `download_db.requeue_downloads(feed_id, download_id, from_status=ERROR)`
  - Trigger `manual_feed_runner.trigger(feed_id, feed_config)`
  - Return 202 Accepted with `RequeueResponse(feed_id, download_id, was_requeued)`

- [ ] Add `RequeueResponse` model (shared by both endpoints)
  - Fields: `feed_id`, `download_id` (optional for feed-level), `requeue_count` or `was_requeued`

- [ ] Add tests for download-level requeue endpoint
  - Test ERROR → QUEUED transition + pipeline trigger
  - Test non-ERROR download returns 400
  - Test download not found returns 404
  - Test ARCHIVED download returns 400

### Phase 4: Documentation

- [ ] Update API documentation in `README.md`
  - Document renamed `requeue` endpoint (feed-level)
  - Document new `requeue` endpoint (download-level)
  - Document `refresh-metadata` behavior (sync metadata, no status change)
