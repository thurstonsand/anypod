# Patreon Download Filtering Analysis

**Date:** 2025-10-07
**Feed:** `https://patreon.com/LemonadeStand`
**Issue:** Only 6 downloads queued instead of expected 26, with download failures

---

## Executive Summary

The current Patreon implementation has three interconnected issues:

1. **Multi-attachment posts are not properly handled** - Posts with both audio and video files are treated as playlists by yt-dlp, resulting in download failures when using strict format filters
2. **Format filter incompatibility** - The user's `-f worst[ext=mp4]` format selector fails on audio-only entries in multi-attachment posts
3. **Missing playlist item selection** - The code doesn't specify which item to download from multi-attachment posts, leading to attempted downloads of all playlist items

---

## Current State Analysis

### Feed Statistics

- **Total posts:** 84
- **Video-capable posts (with vcodec):** 26
- **Currently queued in database:** 6
- **Currently downloaded:** 1
- **Access-restricted posts:** ~28 (estimated from logs)

### Post Structure Types

#### 1. Video-only posts
- Single entry with video formats
- Example: ID `140595551`
- Works correctly with current implementation

#### 2. Audio-only posts
- Single entry with audio format (m4a)
- Gets filtered out by `--match-filter "vcodec"` during metadata fetch ✓
- Would fail with `-f worst[ext=mp4]` during download (no mp4 available)

#### 3. Text-only posts
- No media attachments
- Gets filtered out by `--match-filter "vcodec"` during metadata fetch ✓

#### 4. Video+Audio posts (PROBLEMATIC)
- **Structure:** yt-dlp returns `"_type": "playlist"` with 2 entries:
  - Entry 1: Audio file (`.m4a`) - ID format `{post_id}-1` (e.g., `137318294-1`)
  - Entry 2: Video file (`.mp4`) - ID format `{post_id}` (e.g., `137318294`)
- **Example:** Post ID `137318294` "Ep. 20 Premium - Immigrating, Income Taxes..."
- **Current behavior:**
  - Metadata phase with `--dump-json --match-filter "vcodec"`: Returns ONLY the video entry ✓
  - Download phase with `-f worst[ext=mp4]`: Attempts to download BOTH entries, fails on audio entry with "Requested format is not available" ✗

---

## Root Cause Analysis

### Issue 1: Metadata vs Download Behavior Discrepancy

**Metadata extraction** (in `fetch_new_downloads_metadata`):
```bash
yt-dlp --dump-json --skip-download --match-filter "vcodec" <url>
```
- Returns: 26 entries (only video entries, audio-only companions filtered out)
- The `PatreonHandler.filter_download_entries()` further deduplicates by `playlist_id`
- **Result:** Should create 26 Download objects

**Actual download** (in `download_media_to_file`):
```bash
yt-dlp -f "worst[ext=mp4]" <post_url>
```
- For multi-attachment posts, tries to download ALL playlist items
- Audio entries fail because they don't have mp4 format available
- **Result:** Download failures for 20 posts

### Issue 2: PatreonHandler Filtering Logic Gap

The `filter_download_entries()` method correctly handles deduplication for metadata:

```python
def filter_download_entries(
    self,
    entries: list[YtdlpInfo],
    feed_id: str,
) -> list[YtdlpInfo]:
    """Return one attachment per post, only accepting video-capable entries."""
    filtered_entries: list[YtdlpInfo] = []
    seen_playlists: set[str] = set()

    for raw_entry in entries:
        entry = PatreonEntry(raw_entry, feed_id)
        playlist_id = entry.playlist_id

        if playlist_id is None:
            filtered_entries.append(raw_entry)
            continue

        if playlist_id in seen_playlists:
            continue

        if not entry.has_video:
            continue

        filtered_entries.append(raw_entry)
        seen_playlists.add(playlist_id)

    return filtered_entries
```

**Problem:** This filters the metadata correctly, but when we later try to download from the individual post URL, yt-dlp sees it as a playlist again and tries to download both items.

### Issue 3: No Playlist Item Selection During Download

When downloading a specific post URL that yt-dlp treats as a playlist, the code doesn't tell yt-dlp which playlist item to download. The format selector `-f worst[ext=mp4]` is applied to each item independently, failing on the audio item.

---

## Test Results

### Test 1: Metadata extraction with match-filter
```bash
uvx yt-dlp --cookies cookies.txt --dump-json --skip-download --match-filter "vcodec" \
  "https://patreon.com/LemonadeStand" 2>&1 | grep -E "^{" | wc -l
```
**Result:** 26 entries ✓

### Test 2: Single video+audio post metadata
```bash
uvx yt-dlp --cookies cookies.txt --dump-json --skip-download --match-filter "vcodec" \
  "https://www.patreon.com/posts/ep-20-premium-137318294"
```
**Result:** 1 entry (ID: `137318294`, ext: `mp4`) ✓
**Note:** Audio entry (`137318294-1`) correctly filtered out

### Test 3: Single video+audio post download with strict format
```bash
uvx yt-dlp --cookies cookies.txt -f "worst[ext=mp4]" --skip-download \
  "https://www.patreon.com/posts/ep-20-premium-137318294"
```
**Result:**
```
[download] Downloading item 1 of 2
ERROR: [patreon] 137318294-1: Requested format is not available.
[download] Downloading item 2 of 2
[info] 137318294: Downloading 1 format(s): 561
```
**Analysis:** Attempts both playlist items, fails on audio ✗

### Test 4: Full feed download with strict format
```bash
uvx yt-dlp --cookies cookies.txt -f "worst[ext=mp4]" --skip-download \
  "https://patreon.com/LemonadeStand" 2>&1 | grep -E "ERROR.*Requested format" | wc -l
```
**Result:** 20 format errors (matches the ~20 video+audio posts)

### Test 5: Flexible format selector
```bash
uvx yt-dlp --cookies cookies.txt -f "worst[ext=mp4]/worst[ext=m4a]/worst" --skip-download \
  "https://www.patreon.com/posts/ep-20-premium-137318294"
```
**Result:**
```
[download] Downloading item 1 of 2
[info] 137318294-1: Downloading 1 format(s): 0
[download] Downloading item 2 of 2
[info] 137318294: Downloading 1 format(s): 561
```
**Analysis:** Downloads BOTH files (not desired - breaks one-media-per-post model)

---

## Proposed Solution

### Strategy: Playlist Item Selection via `--playlist-items` flag

**Core Issue:** `--no-playlist` and `--match-filter` don't work during the download phase - they still attempt to download all playlist items for multi-attachment posts.

**Solution:** Use yt-dlp's `--playlist-items <index>` (or `-I` shorthand) flag to download only the specific video item from multi-attachment posts. The metadata already contains `playlist_index` which identifies which item is the video.

### Test Results for Alternative Approaches

#### Test: `--no-playlist` doesn't filter multi-attachment posts
```bash
uvx yt-dlp --cookies cookies.txt --dump-json --skip-download --no-playlist \
  "https://www.patreon.com/posts/ep-20-premium-137318294" 2>&1 | grep -E "^{" | wc -l
```
**Result:** 2 entries (still returns both audio and video) ✗

#### Test: `--playlist-items <index>` selects specific playlist item
```bash
uvx yt-dlp --cookies cookies.txt --skip-download -f "worst[ext=mp4]" --playlist-items 2 \
  "https://www.patreon.com/posts/ep-20-premium-137318294"
```
**Result:**
```
[download] Downloading item 1 of 1
[info] 137318294: Downloading 1 format(s): 561
```
**Analysis:** Successfully downloads ONLY the video item ✓

#### Test: Metadata contains playlist_index
```bash
uvx yt-dlp --cookies cookies.txt --dump-json --skip-download --match-filter "vcodec" \
  "https://www.patreon.com/posts/ep-20-premium-137318294"
```
**Result:**
```json
{
  "id": "137318294",
  "playlist_index": 2,
  "playlist_id": "137318294",
  ...
}
```
**Analysis:** The video entry has `playlist_index: 2` ✓

---

## Recommended Solution

**Store `playlist_index` in Download model and use `--playlist-items` during download**

This approach:
1. Captures `playlist_index` during metadata extraction
2. Persists it in the database
3. Uses `--playlist-items <index>` during download to select the correct playlist item

### Implementation Steps

#### 1. Add `playlist_index` field to Download model

**File:** `src/anypod/db/types/download.py`

```python
class Download(SQLModel, table=True):
    """Represent a download.

    Attributes:
        ...
        playlist_index: Optional index of item within a multi-attachment post.
        ...
    """

    # ... existing fields ...

    # Optional media metadata
    remote_thumbnail_url: str | None = None
    thumbnail_ext: str | None = None
    description: str | None = None
    quality_info: str | None = None
    playlist_index: int | None = None  # NEW FIELD
```

#### 2. Create Alembic migration

**Command:**
```bash
uv run alembic revision -m "Add playlist_index to download table"
```

**Migration file:**
```python
"""Add playlist_index to download table

Revision ID: <generated>
Revises: <previous>
Create Date: 2025-10-07
"""
from alembic import op
import sqlalchemy as sa


def upgrade() -> None:
    op.add_column('download', sa.Column('playlist_index', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('download', 'playlist_index')
```

#### 3. Update PatreonHandler to extract and store playlist_index

**File:** `src/anypod/ytdlp_wrapper/handlers/patreon_handler.py`

Add property to `PatreonEntry`:
```python
@property
def playlist_index(self) -> int | None:
    """Get the playlist index for this entry (1-based)."""
    with self._annotate_exceptions():
        return self._ytdlp_info.get("playlist_index", int)
```

Update `extract_download_metadata()`:
```python
async def extract_download_metadata(
    self,
    feed_id: str,
    ytdlp_info: YtdlpInfo,
) -> Download:
    """Extract metadata from a single Patreon post into a Download object."""
    # ... existing code ...

    parsed_download = Download(
        feed_id=feed_id,
        id=entry.download_id,
        source_url=source_url,
        title=entry.title,
        published=published_dt,
        ext=ext,
        mime_type=mime_type,
        filesize=entry.filesize,
        duration=duration,
        status=DownloadStatus.QUEUED,
        remote_thumbnail_url=entry.thumbnail,
        description=entry.description,
        quality_info=entry.quality_info,
        playlist_index=entry.playlist_index,  # NEW FIELD
    )

    return parsed_download
```

#### 4. Update PatreonHandler download args to use playlist_index

**File:** `src/anypod/ytdlp_wrapper/handlers/patreon_handler.py`

Add new method signature to handler protocol:
```python
def prepare_media_download_args(
    self,
    args: YtdlpArgs,
    download: Download,  # NEW: Pass full Download object
) -> YtdlpArgs:
    """Apply Patreon referer and playlist item selection for media downloads."""
    args = args.referer(_PATREON_REFERER)

    # Use playlist_index to download specific item from multi-attachment posts
    if download.playlist_index is not None:
        args = args.playlist_items(download.playlist_index)

    return args
```

#### 5. Add playlist_items method to YtdlpArgs

**File:** `src/anypod/ytdlp_wrapper/core/args.py`

```python
def playlist_items(self, item_spec: str | int) -> Self:
    """Select specific playlist items using --playlist-items flag.

    Args:
        item_spec: Playlist item specification. Can be:
            - Single index (int or str): "2" or 2
            - Range: "1:5"
            - Multiple: "1,3,5"
            - Complex: "1:5,7,9:11"

    Examples:
        args.playlist_items(2)          # Download item 2
        args.playlist_items("1:5")      # Download items 1-5
        args.playlist_items("1,3,5")    # Download items 1, 3, and 5
    """
    self._args.extend(["--playlist-items", str(item_spec)])
    return self
```

#### 6. Update YtdlpWrapper to pass Download object to handler

**File:** `src/anypod/ytdlp_wrapper/ytdlp_wrapper.py`

```python
async def download_media_to_file(
    self,
    download: Download,
    user_yt_cli_args: list[str],
    cookies_path: Path | None = None,
) -> Path:
    """Download the media for a given Download to a target directory."""
    # ... existing setup code ...

    handler = self._handler_selector.select(download.source_url)
    download_args = handler.prepare_media_download_args(
        download_args,
        download,  # NEW: Pass full Download object
    )

    # ... rest of download logic ...
```

#### 7. Update YouTube handler signature for compatibility

**File:** `src/anypod/ytdlp_wrapper/handlers/youtube_handler.py`

```python
def prepare_media_download_args(
    self,
    args: YtdlpArgs,
    download: Download,  # NEW: Accept Download but don't use it
) -> YtdlpArgs:
    """Apply POT provider args for YouTube media downloads."""
    # YouTube doesn't use playlist_index, so we ignore the download parameter
    return args
```

#### 8. Update base handler protocol

**File:** `src/anypod/ytdlp_wrapper/handlers/base_handler.py`

```python
from typing import Protocol
from ...db.types import Download

class SourceHandlerBase(Protocol):
    """Protocol defining the interface for source-specific handlers."""

    def prepare_media_download_args(
        self,
        args: YtdlpArgs,
        download: Download,
    ) -> YtdlpArgs:
        """Prepare yt-dlp args for media download.

        Args:
            args: Base YtdlpArgs to modify.
            download: The Download object being processed (contains playlist_index, etc).

        Returns:
            Modified YtdlpArgs for the download operation.
        """
        ...
```

### Why This Solution?

1. **Semantically correct** - `playlist_index` is actual metadata about the download's position
2. **Clean separation** - Metadata phase captures index, download phase uses it
3. **Nullable/optional** - When `playlist_index` is `None`, no `-I` flag is added (works for single-item posts)
4. **Future-proof** - Handles any playlist structure (not just 2-item posts)
5. **No URL pollution** - Keeps source_url clean and readable
6. **Preserves user format control** - Format selectors still work as expected

### Migration Safety

- Field is nullable, so existing downloads continue to work
- Downloads without `playlist_index` simply don't get the `--playlist-items` flag
- YouTube downloads unaffected (they don't set `playlist_index`)
- Downward compatible - old code paths work, new code paths only activate when field is present

### Additional Considerations

**Access Control Posts:** The logs show ~28 "You do not have access to this post" errors. These are expected for posts at higher patron tiers and should be handled gracefully (already are - they just get logged and skipped).

**Database State:** Currently has 6 QUEUED + 1 DOWNLOADED. After fix, should have 26 QUEUED (or fewer if some are access-restricted).

---

## Verification Plan

After implementing the solution:

1. **Clear test database:**
   ```bash
   rm tmpdata/db/anypod.db
   ```

2. **Run with `--keep` flag:**
   ```bash
   timeout 30 ./scripts/run_dev.sh --keep
   ```

3. **Verify database state:**
   ```sql
   SELECT COUNT(*), status FROM download WHERE feed_id='patreon' GROUP BY status;
   ```
   **Expected:** ~26 QUEUED (or fewer due to access restrictions)

4. **Monitor download phase:** Check that downloads succeed without "Requested format is not available" errors

5. **Verify file count:**
   ```bash
   ls tmpdata/media/patreon/ | wc -l
   ```
   **Expected:** Should match number of DOWNLOADED entries (1 file per post)

---

## Appendix: Example Post Metadata

### Video+Audio Post (137318294)
```json
{
  "id": "137318294",
  "title": "Ep. 20 Premium - Immigrating, Income Taxes, and Investing [VIDEO]",
  "_type": "playlist",
  "entries": [
    {
      "id": "137318294-1",
      "ext": "m4a",
      "filesize": 114056813,
      "playlist_id": "137318294",
      "vcodec": "unknown",
      "acodec": "unknown"
    },
    {
      "id": "137318294",
      "ext": "mp4",
      "formats": [...],
      "playlist_id": "137318294",
      "vcodec": "avc1.64002a",
      "acodec": "mp4a.40.2"
    }
  ]
}
```

The `filter_download_entries()` logic keeps only the second entry (has video), but when downloading from the post URL, yt-dlp sees both entries again.
