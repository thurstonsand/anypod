# Persist RSS XML to Disk

## Overview

Persist RSS feed XML files to the data directory and serve them like media/images. Today, RSS XML is generated on demand and cached in memory, and the HTTP route `/feeds/{feed_id}.xml` serves from the in-memory cache. This plan adds durable, on-disk XML files while keeping generation semantics and error handling consistent with the rest of the system.

## Goals

- Store RSS XML at stable, per-feed file paths under the data directory.
- Serve RSS XML via FastAPI from the filesystem (like media/images).
- Keep generation controlled by the coordinator (no generation-on-request).
- Use atomic write (tmp → rename) and structured error handling.
- Integrate with existing `PathManager`/`FileManager` patterns.
- Minimize changes to existing behavior and tests where practical.

## Current State Summary

- Generation: `RSSFeedGenerator.update_feed(feed_id, feed)` builds XML and caches bytes in-memory.
  - Files: `src/anypod/rss/rss_feed.py`, `src/anypod/rss/feedgen_core.py`
  - Uses `FeedgenCore` for type-safe feedgen integration
  - Logs an informational message including `paths.feed_media_url(feed_id)`
- Serving: `/feeds/{feed_id}.xml` returns `rss_generator.get_feed_xml(feed_id)` from memory.
  - File: `src/anypod/server/routers/static.py: serve_feed`
- Storage: Media and images are stored/served from disk via `PathManager` + `FileManager`.
  - Files: `src/anypod/path_manager.py`, `src/anypod/file_manager.py`
  - Patterns: async path builders that ensure directories, `aiofiles` for IO, and atomic moves
- Orchestration: DataCoordinator runs phases and calls `rss_generator.update_feed` after enqueue/download/prune.
  - File: `src/anypod/data_coordinator/coordinator.py` (RSS phase)

## Target Data Layout

Mirror media/images layout with a dedicated `feeds/` directory under the data root:

```
/data/
├── media/{feed_id}/{download_id}.{ext}
├── images/{feed_id}.jpg
├── images/{feed_id}/downloads/{download_id}.jpg
└── feeds/{feed_id}.xml   # NEW: persisted RSS XML
```

Notes:
- Keep `feed_url(feed_id)` returning `BASE_URL/feeds/{feed_id}.xml` (already present).
- Add file-path resolution and directory creation for `feeds/` like media/images.

## API and Component Changes

### PathManager

Add feed XML storage helpers to match existing async path methods.

- New property: `base_feeds_dir: Path` → `self._base_data_dir / "feeds"`
- New method: `async def feed_xml_path(self, feed_id: str) -> Path`
  - Validate `feed_id`
  - Ensure `base_feeds_dir` exists
  - Return `<base_feeds_dir>/{feed_id}.xml`

No change to `feed_url(feed_id)` (continues to return the HTTP URL).

### FileManager

Add retrieval helpers for feed XML similar to media/images.

- New method: `async def get_feed_xml_path(self, feed_id: str) -> Path`
  - Build via `paths.feed_xml_path(feed_id)`
  - Check `isfile`, raise `FileNotFoundError` if missing
  - Raise `FileOperationError` for OS-level path checks
- New method: `async def feed_xml_exists(self, feed_id: str) -> bool`
  - Thin wrapper that returns False on `FileNotFoundError`

### RSSFeedGenerator

Persist XML to disk as part of `update_feed`; remove reliance on in-memory caching. The filesystem is the source of truth.

- After generating `xml_bytes`, write to a tmp file under the feed’s tmp dir and atomically rename to the final `feeds/{feed_id}.xml` path.
- Do not maintain an in-memory feed cache. If a helper is kept for compatibility, it should read bytes from disk (not cache) or be removed entirely.

Sketch:

```python
async def update_feed(self, feed_id: str, feed: Feed) -> None:
    downloads = await self._get_feed_downloads(feed_id)
    xml_bytes = (
        FeedgenCore(paths=self._paths, feed_id=feed_id, feed=feed)
        .with_downloads(downloads)
        .xml()
    )

    # Persist to disk (atomic)
    tmp_path = await self._paths.tmp_file(feed_id)
    final_path = await self._paths.feed_xml_path(feed_id)
    async with aiofiles.open(tmp_path, "wb") as f:
        await f.write(xml_bytes)
    await aiofiles.os.replace(tmp_path, final_path)
```

### Static Router: `/feeds/{feed_id}.xml`

Serve from filesystem using `FileManager`, matching media/images patterns.

- Replace use of `rss_generator.get_feed_xml` with `file_manager.get_feed_xml_path` and `FileResponse`.
- Preserve headers: `Cache-Control: public, max-age=300` and media type `application/rss+xml`.
- Keep HEAD support.

Sketch:

```python
@router.api_route("/feeds/{feed_id}.xml", methods=["GET", "HEAD"])
async def serve_feed(feed_id: ValidatedFeedId, file_manager: FileManagerDep) -> FileResponse:
    try:
        path = await file_manager.get_feed_xml_path(feed_id)
    except FileNotFoundError:
        raise HTTPException(404, "Feed not found")
    except FileOperationError:
        raise HTTPException(500, "Internal server error")

    return FileResponse(
        path=path,
        media_type="application/rss+xml",
        headers={"Cache-Control": "public, max-age=300"},
    )
```

### DataCoordinator

No API changes. `update_feed` persists to disk as part of the RSS phase. The RSS phase remains responsible for updating `last_successful_sync` and `mark_rss_generated`.

## Concurrency, Atomicity, and Errors

- Use tmp + rename for atomic replacement of the XML file to avoid partial writes.
- Preserve current error taxonomy:
  - Database failures → `RSSGenerationError` from `_get_feed_downloads`
  - IO failures during persist → wrap as `RSSGenerationError` with `feed_id`
  - Router maps to 404/500 using `FileNotFoundError` vs `FileOperationError`

## Backward Compatibility

- `feed_url(feed_id)` unchanged.
- `get_feed_xml(feed_id)` will be removed or refactored to read from disk. Router serves from disk.
- `/feeds` listing remains DB-driven (enabled feeds). After first successful generation, persisted XML eliminates 404s across restarts. New feeds will 404 until their first generation completes.

## Testing Impact

Update tests that rely on the old serving path. Generation tests can remain unchanged.

- `tests/anypod/test_rss_feed.py`
  - Update to assert on persisted file contents instead of in-memory cache.
  - Pattern: call `await rss_generator.update_feed(...)`, then `path = await paths.feed_xml_path(feed_id)`, then `xml_bytes = path.read_bytes()` for XML parsing.

- `tests/anypod/server/routers/test_static.py`
  - Update `/feeds/{feed_id}.xml` tests to use `FileManager` instead of `RSSFeedGenerator`:
    - `test_serve_feed_success`: patch `FileResponse`, mock `get_feed_xml_path` to return a `Path`, assert media_type `application/rss+xml`, headers include `Cache-Control: public, max-age=300`.
    - `test_serve_feed_not_found`: mock `get_feed_xml_path` to raise `FileNotFoundError`, expect 404.
    - `test_serve_feed_invalid_ids_rejected`: ensure invalid IDs are rejected before hitting `FileManager`.

- Optional new unit coverage (if desired):
  - `PathManager.feed_xml_path` validation/creation behavior
  - `FileManager.get_feed_xml_path` existence/error mapping

Integration tests:
- No changes required unless they assert on `serve_feed` internals. End-to-end should remain green as routes and output are stable.

## Implementation Steps

1. PathManager
   - Add `base_feeds_dir` and `feed_xml_path(feed_id)` (async, mkdirs, validation)
2. FileManager
   - Add `get_feed_xml_path(feed_id)` and `feed_xml_exists(feed_id)`
3. RSSFeedGenerator
   - Extend `update_feed` to write XML to tmp and rename to `feeds/{feed_id}.xml`
   - Wrap IO failures as `RSSGenerationError(feed_id=...)`
4. Static Router
   - Switch `/feeds/{feed_id}.xml` to `FileResponse` served via `FileManager`
5. Tests
   - Update `tests/anypod/server/routers/test_static.py` feed tests to mock `FileManager`
   - Optionally add unit tests for new PathManager/FileManager methods

## Notes and Follow-ups

- Startup bootstrap: No longer required for avoiding 404s across restarts because XML files persist. New feeds still 404 until first generation.
- ETag/Last-Modified: Add strong caching support on `/feeds/{feed_id}.xml`.
  - Compute a weak ETag from file metadata, e.g., `W/"{st.st_mtime_ns:x}-{st.st_size:x}"`.
  - Set `Last-Modified` from `stat().st_mtime` (UTC) and include `Cache-Control`.
  - On requests with `If-None-Match`, compare ETag and return `304 Not Modified` with headers if matched.
  - If `If-None-Match` absent but `If-Modified-Since` present, compare against file mtime, return 304 if not modified.
  - Implementation detail: perform conditional check before constructing `FileResponse` to avoid reading body when serving 304.
  - Optional: apply the same headers to media/images responses for parity (not required for this change).
- Cleanup: If a feed is disabled/deleted, we could consider removing its XML file; not required for this change.
