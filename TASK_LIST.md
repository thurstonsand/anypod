# Anypod – Phase 1 Task List

This checklist covers everything required to reach a functional MVP that aligns with the design document. Tasks are ordered to minimise back-tracking and maximise fast feedback while you work in **Cursor**.

---

## 0  Repo Bootstrap
- [x] **Init git repo** – `git init --initial-branch=main && gh repo create`.
- [x] **`pyproject.toml`** – minimal project metadata, `uv` backend, Python ≥ 3.13.
- [x] **`uv pip install --groups dev`** – add dev deps: `ruff`, `pytest`, `pytest-asyncio`, `mypy`, `pre-commit`.
- [x] **Pre-commit hooks** – formatters & linter.
- [x] **CI** – GitHub Actions workflow running `pytest` on every PR.

## 1  Package Skeleton
```text
src/
    anypod/
        __init__.py
        cli.py                # entry‑point
        config.py             # Pydantic models & loader
        db.py                 # SQLite helpers & migrations, Download model (with from_row)
        file_manager.py       # FileManager implementation
        data_coordinator/     # Orchestrates DB + FS + Download operations
            __init__.py
            coordinator.py      # Main DataCoordinator class (orchestrator)
            enqueuer.py         # Enqueuer class (fetches metadata & enqueues)
            downloader.py       # Downloader class (processes download queue)
            pruner.py           # Pruner class (handles pruning logic)
        ytdlp_wrapper.py      # yt‑dlp direct wrapper
        scheduler.py          # APScheduler initialisation
        worker.py             # cron‑triggered job logic (delegates to DataCoordinator)
        feedgen.py            # thin wrapper
        http.py               # FastAPI app + routing
        utils.py              # misc helpers
        exceptions.py         # Custom exceptions
```

```
 tests/
    anypod/ # tests mirror the src/anypod structure
        integration/ # integration tests can be run with `pytest --integration`
```

## 2  Configuration Loader
- [x] Pydantic V2 models reflecting YAML keys.
- [x] `load_config(path) -> dict[str, FeedConfig]` (implemented via Pydantic Settings).
- [x] Unit tests using fixture YAML.

## 3  Database Layer
- [x] CRUD helpers:
  - [x] `add_download`
  - [x] `update_status`
  - [x] `next_queued_downloads`
  - [x] `get_download_by_id`
  - [x] `get_errors`
  - [x] `get_downloads_to_prune_by_keep_last`
  - [x] `get_downloads_to_prune_by_since`
  - [x] `delete_downloads`
- [x] Tests with tmp in-memory DB.
- [x] Tests to make sure db access is optimized (e.g. uses indexes)

## 3.2 File Manager Layer
- [x] Abstraction seam: encapsulate base directory so future S3/GCS back‑ends can subclass
- [x] Implement `save_download_file(feed, file_name, data_stream) -> Path` (atomic write)
- [x] Implement `delete_download_file(feed, file_name) -> bool`
- [x] Implement `download_exists(feed, file_name) -> bool`
- [x] Implement `get_download_stream(feed, file_name) -> IO[bytes]`
- [x] Ensure directory hygiene: base download and feed directories exist (covered by save_download_file)
- [x] Write unit tests (tmp dir fixtures)

## 3.5 Data Orchestration & Services Layer
This section details the components that manage the lifecycle of downloads, from discovery to storage and pruning. They are organized into a `data_coordinator` module and related services/wrappers.

### 3.5.1 `db.py::Download` model updates
- [x] `from_row(cls, db_row: sqlite3.Row) -> Download` class method for mapping.

### 3.5.2 `YtdlpWrapper` (`ytdlp_wrapper.py`)
- [x] Create `ytdlp_wrapper.py` and class `YtdlpWrapper`.
- [x] `YtdlpWrapper.fetch_metadata(feed_id: str, url: str, yt_cli_args: list[str]) -> list[Download]`: Fetches metadata for all downloads at the given URL using yt-dlp's metadata extraction capabilities.
- [x] `YtdlpWrapper.download_media_to_file(download: Download, yt_cli_args: list[str], download_target_dir: Path) -> Path`:
    - Purpose: Downloads media (video or audio) for the given entry to a specified directory, handling potential merges (e.g., video + audio) via `yt-dlp` and FFmpeg.
    - Arguments:
        - `download`: Metadata of the entry to download, used for naming and context.
        - `yt_cli_args`: List of command-line arguments for `yt-dlp` (e.g., format selection from feed config).
        - `download_target_dir`: The base directory where the feed-specific subfolder and media file will be created.
    - Returns: `Path` to the successfully downloaded media file.
- [x] Sandbox test using a Creative-Commons short video, covered by integration tests using real URLs.
- [x] Unit tests for `YtdlpWrapper`.

### 3.5.2.1 Logger Side Quest
- [x] implement a global logging framework

### 3.5.3 `Enqueuer` (`data_coordinator/enqueuer.py`)
- [x] Constructor accepts `DatabaseManager`, `YtdlpWrapper`.
- [x] `enqueue_new_downloads(feed_config: FeedConfig) -> int`:
    - Phase 1: Re-fetch metadata for existing DB entries with status 'upcoming'; update those now VOD to 'queued'.
    - Phase 2: Fetch metadata for latest N videos via `YtdlpWrapper.fetch_metadata()`.
        - For each download not in DB:
            - If VOD (`live_status=='not_live'` and `is_live==False`), insert with status 'queued'.
            - If live or scheduled (`live_status=='upcoming'` or `is_live==True`), insert with status 'upcoming'.
        - For each existing 'upcoming' entry now VOD, update status to 'queued'.
    - Returns count of newly enqueued or transitioned-to-queued downloads.
- [x] preprocess cli args so you dont do it every time
- [x] handle cookies differently? they need to be included even at discovery stage
  - just include all args for all stages, including discovery; user will have limitations that need to be outlined in docs
- [x] db should not leak any details about sqlite -- should abstract all that away
    - for example, remove all references to sqlite.Row, sqlite.Error
- [x] retries should apply more widely, and with enough failures, should transition to error state
    - maybe db.py needs a `bump_error_count` fn that handles this - bumps it until it becomes too high, then marks as error
- [x] Unit tests for `Enqueuer` with mocked dependencies.
- [ ] a couple integration tests

### 3.5.3.1 TODO Side Quest
- [x] **Refactor Download Status Management (State Machine Implementation)**
    - **Phase 1: Database Layer (`db.py`)**
        - [x] Remove the existing `DatabaseManager.update_status` method.
        - [x] Implement `DatabaseManager.mark_as_queued_from_upcoming(feed: str, id: str) -> None`:
            - Checks that the current status is in fact `UPCOMING`
            - Sets `status = QUEUED`.
            - Preserves `retries` and `last_error`.
        - [x] Implement `DatabaseManager.requeue_download(feed: str, id: str) -> None`:
            - Add a note that this will happen due to:
              - manually requeueing an ERROR'd download,
              - manually requeueing in order to get the latest version of a download (i.e. it was previously DOWNLOAD)
              - un-SKIPping a video
              - don't implement this as logic, but just as a note on the docstring
            - Sets `status = QUEUED`.
            - Sets `retries = 0`, `last_error = NULL`.
        - [x] Implement `DatabaseManager.mark_as_downloaded(feed: str, id: str) -> None`:
            - Sets `status = DOWNLOADED`.
            - Sets `retries = 0`, `last_error = NULL`.
        - [x] Implement `DatabaseManager.skip_download(feed: str, id: str) -> None`:
            - Sets `status = SKIPPED`.
            - Preserves `retries` and `last_error`.
            - Raises `DownloadNotFoundError` or `DatabaseOperationError` on failure.
        - [x] Implement `DatabaseManager.unskip_download(feed_id: str, download_id: str) -> DownloadStatus`:
            - Checks that the download is currently `SKIPPED`.
            - Calls `requeue_download(feed_id, download_id)`.
            - Returns `DownloadStatus.QUEUED`.
            - Raises `DownloadNotFoundError` or `DatabaseOperationError` on failure.
        - [x] Implement `DatabaseManager.archive_download(feed: str, id: str) -> None`:
            - Sets `status = ARCHIVED`.
            - Preserves `retries` and `last_error`.
            - Raises `DownloadNotFoundError` or `DatabaseOperationError` on failure.
        - [x] Modify `DatabaseManager.get_download_by_id(feed: str, id: str) -> Download`:
            - Change return type from `Download | None` to `Download`.
            - Raises `DownloadNotFoundError` if not found.
            - Raises `DatabaseOperationError` for other DB issues.
            - Raises `ValueError` if row parsing fails.
        - [x] Verify `DatabaseManager.upsert_download` correctly handles initial setting of `UPCOMING` or `QUEUED` status based on the input `Download` object, ensuring `retries` and `last_error` are appropriate for new items.
        - [x] Verify `DatabaseManager.bump_retries` remains the sole mechanism for incrementing retries and transitioning to `ERROR` status (and handles `DownloadNotFoundError` from `get_download_by_id`).
    - **Phase 2: Service Layer Updates**
        - [x] **`Enqueuer` (`data_coordinator/enqueuer.py`)**:
            - [x] Update `_process_single_download` to correctly handle `DownloadNotFoundError` from `get_download_by_id`.
            - [x] Refactor `_update_download_status_in_db` (or remove it) and its call sites (`_update_status_to_queued_if_vod`, `_handle_existing_fetched_download`):
                - When an `UPCOMING` download becomes a VOD, call `download_db.mark_as_queued_from_upcoming`. Adapt to its new signature (returns `None`, raises exceptions).
                - For other status changes previously handled by `_update_download_status_in_db` (e.g., an existing `ERROR` record being re-processed from feed and needing to be `QUEUED`), evaluate if `download_db.requeue_download` should be used or if current `upsert_download` logic in `_handle_existing_fetched_download` is sufficient.
            - Ensure `bump_retries` calls remain correct for metadata fetch failures.
        - [x] **`Pruner` (`data_coordinator/pruner.py`)**:
            - When pruning items, call `download_db.archive_download`. Adapt to its new signature (returns `None`, raises exceptions). File deletion logic is already handled by `Pruner` correctly before this step.
    - **Phase 3: Test Updates**
        - [x] **`tests/anypod/db/test_db.py`**:
            - Remove tests for the old `download_db.update_status`.
            - Add/Update comprehensive unit tests for all new/modified `download_db.mark_as_*`, `download_db.requeue_*`, `download_db.unskip_download`, and `download_db.get_download_by_id` methods, including exception checking.
            - Ensure tests for `upsert_download` cover setting initial `UPCOMING` and `QUEUED` states.
            - Ensure tests for `bump_retries` are still valid and cover its role, especially `DownloadNotFoundError` handling.
        - [x] **`tests/anypod/data_coordinator/test_enqueuer.py`**:
            - Update mocks and assertions for `download_db.get_download_by_id` to reflect new exception-raising behavior.
            - Update mocks for status update calls to the new `download_db.mark_as_queued_from_upcoming` or `download_db.requeue_download` methods. Verify correct arguments and exception handling.
            - Verify `upsert_download` is called with correctly statused `Download` objects.
- [x] address various TODOs throughout code base

### 3.5.4 `Downloader` Service (`data_coordinator/downloader.py`)
- [x] Constructor accepts `DatabaseManager`, `FileManager`, `YtdlpWrapper`.
- [x] `download_queued(feed_id: str, feed_config: FeedConfig, limit: int = -1) -> tuple[int, int]`: (success_count, failure_count)
    - Gets queued `Download` objects via `DatabaseManager.get_downloads_by_status`.
    - For each `Download`:
        - Call `YtdlpWrapper.download_media_to_file(download, yt_cli_args)`.
            - Generate final file_name (e.g., using `download.title` and `updated_metadata['ext']`).
            - Call `FileManager.save_download_file(feed_config.name, final_file_name, source_file_path=completed_file_path)`.
                - (Note: `FileManager.save_download_file` will need to implement moving a file from `source_file_path` to its final managed location.)
            - Update DB: status to 'downloaded', store final path from `FileManager`, update `ext`, `filesize` from `updated_metadata`.
        - On failure:
            - Update DB: status to 'error', log error, increment retries.
        - Ensure cleanup of source file regardless of success/failure of the individual download.
- [x] Unit tests for `Downloader` (Service) with mocked dependencies.
- [x] Debug mode for Enqueuer

### 3.5.5 `Pruner` (`data_coordinator/pruner.py`)
- [x] Use old implementation for reference, but prepare for largely a full rewrite
- [x] Constructor accepts `DatabaseManager`, `FileManager`.
- [x] `prune_feed_downloads(feed_id: str, keep_last: int | None, prune_before_date: datetime | None) -> tuple[int, int]`: (archived_count, files_deleted_count)
    - Uses `DatabaseManager` to get candidates
    - Uses `FileManager.delete_download_file()` for download.
    - Uses `DatabaseManager.archive_download()` to archive.
- [x] Unit tests for `Pruner` with mocked dependencies.

### 3.5.5.1 Database Refactoring & Feed Table
- [x] **Split database classes**: Refactor `src/anypod/db/db.py` into separate modules:
  - [x] `DownloadDatabase` class for download-level operations (keep existing methods)
  - [x] `FeedDatabase` class for feed-level operations (new functionality)
- [x] **Feed table schema & operations**:
  - [x] Create `feeds` table with schema: `last_sync_attempt`, `last_successful_sync`, `consecutive_failures`, `last_error`, `is_enabled`, `title`, `subtitle`, `description`, `language`, `author`, `image_url`, `source_type`, `total_downloads`, `downloads_since_last_rss`, `last_rss_generation`
  - [x] add `created_at` and `updated_at` with defaults
  - [x] Implement feed CRUD operations in `FeedDatabase`:
    - [x] Add `FeedNotFoundError` exception (similar to `DownloadNotFoundError`)
    - [x] `upsert_feed(feed: Feed) -> None` - Insert or update a feed record, handling None timestamps to allow database defaults
    - [x] `get_feed_by_id(feed_id: str) -> Feed` - Retrieve a specific feed by ID, raise `FeedNotFoundError` if not found
    - [x] `get_feeds(enabled: bool | None = None) -> list[Feed]` - Get all feeds, or filter by enabled status if provided
    - [x] `mark_sync_success(feed_id: str) -> None` - Set `last_successful_sync` to current timestamp, reset `consecutive_failures` to 0, clear `last_error`
    - [x] `mark_sync_failure(feed_id: str, error_message: str) -> None` - Set `last_failed_sync` to current timestamp, increment `consecutive_failures`, set `last_error`
    - [x] `mark_rss_generated(feed_id: str, new_downloads_count: int) -> None` - Set `last_rss_generation` to current timestamp, increment `total_downloads` by `new_downloads_count`, set `downloads_since_last_rss` to `new_downloads_count`
    - [x] `set_feed_enabled(feed_id: str, enabled: bool) -> None` - Set `is_enabled` to the provided value
    - [x] `update_feed_metadata(feed_id: str, *, title: str | None = None, subtitle: str | None = None, description: str | None = None, language: str | None = None, author: str | None = None, image_url: str | None = None) -> None` - Update feed metadata fields; only updates provided (non-None) fields; no-op if all None
- [x] **Download table enhancements**:
  - [x] Add fields: `quality_info`
  - [x] add fields: `discovered_at` and `updated_at`, potentially `downloaded_at` with sqlite triggers
  - [x] Update `DownloadDatabase` methods to handle new fields
  - [x] Update all places that create/modify downloads to populate new fields (`Enqueuer`, `Downloader`, etc.)
- [x] **Config and model updates**:
  - [x] Rename `FeedMetadata` to `FeedMetadataOverrides` in `feed_config.py`
  - [x] Add `enabled` field to `FeedConfig`
- [x] **Feed metadata synchronization**:
  - [x] Compare `FeedMetadataOverrides` from config with stored feed metadata in DB
  - [x] Update DB when config overrides change
  - [x] Modify `YtdlpWrapper` to make best-effort extraction of non-overridden `FeedMetadataOverrides` fields
  - [x] Mark fields for best-effort extraction when overrides are removed
- [x] Unit tests for both `DownloadDatabase` and `FeedDatabase`
- [ ] on pruning, also update `total_downloads` value
- [ ] feed archiver. this will mean also archiving all downloads associated with that feed

### 3.5.6 `DataCoordinator` Orchestrator (`data_coordinator/coordinator.py`)
- [ ] Constructor accepts `Enqueuer`, `Downloader`, `Pruner`, `RSSFeedGenerator`.
- [ ] `process_feed(feed_id: str, feed_config: FeedConfig) -> ProcessingResults`:
    - Calls `enqueuer.enqueue_new_downloads()` (fetch_since_date is from last sync).
    - Calls `downloader.download_queued()` for the feed.
    - Calls `pruner.prune_feed_downloads()` with feed_config.keep_last and prune_before_date from feed_config.since.
    - Calls `rss_generator.update_feed()` to regenerate RSS feed.
    - Aggregates results and handles cross-phase error scenarios.
    - Returns structured results with counts and any errors.
- [ ] Unit tests for `DataCoordinator` focusing on orchestration flow and error handling with mocked dependencies.

### 3.5.7 Discrepancy Detection (in `Pruner` or new service)
- [ ] Implement discrepancy detection logic:
  - [ ] Find DB entries with `DOWNLOADED` status but no corresponding download file.
  - [ ] Find download files on disk with no corresponding `DOWNLOADED` DB entry.
  - [ ] (Optional) Automated resolution strategies or reporting for discrepancies.
- [ ] Unit tests for discrepancy detection logic.

### 3.5.8 General
- [ ] check for commonalities in generated data in tests and see if we can extract a fixture out of them

## 4  Feed Generation
- [x] Determine if a [read/write lock](https://pypi.org/project/readerwriterlock/) for in-memory feed XML cache is needed for concurrency
- [x] add new fields to Download
  - this will also involve potentially changing how i update values, since some (like title) might get changed down the line. so we should try to store the most recent value
- [x] Implement `generate_feed_xml(feed_id)` to write to in-memory XML after acquiring write lock
- [x] Implement `get_feed_xml(feed_id)` for HTTP handlers to read from in-memory XML after acquiring read lock
- [x] Write unit tests to verify enclosure URLs and MIME types in generated feeds
- [x] Figure out how to bring in host url.
- [x] duration should be an int

## 4.1 Path Management Centralization
- [x] **PathManager Implementation** – Create centralized path/URL coordination class:
  - [x] Single source of truth for file system paths and URLs based on feed_id + download_id
  - [x] Consistent 1:1 mapping between network paths and file paths
  - [x] Methods for feed directories, RSS URLs, and media file paths/URLs
  - [x] Google-style docstrings with proper Args/Returns/Raises sections
- [x] `FileManager` refactor
- [x] `Pruner` refactor
- [x] `RSSFeedGenerator` refactor
- [x] tests refactor

## 5  Scheduler / Worker Loop
- [ ] Init APScheduler (asyncio).
- [ ] For each feed add cron trigger → `DataCoordinator.process_feed()`.
- [ ] Implement scheduler that orchestrates full feed processing pipeline via DataCoordinator.
- [ ] On startup, trigger `DataCoordinator.process_feed()` for all feeds to ensure RSS feeds are available before starting HTTP server.
- [ ] on startup, need to do state reconciliation between db and config file more generally

## 6  HTTP Server
- [ ] Create FastAPI app: static mounts `/feeds` & `/media`.
- [ ] Routes `/errors` and `/healthz`.
- [ ] Route for manual sync override: endpoint to manually set `last_successful_sync` timestamp for feeds.
- [ ] Entry in `cli.py` to start `uvicorn`.
- [ ] Tests with `httpx` for endpoints.

## 7  CLI & Flags
- [ ] `python -m anypod` parses flags: `--ignore-startup-errors`, `--retry-failed`, `--log-level`.
- [ ] Docstrings and `argparse` help messages.
- [ ] Evaluate logged statements and make sure that only relevant things get logged
- [ ] when using `--retry-failed`, should also include a date so that we disregard VERY old failures
  - errors will be common because live videos may be deleted and reuploaded as regular VODs
- [ ] write README
  - outline limitations with using ytdlp flags -- which ones do you have to avoid using?
  - look up some well established open source projects and follow their documentation style

## 8  Docker & Dev Flow
- [ ] `Dockerfile` (python:3.13-slim, default root, overridable UID/GID).
- [ ] `.dockerignore` to exclude tests, .git, caches.
- [ ] set up a dev env with containers.

## 9  Release Automation
- [ ] GH Action `release-yt-dlp.yaml`: on yt-dlp tag → rebuild, test, draft release.
- [ ] GH Action `deps-bump.yaml`: weekly minor‑bump PR; require manual approval for major

---

When all boxes are checked, you'll be able to run:

```bash
docker run \
  -v $(pwd)/config:/config \
  -v $(pwd)/data:/data \
  -p 8000:8000 \
  ghcr.io/thurstonsand/anypod:dev
```

…and subscribe to `http://localhost:8000/feeds/this_american_life.xml` in your podcast player.
