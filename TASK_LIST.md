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
        schedule/             # Scheduled feed processing
            apscheduler_core.py # Type-safe APScheduler wrapper
            scheduler.py      # Main feed scheduler using APScheduler
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
- [x] on pruning, also update `total_downloads` value
- [x] ensure there aren't any read/modify/write loops that arent protected by a transaction

### 3.5.6 `DataCoordinator` Orchestrator (`data_coordinator/coordinator.py`)
- [x] Create `data_coordinator/types/` folder with `__init__.py` and `processing_results.py`
- [x] Create `ProcessingResults` dataclass with counts, error tracking, status, and timing
- [x] Add `archive_feed()` method to `Pruner` class (sets `is_enabled=False`)
- [x] Constructor accepts `Enqueuer`, `Downloader`, `Pruner`, `RSSFeedGenerator`, `FeedDatabase`
- [x] `process_feed(feed_id: str, feed_config: FeedConfig) -> ProcessingResults`:
    - Calculate `fetch_since_date` from `feed.last_successful_sync` (NOT feed_config.since)
    - Execute phases in sequence: enqueue → download → prune → RSS generation
    - Inline error handling with graceful degradation between phases
    - Update `last_successful_sync` or `last_failed_sync` based on outcome
    - Return comprehensive `ProcessingResults` with all counts and errors
- [x] Update `data_coordinator/__init__.py` to export `DataCoordinator`
- [x] Integration tests for `DataCoordinator` focusing on full process_feed flow

### 3.5.7 Discrepancy Detection (in `Pruner` or new service)
- [ ] Implement discrepancy detection logic:
  - [ ] Find DB entries with `DOWNLOADED` status but no corresponding download file.
  - [ ] Find download files on disk with no corresponding `DOWNLOADED` DB entry.
  - [ ] (Optional) Automated resolution strategies or reporting for discrepancies.
- [ ] Unit tests for discrepancy detection logic.

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

## 5  Scheduler

### 5.1 Create Scheduler Module (`src/anypod/schedule/`)
- [x] Core scheduler implementation:
  - [x] Add `apscheduler` to dependencies in pyproject.toml
  - [x] Create type-safe APScheduler wrapper (`apscheduler_core.py`)
  - [x] Use APScheduler with `AsyncIOScheduler` for async support
  - [x] Schedule jobs based on feed cron expressions from config
  - [x] Manage job lifecycle (add/remove/pause/resume)
  - [x] Handle graceful shutdown with proper job cleanup
  - [x] Each feed gets its own job with unique ID (the feed ID)
  - [x] Job-level error handling with proper exception chaining
  - [x] Direct DataCoordinator integration (no separate worker)
  - [x] Context ID injection for log correlation
  - [x] Invalid cron expression validation with SchedulerError
  - [x] remove explicit references to monkeypatch

#### 5.1.1 yt-dlp Day-Level Date Precision Accommodation
- [x] **Date Window Calculation Logic (`DataCoordinator`)**:
  - [x] Replace `_calculate_fetch_until_date` with day-aligned logic
  - [x] `fetch_since_date` should still be `last_successful_sync`
  - [x] `fetch_until_date` should just be now(); let's simplify this logic, no 2 * cron tick or anything. we can remove that from coordinator.py and debug_enqueuer.py
  - [x] that may mean that most of the time, these values will fall on the same day. that's fine, and we will dedup results later
  - [x] Update `last_successful_sync` to `fetch_until_date` to ensure full coverage (was previously `now()`)
  - [x] Enhanced logging: log both high-resolution calculated window and day-aligned yt-dlp window while in the context of ytdlp_wrapper
- [x] **Deduplication Enforcement**:
  - [x] Verify `Enqueuer` properly handles duplicate video IDs across multiple day fetches
  - [x] if whatever is in the db is identical to what we retrieved, don't update (which will trigger `updated_at`)
    - it is possible that some metadata might have updated (e.g. uploader might have changed description); so check for that and update if needed
  - [x] Add deduplication tests: same video appearing in multiple day windows
  - [x] Verify no updates occurred (`updated_at` is unchanged)
  - [x] Verify deduplication works when same video appears in multiple day windows
- [x] **Documentation Updates**:
  - [x] Update method docstrings: document day-aligned window strategy clearly
  - [x] Update DESIGN_DOC.md: add section explaining yt-dlp day-level precision limitation
- [x] **State Reconciler Alignment**:
  - [x] Update `since` parameter handling: should only be a `date`, not a `datetime`
  - [x] When `since` changes, use day-aligned logic for requeuing archived downloads
  - [x] Ensure consistency between enqueue windows and retention policy windows
- [x] Tie Feed table `total_downloads` to the Download table with triggers
- [x] When downloading an individual file but it is out of range, I get an incomplete response back from yt-dlp, which causes internal errors

### 5.2 Init State Reconciliation

#### 5.2.1 Create State Reconciler Module (`src/anypod/state_reconciler.py`)
- [x] Startup reconciliation implementation:
  - [x] Compare config feeds with database feeds
  - [x] Handle **new feeds**: insert into DB and set initial `last_successful_sync`
  - [x] Handle **removed feeds**: mark as disabled in DB (set `is_enabled=False`)
  - [x] Handle **changed feeds**: update metadata and configuration
  - [x] Ensure every active feed has valid `last_successful_sync` before scheduling
  - [x] Evaluate what would happen if it fails midway through. Would simply restarting get back to correct state?
  - [x] time box the sync time -- currently only has start time, but will also need end time

#### 5.2.2 Config Change Handling
- [x] Detect and apply changes to:
  - [x] `enabled`: Update feed's `is_enabled` in database, add/remove from scheduler, trigger initial sync if false->true
    - [x] `last_successful_sync` does not need to be optional as it is set proactively on new feed creation
  - [x] `url`: Update existing feed's `source_url`, reset `consecutive_failures` to 0, clear `last_error`, reset `last_successful_sync` as if it were a new feed; keep download history
  - [x] `since` expansion (earlier date): Query archived downloads with `published` >= new `since`, change status from ARCHIVED to QUEUED (will redownload)
    - [x] modify `get_downloads_by_status` to allow for filtering by date so we don't retrieve the entire db
    - [x] also consider storing these values in the Feed db (`since` and `keep_last`) so we only query the db if there's a change
    - [x] modify `requeue_download` -> `requeue_downloads` that can take a variadic list and batch modify
    - [x] modify pydantic handling of `since` to accept JUST a day, and then derive TZ from tiered sources:
      1. from the `since` value itself, if included
      2. from a TZ env var
      3. from the system clock (user would have had to override `/etc/localtime`)
  - [x] `since` contraction (later date): Mark downloads with `published` < new `since` for archival on next prune cycle
  - [x] `keep_last` increase: Query archived downloads ordered by `published` DESC, restore up to (new_keep_last - current_count) from ARCHIVED to QUEUED (will redownload)
    - [x] modify `count_downloads_by_status` to accept multiple possible statuses and return all of them
  - [x] `keep_last` decrease: No immediate action - will apply naturally on next prune cycle
  - [x] `metadata` changes: Update feed table immediately (title, subtitle, description, language, author, image_url, categories, explicit), trigger RSS regeneration

### 5.3 Dependencies and Testing
- [x] Unit tests for scheduler with mocked jobs
- [x] Unit tests for state reconciler covering:
  - [x] New feed addition
  - [x] Feed removal
  - [x] Feed configuration changes
  - [x] Metadata override changes
- [x] Integration tests for full startup sequence
- [x] Tests for graceful shutdown handling

### 5.4 Update CLI Default Mode (`src/anypod/cli/default.py`)
- [x] Main service orchestration:
  - [x] Initialize all components (databases, services)
  - [x] Run state reconciler on startup
  - [x] Start scheduler with reconciled feeds
  - [x] Perform initial sync for all feeds to populate RSS
  - [x] Keep service running until shutdown signal
  - [x] change path_manager to automatically assume tmp and media dirs -- should only need base dir
    - also, we should divide into data dir and config files (cookies.txt and config_path), so we can separate those out to be docker-friendly
    - also, it seems a little excessive to add PathManager to Enqueuer and Downloader JUST so they can retrieve the cookie
    - especially because theyve duplicated logic on retrieving the cookie -- this needs to be centralized somewhere else
    - ytdlp impl looks fine tho
  - [ ] optimize discover/metadata/download loop to cut down on calls to yt-dlp
    - it looks like we are able to retrieve full video detail when querying a playlist without `--flat-playlist` option
    - jury's still out on channels, but maybe?
    - maybe we can pre-emptively classify these when they are added, store the type in the db, and pick the optimal way to retrieve based on that classification
    - **Future Optimization**: Could fetch detailed metadata in one call (86 fields vs 21) but 10x slower - out of scope
    - [x] not sure we need ReferenceType anymore. SourceType might be good enough
    - [x] i think we can get rid of DISCOVERY type
    - [x] get rid of `set_source_specific_ytdlp_options`
  - [x] Cut down on excessive logs
  - [x] use the shared conftest for more fixtures
  - [x] make the db a folder instead of a file -- it creates `.db-wal` type in the same folder.

#### 5.4.1 Convert to async model for ytdlp

**Context/Goals**: Convert anypod from sync to async to enable cancellable long-running operations (especially yt-dlp calls). Currently yt-dlp operations block and can't be interrupted. The async conversion will wrap yt-dlp in subprocess calls that can be properly cancelled, and ripple async throughout the codebase. Key insight: keep CLI args as `list[str]` instead of converting to dict, eliminating complex dict→CLI conversion.

**Implementation Tasks**:
- [x] **CLI Args Strategy**: Remove dict conversion in `feed_config.py` - keep `yt_args` as `list[str]` throughout pipeline
- [x] **YtdlpCore Async**: Implement subprocess calls with `--dump-single-json --flat-playlist` for metadata, parse JSON to `YtdlpInfo`
- [x] **Cancellation**: Proper subprocess cleanup (`proc.kill()` + `await proc.wait()` on `CancelledError`)
- [x] Isolate yt-dlp cli args into YtdlpCore
- [x] Consistent naming on the ydl vs ytdlp fns
  - also separate out the classes into different files
- [x] Remove unused YtdlpCore methods: parse_options(), set_date_range(), set_playlist_limit(), set_cookies()
- [x] **Conversion Order**: YtdlpCore → YtdlpWrapper → Enqueuer/Downloader/Pruner → DataCoordinator → StateReconciler
- [x] Consider if RSSFeedGenerator needs async updates (probably minimal since it's mostly CPU-bound)
  - the answer is no, for now at least
- [x] Implement graceful shutdown handling - it hard crashes on ctrl-c right now
  - this includes during init when we're not in APScheduler yet (maybe we should be?)

#### 5.4.2 Use SQLAlchemy AsyncIO
- [x] **Phase 1: Environment Setup & Dependencies**
  - [x] Add `sqlalchemy[asyncio]`, `sqlmodel`, `aiosqlite`, and `alembic` to `pyproject.toml` using `uv add`.
- [x] **Phase 2: Refactor Models to SQLModel**
  - [x] Convert data models in `src/anypod/db/types/` (`download.py`, `feed.py`) to inherit from `SQLModel`, marking them with `table=True`.
  - [x] Use `sqlmodel.Field` to define primary keys, indexes, and other constraints.
  - [x] Define the one-to-many relationship between `Feed` and `Download` using `sqlmodel.Relationship`.
  - [x] Integrate enum types into SQLModels:
    - [x] For `Feed.source_type`, declare `sa_column=Column(Enum(SourceType))`.
    - [x] For `Download.status`, declare `sa_column=Column(Enum(DownloadStatus))`.
    - [x] Remove legacy `register_adapter` calls.
- [x] **Phase 3: Implement the Asynchronous Core**
  - [x] Create `src/anypod/db/sqlalchemy_core.py` to centralize database connectivity.
  - [x] Implement `create_async_engine` using the `sqlite+aiosqlite` dialect and `QueuePool` (default).
  - [x] Create an `async_session_maker` for producing `AsyncSession` instances.
  - [x] Implement a `session()` async generator for dependency injection.
- [x] **Phase 4: Refactor Data Access Logic**
  - [x] Convert all methods in `src/anypod/db/feed_db.py` and `src/anypod/db/download_db.py` to `async def`.
  - [x] Refactor methods to accept an `AsyncSession` parameter instead of using a shared instance.
  - [x] Replace `sqlite-utils` calls (`upsert`, `rows_where`, `get`) with `SQLAlchemy` ORM operations (`session.add`, `session.execute(select(...))`).
  - [x] Propagate `async` keyword up the call stack through the `data_coordinator` and `schedule` modules.
- [x] **Phase 5: Database Migrations with Alembic**
  - [x] Initialize Alembic with `alembic init -t async migrations`.
  - [x] Configure `alembic.ini` with the `sqlalchemy.url` for the async driver.
  - [x] Configure `migrations/env.py` to use `SQLModel.metadata` as the `target_metadata`.
  - [x] Generate an initial migration script: `alembic revision --autogenerate -m "Initial schema from SQLModels"`.
  - [x] Replace database triggers (`create_trigger`) with Alembic-managed versions.
  - [x] Review and apply the initial migration: `alembic upgrade head`.

#### 5.4.3 Use aiofiles for file operations
- [x] Add aiofiles dependency and convert FileManager to use async file operations
  - [x] **Dependencies**: Add `aiofiles` for async file operations
  - [x] **File Operations**: Use `aiofiles.os` to replace path operations


### 5.5 Initial Sync Strategy
- [ ] After reconciliation, trigger immediate sync:
  - [x] Process all enabled feeds to populate RSS
  - [ ] Ensure RSS feeds available before HTTP server starts
  - [x] Handle failures gracefully without blocking startup, unless config is wrong -- that should cause failure until fixed

## 6  HTTP Server

- [ ] how do i break out the api and static serving? different ports? for security reasons, we need to expose static but not apis

### 6.1 Project Structure Setup
- [ ] Create new HTTP server module at `src/anypod/server/`
  - [x] `__init__.py` - Server module exports
  - [x] `app.py` - FastAPI application factory
  - [x] `dependencies.py` - Dependency injection setup
  - [ ] `models/` - Pydantic request/response models
  - [ ] `routers/` - API route handlers organized by domain
  - [ ] `middleware.py` - CORS, logging, error handling middleware

### 6.2 FastAPI Application Setup
- [x] Add `fastapi`, `uvicorn` to dependencies in pyproject.toml
- [x] Create FastAPI app factory with proper dependency injection
- [x] Set up CORS, logging, and error handling middleware
  - Ensure logging also includes contextvar
- [ ] Configure OpenAPI documentation with proper metadata

### 6.3 API Models (Pydantic)
- [ ] `FeedResponse` - Feed data for API responses
- [ ] `FeedCreateRequest`/`FeedUpdateRequest` - Feed modification requests
- [ ] `DownloadResponse` - Download data for API responses
- [ ] `PaginatedResponse[T]` - Generic paginated response wrapper
- [ ] `StatsResponse` - System and feed statistics
- [ ] `ErrorResponse` - Standardized error responses

### 6.4 Router Implementation
- [ ] `feeds.py` - All feed management endpoints
  - [ ] `GET    /api/feeds`                   - List all feeds with pagination, filtering, and sorting
  - [ ] `POST   /api/feeds`                   - Create new feed, will write to config file
  - [ ] `GET    /api/feeds/{feed_id}`         - Get detailed feed information
  - [ ] `PUT    /api/feeds/{feed_id}`         - Update feed configuration by modifying config file
  - [ ] `DELETE /api/feeds/{feed_id}`         - Disables feed and archives all downloads
  - [ ] `POST   /api/feeds/{feed_id}/enable`  - Enable feed processing
  - [ ] `POST   /api/feeds/{feed_id}/disable` - Disable feed processing
  - [ ] `POST   /api/feeds/{feed_id}/sync`    - Trigger manual sync/processing
  - [ ] `GET    /api/feeds/valid`             - Validate feed config before writing to config file
- [ ] `downloads.py` - Download management endpoints
  - [ ] `GET    /api/feeds/{feed_id}/downloads`                      - List downloads for feed (paginated, filtered)
  - [ ] `GET    /api/feeds/{feed_id}/downloads/{download_id}`        - Get specific download details
  - [ ] `POST   /api/feeds/{feed_id}/downloads/{download_id}/retry`  - Retry failed download
  - [ ] `POST   /api/feeds/{feed_id}/downloads/{download_id}/skip`   - Mark download as skipped
  - [ ] `POST   /api/feeds/{feed_id}/downloads/{download_id}/unskip` - Remove skip status
  - [ ] `DELETE /api/feeds/{feed_id}/downloads/{download_id}`        - Archive download and delete file
- [ ] `stats.py` - Statistics and monitoring endpoints
  - [ ] `GET    /api/feeds/{feed_id}/stats` - Detailed feed statistics
  - [ ] `GET    /api/stats/summary`         - System-wide statistics summary including storage
- [ ] `health.py` - Health check endpoints
  - [ ] `GET    /api/health` - Application health check
- [ ] `static.py` - Content delivery endpoints
  - [ ] `GET    /feeds`                                  - List all rss feeds in directory
  - [x] `GET    /feeds/{feed_id}.xml`                    - RSS feed XML
  - [ ] `GET    /media`                                  - List all feeds in directory
  - [ ] `GET    /media/{feed_id}`                        - List all files for a feed in directory
  - [x] `GET    /media/{feed_id}/{filename}.{ext}`       - Media file download
  - [ ] `GET    /thumbnails`                             - List all feeds in directory
  - [ ] `GET    /thumbnails/{feed_id}`                   - List all thumbnails for a feed in directory
  - [ ] `GET    /thumbnails/{feed_id}/{filename}.{ext}`  - Thumbnail images
- [ ] Unit tests with `TestClient` for all API endpoints
- [ ] Integration tests with actual database operations

### 6.5 Integration with Existing Components
- [ ] Create service layer to bridge HTTP API with existing DataCoordinator
- [ ] Extend FeedDatabase/DownloadDatabase with new query methods for API needs
- [ ] Add config file read/write utilities for feed CRUD operations
- [x] Implement proper error mapping from domain exceptions to HTTP responses

### 6.6 Key Features Implementation
- [ ] **Pagination**: Implement cursor-based or offset-based pagination
- [ ] **Filtering**: Add query parameters for status, date ranges, search
- [ ] **Sorting**: Support multiple sort fields and directions
- [ ] **Validation**: Comprehensive request validation using Pydantic
- [ ] **Error Handling**: Consistent error responses with proper HTTP status codes

### 6.7 CLI Integration
- [x] Configure server host/port via environment variables
- [x] Ensure proper graceful shutdown handling
- [x] Entry in `default.py` to start `uvicorn`

### 6.8 Documentation
- [ ] Comprehensive OpenAPI documentation
  - [ ] Example requests/responses for all endpoints

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
