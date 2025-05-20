# Anypod – Phase 1 Task List

This checklist covers everything required to reach a functional MVP that aligns with the design document. Tasks are ordered to minimise back-tracking and maximise fast feedback while you work in **Cursor**.

---

## 0  Repo Bootstrap
- [x] **Init git repo** – `git init --initial-branch=main && gh repo create`.
- [x] **`pyproject.toml`** – minimal project metadata, `uv` backend, Python ≥ 3.13.
- [x] **`uv pip install --groups dev`** – add dev deps: `ruff`, `pytest`, `pytest-asyncio`, `mypy`, `pre-commit`.
- [x] **Pre-commit hooks** – formatters & linter.
- [ ] **CI** – GitHub Actions workflow running `uv pip sync && pytest` on every PR.

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
- [ ] Tests to make sure db access is optimized (e.g. uses indexes)

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
- [ ] Unit tests for `Enqueuer` with mocked dependencies.
- [x] preprocess cli args so you dont do it every time
- [ ] handle cookies differently? they need to be included even at discovery stage
- [x] db should not leak any details about sqlite -- should abstract all that away
    - for example, remove all references to sqlite.Row, sqlite.Error
- [x] retries should apply more widely, and with enough failures, should transition to error state
    - maybe db.py needs a `bump_error_count` fn that handles this - bumps it until it becomes too high, then marks as error

### 3.5.4 `Downloader` Service (`data_coordinator/downloader.py`)
- [ ] Set up tmp dirs for writing data for merge: e.g. `/data/.tmp_completed_downloads/<feed_id>/<download.ext>/` and `/data/.tmp_yt_dlp_parts/<feed_id>/<download.ext>/`
- [ ] Constructor accepts `DatabaseManager`, `FileManager`, `YtdlpWrapper`, and application config (for base data paths).
- [ ] `download_queued(feed_config: FeedConfig, limit: int = 0) -> tuple[int, int]`: (success_count, failure_count)
    - Gets queued `Download` objects via `DatabaseManager.next_queued_downloads(feed_config.name, limit)`.
    - For each `Download`:
        - Convert `feed_config.yt_args` string to `list[str]` for `YtdlpWrapper`.
        - get download: Download from the db
        - Call `YtdlpWrapper.download_media_to_file(download, yt_cli_args)`.
            - Generate final file_name (e.g., using `download.title` and `updated_metadata['ext']`).
            - Call `FileManager.save_download_file(feed_config.name, final_file_name, source_file_path=completed_file_path)`.
                - (Note: `FileManager.save_download_file` will need to implement moving a file from `source_file_path` to its final managed location.)
            - Update DB: status to 'downloaded', store final path from `FileManager`, update `ext`, `filesize` from `updated_metadata`.
        - On failure:
            - Update DB: status to 'error', log error, increment retries.
        - Ensure cleanup of source file regardless of success/failure of the individual download.
- [ ] Unit tests for `Downloader` (Service) with mocked dependencies.

### 3.5.5 `Pruner` (`data_coordinator/pruner.py`)
- [ ] Constructor accepts `DatabaseManager`, `FileManager`.
- [ ] `prune_feed_downloads(feed_id: str, keep_last: int | None, prune_before_date: datetime | None) -> tuple[int, int]`: (archived_count, files_deleted_count)
    - Implements logic previously in the old `DataCoordinator.prune_old_downloads`.
    - Uses `DatabaseManager` to get candidates, `Download.from_row` to convert rows.
    - Uses `FileManager.delete_download_file()` for download.
    - Uses `DatabaseManager.update_status()` to 'archived'.
- [ ] Unit tests for `Pruner` with mocked dependencies.

### 3.5.6 `DataCoordinator` Orchestrator (`data_coordinator/coordinator.py`)
- [ ] Constructor accepts `Enqueuer`, `Downloader`, `Pruner`, `FeedGen`, `DatabaseManager`, `FileManager` (for pass-through methods like `get_download_by_id`, `stream_download_by_id`, etc.).
- [ ] `process_feed(feed_config: FeedConfig) -> None`:
    - Calls `Enqueuer.enqueue_new_downloads()`.
    - Calls `Downloader.download_queued()`.
    - Calls `Pruner.prune_feed_downloads()`.
    - Calls `FeedGen.generate_feed_xml()`.
- [ ] `add_download` (delegates to DB, handles file deletion if replacing DOWNLOADED)
- [ ] `update_status` (delegates to DB, handles file deletion on status change from DOWNLOADED)
- [ ] `get_download_by_id` (delegates to DB, uses `Download.from_row`)
- [ ] `stream_download_by_id` (delegates to `FileManager` after checking DB status via `get_download_by_id`, handles `FileNotFoundError` by updating status to ERROR)
- [ ] `get_errors` (delegates to DB, uses `Download.from_row`)
- [ ] Unit tests for `DataCoordinator` focusing on interaction correctness with mocked service dependencies.

### 3.5.7 Discrepancy Detection (in `Pruner` or new service)
- [ ] Implement discrepancy detection logic:
  - [ ] Find DB entries with `DOWNLOADED` status but no corresponding download file.
  - [ ] Find download files on disk with no corresponding `DOWNLOADED` DB entry.
  - [ ] (Optional) Automated resolution strategies or reporting for discrepancies.
- [ ] Unit tests for discrepancy detection logic.

### 3.5.8 General
- [ ] check for commonalities in generated data in tests and see if we can extract a fixture out of them

## 4  Feed Generation
- [ ] Determine if a [read/write lock](https://pypi.org/project/readerwriterlock/) for in-memory feed XML cache is needed for concurrency
- [ ] Implement `generate_feed_xml(feed_id)` to write to in-memory XML after acquiring write lock
- [ ] Implement `get_feed_xml(feed_id)` for HTTP handlers to read from in-memory XML after acquiring read lock
- [ ] On startup, trigger a retrieve-and-update loop for all feeds to generate XML before starting the HTTP server
- [ ] Write unit tests to verify enclosure URLs and MIME types in generated feeds

## 5  Scheduler / Worker Loop
- [ ] Init APScheduler (asyncio).
- [ ] For each feed add cron trigger → `process_feed`.
- [ ] Implement `process_feed` steps via DataCoordinator and FeedGen: ① enqueue → ② download → ③ prune → ④ generate RSS

## 6  Scheduler / Worker Loop
- [ ] Create FastAPI app: static mounts `/feeds` & `/media`.
- [ ] Routes `/errors` and `/healthz`.
- [ ] Entry in `cli.py` to start `uvicorn`.
- [ ] Tests with `httpx` for endpoints.

## 7  CLI & Flags
- [ ] `python -m anypod` parses flags: `--ignore-startup-errors`, `--retry-failed`, `--log-level`.
- [ ] Docstrings and `argparse` help messages.
- [ ] Evaluate logged statements and make sure that only relevant things get logged
- [ ] when using `--retry-failed`, should also include a date so that we disregard VERY old failures
  - errors will be common because live videos may be deleted and reuploaded as regular VODs

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
  ghcr.io/<you>/anypod:dev
```

…and subscribe to `http://localhost:8000/feeds/this_american_life.xml` in your podcast player.
