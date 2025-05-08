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

 tests/
    anypod/ # tests mirror the src/anypod structure
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

## 3.2 File Manager Layer
- [x] Abstraction seam: encapsulate base directory so future S3/GCS back‑ends can subclass
- [x] Implement `save_download_file(feed, filename, data_stream) -> Path` (atomic write)
- [x] Implement `delete_download_file(feed, filename) -> bool`
- [x] Implement `download_exists(feed, filename) -> bool`
- [x] Implement `get_download_stream(feed, filename) -> IO[bytes]`
- [x] Ensure directory hygiene: base download and feed directories exist (covered by save_download_file)
- [x] Write unit tests (tmp dir fixtures)

## 3.5 Data Orchestration & Services Layer
This section details the components that manage the lifecycle of downloads, from discovery to storage and pruning. They are organized into a `data_coordinator` module and related services/wrappers.

### 3.5.1 `db.py::Download` model updates
- [x] `from_row(cls, db_row: sqlite3.Row) -> Download` class method for mapping.

### 3.5.2 `YtdlpWrapper` (`ytdlp_wrapper.py`)
- [ ] Create `ytdlp_wrapper.py` and class `YtdlpWrapper`.
- [ ] `YtdlpWrapper.fetch_metadata(url: str, yt_args: str) -> list[dict]`: Fetches metadata for all items at the given URL using yt-dlp's metadata extraction capabilities. Each dict should contain keys like `id`, `source_url`, `title`, `published` (ISO 8601), `ext`, `duration`, `thumbnail`. This will be used by `Enqueuer`.
- [ ] `YtdlpWrapper.download_media_stream(item_metadata: dict, yt_args: str) -> tuple[IO[bytes], dict]`: Provides a stream of the media content for the given item, allowing the caller to control where and how the data is written to disk. Returns a tuple: `(media_stream, updated_metadata_dict)`, where `media_stream` is a readable binary stream of the downloaded media, and `updated_metadata_dict` may include refined details such as exact filesize or final extension. This will be called by the `Downloader` service, which is responsible for saving the stream to the desired location.
- [ ] Sandbox test using a Creative-Commons short video (no network in CI → use `pytest.mark.skipif` or cached sample JSON for `fetch_metadata` and a small dummy file for `download_media`).
- [ ] Unit tests for `YtdlpWrapper`.

### 3.5.3 `Enqueuer` (`data_coordinator/enqueuer.py`)
- [ ] Constructor accepts `DatabaseManager`, `YtdlpWrapper`.
- [ ] `enqueue_new_downloads(feed_config: FeedConfig) -> int`:
    - Fetches metadata using `YtdlpWrapper.fetch_metadata()`.
    - For each new item, checks existence via `DatabaseManager` (using `Download.from_row` if needed).
    - Adds new items to DB as 'queued' via `DatabaseManager.add_download()`.
    - Returns count of newly enqueued items.
- [ ] Unit tests for `Enqueuer` with mocked dependencies.

### 3.5.4 `Downloader` Service (`data_coordinator/downloader.py`)
- [ ] Constructor accepts `DatabaseManager`, `FileManager`, `YtdlpWrapper`.
- [ ] `download_queued_items(feed_name: str, yt_args: str, limit: int = 0) -> tuple[int, int]`: (success_count, failure_count)
    - Gets queued downloads via `DatabaseManager.next_queued_downloads()`, uses `Download.from_row`.
    - For each item:
        - Calls `YtdlpWrapper.download_media()`.
        - On success: uses `FileManager.save_download_file()` and `DatabaseManager.update_status()` (to 'downloaded').
        - On failure: `DatabaseManager.update_status()` (to 'error', logs error, increments retries).
- [ ] Unit tests for `Downloader` (Service) with mocked dependencies.

### 3.5.5 `Pruner` (`data_coordinator/pruner.py`)
- [ ] Constructor accepts `DatabaseManager`, `FileManager`.
- [ ] `prune_feed_downloads(feed_name: str, keep_last: int | None, prune_before_date: datetime | None) -> tuple[int, int]`: (archived_count, files_deleted_count)
    - Implements logic previously in the old `DataCoordinator.prune_old_downloads`.
    - Uses `DatabaseManager` to get candidates, `Download.from_row` to convert rows.
    - Uses `FileManager.delete_download_file()` for downloaded items.
    - Uses `DatabaseManager.update_status()` to 'archived'.
- [ ] Unit tests for `Pruner` with mocked dependencies.

### 3.5.6 `DataCoordinator` Orchestrator (`data_coordinator/coordinator.py`)
- [ ] Constructor accepts `Enqueuer`, `Downloader`, `Pruner`, `FeedGen`, `DatabaseManager`, `FileManager` (for pass-through methods like `get_download_by_id`, `stream_download_by_id`, etc.).
- [ ] `process_feed(feed_config: FeedConfig) -> None`:
    - Calls `Enqueuer.enqueue_new_downloads()`.
    - Calls `Downloader.download_queued_items()`.
    - Calls `Pruner.prune_feed_downloads()`.
    - Calls `FeedGen.generate_feed_xml()`.
- [ ] `add_download` (delegates to DB, handles file deletion if replacing DOWNLOADED item)
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
- [ ] Determine if a read/write lock for in-memory feed XML cache is needed for concurrency
- [ ] Implement `generate_feed_xml(feed_name)` to write to in-memory XML after acquiring write lock
- [ ] Implement `get_feed_xml(feed_name)` for HTTP handlers to read from in-memory XML after acquiring read lock
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
