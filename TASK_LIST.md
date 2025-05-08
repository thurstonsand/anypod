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
    __init__.py
    cli.py                # entry‑point
    config.py             # Pydantic models & loader
    database.py           # SQLite helpers & migrations
    file_manager.py       # FileManager implementation (new)
    data_coordinator.py   # Orchestrates DB + FS (new)
    downloader.py         # yt‑dlp wrapper
    scheduler.py          # APScheduler initialisation
    worker.py             # cron‑triggered job logic (delegates to coordinator)
    feedgen.py            # thin wrapper
    http.py               # FastAPI app + routing
    utils.py              # misc helpers

 tests/
```

## 2  Configuration Loader
- [x] Pydantic V2 models reflecting YAML keys.
- [x] `load_config(path) -> dict[str, FeedConfig]` (implemented via Pydantic Settings).
- [x] Unit tests using fixture YAML.

## 3  Database Layer
- [x] CRUD helpers:
  - [x] `add_item`
  - [x] `update_status`
  - [x] `next_queued_items`
  - [x] `get_item_by_video_id`
  - [x] `get_errors`
  - [x] `get_items_to_prune_by_keep_last`
  - [x] `get_items_to_prune_by_since`
  - [x] `remove_pruned_items`
- [x] Tests with tmp in-memory DB.

## 3.2 File Manager Layer
- [ ] Abstraction seam: encapsulate base directory so future S3/GCS back‑ends can subclass
- [ ] Implement `save_media_file(feed_name, filename, data_stream) -> Path` (atomic write)
- [ ] Implement `delete_media_file(path_to_file) -> bool`
- [ ] Implement `get_media_path(feed_name, filename) -> Path`
- [ ] Ensure directory hygiene: base media and feed directories exist
- [ ] Write unit tests (tmp dir fixtures)

## 3.5 Data Coordination Layer
Implement `DataCoordinator` class receiving `DatabaseManager` & `FileManager` instances
- [ ] Handles logic for adding/updating items considering existing files and DB entries (e.g., delete old file before replacing DB record).
- [ ] Manages the multi-step pruning process (get items, delete files, delete DB entries).
- [ ] `add_item`
- [ ] `update_status`
- [ ] `get_item_by_video_id`
- [ ] `download_queued_items` (pull queue from DB, stream to FileManager, update statuses)
- [ ] `get_errors`
- [ ] `prune_old_downloads`
- [ ] Implement discrepancy detection logic:
  - [ ] Find DB entries with `DOWNLOADED` status but no corresponding media file.
  - [ ] Find media files on disk with no corresponding `DOWNLOADED` DB entry.
  - [ ] (Optional) Automated resolution strategies or reporting for discrepancies.
  Unit tests with in‑memory DB & tmp FS

## 4  Downloader Stub
- [ ] Wrap yt-dlp library: `download_once(item, yt_args) -> bool`.
- [ ] Dry-run helper: `fetch_metadata(url, yt_args)`.
- [ ] Sandbox test using a Creative-Commons short video (no network in CI → use `pytest.mark.skipif` or cached sample JSON).

## 5  Feed Generation
- [ ] Determine if a read/write lock for in-memory feed XML cache is needed for concurrency
- [ ] Implement `generate_feed_xml(feed_name)` to write to in-memory XML after acquiring write lock
- [ ] Implement `get_feed_xml(feed_name)` for HTTP handlers to read from in-memory XML after acquiring read lock
- [ ] On startup, trigger a retrieve-and-update loop for all feeds to generate XML before starting the HTTP server
- [ ] Write unit tests to verify enclosure URLs and MIME types in generated feeds

## 6  Scheduler / Worker Loop
- [ ] Init APScheduler (asyncio).
- [ ] For each feed add cron trigger → `process_feed`.
- [ ] Implement `process_feed` steps via DataCoordinator and FeedGen: ① enqueue → ② download → ③ prune → ④ generate RSS

## 7  HTTP Server
- [ ] Create FastAPI app: static mounts `/feeds` & `/media`.
- [ ] Routes `/errors` and `/healthz`.
- [ ] Entry in `cli.py` to start `uvicorn`.
- [ ] Tests with `httpx` for endpoints.

## 8  CLI & Flags
- [ ] `python -m anypod` parses flags: `--ignore-startup-errors`, `--retry-failed`, `--log-level`.
- [ ] Docstrings and `argparse` help messages.

## 9  Docker & Dev Flow
- [ ] `Dockerfile` (python:3.13-slim, default root, overridable UID/GID).
- [ ] `.dockerignore` to exclude tests, .git, caches.
- [ ] set up a dev env with containers.

## 10  Release Automation
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
