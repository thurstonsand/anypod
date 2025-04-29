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
    cli.py            # entry-point
    config.py         # Pydantic models & loader
    db.py             # Sqlite helpers & migrations
    scheduler.py      # APScheduler initialisation
    worker.py         # enqueue/download/prune
    feedgen.py        # thin wrapper
    http.py           # FastAPI app + routing
    utils.py          # misc helpers
tests/
```

## 2  Configuration Loader
- [ ] Pydantic V2 models reflecting YAML keys.
- [ ] `load_config(path) -> dict[str, FeedConfig]`.
- [ ] Unit tests using fixture YAML.

## 3  Database Layer
- [ ] Migrations via `sqlite-utils` (`downloads` table + indices).
- [ ] CRUD helpers: `add_item`, `update_status`, `next_queued_items`, `prune_old_items`.
- [ ] Tests with tmp in-memory DB.

## 4  Downloader Stub
- [ ] Wrap yt-dlp library; function `download_once(item, yt_args) -> bool`.
- [ ] Dry-run helper `fetch_metadata(url, yt_args)`.
- [ ] Sandbox test using a Creative-Commons short video (no network in CI → use `pytest.mark.skipif` or cached sample JSON).

## 5  Scheduler / Worker Loop
- [ ] Init APScheduler (asyncio).
- [ ] For each feed (post-validation) add cron trigger → `process_feed`.
- [ ] Implement `process_feed` steps 1–4 (enqueue → download → prune → generate RSS).

## 6  Feed Generation
- [ ] Vendor specific commit of `feedgen` under `vendor/`.
- [ ] `generate_feed_xml(feed_name)` writes to `/feeds/{feed}.xml.tmp` then atomic `mv`.
- [ ] Unit test verifying enclosure URLs and MIME types.

## 7  HTTP Server
- [ ] Create FastAPI app: static mounts `/feeds` & `/media`.
- [ ] Routes `/errors` and `/healthz`.
- [ ] Entry in `cli.py` to start `uvicorn`.
- [ ] Tests with `httpx` for endpoints.

## 8  CLI & Flags
- [ ] `python -m anypod` parses flags: `--config`, `--ignore-startup-errors`, `--retry-failed`, `--log-level`.
- [ ] Docstrings and `argparse` help messages.

## 9  Docker & Dev Flow
- [ ] `Dockerfile` (python:3.13-slim, default root, overridable UID/GID).
- [ ] `.dockerignore` to exclude tests, .git, caches.
- [ ] `make dev-shell` or similar for live-reload mounts.

## 10  Release Automation
- [ ] GH Action `release-yt-dlp.yaml`: on yt-dlp tag → rebuild, test, draft release.
- [ ] GH Action `deps-bump.yaml`: daily `uv pip install --upgrade --groups dev`, open PR if `uv.lock` changes.

---

When all boxes are checked, you’ll be able to run:

```bash
docker run \
  -v $(pwd)/config:/config \
  -v $(pwd)/data:/data \
  -p 8000:8000 \
  ghcr.io/<you>/anypod:dev
```

…and subscribe to `http://localhost:8000/feeds/this_american_life.xml` in your podcast player.
