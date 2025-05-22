# Anypod – Design Document

**Last updated:** 2025-05-07

---

## 1  Purpose
Anypod is a thin Python wrapper around **yt-dlp** that converts any yt-dlp–supported source—**video *or* audio**—into an RSS feed consumable by podcast players. It runs as a long-lived Docker container and is configured solely through YAML.

---

## 2  Non-Goals
* Live-stream capture (only post-VOD downloads)
* Transcoding in the MVP (requires MP4/M4A from source)
* Graphical UI (a JSON-driven admin dashboard can come later)
* Automatic retry loop beyond manual `--retry-failed`

---

## Design Principles

* **Error‑Handling Principle** – *Log‑and‑continue beats crash‑and‑burn.* - Any failure (network timeout, bad YAML, filesystem hiccup) should be isolated to that work item while the daemon keeps running. Only unrecoverable conditions—e.g., corrupted SQLite schema or a process‑wide permission error—warrant terminating the container.

* **Idempotent Operations** – Every scheduled job, download, prune, or feed‑generation step **must** be safe to repeat without side‑effects. This enables retry loops, crash recovery, and manual re‑runs.

* **Graceful Degradation** – When a subsystem reaches a hard limit (disk full, upstream API quota, etc.) the service enters a *degraded* mode: the offending feature pauses, the rest keeps working, and logs/metrics/`/healthz` make the problem obvious.

* **Atomic State Transitions** – Multi‑step changes (e.g., write file → update DB) complete in a single transaction or within an atomic unit so consumers never observe half‑baked state.

* **Observability First** – Emit structured logs (JSON) and Prometheus metrics for all critical paths. A feature isn't "done" until its behaviour can be seen on a dashboard.

* **Extensibility Path** – Public modules expose clean, focused interfaces; internals stay behind those seams. New storage back‑ends, GC algorithms, or auth methods should plug in without rewrites.

* **Dependency Injection & Testability** – External side effects (network, disk, time, randomness) are injected so unit tests can stub them and run fast.

* **Security by Default** – Runs as non‑root when possible, avoids storing secrets in plaintext, and uses the minimum network surface required to fetch media and serve HTTP.

* **Principle of Least Magic** – Configuration lives in YAML/env‑vars, not hidden defaults. Explicit beats implicit for paths, formats, and feature flags.

* **Minimal Footprint** – Prefer standard‑library or single‑purpose libraries; avoid pulling in heavyweight frameworks unless they deliver clear value.

---

## 3  High-Level Architecture
```mermaid
graph TD
  %% ─────── Config & Scheduling ───────
  subgraph Config & Scheduler
    A[YAML feeds.yml]
    B["APScheduler (per-feed cron)"]
    A --> B
  end

  B --> DC

  %% ─────── Storage & Coordination ───────
  subgraph Storage & Coordination
    direction LR
    DC[DataCoordinator]
    DB[SQLite]
    FM[FileManager]
    DC --> DB
    DC --> FM
    FM -- media files --> Disk[Filesystem]
  end

  %% ─────── Feed Generation ───────
  subgraph Feed Generation
    DC --> FG[FeedGen]
    FG --> G[FastAPI Static server]
  end

  %% ─────── HTTP ───────
  subgraph HTTP
    G --> I["/feeds/*.xml"]
    G --> J["/media/*"]
    G --> K["/errors"]
    G --> L["/healthz"]
  end

  %% Runtime look-ups back to storage via coordinator
  J -- read media --> DC
  K -- read errors --> DC
```


### 3.1 Layer Responsibilities

| Layer                               | Responsibility                                                                                                   | Key Points                                                                                           |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| **`ConfigLoader`**                  | Parse & validate YAML into strongly‑typed models.                                                                | Environment‑variable overrides; default value injection; early failure on schema mismatch.           |
| **`Scheduler`**                     | Trigger periodic `DataCoordinator.process_feed` jobs per‑feed cron schedule.                                     | Async scheduler; cron expressions validated at startup; stateless job store.                         |
| **`DatabaseManager`**               | Persistent metadata store (status, retries, paths, etc.). `Download` model includes `from_row` for mapping.      | Single connection pool                                                                               |
| **`FileManager`**                   | All filesystem interaction: save/read/delete media, atomic RSS writes, directory hygiene, free‑space checks.     | Path resolution lives here; future back‑ends (S3/GCS) become plug‑ins.                               |
| **`YtdlpWrapper`**                  | Thin wrapper around `yt-dlp` for fetching media metadata and downloading media content.                          | Abstracts `yt-dlp` specifics; provides `fetch_metadata` and `download_media`.                        |
| **`FeedGen`**                       | Generates RSS XML feed files based on current download metadata.                                                 | Manages in-memory feed cache; uses `DatabaseManager` & `FileManager`. Called by `DataCoordinator`.   |
| **DataCoordinator Module**          | Houses services that orchestrate data lifecycle operations using foundational components and `FeedGen`.         | Uses `DatabaseManager`, `FileManager`, `YtdlpWrapper`, `FeedGen`. Main class is `DataCoordinator`. |
|   ↳ **`DataCoordinator`**           | High-level orchestration of enqueue, download, prune, and feed generation phases.                                | Delegates to `Enqueuer`, `Downloader`, `Pruner`, and calls `FeedGen`. Ensures sequence.            |
|   ↳ **`Enqueuer`**                  | Fetches media metadata in two phases—(1) re-poll existing 'upcoming' entries to transition them to 'queued' when VOD; (2) fetch latest feed media and insert new 'queued' or 'upcoming' entries. | Uses `YtdlpWrapper` for metadata, `DatabaseManager` for DB writes.                                   |
|   ↳ **`Downloader`**                | Processes queued downloads: triggers downloads via `YtdlpWrapper`, saves files, and updates database records.        | Uses `YtdlpWrapper`, `FileManager`, `DatabaseManager`. Handles download success/failure.             |
|   ↳ **`Pruner`**                    | Implements retention policies by identifying and removing old/stale downloads and their files.                   | Uses `DatabaseManager` for selection, `FileManager` for deletion.                                    |
| **HTTP (FastAPI)**                  | Serve static RSS & media and expose health/error JSON.                                                           | Delegates look‑ups to `DataCoordinator` or relevant services; zero business logic.                   |

---

## 4  Configuration Example
```yaml
feeds:
  this_american_life:
    url: https://www.youtube.com/@thisamericanlife/playlists
    yt_args: |
      -f "(bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/best[ext=mp4])"
      --cookies /cookies/tal.txt
    schedule: "0 3 * * *"          # cron (required)
    keep_last: 100                 # prune policy (optional)
    since: "2024-01-01T00:00:00Z"  # ignore older downloads (optional)
  radio_lab:
    ...
```
*Only the keys above are validated; any other text inside `yt_args` is passed verbatim to yt-dlp.*

---

## 5  Database Schema
```sql
CREATE TABLE IF NOT EXISTS downloads (
  feed         TEXT NOT NULL,
  id           TEXT NOT NULL,
  source_url   TEXT NOT NULL,
  title        TEXT NOT NULL,
  published    TEXT NOT NULL,            -- ISO 8601 datetime string
  ext          TEXT NOT NULL,
  duration     REAL NOT NULL,            -- seconds
  thumbnail    TEXT,                     -- URL
  status       TEXT NOT NULL,            -- upcoming | queued | downloaded | error | skipped
  retries      INTEGER NOT NULL DEFAULT 0,
  last_error   TEXT,
  PRIMARY KEY  (feed, id)
);
CREATE INDEX idx_feed_status ON downloads(feed, status);
```
* `ext` is **NOT NULL**; absence indicates a metadata-extraction bug.
* `mime` is derived from `ext` at feed-generation time via lookup table.

### Status Lifecycle
The `status` field in the `downloads` table tracks the state of each download.

```mermaid
---
theme: 'dark'
---
graph TD
    subgraph Initialization
        %%direction LR
        New[New Media Discovered] --> A1{Is Live/Scheduled?}
        A1 -- Yes --> UPCOMING
        A1 -- No  --> QUEUED
    end

    subgraph Processing
        %%direction TB
        UPCOMING --> |VOD Ready| QUEUED
        QUEUED   --> |Download Start| DL_IP[Downloading In Progress]

        DL_IP    --> |Success| DOWNLOADED
        DL_IP    --> |Failure| ERR_BH["Error (via bump_retries)"]
    end

    subgraph ErrorHandling
        %%direction LR
        ERR_BH --> |Retry Limit Not Reached| PREV_S{Previous Status}
        PREV_S ----> QUEUED
        ERR_BH --> |Retry Limit Reached| ERROR["ERROR (Max Retries)"]
        ERROR  --> |Manual Requeue| QUEUED
    end

    subgraph Management & Pruning
        %%direction TB
        DOWNLOADED --> |Pruning Rule| ARCHIVED
        UPCOMING   --> |Outside 'since'| ARCHIVED
        ERROR      --> |Pruning Rule| ARCHIVED
        QUEUED     --> |Manual Skip| SKIPPED
    end

    %% Explicit states
    UPCOMING((Upcoming))
    QUEUED((Queued))
    DOWNLOADED((Downloaded))
    ERROR((Error))
    SKIPPED((Skipped))
    ARCHIVED((Archived))

    %% Class assignments
    class New,A1 initialState;
    class UPCOMING,Q1,Q2,Q3,DL_IP activeState;
    class DOWNLOADED,SKIPPED,ARCHIVED finalState;
    class ERROR,ERR_BH errorState;

  %% Styling for dark mode
    classDef initialState fill:#2e2e2e,stroke:#888888,stroke-width:2px,color:#ffffff;
    classDef activeState  fill:#3e3e3e,stroke:#aaaaaa,stroke-width:2px,color:#ffffff;
    classDef finalState   fill:#264653,stroke:#1b3a4b,stroke-width:2px,color:#ffffff;
    classDef errorState   fill:#c0392b,stroke:#992d22,stroke-width:2px,color:#ffffff;
```

**State Definitions and Transitions:**

**1. UPCOMING**
* **When set:** A newly discovered item is known to be a future live stream or scheduled premiere.
* **Transitions to:**
  * **QUEUED** — as soon as its VOD becomes available.
  * **ARCHIVED** — if it falls outside the “since” or "keep_last" window without ever queuing.

**2. QUEUED**
* **When set:**
  * A new VOD is first discovered.
  * An UPCOMING item transitions to VOD-ready.
  * An ERROR item is manually re-queued.
* **Transitions to:**
  * **DOWNLOADED** — on successful download.
  * **ERROR** — if a download attempt exceeds max retries.
  * **(remains QUEUED)** — if a retryable failure occurs (retries bump, but not max).

**3. DOWNLOADED**
* **When set:** A media file has been fetched and stored successfully.
* **Transitions to:**
  * **ARCHIVED** — later pruned per retention rules.

**4. ERROR**
* **When set:** A fetch or download failure has hit the retry limit.
* **Transitions to:**
  * **QUEUED** — if manually re-queued for another attempt.
  * **ARCHIVED** — when the pruner applies retention.

**5. SKIPPED**
* **When set:** A user explicitly marks a download as “skip.” Any other state can be transitioned to this one.
* **Transitions to:**
  * **QUEUED** - if it is unskipped, automatically transitions back to the queue to be downloaded again
  * **ARCHIVED** - if it is unskipped but falls out of `since` or `keep_last`, transition to ARCHIVED; we may need a way in the future to be able to "bookmark" downloads so that they never get archived; but for now, leaving that out of scope

**6. ARCHIVED**
* **When set:** Pruner moves UPCOMING, DOWNLOADED, or ERROR items out of active retention (`keep_last` or `since`).
* **Terminal:** No automatic re-activation.

---

## 6  Processing Flow
```mermaid
sequenceDiagram
  autonumber
  participant S as Scheduler
  participant DC as DataCoordinator
  participant Store as Persistent_Storage [DB + Filesystem]
  participant YTDLW as YtdlpWrapper
  participant FG as FeedGen

  S->>DC: process_feed(feed_config)

  DC->>YTDLW: Discover New Media
  YTDLW-->>DC: Raw Media Metadata
  DC->>Store: Enqueue New Downloads (Metadata -> DB)

  DC->>Store: Get Queued Downloads (DB)
  Store-->>DC: Queued Downloadables
  DC->>YTDLW: Download Media Content (to temp file path)
  YTDLW-->>DC: Path to Downloaded Media File (in temp location)
  DC->>Store: Store Media & Update Status (Move file to permanent storage, Status -> DB)

  DC->>Store: Identify Old/Stale Media (DB + Policies)
  Store-->>DC: Downloads to Prune
  DC->>Store: Remove Stale Media (Filesystem + DB update)

  DC->>FG: Generate Feed XML
  FG->>Store: Fetch Feed Data (DB)
  Store-->>FG: Data for Feed
  FG->>Store: Save Feed XML (Filesystem)
  FG-->>DC: Feed Generation Complete
```

The `Scheduler` triggers a `DataCoordinator.process_feed(feed_config)` call. The `DataCoordinator` then orchestrates the conceptual flow of data through distinct phases, interacting with key components:

1.  **Media Discovery & Enqueuing**: The `DataCoordinator` uses the `YtdlpWrapper` to discover available media. New metadata is then passed to persistent storage, resulting in new downloads being enqueued in the `Database`.

2.  **Media Downloading & Storage**: The `DataCoordinator` retrieves queued downloads from the `Database`. For each, it uses the `YtdlpWrapper` to download the actual media content. `YtdlpWrapper` saves the completed file to a temporary location on a designated data volume and returns the path to this file. The `DataCoordinator` (via its `Downloader` service and `FileManager`) then moves this file to its final permanent storage location on the `Filesystem`, and the download's status is updated to `downloaded` in the `Database`.

3.  **Pruning**: Based on configured retention policies, the `DataCoordinator` identifies old or stale media by querying the `Database`. The corresponding media files are removed from the `Filesystem`, and their database records are updated (e.g., to `archived`).

4.  **Feed Generation**: Finally, the `DataCoordinator` instructs `FeedGen` to generate the RSS feed. `FeedGen` fetches the required data from the `Database`, constructs the XML, and saves it to the `Filesystem`. The `DataCoordinator` is notified upon completion.

*This high-level flow is managed by the `DataCoordinator`, which internally uses its specialized services (`Enqueuer`, `Downloader`, `Pruner`) to execute these steps. The `YtdlpWrapper` handles direct interactions with yt-dlp, while `DatabaseManager` and `FileManager` (represented collectively as `Persistent_Storage` in the diagram) manage data persistence.*

---

### 7 YouTube URL Handling by `YtdlpWrapper`

The `YtdlpWrapper` is designed to provide a consistent interface for fetching metadata regardless of the exact type of YouTube URL provided in the feed configuration. It intelligently handles:

1.  **Channel URLs** (e.g., `https://www.youtube.com/@ChannelName`):
    *   Anypod first performs a lightweight "discovery" request to identify the URL as a channel page.
    *   It then attempts to locate the channel's primary "Videos" tab (e.g., `https://www.youtube.com/@ChannelName/videos`).
    *   This resolved "Videos" tab URL is then used for the main metadata fetch. All user-provided `yt_args` are applied to this resolved URL.
    *   *Future enhancement*: Allow configuration to target other tabs like "Live" or "Shorts".

2.  **Playlist URLs** (e.g., `https://www.youtube.com/playlist?list=PL...`, or a specific channel tab URL like `https://www.youtube.com/@ChannelName/videos`):
    *   These are treated as direct playlists. Metadata is fetched for the downloads within this playlist, respecting user-provided `yt_args`.

3.  **Single Video URLs** (e.g., `https://www.youtube.com/watch?v=VideoID`):
    *   Metadata for the single video is fetched.

This resolution logic aims to simplify configuration for the end-user, as they can often provide a general channel URL and Anypod will attempt to find the most relevant video list. The `feed_id` provided in the configuration is used as the primary `source_identifier` for associating downloads with their feed, ensuring consistency.

---

## 8  Feed Persistence

- The `FeedGen` module maintains a **write-once/read-many-locked in-memory cache**:
  - When the scheduler generates a feed, it replaces the cached bytes under a write lock.
  - HTTP handlers retrieve the feed after receiving a read lock.
  - On startup the cache is populated since all feeds will be retrieved immediately.

## 9  HTTP Endpoints
| Path | Description |
|------|-------------|
| `/feeds/{feed}.xml` | Podcast RSS |
| `/media/{feed}/{file}` | MP4 / M4A enclosure |
| `/errors` | JSON list of failed downloads |
| `/healthz` | 200 OK |

---

## 10 Logging Guidelines

*   **Structured Logging:** add relevant context to the `extra` dictionary instead of in the message directly.
*   **Context is Key:**
    *   All log messages include a `context_id` for tracing.
    *   Always include `feed_id` and relevant download id (e.g., `source_url` or `download.id`) in the `extra` dictionary for logs related to feed/media processing.
*   **Clear Log Levels:** Adhere to standard log level semantics:
    *   `DEBUG`: For developer tracing, verbose.
    *   `INFO`: For operator awareness of normal system operations and milestones.
    *   `WARNING`: For recoverable issues or potential problems that don't stop current operations but may need attention (e.g., a transient download failure that will be retried).
    *   `ERROR`: For specific, non-recoverable failures of an operation that require attention (e.g., metadata parsing failure for a download). The system should log the error and continue with other tasks.
    *   `CRITICAL`: For severe runtime errors threatening application stability or causing shutdown.
*   **Actionable Messages:** Log messages (especially `WARNING`/`ERROR`) should provide clear, concise information about the event. The `extra` dict carries detailed context.
*   **Error Context Propagation:** Custom exceptions should carry diagnostic data. A utility function (`exc_extract`) is used to gather this data from the exception chain and include it in the `extra` field of error logs.
*   **Security:** Never log secrets (API keys, passwords) or PII. Be cautious with logging overly verbose data structures at `INFO` level or above.

---

## 11  Command-Line Flags (MVP)
* `--config-file PATH` – custom YAML path (default `/config/feeds.yml`)
* `--ignore-startup-errors` – keep running if validation fails (feed disabled in memory)
* `--retry-failed` – reset `error` → `queued` rows before scheduler starts
* `--log-level LEVEL`

---

## 12  Deployment
| Aspect | Setting |
|--------|---------|
| **Image** | `ghcr.io/thurstonsand/anypod:latest` |
| **Base** | `python:3.13-slim` |
| **User** | Runs as **root (UID 0)** by default; override via `user: "#{UID}:{GID}"` in docker-compose |
| **Volumes** | `/config`, `/data`, `/cookies` |
| **Port** | 8000 |

---

## 13  Dependencies & Tooling
* Managed by **uv** (`pyproject.toml` + `uv.lock`).
* yt-dlp pinned to specific commit.
* Dev deps: ruff · pytest-asyncio · pytest-cov · pyright · pre-commit

---

## 14  Future Work
* Admin dashboard (React + shadcn/ui)
* Automatic retries with jitter
* Transcoding fallback (ffmpeg) for non-MP4/M4A sources
* OAuth device-flow
* Prometheus `/metrics`
* Support transcripts/auto-generated (whisper can natively output .srt files)
  * > I'm a podcast author, how can I add transcripts to my show?
    > In order for Pocket Casts to discover transcripts for an episode and offer them within the app, the podcast feed must include the <podcast:transcript> element and the transcript must be in one of the following formats: VTT, SRT, PodcastIndex JSON, or HTML.
* include global size limit such that entire app doesnt exeed certain size
  * need to explore options around how to evict downloads if exceeded; some ideas below
  *
    | Policy                                   | What it does                                                                                                                                | Strengths                                                    | Watch‑outs                                                                                                 |
    |------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
    | **Min‑floor + global LRU** *(recommended)* | Add `min_last:` per feed (default = 1). Delete oldest downloads across *all* feeds until the cap is met **but never go below `min_last` for any feed**. | Simple mental model; prevents "rare" shows from disappearing. | If every feed is at its floor and cap is still breached, system enters "degraded‑full" state and refuses new downloads. |
    | **Time‑window floor**                    | Keep at least *N* days of history per feed (`min_days:`). Evict globally‑oldest downloads that fall outside each feed's window.                  | Users often reason in "last 90 days" rather than episode counts. | Variable episode sizes make space usage less predictable.                                                  |
    | **Weighted eviction**                    | Allow optional `weight:` per feed; compute *effective LRU‑age* = `real_age / weight`. Evict by that metric.                                 | Lets you bias important feeds without hard floors.            | Harder to predict which download will vanish next; extra YAML tuning.                                          |
    | **Quota borrow/return**                  | Each feed gets `quota = max_total/N`. Feeds may borrow unused space from others up to `borrow_limit%`. GC first reclaims borrowed space, then local quota, then uses global LRU. | Self‑balancing; high‑volume feeds thrive while small ones keep minimum. | Most complex to implement; needs periodic re‑balancing pass.                                               |
    | **Archive tier**                         | Move oldest media to a cheap "cold" volume (e.g., S3/Glacier) instead of deleting, while pruning DB rows locally.                           | No data loss; total cap becomes *hot‑tier* only.             | Requires new storage backend; retrieval latency for old episodes.                                          |
* integrate with sponsorblock -- either skip blocked sections, or add chapters to download
* add per-source rate limiting
* issue template include rules on requesting support for new source
* enable a podcast feed that accepts requests to an endpoint to add individual videos to the feed; basically manually curated
  * also include manual audio file uploads
* performance testing once both server and cron exist -- does the cron being active cause slow down for the server?
* allow for download-time quality settings AND feed-time quality settings; you can download in high quality, deliver in low quality (for archival purposes
* consider async'ifying the code base. e.g. https://github.com/omnilib/aiosqlite)