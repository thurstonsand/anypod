# Manual Submission Feed

_This design is prepared for handoff to another AI/collaborator. Every section below captures the intent, expected behaviours, and concrete implementation steps so the follow-on work can proceed without additional clarification._

## Overview

Provide a manual submission workflow that lets a trusted operator POST individual video URLs (YouTube or Patreon) to Anypod and have them delivered via an RSS feed soon after. The HTTP handler must remain fast, while the heavy lifting (yt-dlp metadata + download + RSS regeneration) happens through the existing coordinator pipeline.

## Goals

- Add an admin-only endpoint where operators POST a video URL for a chosen feed.
- Reuse the current download/RSS pipeline so manual submissions produce the same artefacts as scheduled feeds.
- Ensure manual feeds do not require cron schedules; posting should trigger background processing automatically.

## Non-Goals

- Authentication/authorization for admin APIs (still private/local only).
- Supporting livestream/upcoming content -- these should be rejected.
- Broad refactors of scheduler/coordinator beyond what is necessary for manual triggers.

## Style & Consistency Requirements

Any new code must follow the existing patterns in the repo (logging, error handling, structured exceptions, dependency injection via FastAPI `Depends`, etc.). Pay close attention to naming, docstring format, and module organisation—match what is already in `src/anypod`. Do not introduce stylistic deviations unless explicitly required.

## Configuration Model Changes

### Manual schedule sentinel

- Allow feeds to specify `schedule: "manual"` in YAML.
- Update `FeedConfig` parsing so `schedule` accepts:
  - A cron expression (as today).
  - The exact string `"manual"`, which is stored internally as a sentinel value (e.g. `None`).
- When `schedule` is manual, the feed is treated as manual-submission only. StateReconciler should validate that manual feeds provide at least a `metadata.title` override (other overrides remain optional).

### Downstream handling

- `StateReconciler` must recognise the manual schedule sentinel. Manual feeds skip yt-dlp discovery and rely entirely on YAML metadata (title required, description/image optional). The feed DB row should set `source_type=UNKNOWN` and skip resolved URL handling.
- The scheduler should skip creating APScheduler jobs for manual feeds. Manual feeds are processed only through the admin POST triggers.

## Admin Endpoint

### Path & contract

- `POST /admin/feeds/{feed_id}/downloads`
- Request body: `{ "url": "<video url>" }` (no other fields in v1).
- Response body should include:
  - `feed_id`
  - `download_id`
  - `status` (QUEUED, DOWNLOADED, ERROR, etc.)
  - `new` (boolean)
  - `message` user-readable information

### Behaviour

1. Validate the feed exists and is marked with manual schedule. Reject with 400 if not manual.
2. Use a new helper (`YtdlpWrapper.fetch_manual_download_metadata`) to pull metadata for the URL. If the URL is malformed or the handler cannot recognise it, respond with 400 and an explanatory message. If yt-dlp reports it as live/upcoming or cannot determine required fields, respond with 422.
3. Upsert the download via `DownloadDatabase.upsert_download`. If the row already exists:
   - `status=DOWNLOADED`: leave untouched and report `new: False`.
   - `status=ERROR`: move it to QUEUED via `requeue_downloads`, resetting retries/`last_error`.
   - Any other state (UPCOMING/QUEUED): ensure it is QUEUED (again via `requeue_downloads`) so the next coordinator run retries.
4. Schedule background processing (see next section) before returning. The HTTP response should come back immediately; do not wait for downloads to finish.
5. Return the current status (QUEUED, DOWNLOADED if it already existed, etc.) so clients get deterministic idempotent responses.

### Failure scenarios

- **Invalid/unsupported URL**: return HTTP 400 with `message` explaining the URL could not be parsed or mapped to a supported handler.
- **Live or upcoming content**: return HTTP 422 with guidance that only VOD content is accepted.
- **Internal errors**: return HTTP 500 and log details; the implementation should rely on existing exception wrappers for consistency.

## Background Processing Trigger

- Add a utility (e.g., `ManualFeedRunner.trigger(feed_id: str)`) that schedules `DataCoordinator.process_feed` in the background using `asyncio.create_task`.
- The background task must reuse the global semaphore inside `FeedScheduler`/`DataCoordinator`. Since the task is spawned separately, it will wait for the semaphore if another feed is already processing. This wait should **not** delay the POST response—`create_task` is fire-and-forget.
- Maintain per-feed execution state (e.g., in `app.state.manual_feed_tasks[feed_id]`) containing a single entry:
  - `queued_task`: reference to the coordinator `Task` scheduled for the feed (`None` if nothing is queued or running).

  Workflow for each POST:
  1. Insert/queue the download immediately (upsert + optional `requeue_downloads`).
  2. Determine response semantics:
     - If the download is already DOWNLOADED, respond `new: False` and do not schedule any work.
     - Otherwise, respond `new` based on whether the insert created a row vs updated an existing one, and continue to scheduling.
  3. Scheduling rules:
     - If `queued_task` is `None` or the stored task has completed (`queued_task.done()`), create a new `asyncio.create_task` that runs `process_feed` and store it in `queued_task`.
     - If `queued_task` exists and is still pending (either waiting on the semaphore or already running), do nothing—the pending task will process the new QUEUED download.
  4. Inside the background task:
     - After the task successfully acquires the semaphore (i.e., right before calling `process_feed`), clear the registry entry (`queued_task = None`). This allows subsequent submissions during the run to schedule a fresh task that will wait for the semaphore and execute immediately after the current run finishes.
     - Once `process_feed` completes, simply exit; if a new task was scheduled during the run it is already waiting on the semaphore.
- Log outcomes (success/failure, duration) so operators can trace manual submissions.

### Handling semaphore contention

When another feed holds the global semaphore for an extended time:

- Every submission inserts/queues its download immediately so the POST stays non-blocking.
- If `queued_task` is `None`, a new task is created and stored; it waits for the semaphore and clears the entry once it starts running.
- If a (pending) `queued_task` already exists, new submissions do not spawn additional tasks—the waiting job will process all accumulated QUEUED downloads once it starts.
- If a submission arrives while a manual run is actively downloading (the task cleared the registry after locking the semaphore), `queued_task` is `None`, so a fresh task is scheduled; it waits for the semaphore and runs immediately after the active download completes.
- Response semantics remain:
  - Download already DOWNLOADED → `new: False`, `status: DOWNLOADED`, no requeue.
  - Download in ERROR → `requeue_downloads` moves it back to QUEUED; respond `new: False`, `status: QUEUED`.
  - Brand new download → respond `new: True`, `status: QUEUED`.

## Ytdlp Wrapper Enhancements

Implement `YtdlpWrapper.fetch_manual_download_metadata(feed_id, url, default_yt_args, cookies_path) -> ManualDownloadResult` that:

- Runs yt-dlp metadata extraction for the single URL, reusing handler selection logic (YouTube vs Patreon). Reuse as much parsing logic as practical from `fetch_new_downloads_metadata` (e.g., handler preparation, download parsing helpers) to avoid divergence.
- Validates the entry is VOD-ready (not upcoming/live). If not, raise a dedicated exception (e.g., `ManualSubmissionUnavailableError`) (see `exceptions.py`).
- Produces a `Download` instance with `status=DownloadStatus.QUEUED`, including required fields: title, published datetime, ext, filesize.
- Return both the `Download` object and any auxiliary data needed (e.g., resolved URL) so the admin endpoint can log/report effectively.

## Database Considerations

- No schema migration is strictly required. Use existing `DownloadDatabase.upsert_download` and related helpers.
- When resetting retries/last_error, call the existing `DownloadDatabase.requeue_downloads` helper with the single download ID (it already supports targeted invocations) so we reuse well-tested transition logic.

## Server Wiring

- Update FastAPI app creation (`create_app`, `create_admin_app`) to stash `YtdlpWrapper`, `DataCoordinator`, feed configs (`AppSettings.feeds`), and cookies path into `app.state`.
- Extend `server/dependencies.py` to expose new dependency providers for manual endpoints (e.g., `YtdlpWrapperDep`, `DataCoordinatorDep`, `AppSettingsDep`).
- Ensure the admin router only imports dependencies through the dependency module to keep tests simple.

## Scheduler Adjustments

- When building `FeedScheduler` jobs, skip feeds whose `schedule` is the manual sentinel.
- Log a warning when all feeds are manual to indicate the scheduler will stay idle (maintains parity with existing behaviour where zero feeds cause an error).

## Implementation Plan (detailed tasks)

- [ ] **FeedConfig**: Accept `schedule: "manual"`, enforce title override requirement in manual mode, expose an easy-to-inspect property (`is_manual`). Update docstrings/tests.
- [ ] **StateReconciler**: Bypass yt-dlp discovery for manual feeds; seed DB with YAML metadata (title mandatory) and `source_type=UNKNOWN`. Skip image auto-fetch; honour manual override only.
- [ ] **Scheduler**: Exclude manual feeds when registering jobs; ensure runtime logs reflect manual feeds being manual-only.
- [ ] **App State / Dependencies**: Store `YtdlpWrapper`, `DataCoordinator`, feed configs, cookies path on both app instances; add FastAPI dependency helpers.
- [ ] **YtdlpWrapper**: Implement `fetch_manual_download_metadata` helper with validation, heavy reuse of existing `fetch_new_downloads_metadata` parsing logic, and friendly exceptions.
- [ ] **Download upsert logic**: Add helper to reset retries/last_error when upserting manual submissions (reuse `requeue_downloads` semantics as needed).
- [ ] **Manual feed task runner**: Implement background trigger with per-feed deduplication, error logging, and cleanup.
- [ ] **Admin Endpoint**: Add request/response models, endpoint function, validation flow, metadata fetch, download upsert, and background trigger call. Ensure responses are idempotent.
- [ ] **Docs**: Update README/DESIGN_DOC with configuration snippet, admin POST example, explanation of manual schedule, and note about semaphore wait behaviour.

## Test Plan

- Unit tests for `FeedConfig` parsing, including manual schedule path and title requirement enforcement.
- Unit tests for manual `StateReconciler` flow (no discovery, metadata stored correctly, image handling).
- Unit tests covering the new yt-dlp helper (successful metadata, livestream rejection, Patreon video case).
- Tests for download upsert helper (ERROR → QUEUED transition resets retries/last_error).
- Endpoint tests (FastAPI test client) for:
  - Successful manual submission (returns QUEUED, schedules background task).
  - Duplicate submission (detect existing task, `new=False`).
  - Non-manual feed submission (400).
  - Livestream URL submission (422).
- Integration test: POST a URL, run background event loop until coordinator completes, verify download status becomes DOWNLOADED and RSS updated.

## Follow-up / Nice-to-have

- Temporary in-memory status cache (feed_id → last submission outcome) surfaced via admin API.
- User-facing acknowledgement when background job fails (e.g., push failure into response on next duplicate POST).
- Future authentication layer for admin endpoints.
