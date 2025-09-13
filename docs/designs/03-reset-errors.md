# Admin Endpoint: Reset `ERROR` Downloads On Demand

## Summary
- Goal (from `TASK_LIST.md:555`): Provide an HTTP endpoint to reset `ERROR` downloads on demand.
- Admin APIs run on a separate server/port so they need not be publicly exposed.

## Current Behavior (Context)
- `max_errors` is a feed-level config (`src/anypod/config/feed_config.py:53`). It is not stored in DB.
- Transitions to `ERROR` occur when `retries >= max_errors` via `DownloadDatabase.bump_retries` (`src/anypod/db/download_db.py:401`).
- Re-queueing exists via `DownloadDatabase.requeue_downloads(...)` which sets `status=QUEUED`, `retries=0`, `last_error=NULL` (`src/anypod/db/download_db.py:155`).
- HTTP routers include `health` and `static` only (`src/anypod/server/app.py`). No admin endpoints yet.

## Design Overview
- Expose an admin HTTP endpoint to reset (re-queue) downloads currently in `ERROR` for a specified feed.
- Run admin APIs on a separate FastAPI app and uvicorn server bound to a different port/host, so they can remain private (e.g., localhost-only by default).
- Add a bulk requeue operation in the DB layer to efficiently update by `feed_id` + `status` in a single SQL update (no per-ID enumeration).
- Keep admin endpoint under `/api` and assume trusted, private access per project scope.

## API Design
- Route: `POST /api/feeds/{feed_id}/reset-errors`
- Input: `feed_id` path param validated by `ValidatedFeedId`.
- Behavior:
  - Validate feed exists; if missing, return 404.
  - Bulk re-queue via a DB-level update where `feed_id = :feed_id AND status = ERROR`.
  - Idempotent: If none are in `ERROR`, returns count 0.
- Response (JSON): `{ "feed_id": string, "reset_count": number }`.
- Errors:
  - 404 if `feed_id` not found.
  - 500 on DB failures with structured logs.

## Admin Server
- Separate server for admin endpoints to avoid public exposure of admin operations.
- Settings:
  - `ADMIN_SERVER_PORT` (default: `8025`)
- Implementation:
  - Add `create_admin_app()` with admin router and health endpoint.
  - Add `create_admin_server(settings, ...)` similar to `create_server`, but bound to admin port.
  - Start both uvicorn servers concurrently in `cli/default.py` using `asyncio.gather`.
  - Health endpoint exists on both servers for flexibility.
  - Document that operators should only expose the public server port in Docker/compose.

## Implementation Steps
1) HTTP API
   - Add `server/routers/admin.py` with endpoint `POST /api/feeds/{feed_id}/reset-errors`.
   - Define response model and use `FeedDatabaseDep` + `DownloadDatabaseDep`.
   - Validate feed via `FeedDatabase.get_feed_by_id`; return 404 when missing.

2) Admin Server & Settings
   - Add new setting: `ADMIN_SERVER_PORT`.
   - Add `create_admin_app()` and `create_admin_server()`.
   - In default mode, run admin server concurrently with main server.

3) DB Layer: Bulk Requeue
   - Enhance `DownloadDatabase.requeue_downloads` to support bulk operations with `download_ids=None` and required `from_status` parameter.
   - Implement with `UPDATE download SET status=QUEUED, retries=0, last_error=NULL WHERE feed_id=:feed_id AND status=:from_status`.

4) Tests
   - DB: Verify `requeue_downloads(..., from_status=ERROR)` ignores non-`ERROR` statuses.
   - DB: Verify bulk `requeue_downloads(feed_id, None, ERROR)` updates all ERROR rows in one call.
   - API: Endpoint returns `reset_count` and invokes bulk requeue for the feed; 404 on missing feed.

5) Docs
   - Update README/API section with the new admin endpoint and its private nature (local-only by default).

## Acceptance Criteria
- `POST /api/feeds/{feed_id}/reset-errors` responds with `reset_count` and performs the same reset.
- No changes to other statuses.
- Logging includes `feed_id` and `reset_count`.

## Risks & Mitigations
- Large feeds: bulk update is a single SQL statement; should be efficient.
- Ensure both servers shut down gracefully together (use shared shutdown callback in default mode).
- Docker health check uses static server port since health endpoint exists on both servers.

## Open Questions
- Do we also want a global reset endpoint (all feeds) for parity? If yes: `POST /api/feeds/reset-errors`.

## File Touch Points
- `src/anypod/server/routers/admin.py` (new router)
- `src/anypod/server/app.py` or `src/anypod/server/admin_app.py` (admin app factory)
- `src/anypod/server/server.py` or `src/anypod/server/admin_server.py` (admin server factory)
- `src/anypod/config/config.py` (admin server settings)
- `README.md` (document endpoint and admin server)
