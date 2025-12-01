# Database Schema

SQLite database schema and lifecycle details for Anypod.

## Overview

Anypod uses SQLite with SQLModel/SQLAlchemy. The database is managed through Alembic migrations located in `alembic/versions/`.

Database location: `${DATA_DIR}/db/anypod.db`

## Tables

### `feed`

Stores feed metadata and configuration.

| Column                       | Type     | Nullable | Default             | Description                                                                                      |
| ---------------------------- | -------- | -------- | ------------------- | ------------------------------------------------------------------------------------------------ |
| `id`                         | TEXT     | NO       | -                   | **Primary key**, feed identifier from config                                                     |
| `is_enabled`                 | BOOLEAN  | NO       | `1`                 | Whether the feed is enabled for processing                                                       |
| `source_type`                | ENUM     | NO       | -                   | Source type: `CHANNEL`, `PLAYLIST`, `SINGLE_VIDEO`, `MANUAL`, `UNKNOWN`                          |
| `source_url`                 | TEXT     | YES      | NULL                | Original source URL                                                                              |
| `resolved_url`               | TEXT     | YES      | NULL                | URL after redirects                                                                              |
| `last_successful_sync`       | DATETIME | NO       | `CURRENT_TIMESTAMP` | Last successful sync (UTC)                                                                       |
| `created_at`                 | DATETIME | NO       | `CURRENT_TIMESTAMP` | Creation timestamp (UTC)                                                                         |
| `updated_at`                 | DATETIME | NO       | `CURRENT_TIMESTAMP` | Last update timestamp (UTC)                                                                      |
| `last_rss_generation`        | DATETIME | YES      | NULL                | Last RSS generation (UTC)                                                                        |
| `last_failed_sync`           | DATETIME | YES      | NULL                | Last failed sync (UTC)                                                                           |
| `consecutive_failures`       | INTEGER  | NO       | `0`                 | Consecutive sync failure count                                                                   |
| `total_downloads`            | INTEGER  | NO       | `0`                 | Total downloads for this feed                                                                    |
| `since`                      | DATETIME | YES      | NULL                | Retention: only process after this date                                                          |
| `keep_last`                  | INTEGER  | YES      | NULL                | Retention: max downloads to keep                                                                 |
| `transcript_lang`            | TEXT     | YES      | NULL                | Preferred transcript language code (ISO 639-1)                                                   |
| `transcript_source_priority` | TEXT     | YES      | NULL                | Comma-separated transcript source order (`creator`, `auto`)                                      |
| `title`                      | TEXT     | YES      | NULL                | Feed title                                                                                       |
| `subtitle`                   | TEXT     | YES      | NULL                | Feed subtitle                                                                                    |
| `description`                | TEXT     | YES      | NULL                | Feed description                                                                                 |
| `language`                   | TEXT     | YES      | NULL                | Language code (e.g., `en`)                                                                       |
| `author`                     | TEXT     | YES      | NULL                | Feed author                                                                                      |
| `author_email`               | TEXT     | YES      | NULL                | Author email                                                                                     |
| `remote_image_url`           | TEXT     | YES      | NULL                | Original image URL                                                                               |
| `image_ext`                  | TEXT     | YES      | NULL                | Hosted image extension                                                                           |
| `category`                   | TEXT     | NO       | `TV & Film`         | [Apple Podcasts categories](https://podcasters.apple.com/support/1691-apple-podcasts-categories) |
| `podcast_type`               | ENUM     | NO       | `episodic`          | `episodic` or `serial`                                                                           |
| `explicit`                   | ENUM     | NO       | `no`                | `yes`, `no`, or `clean`                                                                          |

**Indexes:**

| Index                | Column(s)    | Purpose              |
| -------------------- | ------------ | -------------------- |
| PRIMARY KEY          | `id`         | Unique feed lookup   |
| `ix_feed_is_enabled` | `is_enabled` | Filter enabled feeds |

---

### `download`

Stores download information, status, and media metadata.

| Column                 | Type     | Nullable | Default             | Description                                    |
| ---------------------- | -------- | -------- | ------------------- | ---------------------------------------------- |
| `feed_id`              | TEXT     | NO       | -                   | **Primary key (part 1)**, FK to `feed.id`      |
| `id`                   | TEXT     | NO       | -                   | **Primary key (part 2)**, download identifier  |
| `source_url`           | TEXT     | NO       | -                   | Source URL for download                        |
| `title`                | TEXT     | NO       | -                   | Episode title                                  |
| `published`            | DATETIME | NO       | -                   | Publication timestamp (UTC)                    |
| `ext`                  | TEXT     | NO       | -                   | File extension                                 |
| `mime_type`            | TEXT     | NO       | -                   | MIME type                                      |
| `filesize`             | INTEGER  | NO       | -                   | File size in bytes (>0)                        |
| `duration`             | INTEGER  | NO       | -                   | Duration in seconds (>0)                       |
| `status`               | ENUM     | NO       | -                   | Download status (see lifecycle below)          |
| `discovered_at`        | DATETIME | NO       | `CURRENT_TIMESTAMP` | First discovered (UTC)                         |
| `updated_at`           | DATETIME | NO       | `CURRENT_TIMESTAMP` | Last update (UTC)                              |
| `remote_thumbnail_url` | TEXT     | YES      | NULL                | Original thumbnail URL                         |
| `thumbnail_ext`        | TEXT     | YES      | NULL                | Hosted thumbnail extension                     |
| `description`          | TEXT     | YES      | NULL                | Episode description                            |
| `quality_info`         | TEXT     | YES      | NULL                | Quality information                            |
| `playlist_index`       | INTEGER  | YES      | NULL                | 1-based index in multi-attachment posts        |
| `retries`              | INTEGER  | NO       | `0`                 | Retry attempt count                            |
| `last_error`           | TEXT     | YES      | NULL                | Last error message                             |
| `download_logs`        | TEXT     | YES      | NULL                | yt-dlp execution logs                          |
| `downloaded_at`        | DATETIME | YES      | NULL                | Download completion time (UTC)                 |
| `transcript_ext`       | TEXT     | YES      | NULL                | Transcript file extension (e.g., "vtt", "srt") |
| `transcript_lang`      | TEXT     | YES      | NULL                | Transcript language code (e.g., "en")          |
| `transcript_source`    | ENUM     | YES      | NULL                | Transcript origin: `CREATOR` or `AUTO`         |

**Indexes:**

| Index                | Column(s)              | Purpose                   |
| -------------------- | ---------------------- | ------------------------- |
| PRIMARY KEY          | `(feed_id, id)`        | Composite unique lookup   |
| `idx_feed_status`    | `(feed_id, status)`    | Filter by feed + status   |
| `idx_feed_published` | `(feed_id, published)` | Order by publication date |

---

### `app_state`

Global application state persistence.

| Column               | Type     | Nullable | Default  | Description                             |
| -------------------- | -------- | -------- | -------- | --------------------------------------- |
| `id`                 | TEXT     | NO       | `global` | **Primary key**, always `"global"`      |
| `last_yt_dlp_update` | DATETIME | NO       | -        | Last yt-dlp `--update-to` attempt (UTC) |

This is a single-row table used to rate-limit yt-dlp update invocations.

---

## Enums

### `DownloadStatus`

```
UPCOMING   → QUEUED   → DOWNLOADED → ARCHIVED
                     ↘ ERROR ↗
                     ↘ SKIPPED
```

| Status       | Description                                    |
| ------------ | ---------------------------------------------- |
| `UPCOMING`   | Metadata fetched, not yet queued for download  |
| `QUEUED`     | Scheduled for download                         |
| `DOWNLOADED` | Successfully downloaded, included in RSS       |
| `ERROR`      | Download failed (can be retried)               |
| `SKIPPED`    | Permanently skipped (e.g., validation failure) |
| `ARCHIVED`   | Pruned by retention policy, excluded from RSS  |

### `SourceType`

| Value          | Description                    |
| -------------- | ------------------------------ |
| `CHANNEL`      | YouTube channel or equivalent  |
| `PLAYLIST`     | YouTube playlist or equivalent |
| `SINGLE_VIDEO` | Individual video URL           |
| `MANUAL`       | Manual submission feed         |
| `UNKNOWN`      | Unrecognized source type       |

### `PodcastType`

| Value      | Description                          |
| ---------- | ------------------------------------ |
| `episodic` | Episodes are independent             |
| `serial`   | Episodes should be consumed in order |

### `PodcastExplicit`

| Value   | Description                         |
| ------- | ----------------------------------- |
| `yes`   | Contains explicit content           |
| `no`    | No explicit content                 |
| `clean` | Cleaned version of explicit content |

### `TranscriptSource`

| Value     | Description                             |
| --------- | --------------------------------------- |
| `CREATOR` | Creator-provided subtitles/transcripts  |
| `AUTO`    | Auto-generated captions (e.g., YouTube) |

---

## State Transitions

**Important**: Download status transitions are implemented as explicit methods, not generic updates. Always use proper state transition methods:

- `db/download_db.py` – Download-specific operations
- `db/feed_db.py` – Feed-specific operations

Do not bypass these helper methods with raw SQL or generic ORM updates.

---

## Migrations

Migrations are managed with Alembic. Migration files are in `alembic/versions/`.

### Workflow

When changing database models in `src/anypod/db/types/`:

1. Make your changes to the SQLModel classes
2. Generate a migration: `uv run alembic revision --autogenerate -m "describe your changes"`
3. Review the generated migration in `alembic/versions/`
4. Commit your changes

The drift check runs automatically via pre-commit on any model or migration changes. You can also run it manually with `./scripts/check_migrations.sh` to verify no uncommitted schema changes exist.

### Commands

```bash
uv run alembic revision --autogenerate -m "description"  # Create migration
uv run alembic upgrade head                              # Apply migrations
uv run alembic downgrade -1                              # Rollback one migration
./scripts/check_migrations.sh                            # Check for schema drift
```

---

## Database Access Pattern

All database access goes through the `SqlalchemyCore` class in `db/sqlalchemy_core.py`. This provides:

- Connection management
- Transaction handling
- Consistent error handling via decorators in `db/decorators.py`

See `DESIGN_DOC.md` for deeper architectural rationale.
