"""Shared SQL helpers for SQLite triggers used by migrations."""

from alembic import op

# Feed trigger identifiers
TRIGGER_FEEDS_UPDATE_UPDATED_AT = "feeds_update_updated_at"
TRIGGER_DOWNLOADS_AFTER_INSERT_TOTAL = "downloads_after_insert_total_downloads"
TRIGGER_DOWNLOADS_AFTER_DELETE_TOTAL = "downloads_after_delete_total_downloads"
TRIGGER_DOWNLOADS_STATUS_TO_DOWNLOADED_TOTAL = (
    "downloads_status_to_downloaded_total_downloads"
)
TRIGGER_DOWNLOADS_STATUS_FROM_DOWNLOADED_TOTAL = (
    "downloads_status_from_downloaded_total_downloads"
)

FEED_TRIGGER_NAMES = (
    TRIGGER_FEEDS_UPDATE_UPDATED_AT,
    TRIGGER_DOWNLOADS_AFTER_INSERT_TOTAL,
    TRIGGER_DOWNLOADS_AFTER_DELETE_TOTAL,
    TRIGGER_DOWNLOADS_STATUS_TO_DOWNLOADED_TOTAL,
    TRIGGER_DOWNLOADS_STATUS_FROM_DOWNLOADED_TOTAL,
)

FEED_TRIGGER_STATEMENTS = (
    f"""
        CREATE TRIGGER IF NOT EXISTS {TRIGGER_FEEDS_UPDATE_UPDATED_AT}
        AFTER UPDATE OF id, is_enabled, source_type, source_url, last_successful_sync,
                         last_rss_generation, last_failed_sync, consecutive_failures,
                         since, keep_last, title, subtitle, description, language, author, image_url,
                         category, explicit ON feed
        FOR EACH ROW
        BEGIN
            UPDATE feed SET updated_at = (datetime('now', 'utc')) WHERE id = NEW.id;
        END;
    """,
    f"""
        CREATE TRIGGER IF NOT EXISTS {TRIGGER_DOWNLOADS_AFTER_INSERT_TOTAL}
        AFTER INSERT ON download
        FOR EACH ROW
        WHEN NEW.status = 'DOWNLOADED'
        BEGIN
            UPDATE feed SET total_downloads = total_downloads + 1 WHERE id = NEW.feed_id;
        END;
    """,
    f"""
        CREATE TRIGGER IF NOT EXISTS {TRIGGER_DOWNLOADS_AFTER_DELETE_TOTAL}
        AFTER DELETE ON download
        FOR EACH ROW
        WHEN OLD.status = 'DOWNLOADED'
        BEGIN
            UPDATE feed SET total_downloads = total_downloads - 1 WHERE id = OLD.feed_id;
        END;
    """,
    f"""
        CREATE TRIGGER IF NOT EXISTS {TRIGGER_DOWNLOADS_STATUS_TO_DOWNLOADED_TOTAL}
        AFTER UPDATE OF status ON download
        FOR EACH ROW
        WHEN NEW.status = 'DOWNLOADED' AND OLD.status != 'DOWNLOADED'
        BEGIN
            UPDATE feed SET total_downloads = total_downloads + 1 WHERE id = NEW.feed_id;
        END;
    """,
    f"""
        CREATE TRIGGER IF NOT EXISTS {TRIGGER_DOWNLOADS_STATUS_FROM_DOWNLOADED_TOTAL}
        AFTER UPDATE OF status ON download
        FOR EACH ROW
        WHEN OLD.status = 'DOWNLOADED' AND NEW.status != 'DOWNLOADED'
        BEGIN
            UPDATE feed SET total_downloads = total_downloads - 1 WHERE id = NEW.feed_id;
        END;
    """,
)

# Download trigger identifiers
TRIGGER_DOWNLOADS_UPDATE_UPDATED_AT = "downloads_update_updated_at"
TRIGGER_DOWNLOADS_UPDATE_DOWNLOADED_AT = "downloads_update_downloaded_at"

DOWNLOAD_TRIGGER_NAMES = (
    TRIGGER_DOWNLOADS_UPDATE_UPDATED_AT,
    TRIGGER_DOWNLOADS_UPDATE_DOWNLOADED_AT,
)

DOWNLOAD_TRIGGER_STATEMENTS = (
    f"""
        CREATE TRIGGER IF NOT EXISTS {TRIGGER_DOWNLOADS_UPDATE_UPDATED_AT}
        AFTER UPDATE OF feed_id, id, source_url, title, published, ext, mime_type, filesize,
                         duration, status, thumbnail, description, quality_info, retries, last_error ON download
        FOR EACH ROW
        BEGIN
            UPDATE download SET updated_at = (datetime('now', 'utc')) WHERE feed_id = NEW.feed_id AND id = NEW.id;
        END;
    """,
    f"""
        CREATE TRIGGER IF NOT EXISTS {TRIGGER_DOWNLOADS_UPDATE_DOWNLOADED_AT}
        AFTER UPDATE OF status ON download
        FOR EACH ROW
        WHEN NEW.status = 'DOWNLOADED' AND OLD.status != 'DOWNLOADED'
        BEGIN
            UPDATE download SET downloaded_at = (datetime('now', 'utc')) WHERE feed_id = NEW.feed_id AND id = NEW.id;
        END;
    """,
)


def create_feed_triggers() -> None:
    """Create feed-related triggers."""
    for statement in FEED_TRIGGER_STATEMENTS:
        op.execute(statement)


def drop_feed_triggers() -> None:
    """Drop feed-related triggers if present."""
    for trigger in FEED_TRIGGER_NAMES:
        op.execute(f"DROP TRIGGER IF EXISTS {trigger}")


def create_download_triggers() -> None:
    """Create download-related triggers."""
    for statement in DOWNLOAD_TRIGGER_STATEMENTS:
        op.execute(statement)


def drop_download_triggers() -> None:
    """Drop download-related triggers if present."""
    for trigger in DOWNLOAD_TRIGGER_NAMES:
        op.execute(f"DROP TRIGGER IF EXISTS {trigger}")
