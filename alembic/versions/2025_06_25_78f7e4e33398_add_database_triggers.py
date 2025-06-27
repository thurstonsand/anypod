"""Add database triggers.

Revision ID: 78f7e4e33398
Revises: 423d964333d1
Create Date: 2025-06-25 23:50:21.208077

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "78f7e4e33398"
down_revision: str | Sequence[str] | None = "423d964333d1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Trigger names as constants to ensure consistency
TRIGGER_FEEDS_UPDATE_UPDATED_AT = "feeds_update_updated_at"
TRIGGER_DOWNLOADS_UPDATE_UPDATED_AT = "downloads_update_updated_at"
TRIGGER_DOWNLOADS_UPDATE_DOWNLOADED_AT = "downloads_update_downloaded_at"
TRIGGER_DOWNLOADS_AFTER_INSERT_TOTAL = "downloads_after_insert_total_downloads"
TRIGGER_DOWNLOADS_AFTER_DELETE_TOTAL = "downloads_after_delete_total_downloads"
TRIGGER_DOWNLOADS_STATUS_TO_DOWNLOADED_TOTAL = (
    "downloads_status_to_downloaded_total_downloads"
)
TRIGGER_DOWNLOADS_STATUS_FROM_DOWNLOADED_TOTAL = (
    "downloads_status_from_downloaded_total_downloads"
)


def upgrade() -> None:
    """Upgrade schema."""
    # First, backfill total_downloads counts for existing feeds
    # This ensures accurate counts before triggers start maintaining them
    op.execute(
        """
        UPDATE feed
        SET total_downloads = (
            SELECT COUNT(*)
            FROM download
            WHERE download.feed_id = feed.id
            AND download.status = 'DOWNLOADED'
        );
        """
    )

    # Now create the triggers
    op.execute(
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
        """
    )

    op.execute(
        f"""
        CREATE TRIGGER IF NOT EXISTS {TRIGGER_DOWNLOADS_UPDATE_UPDATED_AT}
        AFTER UPDATE OF feed_id, id, source_url, title, published, ext, mime_type, filesize,
                         duration, status, thumbnail, description, quality_info, retries, last_error ON download
        FOR EACH ROW
        BEGIN
            UPDATE download SET updated_at = (datetime('now', 'utc')) WHERE feed_id = NEW.feed_id AND id = NEW.id;
        END;
        """
    )

    op.execute(
        f"""
        CREATE TRIGGER IF NOT EXISTS {TRIGGER_DOWNLOADS_UPDATE_DOWNLOADED_AT}
        AFTER UPDATE OF status ON download
        FOR EACH ROW
        WHEN NEW.status = 'DOWNLOADED' AND OLD.status != 'DOWNLOADED'
        BEGIN
            UPDATE download SET downloaded_at = (datetime('now', 'utc')) WHERE feed_id = NEW.feed_id AND id = NEW.id;
        END;
        """
    )

    op.execute(
        f"""
        CREATE TRIGGER IF NOT EXISTS {TRIGGER_DOWNLOADS_AFTER_INSERT_TOTAL}
        AFTER INSERT ON download
        FOR EACH ROW
        WHEN NEW.status = 'DOWNLOADED'
        BEGIN
            UPDATE feed SET total_downloads = total_downloads + 1 WHERE id = NEW.feed_id;
        END;
        """
    )

    op.execute(
        f"""
        CREATE TRIGGER IF NOT EXISTS {TRIGGER_DOWNLOADS_AFTER_DELETE_TOTAL}
        AFTER DELETE ON download
        FOR EACH ROW
        WHEN OLD.status = 'DOWNLOADED'
        BEGIN
            UPDATE feed SET total_downloads = total_downloads - 1 WHERE id = OLD.feed_id;
        END;
        """
    )

    op.execute(
        f"""
        CREATE TRIGGER IF NOT EXISTS {TRIGGER_DOWNLOADS_STATUS_TO_DOWNLOADED_TOTAL}
        AFTER UPDATE OF status ON download
        FOR EACH ROW
        WHEN NEW.status = 'DOWNLOADED' AND OLD.status != 'DOWNLOADED'
        BEGIN
            UPDATE feed SET total_downloads = total_downloads + 1 WHERE id = NEW.feed_id;
        END;
        """
    )

    op.execute(
        f"""
        CREATE TRIGGER IF NOT EXISTS {TRIGGER_DOWNLOADS_STATUS_FROM_DOWNLOADED_TOTAL}
        AFTER UPDATE OF status ON download
        FOR EACH ROW
        WHEN OLD.status = 'DOWNLOADED' AND NEW.status != 'DOWNLOADED'
        BEGIN
            UPDATE feed SET total_downloads = total_downloads - 1 WHERE id = NEW.feed_id;
        END;
        """
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Drop triggers in reverse order of creation
    op.execute(
        f"DROP TRIGGER IF EXISTS {TRIGGER_DOWNLOADS_STATUS_FROM_DOWNLOADED_TOTAL}"
    )
    op.execute(f"DROP TRIGGER IF EXISTS {TRIGGER_DOWNLOADS_STATUS_TO_DOWNLOADED_TOTAL}")
    op.execute(f"DROP TRIGGER IF EXISTS {TRIGGER_DOWNLOADS_AFTER_DELETE_TOTAL}")
    op.execute(f"DROP TRIGGER IF EXISTS {TRIGGER_DOWNLOADS_AFTER_INSERT_TOTAL}")
    op.execute(f"DROP TRIGGER IF EXISTS {TRIGGER_DOWNLOADS_UPDATE_DOWNLOADED_AT}")
    op.execute(f"DROP TRIGGER IF EXISTS {TRIGGER_DOWNLOADS_UPDATE_UPDATED_AT}")
    op.execute(f"DROP TRIGGER IF EXISTS {TRIGGER_FEEDS_UPDATE_UPDATED_AT}")
