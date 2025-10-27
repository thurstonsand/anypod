"""Add database triggers.

Revision ID: 78f7e4e33398
Revises: 423d964333d1
Create Date: 2025-06-25 23:50:21.208077

"""

from collections.abc import Sequence

from alembic import op
from alembic_helpers.triggers import (  # pyright: ignore[reportMissingImports]
    create_download_triggers,  # pyright: ignore[reportUnknownVariableType]
    create_feed_triggers,  # pyright: ignore[reportUnknownVariableType]
    drop_download_triggers,  # pyright: ignore[reportUnknownVariableType]
    drop_feed_triggers,  # pyright: ignore[reportUnknownVariableType]
)

# revision identifiers, used by Alembic.
revision: str = "78f7e4e33398"
down_revision: str | Sequence[str] | None = "423d964333d1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


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

    create_feed_triggers()
    create_download_triggers()


def downgrade() -> None:
    """Downgrade schema."""
    drop_download_triggers()
    drop_feed_triggers()
