"""add thumbnail hosting fields and renames.

Revision ID: 329f6c9ad1b2
Revises: 23cf24f50086
Create Date: 2025-08-25 21:11:47.167726
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op
from alembic_helpers.triggers import (  # pyright: ignore[reportMissingImports]
    create_download_triggers,  # pyright: ignore[reportUnknownVariableType]
    create_download_triggers_v2,  # pyright: ignore[reportUnknownVariableType]
    create_feed_triggers,  # pyright: ignore[reportUnknownVariableType]
    create_feed_triggers_v2,  # pyright: ignore[reportUnknownVariableType]
    drop_download_triggers,  # pyright: ignore[reportUnknownVariableType]
    drop_download_triggers_v2,  # pyright: ignore[reportUnknownVariableType]
    drop_feed_triggers,  # pyright: ignore[reportUnknownVariableType]
    drop_feed_triggers_v2,  # pyright: ignore[reportUnknownVariableType]
)

# revision identifiers, used by Alembic.
revision: str = "329f6c9ad1b2"
down_revision: str | Sequence[str] | None = "23cf24f50086"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    # Drop existing triggers before renaming columns
    # SQLite doesn't update trigger definitions when columns are renamed
    drop_feed_triggers()
    drop_download_triggers()

    # Rename download.thumbnail -> download.remote_thumbnail_url
    op.execute("ALTER TABLE download RENAME COLUMN thumbnail TO remote_thumbnail_url")

    # Add download.thumbnail_ext column
    op.add_column(
        "download",
        sa.Column("thumbnail_ext", sa.String(), nullable=True),
    )

    # Rename feed.image_url -> feed.remote_image_url
    op.execute("ALTER TABLE feed RENAME COLUMN image_url TO remote_image_url")

    # Add feed.image_ext column
    op.add_column(
        "feed",
        sa.Column("image_ext", sa.String(), nullable=True),
    )

    # Recreate triggers with updated column names
    create_feed_triggers_v2()
    create_download_triggers_v2()


def downgrade() -> None:
    """Downgrade schema."""
    # Drop v2 triggers before reverting column names
    drop_feed_triggers_v2()
    drop_download_triggers_v2()

    # Drop download.thumbnail_ext column
    op.drop_column("download", "thumbnail_ext")

    # Rename download.remote_thumbnail_url -> download.thumbnail
    op.execute("ALTER TABLE download RENAME COLUMN remote_thumbnail_url TO thumbnail")

    # Rename feed.remote_image_url -> feed.image_url
    op.execute("ALTER TABLE feed RENAME COLUMN remote_image_url TO image_url")

    # Drop feed.image_ext column
    op.drop_column("feed", "image_ext")

    # Recreate v1 triggers with original column names
    create_feed_triggers()
    create_download_triggers()
