"""add thumbnail hosting fields and renames.

Revision ID: 329f6c9ad1b2
Revises: 23cf24f50086
Create Date: 2025-08-25 21:11:47.167726
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "329f6c9ad1b2"
down_revision: str | Sequence[str] | None = "23cf24f50086"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    # Rename download.thumbnail -> download.original_thumbnail_url
    op.execute("ALTER TABLE download RENAME COLUMN thumbnail TO original_thumbnail_url")

    # Add download.thumbnail_ext column (nullable for seamless upgrade)
    op.add_column(
        "download",
        sa.Column("thumbnail_ext", sa.String(), nullable=True),
    )

    # Rename feed.image_url -> feed.original_image_url
    op.execute("ALTER TABLE feed RENAME COLUMN image_url TO original_image_url")


def downgrade() -> None:
    """Downgrade schema."""
    # Drop download.thumbnail_ext column
    op.drop_column("download", "thumbnail_ext")

    # Rename download.original_thumbnail_url -> download.thumbnail
    op.execute("ALTER TABLE download RENAME COLUMN original_thumbnail_url TO thumbnail")

    # Rename feed.original_image_url -> feed.image_url
    op.execute("ALTER TABLE feed RENAME COLUMN original_image_url TO image_url")
