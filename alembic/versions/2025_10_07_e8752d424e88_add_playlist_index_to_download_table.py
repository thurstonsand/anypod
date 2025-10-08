"""Add playlist_index to download table.

Revision ID: e8752d424e88
Revises: d6b27488612a
Create Date: 2025-10-07 22:18:08.484548
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e8752d424e88"
down_revision: str | Sequence[str] | None = "d6b27488612a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("download", sa.Column("playlist_index", sa.Integer(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("download", "playlist_index")
