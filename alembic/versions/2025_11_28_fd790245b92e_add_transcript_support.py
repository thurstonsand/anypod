"""add transcript support.

Revision ID: fd790245b92e
Revises: cd72a61e713d
Create Date: 2025-11-28 16:22:34.782977
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "fd790245b92e"
down_revision: str | Sequence[str] | None = "cd72a61e713d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("download", sa.Column("transcript_ext", sa.String(), nullable=True))
    op.add_column("download", sa.Column("transcript_lang", sa.String(), nullable=True))
    op.add_column(
        "feed", sa.Column("transcript_source_priority", sa.String(), nullable=True)
    )
    op.add_column(
        "download", sa.Column("transcript_source", sa.String(), nullable=True)
    )
    op.add_column("feed", sa.Column("transcript_lang", sa.String(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("feed", "transcript_lang")
    op.drop_column("download", "transcript_source")
    op.drop_column("feed", "transcript_source_priority")
    op.drop_column("download", "transcript_lang")
    op.drop_column("download", "transcript_ext")
