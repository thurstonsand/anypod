"""Relax feed's url to be nullable.

Revision ID: ad59f082d627
Revises: e8752d424e88
Create Date: 2025-10-26 18:02:32.698331

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op
from alembic_helpers.triggers import (  # pyright: ignore[reportMissingImports]
    create_feed_triggers,  # pyright: ignore[reportUnknownVariableType]
    drop_feed_triggers,  # pyright: ignore[reportUnknownVariableType]
)

# revision identifiers, used by Alembic.
revision: str = "ad59f082d627"
down_revision: str | Sequence[str] | None = "e8752d424e88"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    drop_feed_triggers()
    with op.batch_alter_table("feed") as batch_op:
        batch_op.alter_column(
            "source_url",
            existing_type=sa.String(),
            nullable=True,
        )
    create_feed_triggers()


def downgrade() -> None:
    """Downgrade schema.

    WARNING: Manual feeds will be disabled after downgrade because their
    synthetic source URLs are not valid yt-dlp sources. If you re-upgrade,
    you'll need to re-enable them manually in the configuration.
    """
    drop_feed_triggers()
    op.execute(
        sa.text(
            "UPDATE feed SET source_url = 'manual:' || id, is_enabled = 0 "
            "WHERE source_url IS NULL"
        )
    )
    with op.batch_alter_table("feed") as batch_op:
        batch_op.alter_column(
            "source_url",
            existing_type=sa.String(),
            nullable=False,
        )
    create_feed_triggers()
