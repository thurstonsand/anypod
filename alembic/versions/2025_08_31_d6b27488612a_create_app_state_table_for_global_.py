"""create app_state table for global settings.

Revision ID: d6b27488612a
Revises: 329f6c9ad1b2
Create Date: 2025-08-31 00:37:42.255607
"""

from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from sqlalchemy.sql import column, table

from alembic import op
from anypod.db.types.timezone_aware_datetime import TimezoneAwareDatetime

# revision identifiers, used by Alembic.
revision: str = "d6b27488612a"
down_revision: str | Sequence[str] | None = "329f6c9ad1b2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "appstate",
        sa.Column("id", sa.String(), primary_key=True, nullable=False),
        sa.Column(
            "last_yt_dlp_update",
            TimezoneAwareDatetime(),
            nullable=False,
        ),
    )

    # Seed the global state row with current timestamp
    appstate_table = table(
        "appstate",
        column("id", sa.String),
        column(
            "last_yt_dlp_update",
            TimezoneAwareDatetime,
        ),
    )
    op.bulk_insert(
        appstate_table,
        [{"id": "global", "last_yt_dlp_update": datetime.now(UTC)}],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("appstate")
