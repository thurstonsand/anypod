"""Add download logs column.

Revision ID: e95766c77882
Revises: ad59f082d627
Create Date: 2025-11-04 16:31:59.614160

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlmodel.sql.sqltypes import AutoString

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e95766c77882"
down_revision: str | Sequence[str] | None = "ad59f082d627"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("download", sa.Column("download_logs", AutoString(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("download", "download_logs")
