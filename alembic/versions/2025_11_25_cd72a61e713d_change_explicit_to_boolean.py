"""Change explicit to boolean and set default author_email.

Revision ID: cd72a61e713d
Revises: e95766c77882
Create Date: 2025-11-25 03:57:07.434110
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op
from alembic_helpers.triggers import create_feed_triggers_v2, drop_feed_triggers_v2

# revision identifiers, used by Alembic.
revision: str = "cd72a61e713d"
down_revision: str | Sequence[str] | None = "e95766c77882"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    # Data migration: Set default author_email for existing rows with NULL values
    # This aligns with the model change from `str | None` to `str` with a default
    op.execute(
        "UPDATE feed SET author_email = 'notifications@thurstons.house' "
        "WHERE author_email IS NULL"
    )

    # Data migration: Convert explicit string values to 0/1 strings
    # SQLite is loosely typed so updating a VARCHAR column to '0'/'1' is fine.
    op.execute("UPDATE feed SET explicit = '1' WHERE explicit = 'YES'")
    op.execute("UPDATE feed SET explicit = '0' WHERE explicit IN ('NO', 'CLEAN')")
    # Ensure any other values are also 0 just in case
    op.execute("UPDATE feed SET explicit = '0' WHERE explicit NOT IN ('0', '1')")

    # Drop triggers before altering table, as they depend on the table structure
    # and might interfere with the table recreation process
    drop_feed_triggers_v2()

    # Schema migration: Change type to Boolean using batch_alter_table
    # Note: Foreign key checks are disabled at the env.py level for all migrations
    with op.batch_alter_table("feed", schema=None) as batch_op:
        batch_op.alter_column(
            "explicit",
            existing_type=sa.VARCHAR(length=5),
            type_=sa.Boolean(),
            existing_nullable=False,
            existing_server_default="'NO'",
            server_default="0",
        )
        # Now that there's no more NULLs
        batch_op.alter_column(
            "author_email",
            existing_type=sa.VARCHAR(),
            nullable=False,
        )

    # Recreate triggers
    create_feed_triggers_v2()


def downgrade() -> None:
    """Downgrade schema."""
    # Drop triggers before altering table
    drop_feed_triggers_v2()

    # Schema migration: Change type back to VARCHAR using batch_alter_table
    # Note: Foreign key checks are disabled at the env.py level for all migrations
    with op.batch_alter_table("feed", schema=None) as batch_op:
        batch_op.alter_column(
            "explicit",
            existing_type=sa.Boolean(),
            type_=sa.VARCHAR(length=5),
            existing_nullable=False,
            existing_server_default="0",
            server_default="'NO'",
        )
        # Restore author_email to nullable
        batch_op.alter_column(
            "author_email",
            existing_type=sa.VARCHAR(),
            nullable=True,
        )

    # Recreate triggers
    create_feed_triggers_v2()

    # Data migration: Convert boolean/integer 0/1 back to 'YES'/'NO'
    # After conversion to VARCHAR, the values will likely be '1' and '0' strings
    op.execute("UPDATE feed SET explicit = 'YES' WHERE explicit = '1'")
    op.execute("UPDATE feed SET explicit = 'NO' WHERE explicit = '0'")
