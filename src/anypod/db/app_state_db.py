"""Database access layer for application-wide state."""

from datetime import UTC, datetime, timedelta
import logging

from sqlalchemy import update
from sqlalchemy.dialects.sqlite import insert
from sqlmodel import col

from .decorators import handle_db_errors
from .sqlalchemy_core import SqlalchemyCore
from .types.app_state import AppState

logger = logging.getLogger(__name__)


class AppStateDatabase:
    """Manage persistence of application-wide state.

    Provides methods to fetch and upsert the single "global" state row.
    """

    def __init__(self, db_core: SqlalchemyCore):
        self._db = db_core

    @handle_db_errors("upsert app state")
    async def upsert_last_yt_dlp_update(self, when: datetime | None = None) -> None:
        """Set the last yt-dlp update timestamp to now or provided time.

        Args:
            when: The timestamp to persist. Defaults to current UTC time.
        """
        ts = when or datetime.now(UTC)
        async with self._db.session() as session:
            stmt = insert(AppState).values(id="global", last_yt_dlp_update=ts)
            stmt = stmt.on_conflict_do_update(
                index_elements=[AppState.id],
                set_={"last_yt_dlp_update": ts},
            )
            await session.execute(stmt)
            await session.commit()

    @handle_db_errors("get app state")
    async def get_last_yt_dlp_update(self) -> datetime | None:
        """Return the timestamp of the last yt-dlp update if present."""
        async with self._db.session() as session:
            state = await session.get(AppState, "global")
            return state.last_yt_dlp_update if state else None

    @handle_db_errors("conditional update yt-dlp timestamp")
    async def update_yt_dlp_timestamp_if_stale(self, min_interval: timedelta) -> bool:
        """Update yt-dlp timestamp if enough time has passed since last update.

        Args:
            min_interval: Minimum time that must pass before allowing update.

        Returns:
            True if the timestamp was updated, False if too recent.
        """
        now = datetime.now(UTC)
        cutoff = now - min_interval

        async with self._db.session() as session:
            stmt = (
                update(AppState)
                .where(col(AppState.id) == "global")
                .where(col(AppState.last_yt_dlp_update) <= cutoff)
                .values(last_yt_dlp_update=now)
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0
