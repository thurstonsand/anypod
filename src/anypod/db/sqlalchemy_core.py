"""Core async database components using SQLAlchemy and SQLModel."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import logging
from pathlib import Path
import sqlite3
from typing import Any

from sqlalchemy import event, text
from sqlalchemy.engine import CursorResult, Engine
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import ConnectionPoolEntry
from sqlmodel import SQLModel

from ..exceptions import DatabaseOperationError, NotFoundError
from .types.timezone_aware_datetime import SQLITE_DATETIME_NOW

logger = logging.getLogger(__name__)


class SqlalchemyCore:
    """Core wrapper for SQLAlchemy async operations."""

    def __init__(self, db_dir: Path) -> None:
        db_path = db_dir / "anypod.db"
        db_url = f"sqlite+aiosqlite:///{db_path.resolve()}"
        # Simple setup for low-volume service
        self.engine: AsyncEngine = create_async_engine(
            db_url,
            echo=logger.isEnabledFor(logging.DEBUG),  # enable when DEBUG logging is on
            pool_size=1,  # SQLite only supports single writer anyway
            connect_args={
                "check_same_thread": False,  # Required for async
                "timeout": 60.0,  # Connection timeout in seconds
            },
        )
        self.async_session_maker = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def create_db_and_tables(self) -> None:
        """Create all tables in the database based on SQLModel metadata."""
        async with self.engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)
            # Create SQLite triggers for automatic timestamp updates
            await self._create_timestamp_triggers(conn)

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession]:
        """Provide a transactional session.

        This is the primary entry point for database operations.
        It ensures the session is properly closed after use.

        Yields:
            An active, transactional AsyncSession.
        """
        async with self.async_session_maker() as session:
            yield session

    async def _create_timestamp_triggers(self, conn: AsyncConnection) -> None:
        """Create SQLite triggers for automatic timestamp updates.

        - Feed: UPDATE OF all columns EXCEPT created_at, updated_at
        - Download: UPDATE OF all columns EXCEPT discovered_at, updated_at, downloaded_at
        - Download downloaded_at: UPDATE OF status column with condition
        """
        await conn.execute(
            text(
                f"""
                CREATE TRIGGER IF NOT EXISTS feeds_update_updated_at
                AFTER UPDATE OF id, is_enabled, source_type, source_url, last_successful_sync,
                                 last_rss_generation, last_failed_sync, consecutive_failures,
                                 since, keep_last, title, subtitle, description, language, author, image_url,
                                 category, explicit ON feed
                FOR EACH ROW
                BEGIN
                    UPDATE feed SET updated_at = {SQLITE_DATETIME_NOW} WHERE id = NEW.id;
                END;
                """
            )
        )

        await conn.execute(
            text(
                f"""
                CREATE TRIGGER IF NOT EXISTS downloads_update_updated_at
                AFTER UPDATE OF feed_id, id, source_url, title, published, ext, mime_type, filesize,
                                 duration, status, thumbnail, description, quality_info, retries, last_error ON download
                FOR EACH ROW
                BEGIN
                    UPDATE download SET updated_at = {SQLITE_DATETIME_NOW} WHERE feed_id = NEW.feed_id AND id = NEW.id;
                END;
                """
            )
        )

        await conn.execute(
            text(
                f"""
                CREATE TRIGGER IF NOT EXISTS downloads_update_downloaded_at
                AFTER UPDATE OF status ON download
                FOR EACH ROW
                WHEN NEW.status = 'DOWNLOADED' AND OLD.status != 'DOWNLOADED'
                BEGIN
                    UPDATE download SET downloaded_at = {SQLITE_DATETIME_NOW} WHERE feed_id = NEW.feed_id AND id = NEW.id;
                END;
                """
            )
        )

        # --- Triggers to maintain feed.total_downloads ---
        await conn.execute(
            text(
                """
                CREATE TRIGGER IF NOT EXISTS downloads_after_insert_total_downloads
                AFTER INSERT ON download
                FOR EACH ROW
                WHEN NEW.status = 'DOWNLOADED'
                BEGIN
                    UPDATE feed SET total_downloads = total_downloads + 1 WHERE id = NEW.feed_id;
                END;
                """
            )
        )

        await conn.execute(
            text(
                """
                CREATE TRIGGER IF NOT EXISTS downloads_after_delete_total_downloads
                AFTER DELETE ON download
                FOR EACH ROW
                WHEN OLD.status = 'DOWNLOADED'
                BEGIN
                    UPDATE feed SET total_downloads = total_downloads - 1 WHERE id = OLD.feed_id;
                END;
                """
            )
        )

        await conn.execute(
            text(
                """
                CREATE TRIGGER IF NOT EXISTS downloads_status_to_downloaded_total_downloads
                AFTER UPDATE OF status ON download
                FOR EACH ROW
                WHEN NEW.status = 'DOWNLOADED' AND OLD.status != 'DOWNLOADED'
                BEGIN
                    UPDATE feed SET total_downloads = total_downloads + 1 WHERE id = NEW.feed_id;
                END;
                """
            )
        )

        await conn.execute(
            text(
                """
                CREATE TRIGGER IF NOT EXISTS downloads_status_from_downloaded_total_downloads
                AFTER UPDATE OF status ON download
                FOR EACH ROW
                WHEN OLD.status = 'DOWNLOADED' AND NEW.status != 'downloaded'
                BEGIN
                    UPDATE feed SET total_downloads = total_downloads - 1 WHERE id = NEW.feed_id;
                END;
                """
            )
        )

    async def close(self) -> None:
        """Close the database engine and all its connections."""
        await self.engine.dispose()

    @staticmethod
    def assert_exactly_one_row_affected(
        result: CursorResult[Any], **identifiers: str | None
    ) -> None:
        """Validate that exactly one row was affected by an update operation.

        Args:
            result: The cursor result from the update operation.
            **identifiers: Key-value pairs identifying the entity (e.g., feed_id="test").

        Raises:
            NotFoundError: If no rows were affected.
            DatabaseOperationError: If more than one row was affected.
        """
        match result.rowcount:
            case 0:
                raise NotFoundError("Record not found.")
            case 1:
                pass  # Expected case
            case rowcount:  # >1
                raise DatabaseOperationError(
                    f"Update affected {rowcount} rows, expected 1.", **identifiers
                )


@event.listens_for(Engine, "connect")
def _(
    dbapi_connection: sqlite3.Connection, _connection_record: ConnectionPoolEntry
) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA synchronous = NORMAL;")
    cursor.execute("PRAGMA foreign_keys = ON;")
