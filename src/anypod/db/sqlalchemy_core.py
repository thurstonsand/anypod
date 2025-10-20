"""Core async database components using SQLAlchemy and SQLModel."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import logging
from pathlib import Path
import sqlite3
from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import CursorResult, Engine, Result
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import ConnectionPoolEntry

from ..exceptions import DatabaseOperationError, NotFoundError

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

    async def close(self) -> None:
        """Close the database engine and all its connections."""
        await self.engine.dispose()

    @staticmethod
    def as_cursor_result(result: Result[Any]) -> CursorResult[Any]:
        """Coerce a Result to a CursorResult.

        Args:
            result: Result object returned by ``AsyncSession.execute``.

        Returns:
            The result coerced to :class:`CursorResult` so row-level metadata is available.

        Raises:
            DatabaseOperationError: If the result is not backed by a cursor.
        """
        if isinstance(result, CursorResult):
            return result
        raise DatabaseOperationError(
            f"Expected cursor-backed SQLAlchemy result, got {type(result).__name__}.",
        )

    @staticmethod
    def assert_exactly_one_row_affected(
        result: Result[Any], **identifiers: str | None
    ) -> None:
        """Validate that exactly one row was affected by an update operation.

        Args:
            result: The result from the update operation.
            **identifiers: Key-value pairs identifying the entity (e.g., feed_id="test").

        Raises:
            NotFoundError: If no rows were affected.
            DatabaseOperationError: If more than one row was affected.
        """
        cursor_result = SqlalchemyCore.as_cursor_result(result)
        match cursor_result.rowcount:
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
