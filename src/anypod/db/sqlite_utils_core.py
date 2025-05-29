"""Core SQLite database operations using sqlite-utils.

This module provides a wrapper around sqlite-utils for database operations,
including table creation, indexing, and CRUD operations with proper error
handling and logging.
"""

from collections.abc import Callable, Generator
from contextlib import contextmanager
import logging
from pathlib import Path

# from sqlite_utils.utils import sqlite3  # type: ignore
import sqlite3
from typing import Any

from sqlite_utils import Database
from sqlite_utils.db import NotFoundError

from ..exceptions import DatabaseOperationError, DownloadNotFoundError

logger = logging.getLogger(__name__)


def register_adapter[T: type](tpe: T, value_to_sql: Callable[[T], Any]) -> None:
    """Register a SQLite adapter for a Python type.

    Args:
        tpe: The Python type to register an adapter for.
        value_to_sql: Function to convert the type to SQL-compatible value.
    """
    sqlite3.register_adapter(tpe, value_to_sql)


class SqliteUtilsCore:
    """Core wrapper around sqlite-utils for database operations.

    Provides a simplified interface for database operations with proper
    error handling and logging integration.

    Attributes:
        db: The underlying sqlite-utils Database instance.
    """

    def __init__(self, db_path: Path | None, memory_name: str | None = None):
        def my_tracer(sql: str, params: Any) -> None:
            logger.debug(
                "SQL_TRACE",
                extra={
                    "db_path": db_path,
                    "memory_name": memory_name,
                    "sql": sql,
                    "params": params,
                },
            )

        try:
            self.db = Database(
                db_path, memory_name=memory_name, strict=False, tracer=my_tracer
            )
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to initialize database.") from e

    @contextmanager
    def transaction(self) -> Generator[None]:
        """Provide a database transaction context manager.

        Yields:
            None within a database transaction context.

        Raises:
            DatabaseOperationError: If the transaction fails.
        """
        try:
            with self.db.conn:  # type: ignore
                yield
        except sqlite3.Error as e:
            raise DatabaseOperationError("Database transaction failed.") from e

    def create_table(
        self,
        table_name: str,
        columns: dict[str, type],
        pk: str | tuple[str, ...] | None = None,
        not_null: set[str] | None = None,
        defaults: dict[str, Any] | None = None,
    ) -> None:
        """Create a table with specified columns and constraints.

        Args:
            table_name: Name of the table to create.
            columns: Dictionary mapping column names to types.
            pk: Primary key column(s).
            not_null: Set of columns that should be NOT NULL.
            defaults: Default values for columns.

        Raises:
            DatabaseOperationError: If table creation fails.
        """
        try:
            self.db[table_name].create(  # type: ignore
                columns, pk=pk, not_null=not_null, defaults=defaults, if_not_exists=True
            )
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to create table.") from e

    def create_index(
        self, table_name: str, columns: list[str], index_name: str, unique: bool = True
    ) -> None:
        """Create an index on the specified table columns.

        Args:
            table_name: Name of the table to index.
            columns: List of column names to include in the index.
            index_name: Name for the index.
            unique: Whether the index should enforce uniqueness.

        Raises:
            DatabaseOperationError: If index creation fails.
        """
        try:
            self.db[table_name].create_index(  # type: ignore
                columns, index_name, unique=unique, if_not_exists=True, analyze=True
            )
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to create index.") from e

    def close(self) -> None:
        """Close the database connection."""
        self.db.close()

    def execute(self, sql: str, params: dict[str, Any]) -> int:
        """Execute a SQL statement and return the number of affected rows.

        Args:
            sql: SQL statement to execute.
            params: Parameters for the SQL statement.

        Returns:
            Number of rows affected by the statement.

        Raises:
            DatabaseOperationError: If the query execution fails.
        """
        try:
            return self.db.execute(sql, params).rowcount  # type: ignore
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to execute query.") from e

    def query(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a SQL query and return the results.

        Args:
            sql: SQL query to execute.
            params: Parameters for the SQL query.

        Returns:
            List of dictionaries representing matching rows.

        Raises:
            DatabaseOperationError: If the query execution fails.
        """
        try:
            return list(self.db.execute(sql, params))  # type: ignore
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to execute query.") from e

    # --- CRUD Operations ---

    def upsert(
        self,
        table_name: str,
        record: dict[str, Any],
        pk: str | tuple[str, ...],
        not_null: set[str] | None = None,
    ) -> None:
        """Insert or update a record in the specified table.

        Args:
            table_name: Name of the table to upsert into.
            record: Dictionary containing the row data.
            pk: Primary key column(s).
            not_null: Set of columns that should be NOT NULL.

        Raises:
            DatabaseOperationError: If the upsert operation fails.
        """
        try:
            self.db[table_name].upsert(record, pk=pk, not_null=not_null)  # type: ignore
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to upsert.") from e

    def update(
        self,
        table_name: str,
        pk_values: str | tuple[str, ...],
        updates: dict[str, Any],
    ) -> None:
        """Update a row in the specified table.

        Args:
            table_name: Name of the table to update.
            pk_values: Primary key value(s) identifying the row.
            updates: Dictionary of column updates to apply.

        Raises:
            DownloadNotFoundError: If the row is not found.
            DatabaseOperationError: If the update operation fails.
        """
        try:
            self.db[table_name].update(pk_values, updates)  # type: ignore
        except NotFoundError as e:
            raise DownloadNotFoundError(
                message="Download not found.",
            ) from e
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to update.") from e

    def rows_where(
        self,
        table_name: str,
        where: str,
        where_args: dict[str, Any] | None = None,
        order_by: str | None = None,
        select: str = "*",
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        """Query rows from a table with WHERE conditions.

        Args:
            table_name: Name of the table to query.
            where: WHERE clause condition.
            where_args: Parameters for the WHERE clause.
            order_by: ORDER BY clause.
            select: SELECT clause (default "*").
            limit: Maximum number of rows to return.
            offset: Number of rows to skip.

        Returns:
            List of dictionaries representing matching rows.

        Raises:
            DatabaseOperationError: If the query fails.
        """
        try:
            return list(
                self.db[table_name].rows_where(  # type: ignore
                    where,
                    where_args=where_args,
                    order_by=order_by,
                    select=select,
                    limit=limit,
                    offset=offset,
                )
            )
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to get rows.") from e

    def get(self, table_name: str, pk_values: str | tuple[str, ...]) -> dict[str, Any]:
        """Get a single row by primary key.

        Args:
            table_name: Name of the table to query.
            pk_values: Primary key value(s) identifying the row.

        Returns:
            Dictionary representing the row.

        Raises:
            DatabaseOperationError: If the query fails.
            DownloadNotFoundError: If the row is not found.
        """
        try:
            return self.db[table_name].get(pk_values)  # type: ignore
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to get row.") from e
        except NotFoundError as e:
            raise DownloadNotFoundError(
                message="Download not found.",
            ) from e
