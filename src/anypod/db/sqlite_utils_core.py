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
from sqlite_utils.db import (
    NotFoundError as SqliteUtilsNotFoundError,
    jsonify_if_needed,  # type: ignore
    validate_column_names,  # type: ignore
)

from ..exceptions import DatabaseOperationError, NotFoundError

logger = logging.getLogger(__name__)


def register_adapter[T](tpe: type[T], value_to_sql: Callable[[T], Any]) -> None:
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

    def quote(self, *identifiers: str) -> str:
        """Apply SQLite string quoting to one or more identifiers.

        This method safely quotes SQL identifiers (table names, column names)
        to prevent SQL injection issues. If multiple identifiers are provided,
        they are joined without a separator.

        Args:
            identifiers: One or more strings representing SQL identifiers.

        Returns:
            The quoted identifier string.
        """
        if not identifiers:
            return ""
        # The db.quote method already wraps in single quotes, so we just pass a joined string
        return self.db.quote("".join(identifiers))

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

    def create_trigger(
        self,
        trigger_name: str,
        table_name: str,
        trigger_sql_body: str,
        when_clause: str | None = None,
        trigger_event: str = "AFTER UPDATE",
        of_columns: list[str] | None = None,
        exclude_columns: list[str] | None = None,
        for_each_row: bool = True,
        if_not_exists: bool = True,
        params: dict[str, Any] | None = None,
    ) -> None:
        """Create a database trigger.

        Args:
            trigger_name: The name of the trigger.
            table_name: The table the trigger is associated with.
            trigger_sql_body: The SQL statements to execute when the trigger fires.
                              This should be the content of the BEGIN...END block.
            when_clause: Optional WHEN clause (e.g., "NEW.status = 'downloaded'").
            trigger_event: The event that fires the trigger (e.g., "AFTER INSERT", "BEFORE DELETE").
                           Defaults to "AFTER UPDATE".
            of_columns: Optional list of column names for UPDATE OF clause. If provided, overrides exclude_columns.
            exclude_columns: Optional list of column names to exclude from UPDATE OF clause.
                           Only used for UPDATE triggers when of_columns is not provided.
            for_each_row: If True, the trigger is a FOR EACH ROW trigger. Defaults to True.
            if_not_exists: If True, uses CREATE TRIGGER IF NOT EXISTS. Defaults to True.
            params: Optional dictionary of parameters to bind to the trigger_sql_body
                    or when_clause (for values, not identifiers).

        Raises:
            DatabaseOperationError: If the trigger creation fails.
        """
        if params is None:
            params = {}

        # Safely quote dynamic identifiers
        quoted_table_name = self.quote(table_name)
        quoted_trigger_name = self.quote(trigger_name)

        # Build the trigger SQL statement
        if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        for_each_row_clause = "FOR EACH ROW" if for_each_row else ""
        when_clause_sql = f"WHEN {when_clause}" if when_clause else ""

        match "UPDATE" in trigger_event.upper(), of_columns, exclude_columns:
            # if of_columns is provided, use it
            case True, [_, *_] as of_columns, _:
                pass
            # else if exclude_columns is provided, get all table columns and exclude the specified ones
            case True, [] | None, [_, *_] as exclude_columns:
                table = self.db[table_name]  # type: ignore
                all_columns = set(table.columns_dict.keys())
                trigger_columns = all_columns - set(exclude_columns)
                of_columns = list(trigger_columns)

            # if it is not an UPDATE trigger, or if no columns are provided, don't add an OF clause
            case _:
                of_columns = []

        if of_columns:
            quoted_columns = [self.quote(col) for col in of_columns]
            of_clause = f"OF {', '.join(quoted_columns)}"
        else:
            of_clause = ""
        # Construct the full SQL query string
        full_sql = f"""
            CREATE TRIGGER {if_not_exists_clause}{quoted_trigger_name}
            {trigger_event} {of_clause} ON {quoted_table_name}
            {for_each_row_clause}
            {when_clause_sql}
            BEGIN
                {trigger_sql_body}
            END;
        """

        try:
            self.db.execute(full_sql, params)  # type: ignore
        except sqlite3.Error as e:
            raise DatabaseOperationError(
                f"Failed to create trigger '{trigger_name}'."
            ) from e

    def close(self) -> None:
        """Close the database connection."""
        self.db.close()

    def execute(self, sql: str, params: dict[str, Any] | list[Any]) -> int:
        """Execute a SQL statement and return the number of affected rows.

        Args:
            sql: SQL statement to execute.
            params: Parameters for the SQL statement, either a dictionary for named parameters or a list for positional parameters.

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
        defaults: dict[str, Any] | None = None,
    ) -> None:
        """Insert or update a record in the specified table.

        Args:
            table_name: Name of the table to upsert into.
            record: Dictionary containing the row data.
            pk: Primary key column(s).
            not_null: Set of columns that should be NOT NULL.
            defaults: Default values for columns.

        Raises:
            DatabaseOperationError: If the upsert operation fails.
        """
        try:
            self.db[table_name].upsert(  # type: ignore
                record,
                pk=pk,  # type: ignore
                not_null=not_null,  # type: ignore
                defaults=defaults,  # type: ignore
            )
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to upsert.") from e

    def multi_update(
        self,
        table_name: str,
        updates: dict[str, Any],
        where: str,
        where_args: list[Any] | None = None,
        conversions: dict[str, Any] | None = None,
    ) -> int:
        """Update multiple rows in the specified table.

        Args:
            table_name: Name of the table to update.
            updates: Dictionary of column updates to apply.
            where: WHERE clause conditions for selecting rows to update.
            where_args: Parameters for the WHERE clause.
            conversions: Dictionary of column names to conversion functions.

        Returns:
            Number of rows affected by the update.

        Raises:
            DatabaseOperationError: If the update operation fails.
        """
        if not updates:
            return 0

        conversions = conversions or {}
        where_args = where_args or []
        validate_column_names(updates.keys())

        # Build UPDATE statement
        sets: list[str] = []
        args: list[Any] = []

        for key, value in updates.items():
            sets.append(f"[{key}] = {conversions.get(key, '?')}")
            args.append(jsonify_if_needed(value))

        # Add WHERE args
        args.extend(where_args)

        sql = f"UPDATE [{table_name}] SET {', '.join(sets)} WHERE {where}"

        try:
            return self.execute(sql, args)
        except sqlite3.Error as e:
            raise DatabaseOperationError(f"Failed to update table {table_name}.") from e

    def update(
        self,
        table_name: str,
        pk_values: str | tuple[str, ...],
        updates: dict[str, Any],
        conversions: dict[str, Any] | None = None,
        where: str | None = None,
        where_args: list[Any] | None = None,
    ) -> None:
        """Update a row in the specified table.

        Args:
            table_name: Name of the table to update.
            pk_values: Primary key value(s) identifying the row.
            updates: Dictionary of column updates to apply.
            conversions: Dictionary of column names to conversion functions.
            where: Optional additional WHERE clause conditions. If provided, the update
                   will only occur if both the primary key matches AND the where conditions are met.
            where_args: Parameters for the WHERE clause.

        Raises:
            NotFoundError: If the row is not found or WHERE conditions are not met.
            DatabaseOperationError: If the update operation fails.
        """
        table = self.db[table_name]  # type: ignore
        conversions = conversions or {}
        where_args = where_args or []

        match pk_values:
            case str() as pk_value:
                pk_values_list = [pk_value]
            case tuple() as pk_values:
                pk_values_list = list(pk_values)

        # Soundness check that the record exists (raises error if not) and needs updating:
        self.get(table_name, pk_values)
        if not updates:
            return

        # Build WHERE clause
        wheres = [f"[{pk_name}] = ?" for pk_name in table.pks]  # type: ignore
        all_where_args = pk_values_list[:]

        if where:
            wheres.append(where)
            all_where_args.extend(where_args)

        where_clause = " AND ".join(wheres)

        try:
            with self.transaction():
                match self.multi_update(
                    table_name, updates, where_clause, all_where_args, conversions
                ):
                    case 0:
                        raise NotFoundError("Record not found.")
                    case 1:
                        pass
                    case _ as row_count:
                        raise DatabaseOperationError(
                            f"Update affected {row_count} rows, expected 1. Rolling back transaction."
                        )
                table.last_pk = pk_values[0] if len(table.pks) == 1 else pk_values  # type: ignore
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to update.") from e

    def rows_where(
        self,
        table_name: str,
        where: str | None = None,
        where_args: dict[str, Any] | None = None,
        order_by: str | None = None,
        select: str = "*",
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        """Query rows from a table with WHERE conditions.

        Args:
            table_name: Name of the table to query.
            where: WHERE clause condition. If None, returns all rows.
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

    def count_where(
        self,
        table_name: str,
        where: str | None = None,
        where_args: dict[str, Any] | None = None,
    ) -> int:
        """Count rows in a table matching WHERE conditions.

        Args:
            table_name: Name of the table to count from.
            where: WHERE clause condition. If None, counts all rows.
            where_args: Parameters for the WHERE clause.

        Returns:
            Number of rows matching the conditions.

        Raises:
            DatabaseOperationError: If the count query fails.
        """
        try:
            return self.db[table_name].count_where(where, where_args=where_args)  # type: ignore
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to count rows.") from e

    def get(self, table_name: str, pk_values: str | tuple[str, ...]) -> dict[str, Any]:
        """Get a single row by primary key.

        Args:
            table_name: Name of the table to query.
            pk_values: Primary key value(s) identifying the row.

        Returns:
            Dictionary representing the row.

        Raises:
            DatabaseOperationError: If the query fails.
            NotFoundError: If the row is not found.
        """
        try:
            return self.db[table_name].get(pk_values)  # type: ignore
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to get row.") from e
        except SqliteUtilsNotFoundError as e:
            raise NotFoundError("Record not found.") from e
