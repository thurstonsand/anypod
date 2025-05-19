from collections.abc import Callable
import logging
from pathlib import Path

# from sqlite_utils.utils import sqlite3  # type: ignore
import sqlite3
from typing import Any

from sqlite_utils import Database
from sqlite_utils.db import NotFoundError

from ..exceptions import DatabaseOperationError

logger = logging.getLogger(__name__)


def register_adapter[T: type](tpe: T, value_to_sql: Callable[[T], Any]) -> None:
    sqlite3.register_adapter(tpe, value_to_sql)


class SqliteUtilsCore:
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

    def with_transaction[T](self, func: Callable[[], T]) -> T:
        try:
            with self.db.conn:  # type: ignore
                return func()
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
        try:
            self.db[table_name].create(  # type: ignore
                columns, pk=pk, not_null=not_null, defaults=defaults, if_not_exists=True
            )
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to create table.") from e

    def create_index(
        self, table_name: str, columns: list[str], index_name: str, unique: bool = True
    ) -> None:
        try:
            self.db[table_name].create_index(  # type: ignore
                columns, index_name, unique=unique, if_not_exists=True, analyze=True
            )
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to create index.") from e

    def close(self) -> None:
        self.db.close()

    def execute(self, sql: str, params: dict[str, Any]) -> int:
        try:
            return self.db.execute(sql, params).rowcount  # type: ignore
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to execute query.") from e

    def query(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
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
        # raise Exception(f"record: {record}")
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
        try:
            self.db[table_name].update(pk_values, updates)  # type: ignore
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

    def get(
        self, table_name: str, pk_values: str | tuple[str, ...]
    ) -> dict[str, Any] | None:
        try:
            return self.db[table_name].get(pk_values)  # type: ignore
        except sqlite3.Error as e:
            raise DatabaseOperationError("Failed to get row.") from e
        except NotFoundError:
            return None
