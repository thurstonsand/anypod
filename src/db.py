from dataclasses import dataclass
import datetime
from enum import Enum
from pathlib import Path
import sqlite3
from typing import Any


class DownloadStatus(Enum):
    QUEUED = "queued"
    DOWNLOADED = "downloaded"
    ERROR = "error"
    SKIPPED = "skipped"

    def __str__(self) -> str:
        return self.value


@dataclass
class DownloadItem:
    """Represents a download item's data, used for adding/updating."""

    feed: str
    video_id: str
    source_url: str
    title: str
    published: datetime.datetime  # Should be UTC
    ext: str
    duration: float  # in seconds
    status: DownloadStatus
    thumbnail: str | None = None
    path: str | None = None  # Only set when status is DOWNLOADED
    retries: int = 0
    last_error: str | None = None


# TODO: Use a proper migration tool like sqlite-utils or alembic
# For now, we'll just create the table if it doesn't exist.
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS downloads (
  feed         TEXT NOT NULL,
  video_id     TEXT NOT NULL,
  source_url   TEXT NOT NULL,
  title        TEXT NOT NULL,
  published    TEXT NOT NULL,            -- ISO 8601 datetime string
  ext          TEXT NOT NULL,
  duration     REAL NOT NULL,            -- seconds
  thumbnail    TEXT,                     -- URL
  path         TEXT,                     -- Absolute path to downloaded file
  status       TEXT NOT NULL,            -- queued | downloaded | error | skipped
  retries      INTEGER NOT NULL DEFAULT 0,
  last_error   TEXT,
  PRIMARY KEY  (feed, video_id)
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_feed_status ON downloads(feed, status);
"""


class DatabaseManager:
    def __init__(self, db_path: Path):
        """Initializes the DatabaseManager with the path to the SQLite database."""
        self.db_path = db_path
        self._connection: sqlite3.Connection | None = None

    def _get_connection(self) -> sqlite3.Connection:
        """Establishes and returns the database connection, creating it if necessary."""
        if self._connection is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self._connection.row_factory = sqlite3.Row
            self._initialize_schema()
        return self._connection

    def _initialize_schema(self) -> None:
        """Initializes the database schema (tables and indices) if it doesn't exist."""
        if not self._connection:
            # This should ideally not be reached if called correctly after connection setup
            raise RuntimeError(
                "Database connection not established for schema initialization."
            )
        try:
            with self._connection:  # Use connection as context manager for DDL
                self._connection.execute(_CREATE_TABLE_SQL)
                self._connection.execute(_CREATE_INDEX_SQL)
        except sqlite3.Error as e:
            # Propagate error, schema initialization is critical
            raise RuntimeError("Failed to initialize database schema") from e

    def close(self) -> None:
        """Closes the database connection if it's open."""
        if self._connection:
            self._connection.close()
            self._connection = None

    # --- CRUD Operations ---

    def add_item(
        self,
        item: DownloadItem,
    ) -> None:
        """Adds a new item to the downloads table.
        Raises sqlite3.IntegrityError if an item with the same (feed, video_id) already exists.
        """
        sql = """
        INSERT INTO downloads (
            feed, video_id, source_url, title, published,
            ext, duration, thumbnail, status,
            path, retries, last_error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        with self._get_connection() as conn:
            conn.execute(
                sql,
                (
                    item.feed,
                    item.video_id,
                    item.source_url,
                    item.title,
                    item.published.isoformat(),
                    item.ext,
                    item.duration,
                    item.thumbnail,
                    str(item.status),
                    item.path,
                    item.retries,
                    item.last_error,
                ),
            )

    def update_status(
        self,
        feed: str,
        video_id: str,
        status: DownloadStatus,
        path: str | None = None,
        last_error: str | None = None,
    ) -> bool:
        """Updates the status of a download item.
        - If status is DOWNLOADED: path is set, retries and last_error are cleared.
        - If status is ERROR: last_error is set, retries are incremented.
        - If status is QUEUED: path is set to NULL. Retries & last_error persist.
        - If status is SKIPPED: only status is updated. Path, retries & last_error persist.
        Returns True if a row was updated, False otherwise.
        """
        updates = ["status = ?"]
        params: list[Any] = [str(status)]

        if status == DownloadStatus.DOWNLOADED:
            updates.append("path = ?")
            params.append(path)
            updates.append("last_error = NULL")
            updates.append("retries = 0")
        elif status == DownloadStatus.ERROR:
            updates.append("last_error = ?")
            params.append(last_error)
            updates.append("retries = retries + 1")
        elif status == DownloadStatus.QUEUED:
            updates.append(
                "path = NULL"
            )  # Explicitly set path to NULL for QUEUED status
            # Retries and last_error persist.
        elif status == DownloadStatus.SKIPPED:
            # Path, retries and last_error persist.
            pass

        sql = (
            f"UPDATE downloads SET {', '.join(updates)} WHERE feed = ? AND video_id = ?"
        )
        params.extend([feed, video_id])

        with self._get_connection() as conn:
            cursor = conn.execute(sql, tuple(params))
            return cursor.rowcount > 0

    def next_queued_items(self, feed: str, limit: int = 10) -> list[sqlite3.Row]:
        """Retrieves the next 'queued' items for a given feed, oldest first.
        Returns a list of database rows.
        """
        sql = """
        SELECT *
        FROM downloads
        WHERE feed = ? AND status = ?
        ORDER BY published ASC, video_id ASC
        LIMIT ?
        """
        cursor: sqlite3.Cursor | None = None
        try:
            # For SELECT statements, explicit cursor is often clearer
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, (feed, str(DownloadStatus.QUEUED), limit))
                return cursor.fetchall()
        finally:
            if cursor:
                cursor.close()

    def get_items_to_prune_by_keep_last(
        self, feed: str, keep_last: int
    ) -> list[sqlite3.Row]:
        """Identifies downloadable items to prune based on 'keep_last'.
        Returns a list of rows (video_id, path).
        """
        if keep_last <= 0:
            return []
        sql = """
        SELECT video_id, path
        FROM downloads
        WHERE feed = ? AND status = ?
        ORDER BY published DESC, video_id DESC
        LIMIT -1 OFFSET ?
        """
        cursor: sqlite3.Cursor | None = None
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                params = (feed, str(DownloadStatus.DOWNLOADED), keep_last)
                cursor.execute(sql, params)
                return cursor.fetchall()
        finally:
            if cursor:
                cursor.close()

    def get_items_to_prune_by_since(
        self, feed: str, since: datetime.datetime
    ) -> list[sqlite3.Row]:
        """Identifies downloadable items published before the 'since' datetime (UTC).
        Returns a list of rows (video_id, path).
        'since' MUST be a timezone-aware datetime object in UTC.
        """
        sql = """
        SELECT video_id, path
        FROM downloads
        WHERE feed = ?
          AND status = ?
          AND published < ?
        ORDER BY published ASC
        """
        cursor: sqlite3.Cursor | None = None
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                params = (feed, str(DownloadStatus.DOWNLOADED), since.isoformat())
                cursor.execute(sql, params)
                return cursor.fetchall()
        finally:
            if cursor:
                cursor.close()

    def remove_pruned_items(self, feed: str, video_ids: list[str]) -> int:
        """Removes items from the downloads table for a given feed / video_ids.
        Returns the number of items deleted.
        """
        if not video_ids:
            return 0

        # SQLite has a limit on the number of host parameters (variables, ?).
        # Default is SQLITE_MAX_VARIABLE_NUMBER, which is 999.
        # If video_ids list is very long, we might need to batch this.
        # For now, assume it's within reasonable limits for podcast feeds.
        placeholders = ",".join("?" for _ in video_ids)
        sql = f"DELETE FROM downloads WHERE feed = ? AND video_id IN ({placeholders})"

        params: list[Any] = [feed, *video_ids]

        with self._get_connection() as conn:
            cursor = conn.execute(sql, tuple(params))
            return cursor.rowcount

    def get_item_by_video_id(self, feed: str, video_id: str) -> sqlite3.Row | None:
        """Retrieves a specific download item by feed and video_id.
        Returns a database row or None if not found.
        """
        sql = "SELECT * FROM downloads WHERE feed = ? AND video_id = ?"
        cursor: sqlite3.Cursor | None = None
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, (feed, video_id))
                return cursor.fetchone()
        finally:
            if cursor:
                cursor.close()

    def get_errors(
        self, feed: str | None = None, limit: int = 100
    ) -> list[sqlite3.Row]:
        """Retrieves items with 'error' status, newest first.
        Can be filtered by a specific feed. Returns a list of database rows.
        """
        sql_parts = ["SELECT * FROM downloads WHERE status = ?"]
        params: list[Any] = [str(DownloadStatus.ERROR)]

        if feed:
            sql_parts.append("AND feed = ?")
            params.append(feed)

        sql_parts.append("ORDER BY published DESC, video_id DESC")
        sql_parts.append("LIMIT ?")
        params.append(limit)

        sql = " ".join(sql_parts)
        cursor: sqlite3.Cursor | None = None
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, tuple(params))
                return cursor.fetchall()
        finally:
            if cursor:
                cursor.close()
