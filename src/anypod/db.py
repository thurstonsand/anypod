from dataclasses import dataclass
import datetime
from enum import Enum
from pathlib import Path
import sqlite3
from typing import Any


class DownloadStatus(Enum):
    UPCOMING = "upcoming"
    QUEUED = "queued"
    DOWNLOADED = "downloaded"
    ERROR = "error"
    SKIPPED = "skipped"
    ARCHIVED = "archived"

    def __str__(self) -> str:
        return self.value


@dataclass(eq=False)
class Download:
    """Represents a download's data, used for adding/updating."""

    feed: str
    id: str
    source_url: str
    title: str
    published: datetime.datetime  # Should be UTC
    ext: str
    duration: float  # in seconds
    status: DownloadStatus
    thumbnail: str | None = None
    retries: int = 0
    last_error: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Download":
        """Converts a sqlite3.Row to a Download object."""
        # Ensure datetime conversion is robust
        published_str = row["published"]
        try:
            published_dt = datetime.datetime.fromisoformat(published_str)
        except (TypeError, ValueError) as e:
            # Handle cases where published_str might be invalid format
            raise ValueError(
                f"Invalid date format for 'published' in DB row: {published_str}"
            ) from e

        # Ensure status conversion is robust
        status_str = row["status"]
        try:
            status_enum = DownloadStatus(status_str)
        except ValueError as e:
            # Handle cases where status_str is not a valid DownloadStatus member
            raise ValueError(f"Invalid status value in DB row: {status_str}") from e

        return cls(
            feed=row["feed"],
            id=row["id"],
            source_url=row["source_url"],
            title=row["title"],
            published=published_dt,
            ext=row["ext"],
            duration=float(row["duration"]),  # Ensure duration is float
            thumbnail=row["thumbnail"],
            status=status_enum,
            retries=row["retries"],
            last_error=row["last_error"],
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Download):
            return NotImplemented
        # Equality is based solely on the composite primary key
        return self.feed == other.feed and self.id == other.id

    def __hash__(self) -> int:
        # Hash is based solely on the composite primary key
        return hash((self.feed, self.id))


# TODO: Use a proper migration tool like sqlite-utils or alembic
# For now, we'll just create the table if it doesn't exist.
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS downloads (
  feed         TEXT NOT NULL,
  id           TEXT NOT NULL,
  source_url   TEXT NOT NULL,
  title        TEXT NOT NULL,
  published    TEXT NOT NULL,            -- ISO 8601 datetime string
  ext          TEXT NOT NULL,
  duration     REAL NOT NULL,            -- seconds
  thumbnail    TEXT,                     -- URL
  status       TEXT NOT NULL,            -- upcoming | queued | downloaded | error | skipped
  retries      INTEGER NOT NULL DEFAULT 0,
  last_error   TEXT,
  PRIMARY KEY  (feed, id)
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

    def upsert_download(
        self,
        download: Download,
    ) -> None:
        """Inserts or updates a download in the downloads table (upsert behavior).
        If a download with the same (feed, id) exists, it will be replaced.
        """
        sql = """
        INSERT INTO downloads (
            feed, id, source_url, title, published,
            ext, duration, thumbnail, status,
            retries, last_error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(feed, id) DO UPDATE SET
            source_url = excluded.source_url,
            title = excluded.title,
            published = excluded.published,
            ext = excluded.ext,
            duration = excluded.duration,
            thumbnail = excluded.thumbnail,
            status = excluded.status,
            retries = excluded.retries,
            last_error = excluded.last_error
        """
        with self._get_connection() as conn:
            conn.execute(
                sql,
                (
                    download.feed,
                    download.id,
                    download.source_url,
                    download.title,
                    download.published.isoformat(),
                    download.ext,
                    download.duration,
                    download.thumbnail,
                    str(download.status),
                    download.retries,
                    download.last_error,
                ),
            )

    def update_status(
        self,
        feed: str,
        id: str,
        status: DownloadStatus,
        last_error: str | None = None,
    ) -> bool:
        """Updates the status of a download.
        - If status is DOWNLOADED: retries and last_error are cleared.
        - If status is ERROR: last_error is set, retries are incremented.
        - If status is QUEUED: retries and last_error persist.
        - If status is SKIPPED: only status is updated. retries and last_error persist.
        - If status is ARCHIVED: only status is updated. retries and last_error persist.
        - If status is UPCOMING: only status is updated. retries and last_error persist.
        Returns True if a row was updated, False otherwise.
        """
        updates = ["status = ?"]
        params: list[Any] = [str(status)]

        if status == DownloadStatus.DOWNLOADED:
            updates.append("last_error = NULL")
            updates.append("retries = 0")
        elif status == DownloadStatus.ERROR:
            updates.append("last_error = ?")
            params.append(last_error)
            updates.append("retries = retries + 1")
        elif (
            status == DownloadStatus.UPCOMING
            or status == DownloadStatus.QUEUED
            or status == DownloadStatus.SKIPPED
            or status == DownloadStatus.ARCHIVED
        ):
            # Only update the status
            pass

        sql = f"UPDATE downloads SET {', '.join(updates)} WHERE feed = ? AND id = ?"
        params.extend([feed, id])

        with self._get_connection() as conn:
            cursor = conn.execute(sql, tuple(params))
            return cursor.rowcount > 0

    def get_downloads_to_prune_by_keep_last(
        self, feed: str, keep_last: int
    ) -> list[sqlite3.Row]:
        """Identifies downloads to prune based on 'keep_last'.
        Returns a list of rows.
        Excludes items with status ARCHIVED or UPCOMING.
        """
        if keep_last <= 0:
            return []
        sql = """
        SELECT *
        FROM downloads
        WHERE feed = ? AND status NOT IN (?, ?)
        ORDER BY published DESC, id DESC
        LIMIT -1 OFFSET ?
        """
        cursor: sqlite3.Cursor | None = None
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                params = (
                    feed,
                    str(DownloadStatus.ARCHIVED),
                    str(DownloadStatus.UPCOMING),
                    keep_last,
                )
                cursor.execute(sql, params)
                return cursor.fetchall()
        finally:
            if cursor:
                cursor.close()

    def get_downloads_to_prune_by_since(
        self, feed: str, since: datetime.datetime
    ) -> list[sqlite3.Row]:
        """Identifies downloads published before the 'since' datetime (UTC).
        Returns a list of rows.
        Excludes items with status ARCHIVED or UPCOMING.
        'since' MUST be a timezone-aware datetime object in UTC.
        """
        sql = """
        SELECT *
        FROM downloads
        WHERE feed = ?
          AND published < ?
          AND status NOT IN (?, ?)
        ORDER BY published ASC
        """
        cursor: sqlite3.Cursor | None = None
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                params = (
                    feed,
                    since.isoformat(),
                    str(DownloadStatus.ARCHIVED),
                    str(DownloadStatus.UPCOMING),
                )
                cursor.execute(sql, params)
                return cursor.fetchall()
        finally:
            if cursor:
                cursor.close()

    def get_download_by_id(self, feed: str, id: str) -> sqlite3.Row | None:
        """Retrieves a specific download by feed and id.
        Returns a database row or None if not found.
        """
        sql = "SELECT * FROM downloads WHERE feed = ? AND id = ?"
        cursor: sqlite3.Cursor | None = None
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, (feed, id))
                return cursor.fetchone()
        finally:
            if cursor:
                cursor.close()

    def get_downloads_by_status(
        self,
        status_to_filter: DownloadStatus,
        feed: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[sqlite3.Row]:
        """Retrieves downloads with a specific status, newest first.
        Can be filtered by a specific feed. Returns a list of database rows.
        """
        params: list[Any] = []
        if feed:
            sql = """
            SELECT *
            FROM downloads
            WHERE feed = ? AND status = ?
            ORDER BY published ASC, id ASC
            LIMIT ? OFFSET ?
            """
            params.extend([feed, str(status_to_filter), limit, offset])
        else:
            sql = """
            SELECT *
            FROM downloads
            WHERE status = ?
            ORDER BY published ASC, id ASC
            LIMIT ? OFFSET ?
            """
            params.extend([str(status_to_filter), limit, offset])

        cursor: sqlite3.Cursor | None = None
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, tuple(params))
                return cursor.fetchall()
        finally:
            if cursor:
                cursor.close()
