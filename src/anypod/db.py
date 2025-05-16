from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging
from pathlib import Path
import sqlite3
from typing import Any

from .exceptions import DatabaseOperationError, DownloadNotFoundError

logger = logging.getLogger(__name__)


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
    published: datetime  # Should be UTC
    ext: str
    duration: float  # in seconds
    status: DownloadStatus
    thumbnail: str | None = None
    retries: int = 0
    last_error: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Download":
        """Converts a sqlite3.Row to a Download."""
        published_str = row["published"]
        try:
            published_dt = datetime.fromisoformat(published_str)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"Invalid date format for 'published' in DB row: {published_str}"
            ) from e

        status_str = row["status"]
        try:
            status_enum = DownloadStatus(status_str)
        except ValueError as e:
            raise ValueError(f"Invalid status value in DB row: {status_str}") from e

        return cls(
            feed=row["feed"],
            id=row["id"],
            source_url=row["source_url"],
            title=row["title"],
            published=published_dt,
            ext=row["ext"],
            duration=float(row["duration"]),
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
        logger.debug("DatabaseManager initialized.", extra={"db_path": str(db_path)})

    def _get_connection(self) -> sqlite3.Connection:
        """Establishes and returns the database connection, creating it if necessary."""
        if self._connection is None:
            logger.info(
                "Database connection not found, establishing new connection.",
                extra={"db_path": str(self.db_path)},
            )
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self._connection.row_factory = sqlite3.Row
            logger.info(
                "Database connection established successfully.",
                extra={"db_path": str(self.db_path)},
            )
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
                logger.debug(
                    "Attempting to initialize database schema (CREATE TABLE IF NOT EXISTS)."
                )
                self._connection.execute(_CREATE_TABLE_SQL)
                logger.debug("Table 'downloads' initialization check complete.")
                logger.debug(
                    "Attempting to initialize index 'idx_feed_status' (CREATE INDEX IF NOT EXISTS)."
                )
                self._connection.execute(_CREATE_INDEX_SQL)
                logger.info("Database schema initialization check complete.")
        except sqlite3.Error as e:
            raise DatabaseOperationError(
                message="Failed to initialize database schema",
            ) from e

    def close(self) -> None:
        """Closes the database connection if it's open."""
        if self._connection:
            logger.info(
                "Closing database connection.", extra={"db_path": str(self.db_path)}
            )
            self._connection.close()
            self._connection = None
        else:
            logger.debug(
                "Attempted to close database connection, but it was already closed."
            )

    # --- CRUD Operations ---

    def upsert_download(
        self,
        download: Download,
    ) -> None:
        """Inserts or updates a download in the downloads table (upsert behavior).
        If a download with the same (feed, id) exists, it will be replaced.
        """
        log_params = {
            "feed_id": download.feed,
            "download_id": download.id,
            "status": str(download.status),
        }
        logger.debug("Attempting to upsert download record.", extra=log_params)
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
        try:
            with self._get_connection() as conn:
                logger.debug("Executing upsert SQL for download.", extra=log_params)
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
        except sqlite3.Error as e:
            raise DatabaseOperationError(
                message="Failed to upsert download",
                feed_id=download.feed,
                download_id=download.id,
            ) from e
        logger.debug("Upsert download record execution complete.", extra=log_params)

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
        log_params = {
            "feed_id": feed,
            "download_id": id,
            "new_status": str(status),
            "last_error_param": last_error,
        }
        logger.debug("Attempting to update download status.", extra=log_params)
        updates = ["status = ?"]
        params: list[Any] = [str(status)]

        match status:
            case DownloadStatus.DOWNLOADED:
                updates.append("last_error = NULL")
                updates.append("retries = 0")
            case DownloadStatus.ERROR:
                updates.append("last_error = ?")
                params.append(last_error)
                updates.append("retries = retries + 1")
            case (
                DownloadStatus.UPCOMING
                | DownloadStatus.QUEUED
                | DownloadStatus.SKIPPED
                | DownloadStatus.ARCHIVED
            ):
                pass

        sql = f"UPDATE downloads SET {', '.join(updates)} WHERE feed = ? AND id = ?"
        params.extend([feed, id])

        try:
            with self._get_connection() as conn:
                logger.debug("Executing status update SQL.", extra=log_params)
                cursor = conn.execute(sql, tuple(params))
                return cursor.rowcount > 0
        except sqlite3.Error as e:
            raise DatabaseOperationError(
                message="Failed to update download status",
                feed_id=feed,
                download_id=id,
            ) from e

    def get_downloads_to_prune_by_keep_last(
        self, feed: str, keep_last: int
    ) -> list[Download]:
        """Identifies downloads to prune based on 'keep_last'.
        Returns a list of Downloads.
        Excludes downloads with status ARCHIVED or UPCOMING.
        """
        log_params = {"feed_id": feed, "keep_last": keep_last}
        logger.debug(
            "Attempting to get downloads to prune by keep_last rule.", extra=log_params
        )
        if keep_last <= 0:
            logger.debug(
                "'keep_last' is 0 or negative, returning empty list.", extra=log_params
            )
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
                logger.debug(
                    "Executing SQL to find prunable downloads by keep_last.",
                    extra=log_params,
                )
                cursor.execute(sql, params)
                rows = cursor.fetchall()
        except sqlite3.Error as e:
            raise DatabaseOperationError(
                message=f"Failed to get downloads to prune by keep_last for feed '{feed}'",
                feed_id=feed,
            ) from e
        finally:
            if cursor:
                cursor.close()
        return [Download.from_row(row) for row in rows]

    def get_downloads_to_prune_by_since(
        self, feed: str, since: datetime
    ) -> list[Download]:
        """Identifies downloads published before the 'since' datetime (UTC).
        Returns a list of Downloads.
        Excludes downloads with status ARCHIVED or UPCOMING.
        'since' MUST be a timezone-aware datetime object in UTC.
        """
        log_params = {"feed_id": feed, "prune_before_date": since.isoformat()}
        logger.debug(
            "Attempting to get downloads to prune by 'since' date rule.",
            extra=log_params,
        )
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
                logger.debug(
                    "Executing SQL to find prunable downloads by 'since' date.",
                    extra=log_params,
                )
                cursor.execute(sql, params)
                rows = cursor.fetchall()
        except sqlite3.Error as e:
            raise DatabaseOperationError(
                message=f"Failed to get downloads to prune by since date for feed '{feed}'",
                feed_id=feed,
            ) from e
        finally:
            if cursor:
                cursor.close()
        return [Download.from_row(row) for row in rows]

    def get_download_by_id(self, feed: str, id: str) -> Download | None:
        """Retrieves a specific download by feed and id.
        Returns a Download or None if not found.
        """
        log_params = {"feed_id": feed, "download_id": id}
        logger.debug("Attempting to get download by ID.", extra=log_params)
        sql = "SELECT * FROM downloads WHERE feed = ? AND id = ?"
        cursor: sqlite3.Cursor | None = None
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                logger.debug("Executing SQL to get download by ID.", extra=log_params)
                cursor.execute(sql, (feed, id))
                row = cursor.fetchone()
        except sqlite3.Error as e:
            raise DatabaseOperationError(
                message=f"Failed to get download by ID for feed '{feed}', id '{id}'",
                feed_id=feed,
                download_id=id,
            ) from e
        finally:
            if cursor:
                cursor.close()
        return Download.from_row(row) if row else None

    def get_downloads_by_status(
        self,
        status_to_filter: DownloadStatus,
        feed: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Download]:
        """Retrieves downloads with a specific status, newest first.
        Can be filtered by a specific feed. Returns a list of Downloads.
        """
        log_params = {
            "status": str(status_to_filter),
            "feed_id": feed if feed else "<all>",
            "limit": limit,
            "offset": offset,
        }
        logger.debug("Attempting to get downloads by status.", extra=log_params)
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
                logger.debug(
                    "Executing SQL to get downloads by status.", extra=log_params
                )
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()
        except sqlite3.Error as e:
            raise DatabaseOperationError(
                message="Failed to get downloads by status",
                feed_id=feed,
            ) from e
        finally:
            if cursor:
                cursor.close()
        return [Download.from_row(row) for row in rows]

    def bump_retries(
        self,
        feed_id: str,
        download_id: str,
        error_message: str,
        max_allowed_errors: int,
    ) -> tuple[int, DownloadStatus, bool]:
        """Increments the retry count for a download and potentially updates its status to ERROR.

        Args:
            feed_id: The name of the feed.
            download_id: The ID of the download.
            error_message: The error message to record.
            max_allowed_errors: The maximum number of retries allowed before transitioning to ERROR status.

        Returns:
            A tuple containing:
                - new_retries (int): The updated retry count.
                - final_status (DownloadStatus): The final status of the download after this operation.
                - did_transition_to_error (bool): True if the download transitioned to ERROR, False otherwise.

        Raises:
            DownloadNotFoundError: If the specified download is not found.
            DatabaseOperationError: If any other database operation fails.
        """
        log_params = {
            "feed_id": feed_id,
            "download_id": download_id,
            "error_message": error_message,
            "max_allowed_errors": max_allowed_errors,
        }
        logger.debug("Attempting to bump error count for download.", extra=log_params)

        conn = self._get_connection()
        try:
            with conn:
                # SELECT current state
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT retries, status FROM downloads WHERE feed = ? AND id = ?",
                    (feed_id, download_id),
                )
                row = cursor.fetchone()
                cursor.close()

                if row is None:
                    raise DownloadNotFoundError(
                        message="Download not found.",
                        feed_id=feed_id,
                        download_id=download_id,
                    )

                current_retries = row["retries"]
                current_status = DownloadStatus(row["status"])

                # Calculate new state
                new_retries = current_retries + 1
                is_error_status = new_retries >= max_allowed_errors
                final_status = (
                    DownloadStatus.ERROR if is_error_status else current_status
                )
                final_last_error = error_message

                if is_error_status and current_status != DownloadStatus.ERROR:
                    logger.info(
                        f"Download transitioning to ERROR state after {new_retries} retries (max: {max_allowed_errors}).",
                        extra=log_params,
                    )

                # UPDATE database
                conn.execute(
                    """UPDATE downloads
                       SET retries = ?, status = ?, last_error = ?
                       WHERE feed = ? AND id = ?""",
                    (
                        new_retries,
                        str(final_status),
                        final_last_error,
                        feed_id,
                        download_id,
                    ),
                )

                logger.info(
                    "Successfully bumped error count for download.",
                    extra={
                        **log_params,
                        "new_retries": new_retries,
                        "final_status": str(final_status),
                        "is_error_status": is_error_status,
                    },
                )
                return new_retries, final_status, is_error_status

        except sqlite3.Error as e:
            raise DatabaseOperationError(
                message="Failed to bump retries in database",
                feed_id=feed_id,
                download_id=download_id,
            ) from e
