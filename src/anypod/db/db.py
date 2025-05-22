from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
import logging
from pathlib import Path
from typing import Any

from ..exceptions import DatabaseOperationError, DownloadNotFoundError
from .sqlite_utils_core import SqliteUtilsCore, register_adapter

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


register_adapter(DownloadStatus, lambda status: status.value)
register_adapter(datetime, lambda dt: dt.isoformat())  # type: ignore # not sure why it can't figure this type out


@dataclass(eq=False)
class Download:
    """Represents a download's data, used for adding/updating."""

    feed: str
    id: str
    source_url: str
    title: str
    published: datetime  # Should be UTC
    ext: str
    duration: float  # in seconds # TODO: should be int?
    status: DownloadStatus
    thumbnail: str | None = None
    retries: int = 0
    last_error: str | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Download":
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


class DatabaseManager:
    def __init__(self, db_path: Path | None, memory_name: str | None = None):
        """Initializes the DatabaseManager with the path to the SQLite database."""
        self._db_path = db_path
        self._db = SqliteUtilsCore(db_path, memory_name)
        self._download_table_name = "downloads"
        self._initialize_schema()
        logger.debug("DatabaseManager initialized.", extra={"db_path": str(db_path)})

    def _initialize_schema(self) -> None:
        """Initializes the database schema (tables and indices) if it doesn't exist."""
        self._db.create_table(
            self._download_table_name,
            {
                "feed": str,
                "id": str,
                "source_url": str,
                "title": str,
                "published": datetime,
                "ext": str,
                "duration": float,
                "thumbnail": str,
                "status": str,
                "retries": int,
                "last_error": str,
            },
            pk=("feed", "id"),
            not_null={
                "feed",
                "id",
                "source_url",
                "title",
                "published",
                "ext",
                "duration",
                "status",
                "retries",
            },
            defaults={"retries": 0},
        )
        self._db.create_index(
            self._download_table_name,
            ["feed", "status"],
            "idx_feed_status",
            unique=False,
        )
        self._db.create_index(
            self._download_table_name,
            ["feed", "published"],
            "idx_feed_published",
            unique=False,
        )

    def close(self) -> None:
        """Closes the database connection."""
        logger.info(
            "Closing database connection.", extra={"db_path": str(self._db_path)}
        )
        self._db.close()

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
        try:
            self._db.upsert(
                self._download_table_name,
                asdict(download),
                pk=("feed", "id"),
                not_null={
                    "feed",
                    "id",
                    "source_url",
                    "title",
                    "published",
                    "ext",
                    "duration",
                    "status",
                    "retries",
                },
            )
        except DatabaseOperationError as e:
            e.feed_id = download.feed
            e.download_id = download.id
            raise e
        logger.debug("Upsert download record execution complete.", extra=log_params)

    # TODO: this logic is wrong for ERROR; retries should be handled by bump_retries
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

        set_clauses: list[str] = ["status = :status"]
        params: dict[str, Any] = {"feed": feed, "id": id, "status": str(status)}

        match status:
            case DownloadStatus.DOWNLOADED:
                set_clauses.append("last_error = NULL")
                set_clauses.append("retries = 0")
            case DownloadStatus.ERROR:
                set_clauses.append("retries = retries + 1")
                set_clauses.append("last_error = :last_error")
                params["last_error"] = last_error
            case (
                DownloadStatus.UPCOMING
                | DownloadStatus.QUEUED
                | DownloadStatus.SKIPPED
                | DownloadStatus.ARCHIVED
            ):
                pass

        sql = (
            f"UPDATE {self._download_table_name} "
            f"SET {', '.join(set_clauses)} "
            f"WHERE feed = :feed AND id = :id"
        )

        try:
            rows_updated = self._db.execute(sql, params)
        except DatabaseOperationError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        logger.debug(
            "Updated Download status.",
            extra={**log_params, "rows_updated": rows_updated},
        )
        return rows_updated > 0

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

        try:
            rows = self._db.rows_where(
                self._download_table_name,
                "feed = :feed AND status NOT IN (:archived, :upcoming)",
                where_args={
                    "feed": feed,
                    "archived": str(DownloadStatus.ARCHIVED),
                    "upcoming": str(DownloadStatus.UPCOMING),
                },
                order_by="published DESC",
                limit=-1,
                offset=keep_last,
            )
        except DatabaseOperationError as e:
            e.feed_id = feed
            raise e
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
        try:
            rows = self._db.rows_where(
                self._download_table_name,
                "feed = :feed AND published < :since AND status NOT IN (:archived, :upcoming)",
                where_args={
                    "feed": feed,
                    "since": since,
                    "archived": str(DownloadStatus.ARCHIVED),
                    "upcoming": str(DownloadStatus.UPCOMING),
                },
                order_by="published ASC",
            )
        except DatabaseOperationError as e:
            e.feed_id = feed
            raise e
        return [Download.from_row(row) for row in rows]

    def get_download_by_id(self, feed: str, id: str) -> Download | None:
        """Retrieves a specific download by feed and id.
        Returns a Download or None if not found.
        """
        log_params = {"feed_id": feed, "download_id": id}
        logger.debug("Attempting to get download by ID.", extra=log_params)
        try:
            row = self._db.get(self._download_table_name, (feed, id))
        except DatabaseOperationError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
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

        where = ["status = :status"]
        where_args = {"status": str(status_to_filter)}
        if feed:
            where.append("feed = :feed")
            where_args["feed"] = feed

        try:
            rows = self._db.rows_where(
                self._download_table_name,
                " AND ".join(where),
                where_args=where_args,
                order_by="published ASC",
                limit=limit,
                offset=offset,
            )
        except DatabaseOperationError as e:
            e.feed_id = feed
            raise e
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

        try:
            with self._db.transaction():
                current_download = self.get_download_by_id(feed_id, download_id)
                if current_download is None:
                    raise DownloadNotFoundError(
                        message="Download not found.",
                        feed_id=feed_id,
                        download_id=download_id,
                    )

                # Calculate new state
                new_retries = current_download.retries + 1
                is_error_status = new_retries >= max_allowed_errors
                final_status = (
                    DownloadStatus.ERROR if is_error_status else current_download.status
                )
                final_last_error = error_message

                if is_error_status and current_download.status != DownloadStatus.ERROR:
                    logger.info(
                        f"Download transitioning to ERROR state after {new_retries} retries (max: {max_allowed_errors}).",
                        extra=log_params,
                    )

                try:
                    self._db.update(
                        self._download_table_name,
                        (feed_id, download_id),
                        {
                            "retries": new_retries,
                            "status": str(final_status),
                            "last_error": final_last_error,
                        },
                    )
                except DatabaseOperationError as e:
                    e.feed_id = feed_id
                    e.download_id = download_id
                    raise e
                return new_retries, final_status, is_error_status

        except DatabaseOperationError as e:
            e.feed_id = feed_id
            e.download_id = download_id
            raise e
