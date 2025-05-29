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
    duration: float  # in seconds
    status: DownloadStatus
    thumbnail: str | None = None
    filesize: int | None = None  # Bytes
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
            filesize=row.get("filesize"),
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
                "filesize": int,
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

        If a download with the same (feed, id) exists, it will be
        replaced.
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

    # --- Status Transition Methods ---

    def mark_as_queued_from_upcoming(self, feed: str, id: str) -> None:
        """Transitions a download from UPCOMING to QUEUED.

        - Checks that the current status is UPCOMING.
        - Sets status = QUEUED.
        - Preserves retries and last_error.

        Raises:
            DownloadNotFoundError: If the download is not found.
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {
            "feed_id": feed,
            "download_id": id,
            "target_status": str(DownloadStatus.QUEUED),
        }
        logger.debug(
            "Attempting to mark download as QUEUED from UPCOMING.", extra=log_params
        )

        current_download = self.get_download_by_id(feed, id)
        if current_download.status != DownloadStatus.UPCOMING:
            raise DatabaseOperationError(
                f"Download status is not UPCOMING (is {current_download.status}), cannot transition.",
                feed_id=feed,
                download_id=id,
            )

        try:
            self._db.update(
                self._download_table_name,
                (feed, id),
                {"status": str(DownloadStatus.QUEUED)},
            )
        except DownloadNotFoundError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        except DatabaseOperationError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        logger.info(
            "Download marked as QUEUED from UPCOMING.",
            extra=log_params,
        )

    def requeue_download(self, feed: str, id: str) -> None:
        """Re-queues a download.

        This can happen due to:
        - Manually re-queueing an ERROR'd download.
        - Manually re-queueing to get the latest version of a download (i.e., it was previously DOWNLOADED).
        - Un-SKIPPING a video (if it doesn't get ARCHIVED).
        - Sets status = QUEUED.
        - Sets retries = 0, last_error = NULL.
        """
        log_params = {
            "feed_id": feed,
            "download_id": id,
            "target_status": str(DownloadStatus.QUEUED),
        }
        logger.debug("Attempting to re-queue download.", extra=log_params)
        try:
            self._db.update(
                self._download_table_name,
                (feed, id),
                {
                    "status": str(DownloadStatus.QUEUED),
                    "retries": 0,
                    "last_error": None,
                },
            )
        except DownloadNotFoundError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        except DatabaseOperationError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        logger.info("Download re-queued.", extra=log_params)

    def mark_as_downloaded(self, feed: str, id: str, ext: str, filesize: int) -> None:
        """Marks a download as DOWNLOADED, optionally updating its extension and filesize.

        - Checks that the current status is QUEUED.
        - Sets status = DOWNLOADED.
        - Sets retries = 0, last_error = NULL.
        - Updates ext and filesize if provided.

        Args:
            feed: The feed ID of the download.
            id: The ID of the download.
            ext: The new file extension (optional).
            filesize: The new file size in bytes (optional).

        Raises:
            DownloadNotFoundError: If the download is not found.
            DatabaseOperationError: If the current status is not QUEUED or DB update fails.
        """
        log_params = {
            "feed_id": feed,
            "download_id": id,
            "target_status": str(DownloadStatus.DOWNLOADED),
            "ext": ext,
            "filesize": filesize,
        }
        logger.debug("Attempting to mark download as DOWNLOADED.", extra=log_params)

        current_download = self.get_download_by_id(feed, id)
        if current_download.status != DownloadStatus.QUEUED:
            raise DatabaseOperationError(
                f"Download status is not QUEUED (is {current_download.status}), cannot mark as DOWNLOADED.",
                feed_id=feed,
                download_id=id,
            )

        try:
            self._db.update(
                self._download_table_name,
                (feed, id),
                {
                    "status": str(DownloadStatus.DOWNLOADED),
                    "retries": 0,
                    "last_error": None,
                    "ext": ext,
                    "filesize": filesize,
                },
            )
        except DownloadNotFoundError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        except DatabaseOperationError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        logger.info("Download marked as DOWNLOADED.", extra=log_params)

    def skip_download(self, feed: str, id: str) -> None:
        """Skips a download.

        - Sets status = SKIPPED.
        - Preserves retries and last_error.
        """
        log_params = {
            "feed_id": feed,
            "download_id": id,
            "target_status": str(DownloadStatus.SKIPPED),
        }
        logger.debug("Attempting to mark download as SKIPPED.", extra=log_params)
        try:
            self._db.update(
                self._download_table_name,
                (feed, id),
                {"status": str(DownloadStatus.SKIPPED)},
            )
        except DownloadNotFoundError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        except DatabaseOperationError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        logger.info("Download marked as SKIPPED.", extra=log_params)

    def unskip_download(
        self,
        feed_id: str,
        download_id: str,
    ) -> DownloadStatus:
        """Unskips a download by re-queueing it. The Pruner will later determine if it should be archived based on retention rules.

        - Checks that the current status is SKIPPED.
        - Calls self.requeue_download() to set status to QUEUED and reset retries/errors.

        Args:
            feed_id: The ID of the feed.
            download_id: The ID of the download.

        Returns:
            DownloadStatus.QUEUED if successful.

        Raises:
            DownloadNotFoundError: If the download is not found.
            DatabaseOperationError: If the download is not in SKIPPED status or DB update fails.
        """
        log_params = {"feed_id": feed_id, "download_id": download_id}
        logger.debug("Attempting to unskip download by re-queueing.", extra=log_params)

        current_download = self.get_download_by_id(feed_id, download_id)

        if current_download.status != DownloadStatus.SKIPPED:
            raise DatabaseOperationError(
                f"Download status is not SKIPPED (is {current_download.status}), cannot unskip.",
                feed_id=feed_id,
                download_id=download_id,
            )

        self.requeue_download(feed_id, download_id)
        logger.info("Download unskipped and re-queued.", extra=log_params)
        return DownloadStatus.QUEUED

    def archive_download(self, feed: str, id: str) -> None:
        """Archives a download.

        - Sets status = ARCHIVED.
        - Preserves retries and last_error.
        """
        log_params = {
            "feed_id": feed,
            "download_id": id,
            "target_status": str(DownloadStatus.ARCHIVED),
        }
        logger.debug("Attempting to mark download as ARCHIVED.", extra=log_params)
        try:
            self._db.update(
                self._download_table_name,
                (feed, id),
                {"status": str(DownloadStatus.ARCHIVED)},
            )
        except DownloadNotFoundError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        except DatabaseOperationError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        logger.info(
            "Download marked as ARCHIVED.",
            extra=log_params,
        )

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

                # Calculate new state
                new_retries = current_download.retries + 1

                # Determine if the status should change to ERROR
                # It should only change to ERROR if it's not already DOWNLOADED
                should_transition_to_error = (
                    new_retries >= max_allowed_errors
                    and current_download.status != DownloadStatus.DOWNLOADED
                )

                final_status = (
                    DownloadStatus.ERROR
                    if should_transition_to_error
                    else current_download.status
                )
                final_last_error = error_message
                did_transition_to_error_state = (
                    final_status == DownloadStatus.ERROR
                    and current_download.status != DownloadStatus.ERROR
                )

                if did_transition_to_error_state:
                    logger.info(
                        f"Download transitioning to ERROR state after {new_retries} retries (max: {max_allowed_errors}).",
                        extra=log_params,
                    )
                elif (
                    new_retries >= max_allowed_errors
                    and current_download.status == DownloadStatus.DOWNLOADED
                ):
                    logger.warning(
                        f"Max retries reached for already DOWNLOADED item. Status remains DOWNLOADED. Retries: {new_retries}",
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
                except DownloadNotFoundError as e:
                    e.feed_id = feed_id
                    e.download_id = download_id
                    raise e
                except DatabaseOperationError as e:
                    e.feed_id = feed_id
                    e.download_id = download_id
                    raise e
                return new_retries, final_status, did_transition_to_error_state

        except DatabaseOperationError as e:
            e.feed_id = feed_id
            e.download_id = download_id
            raise e

    # --- Query Methods ---

    def get_downloads_to_prune_by_keep_last(
        self, feed: str, keep_last: int
    ) -> list[Download]:
        """Identifies downloads to prune based on 'keep_last'.

        Returns a list of Downloads. Excludes downloads with status
        ARCHIVED or UPCOMING.
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

        Returns a list of Downloads. Excludes downloads with status
        ARCHIVED or UPCOMING. 'since' MUST be a timezone-aware datetime
        object in UTC.
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

    def get_download_by_id(self, feed: str, id: str) -> Download:
        """Retrieves a specific download by feed and id. Returns a Download object.

        Raises:
            DownloadNotFoundError: If the download is not found.
            DatabaseOperationError: If the database operation fails.
            ValueError: If unable to parse row into a Download
        """
        log_params = {"feed_id": feed, "download_id": id}
        logger.debug("Attempting to get download by ID.", extra=log_params)
        try:
            row = self._db.get(self._download_table_name, (feed, id))
        except DownloadNotFoundError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        except DatabaseOperationError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        return Download.from_row(row)

    def get_downloads_by_status(
        self,
        status_to_filter: DownloadStatus,
        feed: str | None = None,
        limit: int = -1,
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
