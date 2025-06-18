"""Database management for Anypod downloads.

This module provides database operations for managing download records,
including the Download dataclass, DownloadStatus enum, and DownloadDatabase
class for all database interactions.
"""

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime
import logging
from pathlib import Path
from typing import Any

from ..exceptions import DatabaseOperationError, DownloadNotFoundError, NotFoundError
from .sqlite_utils_core import SqliteUtilsCore, register_adapter
from .types import Download, DownloadStatus

register_adapter(DownloadStatus, lambda status: status.value)

logger = logging.getLogger(__name__)


class DownloadDatabase:
    """Manage all database operations for downloads.

    Handles database initialization, CRUD operations, status transitions,
    and queries for download records using SQLite as the backend.

    Attributes:
        _db_path: Path to the database file.
        _db: Core SQLite database wrapper.
        _download_table_name: Name of the downloads table.
    """

    def __init__(
        self,
        db_path: Path | None,
        memory_name: str | None = None,
        *,
        include_triggers: bool = True,
    ):
        """Create a new DownloadDatabase instance.

        Args:
            db_path: Path to the SQLite database file. ``None`` for in-memory.
            memory_name: Shared in-memory DB identifier.
            include_triggers: When *False*, skips creation of triggers that
                update ``feeds.total_downloads``. Useful for unit-testing this
                class in isolation without creating the ``feeds`` table.
        """
        self._db_path = db_path
        self._db = SqliteUtilsCore(db_path, memory_name)
        self._download_table_name = "downloads"
        self._initialize_schema(include_triggers)
        logger.debug(
            "DownloadDatabase initialized.",
            extra={"db_path": str(db_path), "include_triggers": include_triggers},
        )

    def _initialize_schema(self, include_triggers: bool = True) -> None:
        """Initialize tables, indices, and optional triggers.

        Args:
            include_triggers: If True, create triggers that maintain `feeds.total_downloads`.
        """
        self._db.create_table(
            self._download_table_name,
            {
                "feed": str,
                "id": str,
                "source_url": str,
                "title": str,
                "published": datetime,
                "ext": str,
                "mime_type": str,
                "filesize": int,
                "duration": int,
                "status": str,  # from a DownloadStatus
                "discovered_at": datetime,
                "updated_at": datetime,
                "thumbnail": str,
                "description": str,
                "quality_info": str,
                "retries": int,
                "last_error": str,
                "downloaded_at": datetime,
            },
            pk=("feed", "id"),
            not_null={
                "feed",
                "id",
                "source_url",
                "title",
                "published",
                "ext",
                "mime_type",
                "filesize",
                "duration",
                "status",
                "discovered_at",
                "updated_at",
                "retries",
            },
            defaults={
                "retries": 0,
                "discovered_at": "STRFTIME('%Y-%m-%dT%H:%M:%f+00:00','now')",
                "updated_at": "STRFTIME('%Y-%m-%dT%H:%M:%f+00:00','now')",
            },
        )

        # Create trigger to set updated_at on UPDATE
        self._db.create_trigger(
            trigger_name=f"{self._download_table_name}_update_timestamp",
            table_name=self._download_table_name,
            trigger_event="AFTER UPDATE",
            exclude_columns=["discovered_at", "updated_at", "downloaded_at"],
            trigger_sql_body=f"""
                UPDATE {self._db.quote(self._download_table_name)}
                SET updated_at = STRFTIME('%Y-%m-%dT%H:%M:%f+00:00','now')
                WHERE feed = NEW.feed AND id = NEW.id;
            """,
        )

        # Create trigger to set downloaded_at when status changes to DOWNLOADED
        self._db.create_trigger(
            trigger_name=f"{self._download_table_name}_downloaded_timestamp",
            table_name=self._download_table_name,
            trigger_event="AFTER UPDATE",
            of_columns=["status"],
            when_clause=f"NEW.status = '{DownloadStatus.DOWNLOADED}' AND OLD.status != '{DownloadStatus.DOWNLOADED}'",
            trigger_sql_body=f"""
                UPDATE {self._db.quote(self._download_table_name)}
                SET downloaded_at = STRFTIME('%Y-%m-%dT%H:%M:%f+00:00','now')
                WHERE feed = NEW.feed AND id = NEW.id;
            """,
        )

        # --- Triggers to maintain feeds.total_downloads ---
        if include_triggers:
            # After INSERT: increment total_downloads when a newly inserted download has status DOWNLOADED
            self._db.create_trigger(
                trigger_name=f"{self._download_table_name}_after_insert_total_downloads",
                table_name=self._download_table_name,
                trigger_event="AFTER INSERT",
                when_clause=f"NEW.status = '{DownloadStatus.DOWNLOADED}'",
                trigger_sql_body="""
                    UPDATE feeds
                    SET total_downloads = total_downloads + 1
                    WHERE id = NEW.feed;
                """,
            )

            # After DELETE: decrement total_downloads when a deleted download had status DOWNLOADED
            self._db.create_trigger(
                trigger_name=f"{self._download_table_name}_after_delete_total_downloads",
                table_name=self._download_table_name,
                trigger_event="AFTER DELETE",
                when_clause=f"OLD.status = '{DownloadStatus.DOWNLOADED}'",
                trigger_sql_body="""
                    UPDATE feeds
                    SET total_downloads = total_downloads - 1
                    WHERE id = OLD.feed;
                """,
            )

            # After UPDATE (status column):
            # 1. Increment when status changes TO DOWNLOADED
            self._db.create_trigger(
                trigger_name=f"{self._download_table_name}_status_to_downloaded_total_downloads",
                table_name=self._download_table_name,
                trigger_event="AFTER UPDATE",
                of_columns=["status"],
                when_clause=f"NEW.status = '{DownloadStatus.DOWNLOADED}' AND OLD.status != '{DownloadStatus.DOWNLOADED}'",
                trigger_sql_body="""
                    UPDATE feeds
                    SET total_downloads = total_downloads + 1
                    WHERE id = NEW.feed;
                """,
            )

            # 2. Decrement when status changes FROM DOWNLOADED to something else
            self._db.create_trigger(
                trigger_name=f"{self._download_table_name}_status_from_downloaded_total_downloads",
                table_name=self._download_table_name,
                trigger_event="AFTER UPDATE",
                of_columns=["status"],
                when_clause=f"OLD.status = '{DownloadStatus.DOWNLOADED}' AND NEW.status != '{DownloadStatus.DOWNLOADED}'",
                trigger_sql_body="""
                    UPDATE feeds
                    SET total_downloads = total_downloads - 1
                    WHERE id = NEW.feed;
                """,
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

    @contextmanager
    def transaction(self) -> Generator[None]:
        """Provide a database transaction context manager.

        Returns:
            Context manager for database transactions.
        """
        with self._db.transaction():
            yield

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
        """Insert or update a download in the downloads table.

        If a download with the same (feed, id) exists, it will be replaced.

        Args:
            download: The Download object to insert or update.

        Raises:
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {
            "feed_id": download.feed,
            "download_id": download.id,
            "status": str(download.status),
        }
        logger.debug("Attempting to upsert download record.", extra=log_params)
        try:
            # Convert download to dict and exclude None values for timestamp fields
            # so database defaults can take effect
            download_dict = asdict(download)
            if download_dict.get("discovered_at") is None:
                download_dict.pop("discovered_at", None)
            if download_dict.get("updated_at") is None:
                download_dict.pop("updated_at", None)

            self._db.upsert(
                self._download_table_name,
                download_dict,
                pk=("feed", "id"),
                not_null={
                    "feed",
                    "id",
                    "source_url",
                    "title",
                    "published",
                    "ext",
                    "mime_type",
                    "filesize",
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
        """Transition a download from UPCOMING to QUEUED status.

        Updates status to QUEUED only if current status is UPCOMING.
        Preserves retries and last_error values.

        Args:
            feed: The feed identifier.
            id: The download identifier.

        Raises:
            DownloadNotFoundError: If the download is not found.
            DatabaseOperationError: If the database operation fails or current status is not UPCOMING.
        """
        log_params = {
            "feed_id": feed,
            "download_id": id,
            "target_status": str(DownloadStatus.QUEUED),
        }
        logger.debug(
            "Attempting to mark download as QUEUED from UPCOMING.", extra=log_params
        )

        try:
            self._db.update(
                self._download_table_name,
                (feed, id),
                {"status": str(DownloadStatus.QUEUED)},
                where="status = ?",
                where_args=[str(DownloadStatus.UPCOMING)],
            )
        except NotFoundError as e:
            # Check if download exists and get actual status for better error message
            current_download = self.get_download_by_id(feed, id)
            raise DatabaseOperationError(
                f"Download status is not UPCOMING (is {current_download.status}), cannot transition.",
                feed_id=feed,
                download_id=id,
            ) from e
        except DatabaseOperationError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        logger.info(
            "Download marked as QUEUED from UPCOMING.",
            extra=log_params,
        )

    def requeue_downloads(
        self,
        feed: str,
        download_ids: str | list[str],
        from_status: DownloadStatus | None = None,
    ) -> int:
        """Re-queue one or more downloads by resetting their status and error counters.

        This can happen due to:
        - Manually re-queueing ERROR'd downloads.
        - Manually re-queueing to get the latest version of downloads (previously DOWNLOADED).
        - Un-SKIPPING videos (if they don't get ARCHIVED).
        - Restoring ARCHIVED downloads when retention policies change.

        Sets status to QUEUED and resets retries to 0 and last_error to NULL.

        Args:
            feed: The feed identifier.
            download_ids: download identifier(s) to requeue.
            from_status: Optional status to check before updating. If provided, the update
                        will only occur if the current status matches this value.

        Returns:
            Number of downloads successfully requeued.

        Raises:
            DatabaseOperationError: If database operations fail.
        """
        if isinstance(download_ids, str):
            download_ids = [download_ids]

        if not download_ids:
            return 0

        log_params = {
            "feed_id": feed,
            "download_count": len(download_ids),
            "from_status": str(from_status) if from_status else None,
        }
        logger.debug("Attempting to re-queue downloads.", extra=log_params)

        # Build WHERE clause for bulk update
        placeholders = ", ".join(["?"] * len(download_ids))
        where_clause = f"feed = ? AND id IN ({placeholders})"
        where_args = [feed, *list(download_ids)]

        if from_status:
            where_clause += " AND status = ?"
            where_args.append(str(from_status))

        updates = {
            "status": str(DownloadStatus.QUEUED),
            "retries": 0,
            "last_error": None,
        }

        try:
            with self._db.transaction():
                count_requeued = self._db.multi_update(
                    self._download_table_name,
                    updates,
                    where_clause,
                    where_args,
                )

                expected_count = len(download_ids)
                if count_requeued != expected_count:
                    raise DatabaseOperationError(
                        f"Expected to requeue {expected_count} downloads but only {count_requeued} were updated. "
                        f"Some downloads may not exist or may not have the expected status. Rolling back changes.",
                        feed_id=feed,
                    )
        except DatabaseOperationError as e:
            e.feed_id = feed
            logger.error("Failed to requeue downloads.", extra=log_params, exc_info=e)
            raise e
        else:
            logger.info(
                "Downloads requeued.",
                extra={
                    **log_params,
                    "count_requeued": count_requeued,
                },
            )
            return count_requeued

    def mark_as_downloaded(self, feed: str, id: str, ext: str, filesize: int) -> None:
        """Mark a download as DOWNLOADED with updated metadata.

        Updates status to DOWNLOADED only if current status is QUEUED.
        Resets retries to 0, last_error to NULL, and updates ext and filesize.

        Args:
            feed: The feed identifier.
            id: The download identifier.
            ext: The new file extension.
            filesize: The new file size in bytes.

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
                where="status = ?",
                where_args=[str(DownloadStatus.QUEUED)],
            )
        except NotFoundError as e:
            # Check if download exists and get actual status for better error message
            current_download = self.get_download_by_id(feed, id)
            raise DatabaseOperationError(
                f"Download status is not QUEUED (is {current_download.status}), cannot mark as DOWNLOADED.",
                feed_id=feed,
                download_id=id,
            ) from e
        except DatabaseOperationError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        logger.info("Download marked as DOWNLOADED.", extra=log_params)

    def skip_download(self, feed: str, id: str) -> None:
        """Skip a download by setting its status to SKIPPED.

        Preserves retries and last_error values.

        Args:
            feed: The feed identifier.
            id: The download identifier.

        Raises:
            DownloadNotFoundError: If the download is not found.
            DatabaseOperationError: If the database operation fails.
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
        except NotFoundError as e:
            raise DownloadNotFoundError(
                "Download not found.", feed_id=feed, download_id=id
            ) from e
        except DatabaseOperationError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        logger.info("Download marked as SKIPPED.", extra=log_params)

    def archive_download(self, feed: str, id: str) -> None:
        """Archive a download by setting its status to ARCHIVED.

        Preserves retries and last_error values.

        Args:
            feed: The feed identifier.
            id: The download identifier.

        Raises:
            DownloadNotFoundError: If the download is not found.
            DatabaseOperationError: If the database operation fails.
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
        except NotFoundError as e:
            raise DownloadNotFoundError(
                "Download not found.", feed_id=feed, download_id=id
            ) from e
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
        """Increment the retry count and potentially update status to ERROR.

        Args:
            feed_id: The feed identifier.
            download_id: The download identifier.
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
                except NotFoundError as e:
                    raise DownloadNotFoundError(
                        "Download not found.", feed_id=feed_id, download_id=download_id
                    ) from e
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
        """Identify downloads to prune based on 'keep_last' rule.

        Returns downloads that exceed the keep_last limit, excluding
        downloads with status ARCHIVED or SKIPPED.

        Args:
            feed: The feed identifier.
            keep_last: The number of most recent downloads to keep.

        Returns:
            List of Download objects that should be pruned.

        Raises:
            DatabaseOperationError: If the database query fails.
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
                "feed = :feed AND status NOT IN (:archived, :skipped)",
                where_args={
                    "feed": feed,
                    "archived": str(DownloadStatus.ARCHIVED),
                    "skipped": str(DownloadStatus.SKIPPED),
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
        """Identify downloads published before the 'since' datetime (UTC).

        Returns downloads published before the given datetime, excluding
        downloads with status ARCHIVED or SKIPPED. The 'since' parameter
        must be a timezone-aware datetime object in UTC.

        Args:
            feed: The feed identifier.
            since: The cutoff datetime (must be timezone-aware UTC).

        Returns:
            List of Download objects that should be pruned.

        Raises:
            DatabaseOperationError: If the database query fails.
        """
        log_params = {"feed_id": feed, "prune_before_date": since.isoformat()}
        logger.debug(
            "Attempting to get downloads to prune by 'since' date rule.",
            extra=log_params,
        )
        try:
            rows = self._db.rows_where(
                self._download_table_name,
                "feed = :feed AND published < :since AND status NOT IN (:archived, :skipped)",
                where_args={
                    "feed": feed,
                    "since": since,
                    "archived": str(DownloadStatus.ARCHIVED),
                    "skipped": str(DownloadStatus.SKIPPED),
                },
                order_by="published ASC",
            )
        except DatabaseOperationError as e:
            e.feed_id = feed
            raise e
        return [Download.from_row(row) for row in rows]

    def get_download_by_id(self, feed: str, id: str) -> Download:
        """Retrieve a specific download by feed and id.

        Args:
            feed: The feed identifier.
            id: The download identifier.

        Returns:
            Download object for the specified feed and id.

        Raises:
            DownloadNotFoundError: If the download is not found.
            DatabaseOperationError: If the database operation fails.
            ValueError: If unable to parse row into a Download object.
        """
        log_params = {"feed_id": feed, "download_id": id}
        logger.debug("Attempting to get download by ID.", extra=log_params)
        try:
            row = self._db.get(self._download_table_name, (feed, id))
        except NotFoundError as e:
            raise DownloadNotFoundError(
                "Download not found.", feed_id=feed, download_id=id
            ) from e
        except DatabaseOperationError as e:
            e.feed_id = feed
            e.download_id = id
            raise e
        return Download.from_row(row)

    def get_downloads_by_status(
        self,
        status_to_filter: DownloadStatus,
        feed_id: str | None = None,
        limit: int = -1,
        offset: int = 0,
        published_after: datetime | None = None,
        published_before: datetime | None = None,
    ) -> list[Download]:
        """Retrieve downloads with a specific status, newest first.

        Can be filtered by a specific feed and date ranges.

        Args:
            status_to_filter: The DownloadStatus to filter by.
            feed_id: Optional feed name to filter by.
            limit: Maximum number of records to return (-1 for no limit).
            offset: Number of records to skip (for pagination).
            published_after: Optional datetime to filter downloads published after this date (inclusive).
            published_before: Optional datetime to filter downloads published before this date (exclusive).

        Returns:
            List of Download objects matching the status and other criteria, sorted newest first.

        Raises:
            DatabaseOperationError: If the database query fails.
        """
        log_params = {
            "status": str(status_to_filter),
            "feed_id": feed_id if feed_id else "<all>",
            "limit": limit,
            "offset": offset,
            "published_after": published_after.isoformat() if published_after else None,
            "published_before": published_before.isoformat()
            if published_before
            else None,
        }
        logger.debug("Attempting to get downloads by status.", extra=log_params)

        where = ["status = :status"]
        where_args: dict[str, Any] = {"status": str(status_to_filter)}
        if feed_id:
            where.append("feed = :feed")
            where_args["feed"] = feed_id
        if published_after:
            where.append("published >= :published_after")
            where_args["published_after"] = published_after
        if published_before:
            where.append("published < :published_before")
            where_args["published_before"] = published_before

        try:
            rows = self._db.rows_where(
                self._download_table_name,
                " AND ".join(where),
                where_args=where_args,
                order_by="published DESC",
                limit=limit,
                offset=offset,
            )
        except DatabaseOperationError as e:
            e.feed_id = feed_id
            raise e
        return [Download.from_row(row) for row in rows]

    def count_downloads_by_status(
        self,
        status_to_filter: DownloadStatus | list[DownloadStatus],
        feed_id: str | None = None,
    ) -> int:
        """Count downloads with one or more specific statuses.

        Can be filtered by a specific feed.

        Args:
            status_to_filter: Single status or list of statuses to count.
            feed_id: Optional feed identifier to filter by.

        Returns:
            Number of downloads matching the criteria.

        Raises:
            DatabaseOperationError: If the database query fails.
        """
        log_params: dict[str, Any] = {
            "statuses": status_to_filter,
            "feed_id": feed_id,
        }
        logger.debug("Attempting to count downloads by status.", extra=log_params)

        if isinstance(status_to_filter, DownloadStatus):
            where = ["status = :status"]
            where_args: dict[str, Any] = {"status": str(status_to_filter)}
        else:
            placeholders: list[str] = []
            where_args: dict[str, Any] = {}
            for i, status in enumerate(status_to_filter):
                status_key = f"status_{i}"
                placeholders.append(f":{status_key}")
                where_args[status_key] = str(status)
            where = [f"status IN ({', '.join(placeholders)})"]

        if feed_id is not None:
            where.append("feed = :feed_id")
            where_args["feed_id"] = feed_id

        try:
            count = self._db.count_where(
                self._download_table_name,
                " AND ".join(where),
                where_args=where_args,
            )
        except DatabaseOperationError as e:
            e.feed_id = feed_id
            raise e

        logger.debug(
            "Count downloads by status completed.", extra={**log_params, "count": count}
        )
        return count
