"""Database management for Anypod downloads.

This module provides database operations for managing download records,
including the Download dataclass, DownloadStatus enum, and DownloadDatabase
class for all database interactions.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime
import logging
from typing import Any

from sqlalchemy import and_, func, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement
from sqlmodel import col, select

from ..exceptions import DatabaseOperationError, DownloadNotFoundError, NotFoundError
from .decorators import handle_download_db_errors, handle_feed_db_errors
from .sqlalchemy_core import SqlalchemyCore
from .types import Download, DownloadStatus

logger = logging.getLogger(__name__)


class DownloadDatabase:
    """Manage all database operations for downloads.

    Handles database initialization, CRUD operations, status transitions,
    and queries for download records using SQLAlchemy as the backend.

    Attributes:
        _db: Core SQLAlchemy database manager.
    """

    def __init__(self, db_core: SqlalchemyCore):
        """Create a new DownloadDatabase instance.

        Args:
            db_core: The core SQLAlchemy database manager.
        """
        self._db = db_core

    # --- Transaction Support ---
    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession]:
        """Provide a transactional session.

        This is a passthrough to the core SQLAlchemy session manager.
        Use as an async context manager for database transactions.

        Yields:
            An active, transactional AsyncSession.
        """
        async with self._db.session() as session:
            yield session

    # --- CRUD Operations ---

    @handle_download_db_errors(
        "upsert download",
        feed_id_from="download.feed_id",
        download_id_from="download.id",
    )
    async def upsert_download(self, download: Download) -> None:
        """Insert or update a download in the downloads table.

        If a download with the same (feed, id) exists, it will be updated.

        Args:
            download: The Download object to insert or update.

        Raises:
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {
            "feed_id": download.feed_id,
            "download_id": download.id,
            "status": str(download.status),
        }
        logger.debug("Attempting to upsert download record.", extra=log_params)
        async with self._db.session() as session:
            data = download.model_dump_for_insert()

            stmt = insert(Download).values(**data)
            # Update all columns except primary keys on conflict
            data.pop("feed_id", None)
            data.pop("id", None)
            stmt = stmt.on_conflict_do_update(
                index_elements=["feed_id", "id"], set_=data
            )
            await session.execute(stmt)
            await session.commit()
        logger.debug("Upsert download record execution complete.", extra=log_params)

    # --- Status Transition Methods ---

    @handle_download_db_errors("mark download as QUEUED from UPCOMING")
    async def mark_as_queued_from_upcoming(
        self, feed_id: str, download_id: str
    ) -> None:
        """Transition a download from UPCOMING to QUEUED status.

        Updates status to QUEUED only if current status is UPCOMING.
        Preserves retries and last_error values.

        Args:
            feed_id: The feed identifier.
            download_id: The download identifier.

        Raises:
            DownloadNotFoundError: If the download is not found.
            DatabaseOperationError: If the database operation fails or current status is not UPCOMING.
        """
        log_params = {
            "feed_id": feed_id,
            "download_id": download_id,
        }
        logger.debug("Attempting to mark as QUEUED from UPCOMING.", extra=log_params)

        async with self._db.session() as session:
            stmt = (
                update(Download)
                .where(
                    and_(
                        col(Download.feed_id) == feed_id,
                        col(Download.id) == download_id,
                        col(Download.status) == DownloadStatus.UPCOMING,
                    )
                )
                .values(
                    status=DownloadStatus.QUEUED,
                )
            )
            res = await session.execute(stmt)
            await session.commit()

        match res.rowcount:
            case 0:
                raise DownloadNotFoundError(
                    "Download not found.", feed_id=feed_id, download_id=download_id
                )
            case 1:
                pass
            case _ as row_count:
                raise DatabaseOperationError(
                    f"Update affected {row_count} rows, expected 1. Rolling back transaction.",
                    feed_id=feed_id,
                    download_id=download_id,
                )
        logger.debug("Download marked as QUEUED from UPCOMING.", extra=log_params)

    @handle_feed_db_errors("requeue downloads")
    async def requeue_downloads(
        self,
        feed_id: str,
        download_ids: None | list[str] | str,
        from_status: DownloadStatus | None = None,
    ) -> int:
        """Re-queue downloads by resetting status and error counters.

        - Targeted: provide one or more `download_ids` to re-queue those items.
        - Bulk: pass `download_ids=None` to re-queue all downloads for the feed.
          `from_status` is required for bulk operations to avoid unintended re-queuing.

        Sets status to QUEUED and resets retries to 0 and last_error to NULL.

        Args:
            feed_id: The feed identifier.
            download_ids: download identifier(s) to requeue, or None to apply to all downloads of the feed.
            from_status: Optional status precondition. If provided, only downloads currently
                        in this status are updated.

        Returns:
            Number of downloads successfully requeued.

        Raises:
            DatabaseOperationError: If database operations fail.
        """
        where_clauses: list[ColumnElement[bool]] = [col(Download.feed_id) == feed_id]

        match download_ids:
            case None if from_status is None:
                # Safety: bulk mode must specify from_status to avoid re-queuing
                # unintended statuses (e.g., DOWNLOADED).
                raise DatabaseOperationError(
                    "Bulk requeue requires 'from_status' to be specified."
                )
            case None:
                expected_count = None
                download_count_repr = "<all>"
            case list() as id_list:
                if len(id_list) == 0:
                    return 0
                where_clauses.append(col(Download.id).in_(id_list))
                expected_count = len(id_list)
                download_count_repr = expected_count
            case str() as single_id:
                where_clauses.append(col(Download.id) == single_id)
                expected_count = 1
                download_count_repr = 1

        if from_status is not None:
            where_clauses.append(col(Download.status) == from_status)

        log_params = {
            "feed_id": feed_id,
            "download_count": download_count_repr,
            "from_status": str(from_status) if from_status else None,
        }
        logger.debug("Attempting to re-queue downloads.", extra=log_params)

        async with self._db.session() as session:
            stmt = (
                update(Download)
                .where(*where_clauses)
                .values(
                    status=DownloadStatus.QUEUED,
                    retries=0,
                    last_error=None,
                )
            )
            result = await session.execute(stmt)

            if expected_count is not None and expected_count != result.rowcount:
                raise DatabaseOperationError(
                    f"Expected to requeue {expected_count} downloads but only {result.rowcount} were updated. "
                    f"Some downloads may not exist or may not have the expected status. Rolling back changes.",
                )
            await session.commit()

        logger.debug(
            "Downloads requeued.",
            extra={**log_params, "count_requeued": result.rowcount},
        )
        return result.rowcount

    @handle_download_db_errors("mark download as DOWNLOADED")
    async def mark_as_downloaded(
        self, feed_id: str, download_id: str, ext: str, filesize: int
    ) -> None:
        """Mark a download as DOWNLOADED with updated metadata.

        Updates status to DOWNLOADED only if current status is QUEUED.
        Resets retries to 0, last_error to NULL, and updates ext and filesize.

        Args:
            feed_id: The feed identifier.
            download_id: The download identifier.
            ext: The new file extension.
            filesize: The new file size in bytes.

        Raises:
            DownloadNotFoundError: If the download is not found.
            DatabaseOperationError: If the current status is not QUEUED or DB update fails.
        """
        log_params = {"feed_id": feed_id, "download_id": download_id}
        logger.debug("Attempting to mark as DOWNLOADED.", extra=log_params)
        async with self._db.session() as session:
            stmt = (
                update(Download)
                .where(
                    col(Download.feed_id) == feed_id,
                    col(Download.id) == download_id,
                    col(Download.status) == DownloadStatus.QUEUED,
                )
                .values(
                    status=DownloadStatus.DOWNLOADED,
                    retries=0,
                    last_error=None,
                    ext=ext,
                    filesize=filesize,
                )
            )
            result = await session.execute(stmt)
            match result.rowcount:
                case 0:
                    raise DownloadNotFoundError(
                        "Download not found.", feed_id=feed_id, download_id=download_id
                    )
                case 1:
                    pass
                case _ as row_count:
                    raise DatabaseOperationError(
                        f"Update affected {row_count} rows, expected 1. Rolling back transaction.",
                        feed_id=feed_id,
                        download_id=download_id,
                    )
            await session.commit()

        logger.debug("Download marked as DOWNLOADED.", extra=log_params)

    @handle_download_db_errors("set thumbnail extension for download")
    async def set_thumbnail_extension(
        self, feed_id: str, download_id: str, thumbnail_ext: str | None
    ) -> None:
        """Persist the hosted thumbnail extension for a download.

        Args:
            feed_id: The feed identifier.
            download_id: The download identifier.
            thumbnail_ext: File extension for hosted thumbnail (e.g., "jpg"), or None to clear.

        Raises:
            DownloadNotFoundError: If the download is not found.
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {
            "feed_id": feed_id,
            "download_id": download_id,
            "thumbnail_ext": thumbnail_ext,
        }
        logger.debug("Attempting to set thumbnail extension.", extra=log_params)
        async with self._db.session() as session:
            stmt = (
                update(Download)
                .where(
                    col(Download.feed_id) == feed_id,
                    col(Download.id) == download_id,
                )
                .values(thumbnail_ext=thumbnail_ext)
            )
            result = await session.execute(stmt)
            try:
                self._db.assert_exactly_one_row_affected(
                    result, feed_id=feed_id, download_id=download_id
                )
            except NotFoundError as e:
                raise DownloadNotFoundError(
                    "Download not found.", feed_id=feed_id, download_id=download_id
                ) from e
            await session.commit()
        logger.debug("Thumbnail extension updated.", extra=log_params)

    @handle_download_db_errors("mark download as SKIPPED")
    async def skip_download(self, feed_id: str, download_id: str) -> None:
        """Skip a download by setting its status to SKIPPED.

        Preserves retries and last_error values.

        Args:
            feed_id: The feed identifier.
            download_id: The download identifier.

        Raises:
            DownloadNotFoundError: If the download is not found.
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {"feed_id": feed_id, "download_id": download_id}
        logger.debug("Attempting to mark as SKIPPED.", extra=log_params)
        async with self._db.session() as session:
            stmt = (
                update(Download)
                .where(
                    col(Download.feed_id) == feed_id,
                    col(Download.id) == download_id,
                )
                .values(
                    status=DownloadStatus.SKIPPED,
                )
            )
            result = await session.execute(stmt)
            try:
                self._db.assert_exactly_one_row_affected(
                    result, feed_id=feed_id, download_id=download_id
                )
            except NotFoundError as e:
                raise DownloadNotFoundError(
                    "Download not found.", feed_id=feed_id, download_id=download_id
                ) from e
            await session.commit()
        logger.debug("Download marked as SKIPPED.", extra=log_params)

    @handle_download_db_errors("mark download as ARCHIVED")
    async def archive_download(self, feed_id: str, download_id: str) -> None:
        """Archive a download by setting its status to ARCHIVED.

        Preserves retries and last_error values.

        Args:
            feed_id: The feed identifier.
            download_id: The download identifier.

        Raises:
            DownloadNotFoundError: If the download is not found.
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {"feed_id": feed_id, "download_id": download_id}
        logger.debug("Attempting to mark as ARCHIVED.", extra=log_params)
        async with self._db.session() as session:
            stmt = (
                update(Download)
                .where(
                    col(Download.feed_id) == feed_id,
                    col(Download.id) == download_id,
                )
                .values(
                    status=DownloadStatus.ARCHIVED,
                    thumbnail_ext=None,
                )
            )
            result = await session.execute(stmt)
            try:
                self._db.assert_exactly_one_row_affected(
                    result, feed_id=feed_id, download_id=download_id
                )
            except NotFoundError as e:
                raise DownloadNotFoundError(
                    "Download not found.", feed_id=feed_id, download_id=download_id
                ) from e
            await session.commit()
        logger.debug("Download marked as ARCHIVED.", extra=log_params)

    @handle_download_db_errors("bump retry count")
    async def bump_retries(
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
        log_params = {"feed_id": feed_id, "download_id": download_id}
        logger.debug("Attempting to bump retries.", extra=log_params)
        async with self._db.session() as session:
            download = await session.get(Download, (feed_id, download_id))
            if not download:
                raise DownloadNotFoundError(
                    "Download not found.", feed_id=feed_id, download_id=download_id
                )

            download.retries = download.retries + 1
            original_status = download.status

            download.status = (
                DownloadStatus.ERROR
                if download.retries >= max_allowed_errors
                and download.status != DownloadStatus.DOWNLOADED
                else download.status
            )

            # Track if status actually transitioned to ERROR
            transitioned_to_error = (
                original_status != DownloadStatus.ERROR
                and download.status == DownloadStatus.ERROR
            )

            if transitioned_to_error:
                logger.debug(
                    "Download transitioned to ERROR.",
                    extra={**log_params, "retries": download.retries},
                )
            elif (
                download.retries >= max_allowed_errors
                and download.status == DownloadStatus.DOWNLOADED
            ):
                logger.warning(
                    "Max retries reached for already DOWNLOADED item. Status remains DOWNLOADED.",
                    extra={**log_params, "retries": download.retries},
                )
            download.last_error = error_message

            session.add(download)
            await session.commit()
            return (
                download.retries,
                download.status,
                transitioned_to_error,
            )

    # --- Query Methods ---

    @handle_feed_db_errors("get downloads to prune by keep_last")
    async def get_downloads_to_prune_by_keep_last(
        self, feed_id: str, keep_last: int
    ) -> list[Download]:
        """Identify downloads to prune based on 'keep_last' rule.

        Returns downloads that exceed the keep_last limit, excluding
        downloads with status ARCHIVED or SKIPPED.

        Args:
            feed_id: The feed identifier.
            keep_last: The number of most recent downloads to keep.

        Returns:
            List of Download objects that should be pruned.

        Raises:
            DatabaseOperationError: If the database query fails.
        """
        if keep_last <= 0:
            return []

        async with self._db.session() as session:
            stmt = (
                select(Download)
                .where(
                    col(Download.feed_id) == feed_id,
                    col(Download.status).notin_(
                        [DownloadStatus.ARCHIVED, DownloadStatus.SKIPPED]
                    ),
                )
                .order_by(col(Download.published).desc())
                .offset(keep_last)
            )
            results = await session.execute(stmt)
            return list(results.scalars().all())

    @handle_feed_db_errors("get downloads to prune by since")
    async def get_downloads_to_prune_by_since(
        self, feed_id: str, since: datetime
    ) -> list[Download]:
        """Identify downloads published before the 'since' datetime (UTC).

        Returns downloads published before the given datetime, excluding
        downloads with status ARCHIVED or SKIPPED. The 'since' parameter
        must be a timezone-aware datetime object in UTC.

        Args:
            feed_id: The feed identifier.
            since: The cutoff datetime (must be timezone-aware UTC).

        Returns:
            List of Download objects that should be pruned.

        Raises:
            DatabaseOperationError: If the database query fails.
        """
        async with self._db.session() as session:
            stmt = (
                select(Download)
                .where(
                    col(Download.feed_id) == feed_id,
                    col(Download.published) < since,
                    col(Download.status).notin_(
                        [DownloadStatus.ARCHIVED, DownloadStatus.SKIPPED]
                    ),
                )
                .order_by(col(Download.published).desc())
            )
            results = await session.execute(stmt)
            return list(results.scalars().all())

    @handle_download_db_errors("retrieve download by ID")
    async def get_download_by_id(self, feed_id: str, download_id: str) -> Download:
        """Retrieve a specific download by feed and id.

        Args:
            feed_id: The feed identifier.
            download_id: The download identifier.

        Returns:
            Download object for the specified feed and id.

        Raises:
            DownloadNotFoundError: If the download is not found.
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {"feed_id": feed_id, "download_id": download_id}
        logger.debug("Attempting to get download by ID.", extra=log_params)
        async with self._db.session() as session:
            download = await session.get(Download, (feed_id, download_id))
            if not download:
                raise DownloadNotFoundError(
                    "Download not found.", feed_id=feed_id, download_id=download_id
                )
            return download

    @handle_feed_db_errors("get downloads by status")
    async def get_downloads_by_status(
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
        async with self._db.session() as session:
            stmt = select(Download).where(col(Download.status) == status_to_filter)
            if feed_id:
                stmt = stmt.where(col(Download.feed_id) == feed_id)
            if published_after:
                stmt = stmt.where(col(Download.published) >= published_after)
            if published_before:
                stmt = stmt.where(col(Download.published) < published_before)

            stmt = (
                stmt.order_by(col(Download.published).desc())
                .limit(limit)
                .offset(offset)
            )

            results = await session.execute(stmt)
            return list(results.scalars().all())

    @handle_feed_db_errors("count downloads by status")
    async def count_downloads_by_status(
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

        match status_to_filter:
            case DownloadStatus() as s:
                status_where_clause = col(Download.status) == s
            case list() as ss:
                status_where_clause = col(Download.status).in_(ss)

        async with self._db.session() as session:
            stmt = select(func.count(col(Download.id))).where(status_where_clause)
            if feed_id:
                stmt = stmt.where(col(Download.feed_id) == feed_id)

            result = await session.execute(stmt)
            count = result.scalar_one_or_none()
        if count is None:
            raise DatabaseOperationError(
                "Failed to count downloads by status.",
                feed_id=feed_id,
            )
        return count
