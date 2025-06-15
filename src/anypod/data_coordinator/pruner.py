"""Handles the pruning of old downloads based on retention policies.

This module defines the Pruner class, which is responsible for identifying
and removing old downloads according to configured retention rules, including
file deletion and database record archiving.
"""

from datetime import datetime
import logging
from typing import Any

from ..db import DownloadDatabase, FeedDatabase
from ..db.types import Download, DownloadStatus
from ..exceptions import (
    DatabaseOperationError,
    DownloadNotFoundError,
    FeedNotFoundError,
    FileOperationError,
    PruneError,
)
from ..file_manager import FileManager

logger = logging.getLogger(__name__)


class Pruner:
    """Manage the pruning of old downloads based on retention policies.

    The Pruner identifies downloads that should be removed according to
    configured retention rules (keep_last count and prune_before_date),
    deletes associated files, and archives database records.

    Attributes:
        _download_db: Database manager for download record operations.
        _feed_db: Database manager for feed record operations.
        _file_manager: File manager for file system operations.
    """

    def __init__(
        self,
        feed_db: FeedDatabase,
        download_db: DownloadDatabase,
        file_manager: FileManager,
    ):
        self._feed_db = feed_db
        self._download_db = download_db
        self._file_manager = file_manager
        logger.debug("Pruner initialized.")

    def _identify_prune_candidates(
        self,
        feed_id: str,
        keep_last: int | None,
        prune_before_date: datetime | None,
    ) -> set[Download]:
        """Identify downloads that are candidates for pruning.

        Combines candidates from both keep_last and prune_before_date rules.

        Args:
            feed_id: The feed identifier.
            keep_last: Number of most recent downloads to keep (None to ignore).
            prune_before_date: Downloads published before this date are candidates (None to ignore).

        Returns:
            Set of Download objects that are candidates for pruning.

        Raises:
            PruneError: If database operations fail during candidate identification.
        """
        log_params: dict[str, Any] = {
            "feed_id": feed_id,
            "keep_last": keep_last,
            "prune_before_date": (
                prune_before_date.isoformat() if prune_before_date else None
            ),
        }
        candidate_downloads: set[Download] = set()

        # Identify candidates by keep_last rule
        if keep_last is not None and keep_last > 0:
            logger.debug(
                "Identifying prune candidates by keep_last rule.", extra=log_params
            )
            try:
                downloads_for_keep_last = (
                    self._download_db.get_downloads_to_prune_by_keep_last(
                        feed_id, keep_last
                    )
                )
            except DatabaseOperationError as e:
                raise PruneError(
                    message="Failed to identify downloads for keep_last pruning rule.",
                    feed_id=feed_id,
                ) from e
            else:
                candidate_downloads.update(downloads_for_keep_last)

        # Identify candidates by prune_before_date rule
        if prune_before_date is not None:
            logger.debug("Identifying prune candidates by date rule.", extra=log_params)
            try:
                downloads_for_since = self._download_db.get_downloads_to_prune_by_since(
                    feed_id, prune_before_date
                )
            except DatabaseOperationError as e:
                raise PruneError(
                    message="Failed to identify downloads for date pruning rule.",
                    feed_id=feed_id,
                ) from e
            else:
                candidate_downloads.update(downloads_for_since)

        logger.debug(
            "Identified candidates for pruning.",
            extra={**log_params, "candidate_count": len(candidate_downloads)},
        )
        return candidate_downloads

    def _handle_file_deletion(self, download: Download, feed_id: str) -> None:
        """Handle file deletion for a DOWNLOADED item being pruned.

        Args:
            download: The Download object with DOWNLOADED status.
            feed_id: The feed identifier.

        Raises:
            PruneError: If file deletion fails with an OS-level error.
            FileNotFoundError: If the file does not exist or is not a regular file.
        """
        file_name = f"{download.id}.{download.ext}"
        log_params: dict[str, Any] = {
            "feed_id": feed_id,
            "download_id": download.id,
            "file_name": file_name,
        }
        logger.debug(
            "Attempting to delete file for downloaded item being pruned.",
            extra=log_params,
        )

        try:
            self._file_manager.delete_download_file(feed_id, download.id, download.ext)
        except FileOperationError as e:
            raise PruneError(
                message="Failed to delete file during pruning.",
                feed_id=feed_id,
                download_id=download.id,
            ) from e
        logger.info("File deleted successfully during pruning.", extra=log_params)

    def _archive_download(self, download: Download, feed_id: str) -> None:
        """Archive a download in the database.

        Args:
            download: The Download object to archive.
            feed_id: The feed identifier.

        Raises:
            PruneError: If the database archival operation fails.
        """
        log_params: dict[str, Any] = {
            "feed_id": feed_id,
            "download_id": download.id,
        }
        logger.debug("Attempting to archive download.", extra=log_params)

        try:
            self._download_db.archive_download(feed_id, download.id)
        except (DownloadNotFoundError, DatabaseOperationError) as e:
            raise PruneError(
                message="Failed to archive download.",
                feed_id=feed_id,
                download_id=download.id,
            ) from e
        logger.info("Download archived successfully.", extra=log_params)

    def _process_single_download_for_pruning(
        self, download: Download, feed_id: str
    ) -> bool:
        """Process a single download for pruning.

        Handles file deletion (if DOWNLOADED) and database archival.

        Args:
            download: The Download object to process.
            feed_id: The feed identifier.

        Returns:
            True if a file was successfully deleted, False otherwise.

        Raises:
            PruneError: If any step in the pruning process fails.
        """
        log_params: dict[str, Any] = {
            "feed_id": feed_id,
            "download_id": download.id,
            "download_status": download.status,
        }
        logger.debug("Processing single download for pruning.", extra=log_params)

        file_deleted = False

        # Delete file if the download is DOWNLOADED
        if download.status == DownloadStatus.DOWNLOADED:
            try:
                self._handle_file_deletion(download, feed_id)
            except FileNotFoundError:
                logger.warning(
                    "File not found during pruning, but DB record will still be archived.",
                    extra={**log_params, "download_id": download.id},
                )
            else:
                file_deleted = True

        # Always archive the download
        self._archive_download(download, feed_id)

        return file_deleted

    def _recalculate_total_downloads(self, feed_id: str) -> None:
        """Recalculate and update total_downloads for a feed by counting DOWNLOADED downloads.

        Args:
            feed_id: The feed identifier.

        Raises:
            FeedNotFoundError: If the feed is not found in the database.
        """
        log_params: dict[str, Any] = {"feed_id": feed_id}
        logger.debug("Recalculating total_downloads for feed.", extra=log_params)

        try:
            with self._download_db.transaction():
                # Count only DOWNLOADED downloads (those that appear in RSS feed)
                total_count = self._download_db.count_downloads_by_status(
                    DownloadStatus.DOWNLOADED, feed_id=feed_id
                )

                # Update the feed's total_downloads in the same transaction
                self._feed_db.update_total_downloads(feed_id, total_count)
        except DatabaseOperationError as e:
            logger.error(
                "Failed to count downloaded items for total_downloads recalculation.",
                exc_info=e,
                extra=log_params,
            )
        except FeedNotFoundError as e:
            raise PruneError(
                message="Feed not found during total_downloads recalculation.",
                feed_id=feed_id,
            ) from e
        else:
            logger.info(
                "Total downloads recalculated for feed.",
                extra={**log_params, "new_total_downloads": total_count},
            )

    def prune_feed_downloads(
        self,
        feed_id: str,
        keep_last: int | None,
        prune_before_date: datetime | None,
    ) -> tuple[int, int]:
        """Prune old downloads for a feed based on retention rules.

        This method identifies download candidates for pruning based on two criteria:
        1. keep_last: Retains only the specified number of the most recent downloads.
           Older downloads become candidates for pruning.
        2. prune_before_date: Downloads published before this timestamp become candidates.

        The union of downloads identified by both criteria is processed. For each candidate:
        - If its status is DOWNLOADED, its associated media file is deleted from the filesystem.
        - The download's database record status is then updated to ARCHIVED.

        Args:
            feed_id: The unique identifier of the feed to prune.
            keep_last: The number of most recent downloads to retain. If None, this rule is ignored.
            prune_before_date: Downloads published before this date are pruned. If None, this rule is ignored.

        Returns:
            A tuple (archived_count, files_deleted_count) indicating the number of
            downloads archived and the number of files successfully deleted.

        Raises:
            PruneError: If candidate identification fails.
        """
        log_params: dict[str, Any] = {
            "feed_id": feed_id,
            "keep_last": keep_last,
            "prune_before_date": (
                prune_before_date.isoformat() if prune_before_date else None
            ),
        }
        logger.info("Starting pruning process for feed.", extra=log_params)

        candidate_downloads = self._identify_prune_candidates(
            feed_id, keep_last, prune_before_date
        )

        if not candidate_downloads:
            logger.info("No downloads found to prune for feed.", extra=log_params)
            return 0, 0

        logger.info(
            "Found candidates for pruning.",
            extra={**log_params, "candidate_count": len(candidate_downloads)},
        )

        archived_count = 0
        files_deleted_count = 0

        for download in candidate_downloads:
            try:
                file_deleted = self._process_single_download_for_pruning(
                    download, feed_id
                )
                archived_count += 1
                if file_deleted:
                    files_deleted_count += 1
            except PruneError as e:
                logger.error(
                    "Failed to process download for pruning.",
                    exc_info=e,
                    extra={
                        **log_params,
                        "download_id": download.id,
                    },
                )

        # Recalculate total_downloads after archiving
        if archived_count > 0:
            self._recalculate_total_downloads(feed_id)

        logger.info(
            "Pruning process completed for feed.",
            extra={
                **log_params,
                "archived_count": archived_count,
                "files_deleted_count": files_deleted_count,
            },
        )
        return archived_count, files_deleted_count

    def archive_feed(self, feed_id: str) -> tuple[int, int]:
        """Archive an entire feed by disabling it and archiving all downloads.

        This method disables a feed by setting is_enabled=False and archives
        all downloads associated with the feed regardless of their current status.
        Files for DOWNLOADED items are deleted from the filesystem.

        Args:
            feed_id: The unique identifier of the feed to archive.

        Returns:
            A tuple (archived_count, files_deleted_count) indicating the number of
            downloads archived and the number of files successfully deleted.

        Raises:
            PruneError: If feed disabling or download archiving fails.
        """
        log_params: dict[str, Any] = {"feed_id": feed_id}
        logger.info("Starting feed archival process.", extra=log_params)

        with self._feed_db.transaction():
            # Get all downloads for this feed by collecting from all status types
            all_downloads: list[Download] = []
            for status in DownloadStatus:
                if status in (DownloadStatus.ARCHIVED, DownloadStatus.SKIPPED):
                    continue  # Skip already archived or skipped
                try:
                    downloads = self._download_db.get_downloads_by_status(
                        status, feed_id=feed_id
                    )
                    all_downloads.extend(downloads)
                except DatabaseOperationError as e:
                    raise PruneError(
                        message="Failed to retrieve downloads for feed archival.",
                        feed_id=feed_id,
                    ) from e

            logger.info(
                "Downloads to archive for feed.",
                extra={**log_params, "downloads_count": len(all_downloads)},
            )

            archived_count = 0
            files_deleted_count = 0

            for download in all_downloads:
                file_deleted = self._process_single_download_for_pruning(
                    download, feed_id
                )
                archived_count += 1
                if file_deleted:
                    files_deleted_count += 1

            # Recalculate total_downloads after archiving (should be 0)
            if archived_count > 0:
                self._recalculate_total_downloads(feed_id)

            # Disable the feed last
            self._feed_db.set_feed_enabled(feed_id, False)
            logger.info("Feed disabled successfully.", extra=log_params)

            logger.info(
                "Feed archival process completed.",
                extra={
                    **log_params,
                    "archived_count": archived_count,
                    "files_deleted_count": files_deleted_count,
                },
            )
            return archived_count, files_deleted_count
