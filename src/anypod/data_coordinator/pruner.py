"""Handles the pruning of old downloads based on retention policies.

This module defines the Pruner class, which is responsible for identifying
and removing old downloads according to configured retention rules, including
file deletion and database record archiving.
"""

import datetime
import logging

from ..db import DatabaseManager, Download, DownloadStatus
from ..exceptions import (
    DatabaseOperationError,
    DownloadNotFoundError,
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
        db_manager: Database manager for download record operations.
        file_manager: File manager for file system operations.
    """

    def __init__(self, db_manager: DatabaseManager, file_manager: FileManager):
        self.db_manager = db_manager
        self.file_manager = file_manager
        logger.debug("Pruner initialized.")

    def prune_feed_downloads(
        self,
        feed_id: str,
        keep_last: int | None,
        prune_before_date: datetime.datetime | None,
    ) -> tuple[list[str], list[str]]:
        """Prune old downloads for a feed based on retention rules.

        This method identifies download candidates for pruning based on two criteria:
        1. keep_last: Retains only the specified number of the most recent DOWNLOADED downloads.
           Older DOWNLOADED items become candidates for pruning.
        2. prune_before_date: DOWNLOADED downloads published before this timestamp become candidates.

        The union of downloads identified by both criteria is processed. For each candidate:
        - If its status is DOWNLOADED, its associated media file is deleted from the filesystem.
        - The download's database record status is then updated to ARCHIVED.

        Malformed database records encountered during candidate selection are logged and skipped.

        Args:
            feed_id: The unique identifier of the feed to prune.
            keep_last: The number of most recent downloaded items to retain. If None, this rule is ignored.
            prune_before_date: Downloads published before this date are pruned. If None, this rule is ignored.

        Returns:
            A tuple containing two lists of strings:
            - The first list contains the IDs of download records successfully updated to ARCHIVED status.
            - The second list contains the IDs of downloads whose associated media files were successfully deleted.

        Raises:
            DatabaseOperationError: If a database query or update fails during the pruning process
                                    (e.g., fetching candidates, updating status to ARCHIVED).
            FileOperationError: If a critical file deletion operation fails for a specific download.
                                This typically halts pruning for that item but allows others to proceed
                                if the error is isolated.
        """
        log_params = {
            "feed_id": feed_id,
            "keep_last": keep_last,
            "prune_before_date": (
                prune_before_date.isoformat() if prune_before_date else None
            ),
        }
        logger.info("Starting pruning process for feed.", extra=log_params)

        ids_of_downloads_archived: list[str] = []
        ids_of_files_deleted: list[str] = []
        candidate_downloads_to_prune: set[Download] = set()

        if keep_last is not None and keep_last > 0:
            logger.debug(
                "Identifying prune candidates by keep_last rule.", extra=log_params
            )
            try:
                downloads_for_keep_last = (
                    self.db_manager.get_downloads_to_prune_by_keep_last(
                        feed_id, keep_last
                    )
                )
            except (DatabaseOperationError, ValueError) as e:
                # TODO raise a domain specific exception instead
                raise DatabaseOperationError(
                    message="Database error identifying downloads to prune by keep_last rule.",
                    feed_id=feed_id,
                    download_id=f"keep_last:{keep_last}",
                ) from e
            candidate_downloads_to_prune.update(downloads_for_keep_last)

        if prune_before_date is not None:
            logger.debug("Identifying prune candidates by date rule.", extra=log_params)
            try:
                downloads_for_since = self.db_manager.get_downloads_to_prune_by_since(
                    feed_id, prune_before_date
                )
            except (DatabaseOperationError, ValueError) as e:
                raise DatabaseOperationError(
                    message="Database error identifying downloads to prune by date rule.",
                    feed_id=feed_id,
                    download_id=f"prune_before_date:{prune_before_date.isoformat()}",
                ) from e
            candidate_downloads_to_prune.update(downloads_for_since)

        if not candidate_downloads_to_prune:
            logger.info("No downloads found to prune for feed.", extra=log_params)
            return [], []

        logger.info(
            f"Identified {len(candidate_downloads_to_prune)} candidate(s) for pruning.",
            extra=log_params,
        )

        successfully_processed_ids_for_db_deletion: list[str] = []

        for download_to_prune in candidate_downloads_to_prune:
            download_prune_log_params = {
                "feed_id": feed_id,
                "download_id": download_to_prune.id,
                "download_status": download_to_prune.status,
            }

            if download_to_prune.status == DownloadStatus.DOWNLOADED:
                file_name_to_delete = f"{download_to_prune.id}.{download_to_prune.ext}"
                download_prune_log_params["file_name"] = file_name_to_delete
                logger.debug(
                    "Attempting to delete file for downloaded download being pruned.",
                    extra=download_prune_log_params,
                )
                try:
                    deleted_on_fs = self.file_manager.delete_download_file(
                        feed_id, file_name_to_delete
                    )
                    if deleted_on_fs:
                        logger.info(
                            "File deleted successfully during pruning.",
                            extra=download_prune_log_params,
                        )
                        ids_of_files_deleted.append(download_to_prune.id)
                    else:
                        logger.warning(
                            "File for download not found on disk during pruning. DB record will still be archived.",
                            extra=download_prune_log_params,
                        )
                except FileOperationError as e_fs:
                    raise FileOperationError(
                        message="Error deleting file during pruning. Download not modified.",
                        feed_id=feed_id,
                        download_id=download_to_prune.id,
                        file_name=file_name_to_delete,
                    ) from e_fs

            successfully_processed_ids_for_db_deletion.append(download_to_prune.id)

        if successfully_processed_ids_for_db_deletion:
            logger.debug(
                f"Attempting to archive {len(successfully_processed_ids_for_db_deletion)} download records.",
                extra={"feed_id": feed_id},
            )
        for id_to_archive in successfully_processed_ids_for_db_deletion:
            archive_log_params = {"feed_id": feed_id, "download_id": id_to_archive}
            try:
                self.db_manager.archive_download(feed_id, id_to_archive)
                logger.info(
                    "Download record archived successfully.",
                    extra=archive_log_params,
                )
                ids_of_downloads_archived.append(id_to_archive)
            except (DownloadNotFoundError, DatabaseOperationError) as e:
                raise PruneError(
                    "Failed to archive download record.",
                    feed_id=feed_id,
                    download_id=id_to_archive,
                ) from e

        logger.info(
            "Pruning process completed for feed.",
            extra={
                **log_params,
                "downloads_archived_count": len(ids_of_downloads_archived),
                "files_deleted_count": len(ids_of_files_deleted),
            },
        )
        return ids_of_downloads_archived, ids_of_files_deleted
