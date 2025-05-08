import datetime
import sqlite3
from typing import IO

from .db import DatabaseManager, Download, DownloadStatus
from .exceptions import (
    DatabaseOperationError,
    DataCoordinatorError,
    DownloadNotFoundError,
    FileOperationError,
)
from .file_manager import FileManager


class DataCoordinator:
    """
    Orchestrates operations between the DatabaseManager and FileManager.
    Handles the business logic involving both database records and filesystem files.
    """

    def __init__(self, db_manager: DatabaseManager, file_manager: FileManager):
        """
        Initializes the DataCoordinator.

        Args:
            db_manager: An instance of DatabaseManager.
            file_manager: An instance of FileManager.
        """
        self.db_manager = db_manager
        self.file_manager = file_manager

    def add_download(self, download_to_add: Download) -> None:
        """
        Adds or replaces a download.

        If a download with the same feed and id already exists, this method
        will first delete its associated downloaded file (if the status was DOWNLOADED),
        then delete the existing database record, before finally adding the new download's record.

        Args:
            download_to_add: The Download object representing the desired state.

        Raises:
            DatabaseOperationError: If a database operation fails.
            FileOperationError: If a file operation fails (e.g., expected file not found, or cannot delete).
            DownloadNotFoundError: If an existing download is expected but not found during DB deletion.
        """
        try:
            existing_db_row = self.db_manager.get_download_by_id(
                download_to_add.feed, download_to_add.id
            )
        except sqlite3.Error as e:
            raise DatabaseOperationError(
                f"Error checking for existing download {download_to_add.feed}/{download_to_add.id}",
            ) from e

        if existing_db_row and existing_db_row["status"] == str(
            DownloadStatus.DOWNLOADED
        ):
            # Download exists, so we need to replace it.
            # We assume ext is present due to NOT NULL constraint in DB.
            filename_to_delete = f"{download_to_add.id}.{existing_db_row['ext']}"
            try:
                deleted = self.file_manager.delete_download_file(
                    feed=download_to_add.feed, filename=filename_to_delete
                )
            except OSError as e:
                # Handles FS errors like permission issues during delete_download_file
                raise FileOperationError(
                    f"Error deleting file {filename_to_delete} for download {download_to_add.feed}/{download_to_add.id}",
                ) from e
            if not deleted:
                print(
                    f"Warning: Expected file {filename_to_delete} for downloaded item {download_to_add.feed}/{download_to_add.id} not found on disk for deletion."
                )

        try:
            self.db_manager.upsert_download(download_to_add)
        except sqlite3.Error as e:
            raise DatabaseOperationError(
                f"Error adding new download {download_to_add.feed}/{download_to_add.id}",
            ) from e

    def update_status(
        self,
        feed: str,
        id: str,
        status: DownloadStatus,
        last_error: str | None = None,
    ) -> bool:
        """
        Updates the status of a download in the database.
        If the status changes from DOWNLOADED to any other state, this method
        will attempt to delete the associated media file.

        Args:
            feed: The feed name of the download.
            id: The ID of the download.
            status: The new DownloadStatus.
            last_error: An optional error message if the status is ERROR.

        Returns:
            True if a row was updated, False otherwise.

        Raises:
            DatabaseOperationError: If a database operation fails.
            FileOperationError: If an essential file operation fails (e.g., deleting an existing file).
            DownloadNotFoundError: If the download to update is not found in the database initially.
        """
        try:
            current_download_row = self.db_manager.get_download_by_id(feed, id)
        except sqlite3.Error as e:
            raise DatabaseOperationError(
                f"Error retrieving download {feed}/{id} for status update to {status}",
            ) from e

        if not current_download_row:
            raise DownloadNotFoundError(
                f"Cannot update status: Download {feed}/{id} not found."
            )

        current_status_str = current_download_row["status"]

        # If status is changing FROM DOWNLOADED to something else, delete the file.
        if (
            current_status_str == str(DownloadStatus.DOWNLOADED)
            and status != DownloadStatus.DOWNLOADED
        ):
            current_ext = current_download_row["ext"]
            filename_to_delete = f"{id}.{current_ext}"
            try:
                deleted = self.file_manager.delete_download_file(
                    feed=feed, filename=filename_to_delete
                )
            except OSError as e:
                # If file deletion fails with an OS error, this is more critical.
                # We should raise an error and not proceed with the DB status update to avoid inconsistency.
                raise FileOperationError(
                    f"Failed to delete file {filename_to_delete} for {feed}/{id} when changing status from DOWNLOADED",
                ) from e
            if not deleted:
                print(
                    f"Warning: Tried to delete file {filename_to_delete} for download {feed}/{id} but it was not found on disk when changing status from DOWNLOADED."
                )

        # Proceed to update the status in the database
        try:
            updated_in_db = self.db_manager.update_status(
                feed=feed, id=id, status=status, last_error=last_error
            )
        except sqlite3.Error as e:
            raise DatabaseOperationError(
                f"Error updating status for download {feed}/{id} to {status}",
            ) from e
        # If db_manager.update_status itself returns False after all the above checks,
        # it implies the item disappeared between the get and update, which is highly unlikely
        # but we return its result.
        return updated_in_db

    def _row_to_download(self, row: sqlite3.Row) -> Download:
        """Converts a sqlite3.Row to a Download object."""
        # Ensure datetime conversion is robust
        published_str = row["published"]
        try:
            published_dt = datetime.datetime.fromisoformat(published_str)
        except (TypeError, ValueError) as e:
            # Handle cases where published_str might be None or invalid format
            # This should ideally not happen if DB data is clean.
            # For now, let's raise an error or return None/default, depending on strictness.
            # Raising an error is safer to highlight data integrity issues.
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

        return Download(
            feed=row["feed"],
            id=row["id"],
            source_url=row["source_url"],
            title=row["title"],
            published=published_dt,
            ext=row["ext"],
            duration=row["duration"],
            thumbnail=row["thumbnail"],
            status=status_enum,
            retries=row["retries"],
            last_error=row["last_error"],
        )

    def get_download_by_id(self, feed: str, id: str) -> Download | None:
        """
        Retrieves a specific download by its feed and ID.

        Args:
            feed: The feed name of the download.
            id: The ID of the download.

        Returns:
            A Download object if found, otherwise None.

        Raises:
            DatabaseOperationError: If the database lookup fails.
        """
        try:
            row = self.db_manager.get_download_by_id(feed, id)
            return None if row is None else self._row_to_download(row)
        except sqlite3.Error as e:
            raise DatabaseOperationError(
                f"Database lookup failed for download {feed}/{id}",
            ) from e
        except ValueError as e:
            # Catch potential ValueError from _row_to_download if data is malformed
            # This indicates a data integrity issue rather than a direct DB operation failure.
            # Re-raise as a DataCoordinatorError or a specific DataIntegrityError for clarity.
            raise DataCoordinatorError(
                f"Data integrity issue for download {feed}/{id}: {e}"
            ) from e

    def stream_download_by_id(self, feed: str, id: str) -> IO[bytes] | None:
        """
        Retrieves a readable stream for a downloaded file.

        Checks if the download exists in the database and has a status of DOWNLOADED.
        If so, it requests the file stream from the FileManager.

        Args:
            feed: The feed name of the download.
            id: The ID of the download.

        Returns:
            A binary IO stream if the download is found, downloaded, and the file exists.
            None if the download record is not found or its status is not DOWNLOADED.

        Raises:
            DatabaseOperationError: If the database lookup fails.
            FileOperationError: If there's an issue retrieving the file stream from the FileManager.
            DataCoordinatorError: For data integrity issues (e.g. malformed DB data).
        """
        download = self.get_download_by_id(feed, id)

        if download is None or download.status != DownloadStatus.DOWNLOADED:
            return None

        filename = f"{download.id}.{download.ext}"

        try:
            return self.file_manager.get_download_stream(feed, filename)
        except FileNotFoundError as e:
            # DB says DOWNLOADED, but file is missing. Change the status to ERROR.
            error_msg = f"File {filename} not found for DOWNLOADED item despite being marked as downloaded."

            # TODO: This should be done in a single transaction with the DB update.
            self.update_status(
                feed=feed,
                id=id,
                status=DownloadStatus.ERROR,
                last_error=error_msg,
            )

            raise FileOperationError(error_msg) from e

        except OSError as e:
            # Other filesystem errors (e.g., permissions)
            raise FileOperationError(
                f"Error while trying to stream file {filename} for download {feed}/{id}",
            ) from e

    def get_errors(
        self, feed: str | None = None, limit: int = 100, offset: int = 0
    ) -> list[Download]:
        """
        Retrieves downloads that are in an ERROR state.

        Args:
            feed: Optional feed name to filter errors by.
            limit: Maximum number of error records to return.
            offset: Number of error records to skip (for pagination).

        Returns:
            A list of Download objects in ERROR state.

        Raises:
            DatabaseOperationError: If a database query fails.
            DataCoordinatorError: If there's an issue converting database rows to Download objects.
        """
        try:
            error_rows = self.db_manager.get_errors(
                feed=feed, limit=limit, offset=offset
            )
        except sqlite3.Error as e:
            raise DatabaseOperationError(
                "Database error while "
                + (
                    f"querying errors for feed '{feed}' with offset {offset}"
                    if feed
                    else f"querying all errors with offset {offset}"
                )
            ) from e

        downloads_with_errors: list[Download] = []
        for row in error_rows:
            try:
                download = self._row_to_download(row)
                downloads_with_errors.append(download)
            except ValueError as e:
                item_id = row["id"] if row and "id" in row else "unknown_id"
                item_feed = row["feed"] if row and "feed" in row else "unknown_feed"
                raise DataCoordinatorError(
                    f"Data integrity issue converting error row for {item_feed}/{item_id} to Download object",
                ) from e
        return downloads_with_errors

    def prune_old_downloads(
        self,
        feed: str,
        keep_last: int | None,
        prune_before_date: datetime.datetime | None,
    ) -> tuple[list[str], list[str]]:
        """
        Prunes old downloads for a given feed based on retention rules.

        Deletion Logic:
        1. Identifies downloads to prune based on `keep_last` (number of latest items to keep).
        2. Identifies downloads to prune based on `prune_before_date` (items published before this date).
        3. The union of these two sets of downloads is considered for pruning.
        4. For each candidate download:
           a. If its status is DOWNLOADED, its associated file is deleted from the filesystem.
           b. The download record is deleted from the database.

        Args:
            feed: The name of the feed to prune.
            keep_last: If not None, keeps only this many of the most recent DOWNLOADED items.
                       Older DOWNLOADED items are candidates for pruning.
            prune_before_date: If not None, DOWNLOADED items published before this date are
                               candidates for pruning.

        Returns:
            A tuple (ids of downloads archived, ids of files deleted).

        Raises:
            DatabaseOperationError: If a database operation fails during candidate fetching or deletion.
            FileOperationError: If a file deletion operation fails critically for a specific item.
                                 Pruning for that item might be skipped, or the error could be aggregated.
                                 (Current: Critical failures halt and raise).
            DataCoordinatorError: If there are unexpected data integrity issues (e.g. missing extension for a downloaded file).
        """
        ids_of_downloads_archived: list[str] = []
        ids_of_files_deleted: list[str] = []
        # Use a set now that Download is hashable
        candidate_downloads_to_prune: set[Download] = set()

        # 1. Get candidates from keep_last rule
        if keep_last is not None and keep_last > 0:
            # get_downloads_to_prune_by_keep_last returns IDs of items TO BE PRUNED
            try:
                rows_for_keep_last = (
                    self.db_manager.get_downloads_to_prune_by_keep_last(feed, keep_last)
                )
            except sqlite3.Error as e:
                raise DatabaseOperationError(
                    f"Database error while identifying downloads to prune (keep_last={keep_last}) for feed '{feed}'"
                ) from e

            for row in rows_for_keep_last:
                try:
                    dl = self._row_to_download(row)
                    candidate_downloads_to_prune.add(dl)
                except ValueError as e:
                    item_id = row["id"] if row and "id" in row else "unknown_id"
                    item_feed = row["feed"] if row and "feed" in row else "unknown_feed"
                    raise DataCoordinatorError(
                        f"Cannot convert keep_last row for {item_feed}/{item_id} to Download object",
                    ) from e

        # 2. Get candidates from prune_before_date rule
        if prune_before_date is not None:
            # get_downloads_to_prune_by_since returns IDs of items TO BE PRUNED
            try:
                rows_for_since = self.db_manager.get_downloads_to_prune_by_since(
                    feed, prune_before_date
                )
            except sqlite3.Error as e:
                raise DatabaseOperationError(
                    f"Database error while identifying downloads to prune (prune_before_date={prune_before_date}) for feed '{feed}'"
                ) from e
            for row in rows_for_since:
                try:
                    dl = self._row_to_download(row)
                    candidate_downloads_to_prune.add(dl)
                except ValueError as e:
                    item_id = row["id"] if row and "id" in row else "unknown_id"
                    item_feed = row["feed"] if row and "feed" in row else "unknown_feed"
                    raise DataCoordinatorError(
                        f"Cannot convert prune_before_date row for {item_feed}/{item_id} to Download object",
                    ) from e

        if not candidate_downloads_to_prune:
            return [], []

        # 3. Process each candidate for deletion
        successfully_processed_ids_for_db_deletion: list[str] = []

        for download_to_prune in candidate_downloads_to_prune:
            if download_to_prune.status == DownloadStatus.DOWNLOADED:
                filename_to_delete = f"{download_to_prune.id}.{download_to_prune.ext}"
                try:
                    deleted_on_fs = self.file_manager.delete_download_file(
                        feed, filename_to_delete
                    )
                    if deleted_on_fs:
                        ids_of_files_deleted.append(download_to_prune.id)
                    else:
                        print(
                            f"Warning: File {filename_to_delete} for downloaded item {feed}/{download_to_prune.id} not found on disk during pruning. DB record will still be deleted."
                        )
                except OSError as e_fs:
                    raise FileOperationError(
                        f"OS error deleting file {filename_to_delete} for {feed}/{download_to_prune.id} during pruning. DB record NOT deleted.",
                    ) from e_fs

                successfully_processed_ids_for_db_deletion.append(download_to_prune.id)
            else:
                # Everything else is just a DB record deletion.
                successfully_processed_ids_for_db_deletion.append(download_to_prune.id)

        # 4. Update status to ARCHIVED for successfully processed items
        for id_to_archive in successfully_processed_ids_for_db_deletion:
            try:
                updated_in_db = self.db_manager.update_status(
                    feed, id_to_archive, DownloadStatus.ARCHIVED
                )
            except sqlite3.Error as e:
                # This is an error during the DB update itself.
                raise DatabaseOperationError(
                    f"Database error while updating status to ARCHIVED for {id_to_archive} during pruning for feed '{feed}'"
                ) from e
            if updated_in_db:
                ids_of_downloads_archived.append(id_to_archive)
            else:
                print(
                    f"Warning: Failed to archive {feed}/{id_to_archive} during pruning. DB record NOT updated."
                )

        return ids_of_downloads_archived, ids_of_files_deleted

    # Placeholder for other methods from the task list:
    # def download_queued(self, feed: str, limit: int) -> list[Download]:
    #    # Similar to above, convert rows to Download objects
    # def find_db_downloads_without_files(self, feed: str | None = None) -> list[Download]:
    # def find_files_without_db_downloads(self, feed: str | None = None) -> list[Path]:
