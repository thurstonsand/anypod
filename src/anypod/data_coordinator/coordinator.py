from datetime import datetime
import logging
from typing import IO

from anypod.config import FeedConfig

from ..db import DatabaseManager, Download, DownloadStatus

# Import specific exceptions needed by the migrated methods
from ..exceptions import (
    DatabaseOperationError,
    DataCoordinatorError,
    DownloadNotFoundError,
    FileOperationError,
)

# from ..feed_gen import FeedGen  # Assuming path, might need adjustment
from ..file_manager import FileManager
from .downloader import Downloader
from .enqueuer import Enqueuer
from .pruner import Pruner

logger = logging.getLogger(__name__)


class DataCoordinator:
    def __init__(
        self,
        db_manager: DatabaseManager,
        file_manager: FileManager,
        enqueuer: Enqueuer,
        downloader: Downloader,
        pruner: Pruner,
        # feed_gen: FeedGen,  # feed_gen type placeholder if not available
    ):
        self.db_manager = db_manager
        self.file_manager = file_manager
        self.enqueuer = enqueuer
        self.downloader = downloader
        self.pruner = pruner
        # self.feed_gen = feed_gen
        logger.debug("DataCoordinator (orchestrator) initialized.")

    def process_feed(
        self, feed_id: str, feed_config: FeedConfig, last_processed_date: datetime
    ) -> None:  # feed_config type placeholder
        """
        Orchestrates the processing of a single feed.
        """
        # Orchestration logic to be implemented fully later
        logger.info("Starting feed processing.", extra={"feed_id": feed_id})
        try:
            # 1. Enqueue new downloads
            logger.info("Enqueueing new downloads...", extra={"feed_id": feed_id})
            self.enqueuer.enqueue_new_downloads(
                feed_id, feed_config, last_processed_date
            )

            # 2. Download queued items
            logger.info("Downloading queued items...", extra={"feed_id": feed_id})
            # Assuming download_queued takes the feed config directly
            self.downloader.download_queued(
                feed_id, feed_config
            )  # Limit can be added if needed

            # 3. Prune old downloads

            if feed_config.keep_last is not None or feed_config.since is not None:
                logger.info("Pruning old downloads...", extra={"feed_id": feed_id})
                # Assuming prune_feed_downloads takes feed_id, keep_last, prune_before_date
                self.pruner.prune_feed_downloads(
                    feed_id, feed_config.keep_last, feed_config.since
                )
            else:
                logger.info(
                    "Skipping pruning (no policy defined).",
                    extra={"feed_id": feed_id},
                )

            # 4. Generate Feed XML
            logger.info("Generating feed XML...", extra={"feed_id": feed_id})
            # Assuming feed_gen has a method like this. Adjust based on actual FeedGen design.
            # await self.feed_gen.generate_feed_xml(feed_id)

            logger.info(
                "Feed processing completed successfully.", extra={"feed_id": feed_id}
            )

        except Exception as e:
            # Catching general Exception for now, specific errors should be handled by services
            logger.critical(
                "Unhandled error during feed processing pipeline.",
                extra={"feed_id": feed_id},
                exc_info=e,
            )
            # Optionally re-raise or handle specific orchestration failures

    # --- Migrated methods from old DataCoordinator ---

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
        """
        logger.debug(
            "Attempting to add download.",
            extra={"feed_id": download_to_add.feed, "download_id": download_to_add.id},
        )
        try:
            existing_db_download = self.db_manager.get_download_by_id(
                download_to_add.feed, download_to_add.id
            )
        except (DatabaseOperationError, ValueError) as e:
            raise DatabaseOperationError(
                message="Error checking for existing download.",
                feed_id=download_to_add.feed,
                download_id=download_to_add.id,
            ) from e

        if (
            existing_db_download
            and existing_db_download.status == DownloadStatus.DOWNLOADED
        ):
            logger.info(
                "Existing download found, preparing to replace.",
                extra={
                    "feed_id": download_to_add.feed,
                    "download_id": download_to_add.id,
                },
            )
            # Download exists, so we need to replace it.
            file_name_to_delete = f"{download_to_add.id}.{existing_db_download.ext}"
            try:
                deleted = self.file_manager.delete_download_file(
                    feed=download_to_add.feed, file_name=file_name_to_delete
                )
            except FileOperationError as e:
                raise FileOperationError(
                    message="Error deleting file.",
                    feed_id=download_to_add.feed,
                    download_id=download_to_add.id,
                    file_name=file_name_to_delete,
                ) from e
            if deleted:
                logger.debug(
                    "Successfully deleted existing file for replacement.",
                    extra={
                        "feed_id": download_to_add.feed,
                        "download_id": download_to_add.id,
                        "file_name": file_name_to_delete,
                    },
                )
            else:
                logger.warning(
                    "Expected file for existing download not found on disk for deletion.",
                    extra={
                        "feed_id": download_to_add.feed,
                        "download_id": download_to_add.id,
                        "file_name": file_name_to_delete,
                    },
                )

        try:
            self.db_manager.upsert_download(download_to_add)
            logger.info(
                "Download record upserted successfully.",
                extra={
                    "feed_id": download_to_add.feed,
                    "download_id": download_to_add.id,
                    "status": download_to_add.status,
                },
            )
        except DatabaseOperationError as e:
            raise DatabaseOperationError(
                message="Error adding or updating download record.",
                feed_id=download_to_add.feed,
                download_id=download_to_add.id,
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
        logger.debug(
            "Attempting to update download status.",
            extra={
                "feed_id": feed,
                "download_id": id,
                "new_status": status,
                "last_error": last_error,
            },
        )
        try:
            current_download_row = self.db_manager.get_download_by_id(feed, id)
        except (DatabaseOperationError, ValueError) as e:
            raise DatabaseOperationError(
                message=f"Error retrieving download for status update to {status}.",
                feed_id=feed,
                download_id=id,
            ) from e

        if not current_download_row:
            raise DownloadNotFoundError(
                message=f"Cannot update status to {status}: Download not found.",
                feed_id=feed,
                download_id=id,
            )

        # If status is changing FROM DOWNLOADED to something else, delete the file.
        if (
            current_download_row.status == DownloadStatus.DOWNLOADED
            and status != DownloadStatus.DOWNLOADED
        ):
            file_name_to_delete = f"{id}.{current_download_row.ext}"
            logger.info(
                "Status changing from DOWNLOADED, attempting to delete associated file.",
                extra={
                    "feed_id": feed,
                    "download_id": id,
                    "file_name": file_name_to_delete,
                    "new_status": status,
                },
            )
            try:
                deleted = self.file_manager.delete_download_file(
                    feed=feed, file_name=file_name_to_delete
                )
            except FileOperationError as e:
                raise FileOperationError(
                    message=f"Failed to delete file when changing status from {DownloadStatus.DOWNLOADED}.",
                    feed_id=feed,
                    download_id=id,
                    file_name=file_name_to_delete,
                ) from e
            if deleted:
                logger.debug(
                    "Successfully deleted file due to status change.",
                    extra={
                        "feed_id": feed,
                        "download_id": id,
                        "file_name": file_name_to_delete,
                    },
                )
            else:
                logger.warning(
                    "File not found on disk during status change from DOWNLOADED.",
                    extra={
                        "feed_id": feed,
                        "download_id": id,
                        "file_name": file_name_to_delete,
                    },
                )

        try:
            updated_in_db = self.db_manager.update_status(
                feed=feed, id=id, status=status, last_error=last_error
            )
            if updated_in_db:
                logger.info(
                    "Download status updated successfully in database.",
                    extra={
                        "feed_id": feed,
                        "download_id": id,
                        "new_status": status,
                        "previous_status": current_download_row.status,
                    },
                )
            else:
                logger.warning(
                    "Download status update in database reported no rows changed.",
                    extra={
                        "feed_id": feed,
                        "download_id": id,
                        "target_status": status,
                        "last_error": last_error,
                    },
                )
        except DatabaseOperationError as e:
            raise DatabaseOperationError(
                message=f"Error updating status to {status} for download.",
                feed_id=feed,
                download_id=id,
            ) from e

        return updated_in_db

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
        logger.debug(
            "Attempting to get download by ID.",
            extra={"feed_id": feed, "download_id": id},
        )
        try:
            download = self.db_manager.get_download_by_id(feed, id)
        except (DatabaseOperationError, ValueError) as e:
            raise DatabaseOperationError(
                message="Database lookup failed for download.",
                feed_id=feed,
                download_id=id,
            ) from e
        if download is None:
            logger.debug(
                "Download not found by ID.",
                extra={"feed_id": feed, "download_id": id},
            )
        return download

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
        log_params = {"feed_id": feed, "download_id": id}
        logger.debug(
            "Attempting to stream download by ID.",
            extra=log_params,
        )
        download = self.get_download_by_id(feed, id)

        if download is None:
            logger.info(
                "Stream requested for non-existent or unparsable download record.",
                extra=log_params,
            )
            return None
        if download.status != DownloadStatus.DOWNLOADED:
            logger.info(
                "Stream requested for download not in DOWNLOADED status.",
                extra={**log_params, "status": download.status},
            )
            return None

        file_name = f"{download.id}.{download.ext}"
        log_params["file_name"] = file_name

        try:
            # Return the stream directly if found
            return self.file_manager.get_download_stream(feed, file_name)
        except FileNotFoundError as e:
            error_msg = (
                f"File marked as {DownloadStatus.DOWNLOADED} but not found on disk"
            )
            logger.error(
                f"{error_msg}: {file_name}. Attempting to update status to ERROR.",
                extra=log_params,
                exc_info=True,  # Log stack trace for FileNotFoundError
            )
            try:
                self.update_status(
                    feed=feed,
                    id=id,
                    status=DownloadStatus.ERROR,
                    last_error=f"{error_msg}: {file_name}",
                )
            except (
                DatabaseOperationError,
                DownloadNotFoundError,
                FileOperationError,
            ) as e_update:
                logger.error(
                    "Failed to update status to ERROR after file not found during streaming attempt.",
                    extra={**log_params, "original_error": repr(e)},  # Use repr(e)
                    exc_info=e_update,
                )
            # Whether status update succeeded or failed, the file is missing.
            # Raise a specific error indicating the problem.
            raise DataCoordinatorError(
                f"{error_msg}: {file_name}. Status updated to ERROR if possible."
            ) from e
        except FileOperationError as e:
            # Catch other FileManager errors (permissions, etc.)
            logger.error(
                "File system error occurred while trying to get download stream.",
                extra=log_params,
                exc_info=e,
            )
            raise  # Re-raise the original FileOperationError

    def get_downloads_by_status(
        self,
        status_to_filter: DownloadStatus,
        feed: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Download]:
        """
        Retrieves downloads with a specific status.
        Malformed records from the database will be logged and skipped.

        Args:
            status_to_filter: The DownloadStatus to filter by.
            feed: Optional feed name to filter by.
            limit: Maximum number of records to return.
            offset: Number of records to skip (for pagination).

        Returns:
            A list of Download objects matching the status and other criteria.

        Raises:
            DatabaseOperationError: If the database query fails.
        """
        logger.debug(
            "Attempting to get downloads by status.",
            extra={
                "status_filter": str(status_to_filter),
                "limit": limit,
                "offset": offset,
                "feed_id": feed or "<all>",
            },
        )

        downloads: list[Download] = []
        try:
            downloads = self.db_manager.get_downloads_by_status(
                status_to_filter=status_to_filter,
                feed=feed,
                limit=limit,
                offset=offset,
            )
        except (DatabaseOperationError, ValueError) as e:
            raise DatabaseOperationError(
                message=f"Database query failed for downloads by status {status_to_filter}.",
                feed_id=feed,
                download_id=f"limit:{limit}_offset:{offset}",
            ) from e
        logger.debug(
            f"Retrieved {len(downloads)} downloads by status.",
            extra={
                "status_filter": str(status_to_filter),
                "limit": limit,
                "offset": offset,
                "feed_id": feed or "<all>",
            },
        )
        return downloads
