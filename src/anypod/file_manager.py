"""File system management for Anypod downloads.

This module provides the FileManager class for managing download files
on the filesystem, including file deletion, existence checks, and stream access
operations. Notably does not handle file creation, as that is done by yt-dlp.
"""

import logging
from pathlib import Path
from typing import IO

from .exceptions import FileOperationError

logger = logging.getLogger(__name__)


class FileManager:
    """Manage download files on the filesystem.

    This class provides an abstraction layer for file operations,
    handling the organization and management of downloaded media files
    in feed-specific subdirectories.

    Attributes:
        base_download_path: The root path where all download files are stored.
    """

    def __init__(self, base_download_path: Path):
        """Initializes the FileManager with the base directory for download storage.

        Args:
            base_download_path: The root path where all download files will be stored.
                               Feed-specific subdirectories will be created under this path.

        Raises:
            FileOperationError: If the base download directory cannot be created.
        """
        self.base_download_path = Path(base_download_path).resolve()
        logger.debug(
            "FileManager initialized.",
            extra={"base_download_path": str(self.base_download_path)},
        )
        # Ensure the base download directory exists upon instantiation.
        try:
            self.base_download_path.mkdir(parents=True, exist_ok=True)
            logger.debug(
                "Base download directory exists.",
                extra={"base_download_path": str(self.base_download_path)},
            )
        except OSError as e:
            raise FileOperationError(
                "Failed to create base download directory.",
                file_name=str(self.base_download_path),
            ) from e

    def delete_download_file(self, feed: str, file_name: str) -> None:
        """Deletes a download file from the filesystem.

        Args:
            feed: The name of the feed.
            file_name: The name of the download file to be deleted.

        Raises:
            FileNotFoundError: If the file does not exist or is not a regular file.
            FileOperationError: If an OS-level error occurs during file deletion (e.g., PermissionError).
        """
        file_path = self.base_download_path / feed / file_name
        log_params = {
            "feed_id": feed,
            "file_name": file_name,
            "file_path": str(file_path),
        }
        logger.debug("Attempting to delete download file.", extra=log_params)

        try:
            if file_path.is_file():
                file_path.unlink()
                logger.debug("File unlinked successfully.", extra=log_params)
            else:
                raise FileNotFoundError(f"Download file not found: {file_path}")
        except OSError as e:
            raise FileOperationError(
                "Failed to delete download file.",
                file_name=file_name,
            ) from e

    def download_exists(self, feed: str, file_name: str) -> bool:
        """Checks if a specific download file exists.

        Args:
            feed: The name of the feed (subdirectory).
            file_name: The name of the download file.

        Returns:
            True if the file exists and is a file, False otherwise.

        Raises:
            FileOperationError: If an OS-level error occurs during the file existence check (e.g., PermissionError on a parent directory).
        """
        file_path = self.base_download_path / feed / file_name
        log_params = {
            "feed_id": feed,
            "file_name": file_name,
            "file_path": str(file_path),
        }
        logger.debug("Checking if download file exists.", extra=log_params)

        try:
            exists = file_path.is_file()
            return exists
        except OSError as e:
            raise FileOperationError(
                "Failed to check if download file exists.",
                file_name=str(file_path),
            ) from e

    def get_download_stream(self, feed: str, file_name: str) -> IO[bytes]:
        """Opens and returns a binary read stream for a download file.

        Args:
            feed: The name of the feed (subdirectory).
            file_name: The name of the download file.

        Returns:
            An IO[bytes] stream for the download file.

        Raises:
            FileNotFoundError: If the file does not exist or is not a regular file.
            FileOperationError: If an OS-level error occurs while trying to open the file (e.g., PermissionError).
        """
        file_path = self.base_download_path / feed / file_name
        log_params = {
            "feed_id": feed,
            "file_name": file_name,
            "file_path": str(file_path),
        }
        logger.debug("Attempting to get download stream.", extra=log_params)

        if not self.download_exists(feed, file_name):
            logger.debug(
                "File not found, cannot get stream.",
                extra=log_params,
            )
            raise FileNotFoundError(
                f"Download file not found or is not a file: {file_path}"
            )
        logger.debug("File confirmed, opening for binary read.", extra=log_params)
        return file_path.open("rb")
