import logging
from pathlib import Path
import shutil
from typing import IO

from .exceptions import FileOperationError

logger = logging.getLogger(__name__)


class FileManager:
    """Manages download files on the filesystem.

    This class provides an abstraction layer for file operations.
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

    def save_download_file(
        self, feed: str, file_name: str, data_stream: IO[bytes]
    ) -> Path:
        """Saves a binary data stream to a file in a feed-specific directory, ensuring atomicity by using an '.incomplete' suffix during writing.

        The method first writes the data to a file named 'file_name.incomplete'.
        If successful, it renames this file to the final 'file_name'.
        This ensures that incomplete files are clearly marked and can be cleaned up.

        Args:
            feed: The name of the feed, used to create a subdirectory.
            file_name: The desired name for the file (without the .incomplete suffix).
            data_stream: An open binary file-like object containing the data to save.

        Returns:
            The Path object pointing to the newly saved file.

        Raises:
            FileOperationError: If any file system operations fail (e.g., creating directories,
                                writing to the file, renaming the file, or disk full).
        """
        log_params = {"feed_id": feed, "file_name": file_name}
        logger.debug("Attempting to save download file.", extra=log_params)

        feed_dir = self.base_download_path / feed
        feed_dir.mkdir(parents=True, exist_ok=True)

        final_path = feed_dir / file_name
        incomplete_path = feed_dir / (file_name + ".incomplete")
        log_params["file_path"] = str(final_path)
        log_params["incomplete_path"] = str(incomplete_path)

        logger.debug("Writing to incomplete file.", extra=log_params)
        try:
            with Path.open(incomplete_path, "wb") as tmp_file:
                shutil.copyfileobj(data_stream, tmp_file)
            logger.debug("Successfully wrote to incomplete file.", extra=log_params)

            incomplete_path.replace(final_path)
            logger.debug("Renamed incomplete file to final path.", extra=log_params)

        except OSError as e:
            logger.debug(
                "OSError occurred during save_download_file operation, attempting cleanup.",
                extra=log_params,
            )
            if incomplete_path.exists():
                logger.debug(
                    "Incomplete file exists, attempting to delete.", extra=log_params
                )
                try:
                    incomplete_path.unlink()
                    logger.debug(
                        "Successfully deleted incomplete file after error.",
                        extra=log_params,
                    )
                except OSError as e_unlink:
                    logger.warning(
                        "Failed to delete incomplete file after an error during save operation.",
                        extra=log_params,
                        exc_info=e_unlink,
                    )
            raise FileOperationError(
                "Failed to save download file.",
                file_name=str(final_path),
            ) from e

        return final_path

    def delete_download_file(self, feed: str, file_name: str) -> bool:
        """Deletes a download file from the filesystem.

        Args:
            feed: The name of the feed.
            file_name: The name of the download file to be deleted.

        Returns:
            True if the file was successfully deleted, False if the file was not found.

        Raises:
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
                return True
            else:
                logger.debug(
                    "File not found or is not a file, deletion skipped.",
                    extra=log_params,
                )
                return False
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

        try:
            file_exists = file_path.is_file()
        except OSError as e:
            raise FileOperationError(
                "Failed to get download stream.",
                file_name=str(file_path),
            ) from e
        if not file_exists:
            logger.debug(
                "File not found or is not a file, cannot get stream.",
                extra=log_params,
            )
            raise FileNotFoundError(
                f"Download file not found or is not a file: {file_path}"
            )
        logger.debug("File confirmed, opening for binary read.", extra=log_params)
        return file_path.open("rb")
