import contextlib
from pathlib import Path
import shutil
from typing import IO


class FileManager:
    """
    Manages download files on the filesystem.
    This class provides an abstraction layer for file operations.
    """

    def __init__(self, base_download_path: Path):
        """
        Initializes the FileManager with the base directory for download storage.

        Args:
            base_download_path: The root path where all download files will be stored.
                               Feed-specific subdirectories will be created under this path.
        """
        self.base_download_path = Path(base_download_path).resolve()
        # Ensure the base download directory exists upon instantiation.
        self.base_download_path.mkdir(parents=True, exist_ok=True)

    def save_download_file(
        self, feed: str, filename: str, data_stream: IO[bytes]
    ) -> Path:
        """
        Saves a binary data stream to a file in a feed-specific directory,
        ensuring atomicity by using an '.incomplete' suffix during writing.

        The method first writes the data to a file named 'filename.incomplete'.
        If successful, it renames this file to the final 'filename'.
        This ensures that incomplete files are clearly marked and can be cleaned up.

        Args:
            feed: The name of the feed, used to create a subdirectory.
            filename: The desired name for the file (without the .incomplete suffix).
            data_stream: An open binary file-like object containing the data to save.

        Returns:
            The Path object pointing to the newly saved file.

        Raises:
            OSError: If file system operations fail (e.g., disk full,
                     PermissionError for file/dir operations).
            Exception: Other exceptions from shutil.copyfileobj or file operations.
        """
        feed_dir = self.base_download_path / feed
        feed_dir.mkdir(parents=True, exist_ok=True)

        final_path = feed_dir / filename
        incomplete_path = feed_dir / (filename + ".incomplete")

        try:
            with Path.open(incomplete_path, "wb") as tmp_file:
                shutil.copyfileobj(data_stream, tmp_file)

            # Atomically move the temporary file to its final destination.
            # This will overwrite an existing file if one exists at final_path.
            incomplete_path.replace(final_path)

        except Exception:
            if incomplete_path.exists():
                with contextlib.suppress(OSError):
                    incomplete_path.unlink()
            raise

        return final_path

    def delete_download_file(self, feed: str, filename: str) -> bool:
        """
        Deletes a download file from the filesystem.
        Constructs path from feed and filename relative to base_download_path.

        Args:
            feed: The name of the feed.
            filename: The name of the download file to be deleted.

        Returns:
            True if the file was successfully deleted, False if the file was not found.

        Raises:
            OSError: For OS-level errors during unlink.
        """
        file_path = self.base_download_path / feed / filename

        # Check if it's a file. This can also raise PermissionError if dir not accessible.
        if file_path.is_file():
            file_path.unlink()  # Let PermissionError or other OSErrors propagate
            return True
        return False  # File not found or was not a file

    def download_exists(self, feed: str, filename: str) -> bool:
        """
        Checks if a specific download file exists.

        Args:
            feed: The name of the feed (subdirectory).
            filename: The name of the download file.

        Returns:
            True if the file exists and is a file, False otherwise.
        """
        file_path = self.base_download_path / feed / filename
        return file_path.is_file()

    def get_download_stream(self, feed: str, filename: str) -> IO[bytes]:
        """
        Opens and returns a binary read stream for a download file.
        Raises FileNotFoundError if the file does not exist or is not a file.
        Propagates PermissionError if file opening fails due to permissions.

        Args:
            feed: The name of the feed (subdirectory).
            filename: The name of the download file.

        Returns:
            An IO[bytes] stream for the download file.

        Raises:
            OSError: For OS-level errors during file opening.
        """
        file_path = self.base_download_path / feed / filename
        if not file_path.is_file():
            raise FileNotFoundError(
                f"Download file not found or is not a file: {file_path}"
            )

        return file_path.open("rb")
