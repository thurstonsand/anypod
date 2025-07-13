"""File system management for Anypod downloads.

This module provides the FileManager class for managing download files
on the filesystem, including file deletion, existence checks, and stream access
operations. Notably does not handle file creation, as that is done by yt-dlp.
"""

from collections.abc import AsyncIterator
import logging
from pathlib import Path

import aiofiles
import aiofiles.os

from .exceptions import FileOperationError
from .path_manager import PathManager

logger = logging.getLogger(__name__)


class FileManager:
    """Manage download files on the filesystem.

    This class provides an abstraction layer for file operations,
    handling the organization and management of downloaded media files
    in feed-specific subdirectories.

    Attributes:
        _paths: PathManager instance for coordinating file paths and URLs.
    """

    def __init__(self, paths: PathManager):
        self._paths = paths
        logger.debug(
            "FileManager initialized.",
            extra={"base_download_path": str(self._paths.base_data_dir)},
        )

    async def delete_download_file(self, feed: str, download_id: str, ext: str) -> None:
        """Deletes a download file from the filesystem.

        Args:
            feed: The name of the feed.
            download_id: The unique identifier for the download.
            ext: File extension without the leading dot.

        Raises:
            FileNotFoundError: If the file does not exist or is not a regular file.
            FileOperationError: If an OS-level error occurs during file deletion, or if feed/download identifiers are invalid.
        """
        try:
            file_path = await self._paths.media_file_path(feed, download_id, ext)
        except ValueError as e:
            raise FileOperationError(
                "Invalid feed or download identifier.",
                feed_id=feed,
                download_id=download_id,
            ) from e
        log_params = {
            "feed_id": feed,
            "file_path": str(file_path),
        }
        logger.debug("Attempting to delete download file.", extra=log_params)

        if not await aiofiles.os.path.isfile(file_path):
            raise FileNotFoundError(f"Download file not found: {file_path}")
        else:
            try:
                await aiofiles.os.remove(file_path)
                logger.debug("File unlinked successfully.", extra=log_params)
            except OSError as e:
                raise FileOperationError(
                    "Failed to delete download file.",
                    file_name=f"{download_id}.{ext}",
                ) from e

    async def get_download_file_path(
        self, feed: str, download_id: str, ext: str
    ) -> Path:
        """Get the file path for a download file.

        Args:
            feed: The name of the feed (subdirectory).
            download_id: The unique identifier for the download.
            ext: File extension without the leading dot.

        Returns:
            Path to the download file.

        Raises:
            FileNotFoundError: If the file does not exist or is not a regular file.
            FileOperationError: If an OS-level error occurs or if feed/download identifiers are invalid.
        """
        try:
            file_path = await self._paths.media_file_path(feed, download_id, ext)
        except ValueError as e:
            raise FileOperationError(
                "Invalid feed or download identifier.",
                feed_id=feed,
                download_id=download_id,
            ) from e

        log_params = {
            "feed_id": feed,
            "file_path": str(file_path),
        }
        logger.debug("Getting download file path.", extra=log_params)

        try:
            exists = await aiofiles.os.path.isfile(file_path)
        except OSError as e:
            raise FileOperationError(
                "Failed to check if download file exists.",
                file_name=str(file_path),
            ) from e

        if not exists:
            raise FileNotFoundError(
                f"Download file not found or is not a file: {file_path}"
            )
        return file_path

    async def download_exists(self, feed: str, download_id: str, ext: str) -> bool:
        """Checks if a specific download file exists.

        Args:
            feed: The name of the feed (subdirectory).
            download_id: The unique identifier for the download.
            ext: File extension without the leading dot.

        Returns:
            True if the file exists and is a file, False otherwise.

        Raises:
            FileOperationError: If an OS-level error occurs during the file existence check, or if feed/download identifiers are invalid.
        """
        try:
            await self.get_download_file_path(feed, download_id, ext)
            return True
        except FileNotFoundError:
            return False

    async def get_download_stream(
        self, feed: str, download_id: str, ext: str
    ) -> AsyncIterator[bytes]:
        """Opens and returns a binary read stream for a download file.

        Args:
            feed: The name of the feed (subdirectory).
            download_id: The unique identifier for the download.
            ext: File extension without the leading dot.

        Returns:
            An async iterator yielding bytes from the download file.

        Raises:
            FileNotFoundError: If the file does not exist or is not a regular file.
            FileOperationError: If an OS-level error occurs while trying to open the file, or if feed/download identifiers are invalid.
        """
        file_path = await self.get_download_file_path(feed, download_id, ext)
        logger.debug(
            "Attempting to get download stream.",
            extra={"feed_id": feed, "file_path": str(file_path)},
        )
        return self._stream_file_chunks(file_path, feed, download_id, ext)

    async def _stream_file_chunks(
        self, file_path: Path, feed: str, download_id: str, ext: str
    ) -> AsyncIterator[bytes]:
        """Internal method to stream file chunks.

        This is separated so that validation happens before the iterator is returned.
        """
        try:
            async with aiofiles.open(file_path, mode="rb") as file:
                async for chunk in file:
                    yield chunk
        except OSError as e:
            raise FileOperationError(
                "Failed to open download file for reading.",
                feed_id=feed,
                download_id=download_id,
                file_name=f"{download_id}.{ext}",
            ) from e
