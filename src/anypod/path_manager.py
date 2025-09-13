"""Helpers for resolving file system paths and URLs."""

import logging
from pathlib import Path
from urllib.parse import urljoin
import uuid

import aiofiles.os

from .exceptions import FileOperationError

logger = logging.getLogger(__name__)


class PathManager:
    """Centralized management of file system paths and URLs for all feeds.

    Provides a single source of truth for both file system location and URL
    construction for feeds and their media files. Ensures consistent 1:1 mapping
    between network paths and file paths based on feed_id and download_id.

    Automatically manages media and tmp subdirectories within the base data directory.

    Attributes:
        _base_data_dir: Root directory for all application data.
        _base_url: Base URL for constructing feed and media URLs.
    """

    def __init__(self, base_data_dir: Path, base_url: str):
        self._base_data_dir = Path(base_data_dir).resolve()
        self._base_url = base_url.rstrip("/")

    @property
    def base_data_dir(self) -> Path:
        """Return the directory used for permanent downloads."""
        return self._base_data_dir / "media"

    @property
    def base_tmp_dir(self) -> Path:
        """Return the directory used for temporary downloads."""
        return self._base_data_dir / "tmp"

    @property
    def base_images_dir(self) -> Path:
        """Return the directory used for image files."""
        return self._base_data_dir / "images"

    @property
    def base_feeds_dir(self) -> Path:
        """Return the directory used for persisted RSS XML files."""
        return self._base_data_dir / "feeds"

    @property
    def base_url(self) -> str:
        """Return the base URL for feed and media links."""
        return self._base_url

    async def db_dir(self) -> Path:
        """Return the directory containing the database file.

        Creates the directory if it doesn't exist.

        Returns:
            Path to the database directory.

        Raises:
            FileOperationError: If the directory cannot be created.
        """
        path = self._base_data_dir / "db"
        try:
            await aiofiles.os.makedirs(path, exist_ok=True)
        except OSError as e:
            raise FileOperationError(
                "Failed to create database directory.",
                file_name=str(path),
            ) from e
        return path

    async def feed_data_dir(self, feed_id: str) -> Path:
        """Return the directory for a feed's downloaded files.

        Creates the directory if it doesn't exist.

        Args:
            feed_id: Unique identifier for the feed.

        Returns:
            Path to the feed's data directory.

        Raises:
            ValueError: If feed_id is empty or whitespace-only.
            FileOperationError: If the directory cannot be created.
        """
        if not feed_id or not feed_id.strip():
            raise ValueError("feed_id cannot be empty or whitespace-only")

        path = self.base_data_dir / feed_id
        try:
            await aiofiles.os.makedirs(path, exist_ok=True)
        except OSError as e:
            raise FileOperationError(
                "Failed to create feed data directory.",
                file_name=str(path),
            ) from e
        return path

    async def feed_tmp_dir(self, feed_id: str) -> Path:
        """Return the temporary directory for a feed.

        Creates the directory if it doesn't exist.

        Args:
            feed_id: Unique identifier for the feed.

        Returns:
            Path to the feed's temporary directory.

        Raises:
            ValueError: If feed_id is empty or whitespace-only.
            FileOperationError: If the directory cannot be created.
        """
        if not feed_id or not feed_id.strip():
            raise ValueError("feed_id cannot be empty or whitespace-only")

        path = self.base_tmp_dir / feed_id
        try:
            await aiofiles.os.makedirs(path, exist_ok=True)
        except OSError as e:
            raise FileOperationError(
                "Failed to create feed temporary directory.",
                file_name=str(path),
            ) from e
        return path

    async def feed_images_dir(self, feed_id: str) -> Path:
        """Return the directory for a feed's image files.

        Creates the directory if it doesn't exist.

        Args:
            feed_id: Unique identifier for the feed.

        Returns:
            Path to the feed's images directory.

        Raises:
            ValueError: If feed_id is empty or whitespace-only.
            FileOperationError: If the directory cannot be created.
        """
        if not feed_id or not feed_id.strip():
            raise ValueError("feed_id cannot be empty or whitespace-only")

        path = self.base_images_dir / feed_id
        try:
            await aiofiles.os.makedirs(path, exist_ok=True)
        except OSError as e:
            raise FileOperationError(
                "Failed to create feed images directory.",
                file_name=str(path),
            ) from e
        return path

    async def download_images_dir(self, feed_id: str) -> Path:
        """Return the directory for a feed's per-download image files.

        Creates the directory if it doesn't exist. This directory contains
        images associated with individual downloads for the given feed.

        Args:
            feed_id: Unique identifier for the feed.

        Returns:
            Path to the feed's downloads images directory.

        Raises:
            ValueError: If feed_id is empty or whitespace-only.
            FileOperationError: If the directory cannot be created.
        """
        # feed_images_dir performs validation and creation of the base feed image dir
        feed_dir = await self.feed_images_dir(feed_id)
        downloads_dir = feed_dir / "downloads"
        try:
            await aiofiles.os.makedirs(downloads_dir, exist_ok=True)
        except OSError as e:
            raise FileOperationError(
                "Failed to create downloads images directory.",
                file_name=str(downloads_dir),
            ) from e
        return downloads_dir

    async def tmp_file(self, feed_id: str) -> Path:
        """Return a temporary file path for a feed.

        Args:
            feed_id: Unique identifier for the feed.

        Returns:
            Path to a temporary file within the feed's tmp directory.

        Raises:
            ValueError: If feed_id is empty or whitespace-only.
            FileOperationError: If the directory cannot be created.
        """
        tmp_dir = await self.feed_tmp_dir(feed_id)
        return tmp_dir / f"tmp_{uuid.uuid4().hex}"

    def feed_url(self, feed_id: str) -> str:
        """Return the full URL for a feed's RSS XML.

        Args:
            feed_id: Unique identifier for the feed.

        Returns:
            Complete URL to the RSS feed XML file.

        Raises:
            ValueError: If feed_id is empty or whitespace-only.
        """
        if not feed_id or not feed_id.strip():
            raise ValueError("feed_id cannot be empty or whitespace-only")

        return urljoin(self._base_url, f"/feeds/{feed_id}.xml")

    def feed_media_url(self, feed_id: str) -> str:
        """Return the base URL for a feed's media files.

        Args:
            feed_id: Unique identifier for the feed.

        Returns:
            Base URL for accessing the feed's media files.

        Raises:
            ValueError: If feed_id is empty or whitespace-only.
        """
        if not feed_id or not feed_id.strip():
            raise ValueError("feed_id cannot be empty or whitespace-only")

        return urljoin(self._base_url, f"/media/{feed_id}/")

    async def feed_xml_path(self, feed_id: str) -> Path:
        """Return the full file system path for a feed's RSS XML file.

        Creates the directory if it doesn't exist.

        Args:
            feed_id: Unique identifier for the feed.

        Returns:
            Complete path to the RSS XML file on disk.

        Raises:
            ValueError: If feed_id is empty or whitespace-only.
            FileOperationError: If the directory cannot be created.
        """
        if not feed_id or not feed_id.strip():
            raise ValueError("feed_id cannot be empty or whitespace-only")

        feeds_dir = self.base_feeds_dir
        try:
            await aiofiles.os.makedirs(feeds_dir, exist_ok=True)
        except OSError as e:
            raise FileOperationError(
                "Failed to create feeds directory.",
                file_name=str(feeds_dir),
            ) from e

        return feeds_dir / f"{feed_id}.xml"

    async def media_file_path(self, feed_id: str, download_id: str, ext: str) -> Path:
        """Return the full file system path for a specific downloaded media file.

        Args:
            feed_id: Unique identifier for the feed.
            download_id: Unique identifier for the download.
            ext: File extension without the leading dot.

        Returns:
            Complete path to the media file on disk.

        Raises:
            ValueError: If feed_id or download_id is empty or whitespace-only.
        """
        if not download_id or not download_id.strip():
            raise ValueError("download_id cannot be empty or whitespace-only")

        feed_dir = await self.feed_data_dir(feed_id)
        return feed_dir / f"{download_id}.{ext}"

    def media_file_url(self, feed_id: str, download_id: str, ext: str) -> str:
        """Return the HTTP URL for a specific downloaded media file.

        Args:
            feed_id: Unique identifier for the feed.
            download_id: Unique identifier for the download.
            ext: File extension without the leading dot.

        Returns:
            Complete URL for accessing the media file via HTTP.

        Raises:
            ValueError: If feed_id or download_id is empty or whitespace-only.
        """
        if not download_id or not download_id.strip():
            raise ValueError("download_id cannot be empty or whitespace-only")

        return urljoin(self.feed_media_url(feed_id), f"{download_id}.{ext}")

    def image_url(self, feed_id: str, download_id: str | None, ext: str) -> str:
        """Return the HTTP URL for an image file.

        Args:
            feed_id: Unique identifier for the feed.
            download_id: Unique identifier for the download, or None for feed-level images.
            ext: File extension without the leading dot.

        Returns:
            Complete URL for accessing the image file via HTTP.

        Raises:
            ValueError: If feed_id is empty or whitespace-only, or if download_id is empty or whitespace-only.
        """
        if not feed_id or not feed_id.strip():
            raise ValueError("feed_id cannot be empty or whitespace-only")

        if download_id is not None and (not download_id or not download_id.strip()):
            raise ValueError("download_id cannot be empty or whitespace-only")

        if download_id is None:
            # Feed-level image
            return urljoin(self._base_url, f"/images/{feed_id}.{ext}")
        else:
            # Download-level image
            return urljoin(self._base_url, f"/images/{feed_id}/{download_id}.{ext}")

    async def image_path(self, feed_id: str, download_id: str | None, ext: str) -> Path:
        """Return the full file system path for an image file.

        Args:
            feed_id: Unique identifier for the feed.
            download_id: Unique identifier for the download, or None for feed-level images.
            ext: File extension without the leading dot.

        Returns:
            Complete path to the image file on disk.

        Raises:
            ValueError: If feed_id is empty or whitespace-only, or if download_id is empty or whitespace-only.
            FileOperationError: If the directory cannot be created.
        """
        if not feed_id or not feed_id.strip():
            raise ValueError("feed_id cannot be empty or whitespace-only")

        if download_id is not None and (not download_id or not download_id.strip()):
            raise ValueError("download_id cannot be empty or whitespace-only")

        if download_id is None:
            return (await self.feed_images_dir(feed_id)) / f"{feed_id}.{ext}"
        else:
            return (await self.download_images_dir(feed_id)) / f"{download_id}.{ext}"
