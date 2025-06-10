"""Helpers for resolving file system paths and URLs."""

from pathlib import Path
from urllib.parse import urljoin

from .exceptions import FileOperationError


class PathManager:
    """Centralized management of file system paths and URLs for all feeds.

    Provides a single source of truth for both file system location and URL
    construction for feeds and their media files. Ensures consistent 1:1 mapping
    between network paths and file paths based on feed_id and download_id.

    Attributes:
        _base_data_dir: Root directory for permanent download storage.
        _base_tmp_dir: Root directory for temporary download operations.
        _base_url: Base URL for constructing feed and media URLs.
    """

    def __init__(self, base_data_dir: Path, base_tmp_dir: Path, base_url: str):
        self._base_data_dir = Path(base_data_dir).resolve()
        self._base_tmp_dir = Path(base_tmp_dir).resolve()
        self._base_url = base_url.rstrip("/")

    @property
    def base_data_dir(self) -> Path:
        """Return the directory used for permanent downloads."""
        return self._base_data_dir

    @property
    def base_tmp_dir(self) -> Path:
        """Return the directory used for temporary downloads."""
        return self._base_tmp_dir

    @property
    def base_url(self) -> str:
        """Return the base URL for feed and media links."""
        return self._base_url

    def feed_data_dir(self, feed_id: str) -> Path:
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

        path = self._base_data_dir / feed_id
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise FileOperationError(
                "Failed to create feed data directory.",
                file_name=str(path),
            ) from e
        return path

    def feed_tmp_dir(self, feed_id: str) -> Path:
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

        path = self._base_tmp_dir / feed_id
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise FileOperationError(
                "Failed to create feed temporary directory.",
                file_name=str(path),
            ) from e
        return path

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

    def media_file_path(self, feed_id: str, download_id: str, ext: str) -> Path:
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

        return self.feed_data_dir(feed_id) / f"{download_id}.{ext}"

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
