"""Helpers for resolving file system paths and URLs."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urljoin


class PathManager:
    """Coordinate download paths and URLs for feeds."""

    def __init__(self, base_data_dir: Path, base_tmp_dir: Path, base_url: str):
        """Initialize the manager with base directories and URL."""
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
        """Return the directory for a feed's downloaded files."""
        path = self._base_data_dir / feed_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def feed_tmp_dir(self, feed_id: str) -> Path:
        """Return the temporary directory for a feed."""
        path = self._base_tmp_dir / feed_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def feed_url(self, feed_id: str) -> str:
        """Return the full URL for a feed's RSS XML."""
        return urljoin(self._base_url, f"/feeds/{feed_id}.xml")

    def feed_media_url(self, feed_id: str) -> str:
        """Return the base URL for a feed's media files."""
        return urljoin(self._base_url, f"/media/{feed_id}/")

    def media_file_path(self, feed_id: str, download_id: str, ext: str) -> Path:
        """Return the full path for a specific downloaded file."""
        return self.feed_data_dir(feed_id) / f"{download_id}.{ext}"

    def media_file_url(self, feed_id: str, download_id: str, ext: str) -> str:
        """Return the URL for a specific downloaded file."""
        return urljoin(self._base_url, f"/media/{feed_id}/{download_id}.{ext}")
