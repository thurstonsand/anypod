"""Source-specific handler selection for yt-dlp operations.

This module provides the `HandlerSelector` class that routes yt-dlp operations
to specialized handlers based on URL hostnames. Each handler encapsulates
source-specific logic for metadata extraction and download configuration.
"""

from urllib.parse import urlparse

from ...exceptions import YtdlpError
from ...ffprobe import FFProbe
from .base_handler import SourceHandlerBase
from .patreon_handler import PatreonHandler
from .youtube_handler import YoutubeHandler


class HandlerSelector:
    """Resolve source handlers based on URL hostnames."""

    def __init__(self, ffprobe: FFProbe):
        self._default_handler = YoutubeHandler()
        self._hostname_handlers = {
            "patreon.com": PatreonHandler(ffprobe),
        }

    def select(self, url: str) -> SourceHandlerBase:
        """Return the registered handler for `url`.

        Falls back to the default handler when no hostname-specific handler matches.
        """
        try:
            hostname = urlparse(url).hostname
        except ValueError as e:
            raise YtdlpError(f"Invalid url found: {url}") from e

        if not hostname:
            raise YtdlpError(f"URL has no hostname: {url}")

        hostname = hostname.lower()
        for suffix, handler in self._hostname_handlers.items():
            if hostname == suffix or hostname.endswith(f".{suffix}"):
                return handler

        return self._default_handler
