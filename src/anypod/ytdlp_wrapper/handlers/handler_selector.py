"""Source-specific handler selection for yt-dlp operations.

This module provides the `HandlerSelector` class that routes yt-dlp operations
to specialized handlers based on URL hostnames, falling back to
`GenericHandler` for arbitrary yt-dlp-supported sites.
"""

from urllib.parse import urlparse

from ...exceptions import YtdlpError
from ...ffprobe import FFProbe
from .base_handler import SourceHandlerBase
from .generic_handler import GenericHandler
from .patreon_handler import PatreonHandler
from .twitter_handler import TwitterHandler
from .youtube_handler import YoutubeHandler


class HandlerSelector:
    """Resolve source handlers based on URL hostnames."""

    def __init__(self, ffprobe: FFProbe):
        self._default_handler: SourceHandlerBase = GenericHandler()
        youtube_handler = YoutubeHandler()
        twitter_handler = TwitterHandler()
        self._hostname_handlers: dict[str, SourceHandlerBase] = {
            "youtube.com": youtube_handler,
            "youtu.be": youtube_handler,
            "youtube-nocookie.com": youtube_handler,
            "patreon.com": PatreonHandler(ffprobe),
            "x.com": twitter_handler,
            "twitter.com": twitter_handler,
        }

    def select(self, url: str) -> SourceHandlerBase:
        """Return the registered handler for `url`.

        Falls back to `GenericHandler` when no hostname-specific handler matches.
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
