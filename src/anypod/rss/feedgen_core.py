"""Core wrapper around feedgen library for type-safe RSS feed generation.

This module provides a thin wrapper around the feedgen library to handle
RSS feed generation with podcast extensions. It encapsulates the type-unsafe
operations of feedgen and provides a clean interface for creating RSS feeds
from Anypod download data.
"""

from feedgen.feed import FeedGenerator  # type: ignore

from anypod.config import FeedConfig
from anypod.db import Download
from anypod.exceptions import RSSGenerationError
from anypod.path_manager import PathManager


class FeedgenCore:
    """Type-safe wrapper for feedgen library with podcast support.

    Provides a clean interface for creating RSS feeds with podcast extensions
    from Anypod download data. Handles all feedgen-specific operations and
    type conversions internally.

    Args:
        host: Base URL for the feed and download links.
        feed_id: Unique identifier for the feed.
        feed_config: Configuration containing metadata and settings.

    Attributes:
        _fg: Internal FeedGenerator instance.
        _paths: PathManager instance for resolving URLs and paths.
        _feed_config: Feed configuration reference.
    """

    def __init__(self, paths: PathManager, feed_id: str, feed_config: FeedConfig):
        if feed_config.metadata is None:
            raise ValueError("Feed metadata is required when creating an RSS feed.")

        fg = FeedGenerator()  # type: ignore
        fg.load_extension("podcast")  # type: ignore

        try:
            feed_self_url = paths.feed_url(feed_id)
        except ValueError as e:
            raise RSSGenerationError(
                "Invalid feed identifier for RSS URL.",
                feed_id=feed_id,
            ) from e

        fg.title(feed_config.metadata.title)  # type: ignore
        fg.link(href=feed_self_url, rel="self")  # type: ignore
        fg.link(href=feed_config.url, rel="alternate")  # type: ignore
        fg.description(feed_config.metadata.description)  # type: ignore
        fg.podcast.itunes_summary(feed_config.metadata.description)  # type: ignore
        fg.language(feed_config.metadata.language)  # type: ignore
        fg.podcast.itunes_category(  # type: ignore
            [cat.asdict() for cat in feed_config.metadata.category]
        )
        fg.podcast.itunes_explicit(  # type: ignore
            str(feed_config.metadata.explicit)
            if feed_config.metadata.explicit
            else None
        )
        fg.podcast.itunes_image(feed_config.metadata.image_url)  # type: ignore
        fg.podcast.itunes_author(feed_config.metadata.author)  # type: ignore
        # always prevent this feed from appearing in the podcast directory
        fg.podcast.itunes_block("yes")  # type: ignore
        fg.lastBuildDate(None)  # type: ignore
        fg.generator(  # type: ignore
            "AnyPod: https://github.com/thurstonsan/anypod"
        )
        # for now, hardcode to 60 minutes
        fg.ttl(60)  # type: ignore

        self._fg = fg  # type: ignore
        self._paths = paths
        self._feed_config = feed_config
        self._feed_metadata = feed_config.metadata

    # TODO: incorporate `updated` and `transcript`
    def with_downloads(self, downloads: list[Download]) -> "FeedgenCore":
        """Add download entries to the feed.

        Args:
            downloads: List of Download objects sorted by published date, descending.

        Returns:
            Self for method chaining.
        """
        for download in downloads:
            fe = self._fg.add_entry(order="append")  # type: ignore

            fe.guid(  # type: ignore
                download.source_url, permalink=True
            )
            fe.title(download.title)  # type: ignore
            fe.podcast.itunes_title(download.title)  # type: ignore

            # Use description from download if available
            description = download.description or download.title
            fe.description(description)  # type: ignore
            fe.podcast.itunes_summary(description)  # type: ignore

            if download.thumbnail:
                fe.podcast.itunes_image(download.thumbnail)  # type: ignore

            try:
                media_url = self._paths.media_file_url(
                    download.feed, download.id, download.ext
                )
            except ValueError as e:
                raise RSSGenerationError(
                    "Invalid feed or download identifier for media URL.",
                    feed_id=download.feed,
                ) from e

            fe.enclosure(  # type: ignore
                url=media_url,
                length=download.filesize or 0,
                type=download.mime_type,
            )
            fe.link(href=download.source_url, rel="alternate")  # type: ignore
            fe.published(download.published)  # type: ignore
            fe.source(  # type: ignore
                url=self._feed_config.url,
                title=self._feed_metadata.title,
            )
            fe.podcast.itunes_duration(download.duration)  # type: ignore
            # always prevent this entry from appearing in the podcast directory
            fe.podcast.itunes_block("yes")  # type: ignore

        return self

    def xml(self) -> bytes:
        """Generate RSS XML output.

        Returns:
            RSS feed as XML bytes in UTF-8 encoding.
        """
        return self._fg.rss_str(pretty=True)  # type: ignore
