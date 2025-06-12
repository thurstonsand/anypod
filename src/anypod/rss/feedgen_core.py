"""Core wrapper around feedgen library for type-safe RSS feed generation.

This module provides a thin wrapper around the feedgen library to handle
RSS feed generation with podcast extensions. It encapsulates the type-unsafe
operations of feedgen and provides a clean interface for creating RSS feeds
from Anypod download data.
"""

import logging

from feedgen.feed import FeedGenerator  # type: ignore

from ..db.types import Download, Feed
from ..exceptions import RSSGenerationError
from ..path_manager import PathManager

logger = logging.getLogger(__name__)


class FeedgenCore:
    """Type-safe wrapper for feedgen library with podcast support.

    Provides a clean interface for creating RSS feeds with podcast extensions
    from Anypod download data. Handles all feedgen-specific operations and
    type conversions internally.

    Args:
        paths: PathManager instance for resolving URLs and paths.
        feed_id: Unique identifier for the feed.
        feed: Feed database object containing metadata and settings.

    Attributes:
        _fg: Internal FeedGenerator instance.
        _paths: PathManager instance for resolving URLs and paths.
        _feed: Feed database object reference.
    """

    def __init__(self, paths: PathManager, feed_id: str, feed: Feed):
        # Check if required metadata is available
        if not feed.title:
            raise ValueError("Feed title is required when creating an RSS feed.")
        if not feed.description:
            raise ValueError("Feed description is required when creating an RSS feed.")

        fg = FeedGenerator()  # type: ignore
        fg.load_extension("podcast")  # type: ignore

        try:
            feed_self_url = paths.feed_url(feed_id)
        except ValueError as e:
            raise RSSGenerationError(
                "Invalid feed identifier for RSS URL.",
                feed_id=feed_id,
            ) from e

        fg.title(feed.title)  # type: ignore
        fg.link(href=feed_self_url, rel="self")  # type: ignore
        fg.link(href=feed.source_url, rel="alternate")  # type: ignore
        fg.description(feed.description)  # type: ignore
        fg.podcast.itunes_summary(feed.description)  # type: ignore
        fg.language(feed.language or "en")  # type: ignore

        # Handle optional fields with null checks
        if feed.category:
            fg.podcast.itunes_category(  # type: ignore
                feed.category.as_dict_list()
            )
        if feed.explicit:
            fg.podcast.itunes_explicit(str(feed.explicit))  # type: ignore
        if feed.image_url:
            try:
                fg.podcast.itunes_image(feed.image_url)  # type: ignore
            except ValueError as e:
                logger.warning(
                    f"Invalid feed image URL format: {e}",
                    extra={
                        "feed_id": feed_id,
                    },
                )
        if feed.author:
            fg.podcast.itunes_author(feed.author)  # type: ignore

        # always prevent this feed from appearing in the podcast directory
        fg.podcast.itunes_block("yes")  # type: ignore
        fg.lastBuildDate(None)  # type: ignore # None == now()
        fg.generator(  # type: ignore
            "AnyPod: https://github.com/thurstonsan/anypod"
        )
        # for now, hardcode to 60 minutes
        fg.ttl(60)  # type: ignore

        self._fg = fg  # type: ignore
        self._paths = paths
        self._feed = feed

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
                try:
                    fe.podcast.itunes_image(download.thumbnail)  # type: ignore
                except ValueError:
                    # Skip invalid thumbnail URLs rather than failing entire feed generation
                    # Log warning but continue processing
                    logger.warning(
                        "Skipping invalid thumbnail URL for download.",
                        extra={
                            "feed_id": download.feed,
                            "download_id": download.id,
                            "thumbnail_url": download.thumbnail,
                        },
                    )

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
                url=self._feed.source_url,
                title=self._feed.title,
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
