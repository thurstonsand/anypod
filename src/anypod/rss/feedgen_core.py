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
from ..mimetypes import mimetypes
from ..path_manager import PathManager
from .podcast_extension import Podcast, PodcastEntryExtension

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
        if feed.description is None:
            raise ValueError("Feed description is required when creating an RSS feed.")

        fg = FeedGenerator()  # type: ignore

        # Register our custom Podcast extension to override the default one.
        # We must provide both the feed extension (Podcast) and the entry extension
        # (PodcastEntryExtension) so that entries created via add_entry() also get
        # the podcast extension loaded (e.g., fe.podcast.itunes_title()).
        fg.register_extension("podcast", Podcast, PodcastEntryExtension)  # type: ignore

        try:
            feed_self_url = paths.feed_url(feed_id)
        except ValueError as e:
            raise RSSGenerationError(
                "Invalid feed identifier for RSS URL.",
                feed_id=feed_id,
            ) from e
        self._source_url = feed.source_url or feed_self_url

        fg.title(feed.title)  # type: ignore
        if feed.subtitle:
            fg.podcast.itunes_subtitle(feed.subtitle)  # type: ignore
        fg.link(href=feed_self_url, rel="self")  # type: ignore
        fg.link(href=self._source_url, rel="alternate")  # type: ignore
        # Use a default description if "" to satisfy feedgen requirements
        description = feed.description or "No description available"
        fg.description(description)  # type: ignore
        fg.podcast.itunes_summary(description)  # type: ignore
        fg.description(description)  # type: ignore
        fg.language(feed.language or "en")  # type: ignore

        # Handle optional fields with null checks
        fg.category(feed.category.rss_list())  # type: ignore
        fg.podcast.itunes_category(  # type: ignore
            feed.category.itunes_rss_list()
        )
        fg.podcast.itunes_type(feed.podcast_type.rss_str())  # type: ignore

        # Explicit is a boolean, convert to "true"/"false" for RSS
        explicit_str = "true" if feed.explicit else "false"
        fg.podcast.itunes_explicit(explicit_str)  # type: ignore

        # Prefer hosted feed image; fall back to remote URL
        if feed.image_ext:
            try:
                hosted_feed_image_url = paths.image_url(feed_id, None, feed.image_ext)
            except ValueError as e:
                raise RSSGenerationError(
                    "Invalid feed identifier for image URL", feed_id=feed_id
                ) from e
            fg.podcast.itunes_image(hosted_feed_image_url)  # type: ignore
            fg.image(  # type: ignore
                url=hosted_feed_image_url,
                title=feed.title,
                link=self._source_url,
                description=f"Artwork for {feed.title}",
            )
        elif feed.remote_image_url:
            fg.podcast.itunes_image(feed.remote_image_url)  # type: ignore
            fg.image(  # type: ignore
                url=feed.remote_image_url,
                title=feed.title,
                link=self._source_url,
                description=f"Artwork for {feed.title}",
            )
        if feed.author:
            fg.podcast.itunes_author(feed.author)  # type: ignore
            fg.podcast.itunes_owner(  # type: ignore
                name=feed.author, email=feed.author_email
            )

        fg.lastBuildDate(None)  # type: ignore # None == now()
        fg.generator(  # type: ignore
            "AnyPod: https://github.com/thurstonsan/anypod"
        )
        # for now, hardcode to 60 minutes
        fg.ttl(60)  # type: ignore

        self._fg = fg  # type: ignore
        self._paths = paths
        self._feed = feed

    def with_downloads(self, downloads: list[Download]) -> FeedgenCore:
        """Add download entries to the feed.

        Args:
            downloads: List of Download objects sorted by published date, descending.

        Returns:
            Self for method chaining.
        """
        # Set feed publication date to the newest episode date
        if downloads:
            self._fg.pubDate(downloads[0].published)  # type: ignore

        for download in downloads:
            fe = self._fg.add_entry(order="append")  # type: ignore

            fe.guid(  # type: ignore
                download.source_url, permalink=True
            )
            fe.title(download.title)  # type: ignore
            fe.podcast.itunes_title(download.title)  # type: ignore

            # Use description from download if available
            description = download.description or download.title

            fe.podcast.itunes_summary(description)  # type: ignore
            fe.description(description)  # type: ignore

            # Prefer hosted per-episode thumbnail when thumbnail_ext is present; fall back to remote URL
            if download.thumbnail_ext:
                try:
                    thumbnail_url = self._paths.image_url(
                        download.feed_id, download.id, download.thumbnail_ext
                    )
                except ValueError as e:
                    raise RSSGenerationError(
                        "Invalid feed or download identifier for image URL",
                        feed_id=download.feed_id,
                        download_id=download.id,
                    ) from e
                fe.podcast.itunes_image(thumbnail_url)  # type: ignore
            elif download.remote_thumbnail_url:
                try:
                    fe.podcast.itunes_image(download.remote_thumbnail_url)  # type: ignore
                except ValueError:
                    logger.warning(
                        "Skipping invalid thumbnail URL for download.",
                        extra={
                            "feed_id": download.feed_id,
                            "download_id": download.id,
                            "thumbnail_url": download.remote_thumbnail_url,
                        },
                    )

            try:
                media_url = self._paths.media_file_url(
                    download.feed_id, download.id, download.ext
                )
            except ValueError as e:
                raise RSSGenerationError(
                    "Invalid feed or download identifier for media URL.",
                    feed_id=download.feed_id,
                ) from e

            fe.enclosure(  # type: ignore
                url=media_url,
                length=download.filesize or 0,
                type=download.mime_type,
            )
            fe.link(href=download.source_url, rel="alternate")  # type: ignore
            fe.published(download.published)  # type: ignore
            fe.source(  # type: ignore
                url=self._source_url,
                title=self._feed.title,
            )
            fe.podcast.itunes_duration(self._format_duration(download.duration))  # type: ignore
            # Always set episode type to full for now
            fe.podcast.itunes_episode_type("full")  # type: ignore

            # Add transcript if available
            if download.transcript_lang and download.transcript_ext:
                try:
                    transcript_url = self._paths.transcript_url(
                        download.feed_id,
                        download.id,
                        download.transcript_lang,
                        download.transcript_ext,
                    )
                except ValueError as e:
                    logger.warning(
                        "Skipping transcript with invalid URL.",
                        extra={
                            "feed_id": download.feed_id,
                            "download_id": download.id,
                        },
                        exc_info=e,
                    )
                else:
                    transcript_type = mimetypes.guess_type(
                        f"file.{download.transcript_ext}"
                    )[0]
                    fe.podcast.transcript(  # type: ignore
                        url=transcript_url,
                        type=transcript_type,
                        language=download.transcript_lang,
                        rel="captions",  # VTT files are timed captions
                    )

        return self

    def _format_duration(self, seconds: int) -> str:
        """Convert seconds to HH:MM:SS format for iTunes duration.

        Args:
            seconds: Duration in seconds.

        Returns:
            Duration in HH:MM:SS format.
        """
        if seconds < 0:
            seconds = 0  # Duration must be a positive value

        mins, sec = divmod(seconds, 60)
        hr, mins = divmod(mins, 60)
        return f"{int(hr):02d}:{int(mins):02d}:{int(sec):02d}"

    def xml(self) -> bytes:
        """Generate RSS XML output.

        Returns:
            RSS feed as XML bytes in UTF-8 encoding.
        """
        return self._fg.rss_str(pretty=True)  # type: ignore
