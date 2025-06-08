"""RSS feed generation and management for Anypod podcast feeds.

This module provides the RSSFeedGenerator class for creating and caching
RSS podcast feeds from download metadata, with thread-safe in-memory caching
and integration with the feedgen library.
"""

import logging

from readerwriterlock import rwlock

from anypod.rss.feedgen_core import FeedgenCore

from ..config import AppSettings, FeedConfig
from ..db import DatabaseManager, Download, DownloadStatus
from ..exceptions import DatabaseOperationError, RSSGenerationError

logger = logging.getLogger(__name__)


class RSSFeedGenerator:
    """Generate and cache RSS podcast feeds from download metadata.

    Manages RSS feed generation using feedgen with podcast extensions,
    providing thread-safe in-memory caching with read/write locks for
    concurrent access by HTTP servers and periodic updates.

    Attributes:
        _db_manager: Database manager for querying download data.
        _app_settings: Application settings for configuration.
        _feed_cache: In-memory cache mapping feed_id to XML bytes.
        _cache_lock: Read/write lock for thread-safe cache access.
    """

    def __init__(
        self,
        db_manager: DatabaseManager,
        app_settings: AppSettings,
    ):
        self._db_manager = db_manager
        self._app_settings = app_settings
        self._feed_cache: dict[str, bytes] = {}
        self._cache_lock = rwlock.RWLockWrite()
        logger.debug("RSSFeedGenerator initialized.")

    def _get_feed_downloads(self, feed_id: str) -> list[Download]:
        """Get downloads for feed generation.

        Args:
            feed_id: The feed identifier.
            feed_config: Configuration for the feed.

        Returns:
            List of Download objects for the feed.

        Raises:
            RSSGenerationError: If database query fails.
        """
        try:
            # Sorted by newest first
            downloads = self._db_manager.get_downloads_by_status(
                status_to_filter=DownloadStatus.DOWNLOADED,
                feed=feed_id,
            )
        except DatabaseOperationError as e:
            raise RSSGenerationError(
                "Failed to retrieve downloads for feed.",
                feed_id=feed_id,
            ) from e
        else:
            logger.debug(
                "Retrieved downloads for feed generation.",
                extra={
                    "feed_id": feed_id,
                    "total_downloads": len(downloads),
                },
            )

            return downloads

    def _cache_feed_xml(self, feed_id: str, xml_bytes: bytes) -> None:
        """Cache the generated XML with write lock.

        Args:
            feed_id: The feed identifier.
            xml_bytes: The XML content as bytes.
        """
        with self._cache_lock.gen_wlock():
            self._feed_cache[feed_id] = xml_bytes

        logger.debug(
            "Feed XML cached.",
            extra={
                "feed_id": feed_id,
            },
        )

    def update_feed(self, feed_id: str, feed_config: FeedConfig) -> None:
        """Generate RSS XML for a feed and cache it.

        Args:
            feed_id: The feed identifier.
            feed_config: Configuration for the feed.

        Returns:
            Generated RSS XML as bytes.

        Raises:
            RSSGenerationError: If feed generation fails.
        """
        logger.info(
            "Generating RSS feed XML.",
            extra={"feed_id": feed_id},
        )

        downloads = self._get_feed_downloads(feed_id)
        feed_xml = (
            FeedgenCore(
                host=self._app_settings.base_url,
                feed_id=feed_id,
                feed_config=feed_config,
            )
            .with_downloads(downloads)
            .xml()
        )

        self._cache_feed_xml(feed_id, feed_xml)
        logger.info(
            "RSS feed XML generated successfully.",
            extra={
                "feed_id": feed_id,
                "num_episodes": len(downloads),
            },
        )

    def get_feed_xml(self, feed_id: str) -> bytes:
        """Get cached RSS XML for a feed.

        Args:
            feed_id: The feed identifier.

        Returns:
            Cached RSS XML as bytes.

        Raises:
            RSSGenerationError: If the feed is not found in cache.
        """
        logger.debug(
            "Retrieving cached RSS feed XML.",
            extra={"feed_id": feed_id},
        )

        with self._cache_lock.gen_rlock():
            if feed_id not in self._feed_cache:
                raise RSSGenerationError(
                    "Feed not found in cache.",
                    feed_id=feed_id,
                )
            return self._feed_cache[feed_id]
