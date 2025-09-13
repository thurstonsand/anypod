"""RSS feed generation and management for Anypod podcast feeds.

This module provides the RSSFeedGenerator class for creating RSS podcast
feeds from download metadata and persisting the XML to disk for serving, including integration with the feedgen library.
"""

import logging

import aiofiles.os

from ..db import DownloadDatabase
from ..db.types import Download, DownloadStatus, Feed
from ..exceptions import DatabaseOperationError, RSSGenerationError
from ..path_manager import PathManager
from .feedgen_core import FeedgenCore

logger = logging.getLogger(__name__)


class RSSFeedGenerator:
    """Generate and persist RSS podcast feeds from download metadata.

    Manages RSS feed generation using feedgen with podcast extensions and
    persists the resulting XML to disk for serving by the HTTP layer.

    Attributes:
        _download_db: Database manager for querying download data.
        _paths: Path manager for resolving URLs and download paths.
    """

    def __init__(
        self,
        download_db: DownloadDatabase,
        paths: PathManager,
    ):
        self._download_db = download_db
        self._paths = paths
        logger.debug("RSSFeedGenerator initialized.")

    async def _get_feed_downloads(self, feed_id: str) -> list[Download]:
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
            downloads = await self._download_db.get_downloads_by_status(
                status_to_filter=DownloadStatus.DOWNLOADED,
                feed_id=feed_id,
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

    async def update_feed(self, feed_id: str, feed: Feed) -> None:
        """Generate RSS XML for a feed and cache it.

        Args:
            feed_id: The feed identifier.
            feed: Feed database object containing metadata.

        Raises:
            RSSGenerationError: If feed generation fails.
        """
        logger.debug(
            "Generating RSS feed XML.",
            extra={"feed_id": feed_id},
        )

        downloads = await self._get_feed_downloads(feed_id)
        feed_xml = (
            FeedgenCore(
                paths=self._paths,
                feed_id=feed_id,
                feed=feed,
            )
            .with_downloads(downloads)
            .xml()
        )
        # Persist RSS XML to disk atomically
        try:
            tmp_path = await self._paths.tmp_file(feed_id)
            final_path = await self._paths.feed_xml_path(feed_id)
            async with aiofiles.open(tmp_path, "wb") as f:
                await f.write(feed_xml)
            await aiofiles.os.rename(tmp_path, final_path)
        except (OSError, ValueError) as e:
            raise RSSGenerationError(
                "Failed to persist RSS XML to disk.", feed_id=feed_id
            ) from e

        logger.info(
            "RSS feed generated and saved.",
            extra={
                "feed_id": feed_id,
                "url": self._paths.feed_url(feed_id),
                "num_episodes": len(downloads),
            },
        )
