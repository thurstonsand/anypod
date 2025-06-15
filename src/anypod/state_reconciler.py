"""State reconciliation for startup and configuration changes.

This module provides the StateReconciler class, which handles synchronization
between YAML configuration and database state during application startup and
when configuration changes are detected.
"""

from copy import deepcopy
from datetime import UTC, datetime
import logging
from typing import Any

from .config import FeedConfig
from .data_coordinator import Pruner
from .db import DownloadDatabase
from .db.feed_db import FeedDatabase
from .db.types import DownloadStatus, Feed, SourceType
from .exceptions import (
    DatabaseOperationError,
    PruneError,
    StateReconciliationError,
)

logger = logging.getLogger(__name__)


class StateReconciler:
    """Manage state reconciliation between configuration and database.

    The StateReconciler handles synchronization of feed configuration with
    database state during startup and when configuration changes are detected.
    It ensures database consistency and applies configuration changes properly.

    Attributes:
        _feed_db: Database manager for feed record operations.
        _download_db: Database manager for download record operations.
        _pruner: Pruner for feed pruning on deletion.
    """

    def __init__(
        self,
        feed_db: FeedDatabase,
        download_db: DownloadDatabase,
        pruner: Pruner,
    ):
        self._feed_db = feed_db
        self._download_db = download_db
        self._pruner = pruner
        logger.debug("StateReconciler initialized.")

    def _handle_new_feed(self, feed_id: str, feed_config: FeedConfig) -> None:
        """Handle a new feed by inserting it into the database.

        Args:
            feed_id: The feed identifier.
            feed_config: The FeedConfig from YAML.

        Raises:
            StateReconciliationError: If database operations fail.
        """
        log_params = {"feed_id": feed_id, "feed_url": feed_config.url}
        logger.info("Processing new feed.", extra=log_params)

        metadata = feed_config.metadata
        title = metadata.title if metadata else None
        subtitle = metadata.subtitle if metadata else None
        description = metadata.description if metadata else None
        language = metadata.language if metadata else None
        author = metadata.author if metadata else None
        image_url = metadata.image_url if metadata else None
        category = metadata.categories if metadata and metadata.categories else None
        explicit = metadata.explicit if metadata and metadata.explicit else None

        # Set initial sync timestamp to 'since' if provided, otherwise now
        initial_sync = feed_config.since if feed_config.since else datetime.now(UTC)

        new_feed = Feed(
            id=feed_id,
            is_enabled=feed_config.enabled,
            source_type=SourceType.UNKNOWN,  # to be defined later by ytdlp_wrapper
            source_url=feed_config.url,
            last_successful_sync=initial_sync,
            # Retention policies
            since=feed_config.since,
            keep_last=feed_config.keep_last,
            # Feed metadata
            title=title,
            subtitle=subtitle,
            description=description,
            language=language,
            author=author,
            image_url=image_url,
            category=category,
            explicit=explicit,
        )
        try:
            self._feed_db.upsert_feed(new_feed)
        except (DatabaseOperationError, ValueError) as e:
            raise StateReconciliationError(
                "Failed to insert new feed into database.",
                feed_id=feed_id,
            ) from e

    def _handle_since_changes(
        self, feed_id: str, config: FeedConfig, log_params: dict[str, Any]
    ) -> bool:
        """Handle changes to the 'since' retention policy.

        Args:
            feed_id: The feed identifier.
            config: The FeedConfig from YAML.
            log_params: Logging parameters for context.

        Returns:
            True if changes were applied.

        Raises:
            StateReconciliationError: If database operations fail.
        """
        # TODO: this is bad behavior, we NEED to store the since value to detect changes (could have been previously defined)
        if not config.since:
            return False

        # Find archived downloads that should be restored due to 'since' expansion
        try:
            downloads_to_restore = self._download_db.get_downloads_by_status(
                DownloadStatus.ARCHIVED, feed_id=feed_id, published_after=config.since
            )
        except DatabaseOperationError as e:
            raise StateReconciliationError(
                "Failed to fetch archived downloads for 'since' policy check.",
                feed_id=feed_id,
            ) from e

        if not downloads_to_restore:
            logger.debug(
                "No archived downloads to restore for 'since' expansion.",
                extra=log_params,
            )
            return False

        logger.info(
            f"Restoring {len(downloads_to_restore)} archived downloads due to 'since' expansion.",
            extra={**log_params, "since_date": config.since.isoformat()},
        )

        # Restore downloads to QUEUED status in batch
        download_ids = [dl.id for dl in downloads_to_restore]
        try:
            count_restored = self._download_db.requeue_downloads(
                feed_id, download_ids, from_status=DownloadStatus.ARCHIVED
            )
            logger.info(
                f"Successfully restored {count_restored} archived downloads to QUEUED.",
                extra={**log_params, "count_restored": count_restored},
            )
        except DatabaseOperationError as e:
            raise StateReconciliationError(
                "Failed to restore archived downloads.",
                feed_id=feed_id,
            ) from e

        return True

    def _handle_keep_last_changes(
        self,
        feed_id: str,
        config: FeedConfig,
        db_feed: Feed,
        log_params: dict[str, Any],
    ) -> bool:
        """Handle changes to the 'keep_last' retention policy.

        Args:
            feed_id: The feed identifier.
            config: The FeedConfig from YAML.
            db_feed: The existing Feed from database.
            log_params: Logging parameters for context.

        Returns:
            True if changes were applied.

        Raises:
            StateReconciliationError: If database operations fail.
        """
        if not config.keep_last:
            return False

        # Count current non-archived downloads
        current_downloaded_count = db_feed.total_downloads

        try:
            current_active_count = self._download_db.count_downloads_by_status(
                [DownloadStatus.QUEUED, DownloadStatus.UPCOMING], feed_id=feed_id
            )
        except DatabaseOperationError as e:
            raise StateReconciliationError(
                "Failed to count current downloads for 'keep_last' policy check.",
                feed_id=feed_id,
            ) from e

        current_total = current_downloaded_count + current_active_count

        # If we're under the keep_last limit, try to restore archived downloads
        if current_total < config.keep_last:
            restore_count = config.keep_last - current_total

            logger.info(
                f"Current download count ({current_total}) is below keep_last ({config.keep_last}). "
                f"Attempting to restore {restore_count} archived downloads.",
                extra=log_params,
            )

            try:
                # Get archived downloads, newest first
                archived_downloads = self._download_db.get_downloads_by_status(
                    DownloadStatus.ARCHIVED,
                    feed_id=feed_id,
                    limit=restore_count,
                )
            except DatabaseOperationError as e:
                raise StateReconciliationError(
                    "Failed to fetch archived downloads for 'keep_last' restoration.",
                    feed_id=feed_id,
                ) from e

            if not archived_downloads:
                logger.debug(
                    "No archived downloads to restore for 'keep_last' policy.",
                    extra=log_params,
                )
                return False

            logger.info(
                f"Restoring {len(archived_downloads)} archived downloads due to 'keep_last' increase.",
                extra={**log_params, "keep_last": config.keep_last},
            )

            # Restore downloads to QUEUED status in batch
            download_ids = [dl.id for dl in archived_downloads]
            try:
                count_restored = self._download_db.requeue_downloads(
                    feed_id, download_ids, from_status=DownloadStatus.ARCHIVED
                )
                logger.info(
                    f"Successfully restored {count_restored} archived downloads to QUEUED.",
                    extra={**log_params, "count_restored": count_restored},
                )
            except DatabaseOperationError as e:
                raise StateReconciliationError(
                    "Failed to restore archived downloads.",
                    feed_id=feed_id,
                ) from e

            return True

        else:
            logger.debug(
                f"Current download count ({current_total}) meets or exceeds keep_last ({config.keep_last}). No restoration needed.",
                extra=log_params,
            )
            return False

    def _handle_existing_feed(
        self, feed_id: str, feed_config: FeedConfig, db_feed: Feed
    ) -> bool:
        """Handle an existing feed by applying configuration changes.

        Args:
            feed_id: The feed identifier.
            feed_config: The FeedConfig from YAML.
            db_feed: The existing Feed from database.

        Returns:
            True if any changes were applied.

        Raises:
            StateReconciliationError: If database operations fail.
        """
        log_params = {"feed_id": feed_id}

        # Collect all changes to apply in a single update
        updated_feed = deepcopy(db_feed)

        match (feed_config.enabled, db_feed.is_enabled):
            # Feed has been enabled
            case (True, False):
                logger.info(
                    "Feed has been enabled.",
                    extra=log_params,
                )
                updated_feed.is_enabled = feed_config.enabled
                updated_feed.consecutive_failures = 0
                updated_feed.last_failed_sync = None
                updated_feed.last_error = None
                updated_feed.last_successful_sync = datetime.now(UTC)
            # Feed has been disabled
            case (False, True):
                logger.info(
                    "Feed has been disabled.",
                    extra=log_params,
                )
                updated_feed.is_enabled = feed_config.enabled
            # Feed status has not changed
            case _:
                pass

        # Check URL changes
        if feed_config.url != db_feed.source_url:
            logger.info(
                "Feed URL changed, updating and resetting error state.",
                extra={
                    **log_params,
                    "old_url": db_feed.source_url,
                    "new_url": feed_config.url,
                },
            )
            updated_feed.source_url = feed_config.url
            updated_feed.consecutive_failures = 0
            updated_feed.last_failed_sync = None
            updated_feed.last_error = None

        # Check metadata changes
        if metadata := feed_config.metadata:
            if metadata.title != db_feed.title:
                updated_feed.title = metadata.title

            if metadata.subtitle != db_feed.subtitle:
                updated_feed.subtitle = metadata.subtitle

            if metadata.description != db_feed.description:
                updated_feed.description = metadata.description

            if metadata.language != db_feed.language:
                updated_feed.language = metadata.language

            if metadata.author != db_feed.author:
                updated_feed.author = metadata.author

            if metadata.image_url != db_feed.image_url:
                updated_feed.image_url = metadata.image_url

            if metadata.categories != db_feed.category:
                updated_feed.category = metadata.categories

            if metadata.explicit != db_feed.explicit:
                updated_feed.explicit = metadata.explicit

        if feed_config.since != db_feed.since:
            updated_feed.since = feed_config.since
            self._handle_since_changes(feed_id, feed_config, log_params)

        if feed_config.keep_last != db_feed.keep_last:
            updated_feed.keep_last = feed_config.keep_last
            self._handle_keep_last_changes(feed_id, feed_config, db_feed, log_params)

        if updated_feed != db_feed:
            logger.info("Feed configuration changes applied.", extra=log_params)
            try:
                self._feed_db.upsert_feed(updated_feed)
            except DatabaseOperationError as e:
                raise StateReconciliationError(
                    "Failed to update feed configuration.",
                    feed_id=feed_id,
                ) from e
            return True
        else:
            logger.debug("No feed configuration changes detected.", extra=log_params)
            return False

    def _handle_removed_feed(self, feed_id: str) -> None:
        """Handle a removed feed by marking it as disabled in the database.

        Args:
            feed_id: The feed identifier.
            db_feed: The existing Feed from database.

        Raises:
            StateReconciliationError: If archive action fails.
        """
        try:
            self._pruner.archive_feed(feed_id)
        except PruneError as e:
            raise StateReconciliationError(
                "Failed to archive feed.",
                feed_id=feed_id,
            ) from e

    def reconcile_startup_state(self, config_feeds: dict[str, FeedConfig]) -> list[str]:
        """Reconcile configuration feeds with database state on startup.

        Compares the current YAML configuration with database feeds and performs
        necessary updates:
        - New feeds: Insert into database with initial sync timestamp
        - Removed feeds: Mark as disabled in database (only if currently enabled)
        - Changed feeds: Update metadata and configuration
        - Paused feeds: Feeds disabled in config are kept but not scheduled

        Args:
            config_feeds: Dictionary mapping feed_id to FeedConfig from YAML.

        Returns:
            List of feed IDs that are ready for scheduling (enabled and valid).

        Raises:
            StateReconciliationError: If reconciliation fails for critical operations.
        """
        logger.info(
            "Starting state reconciliation for startup.",
            extra={"config_feed_count": len(config_feeds)},
        )

        # Get all existing feeds from database
        try:
            db_feeds = self._feed_db.get_feeds()
        except (DatabaseOperationError, ValueError) as e:
            raise StateReconciliationError(
                "Failed to fetch feeds from database.",
            ) from e

        db_feed_lookup = {feed.id: feed for feed in db_feeds}
        ready_feeds: list[str] = []
        new_count = 0
        changed_count = 0
        processed_feed_ids: set[str] = set()

        # Process all feeds from configuration
        for feed_id, feed_config in config_feeds.items():
            processed_feed_ids.add(feed_id)
            db_feed = db_feed_lookup.get(feed_id)

            if db_feed is None:
                # New feed - add to database
                try:
                    self._handle_new_feed(feed_id, feed_config)
                except StateReconciliationError as e:
                    logger.warning(
                        "Failed to add new feed, continuing with others.",
                        extra={"feed_id": feed_id},
                        exc_info=e,
                    )
                else:
                    new_count += 1
                    if feed_config.enabled:
                        ready_feeds.append(feed_id)
            else:
                # Existing feed - check for changes
                try:
                    if self._handle_existing_feed(feed_id, feed_config, db_feed):
                        changed_count += 1
                except StateReconciliationError as e:
                    logger.warning(
                        "Failed to update existing feed, continuing with others.",
                        extra={"feed_id": feed_id},
                        exc_info=e,
                    )
                else:
                    if feed_config.enabled:
                        ready_feeds.append(feed_id)

        # Handle removed feeds - only those that are enabled in DB but not in config
        removed_count = 0
        for feed_id, db_feed in db_feed_lookup.items():
            if feed_id not in processed_feed_ids and db_feed.is_enabled:
                # Feed is enabled in DB but not present in config - mark as removed
                try:
                    self._handle_removed_feed(feed_id)
                except StateReconciliationError as e:
                    logger.warning(
                        "Failed to disable removed feed, continuing with others.",
                        extra={"feed_id": feed_id},
                        exc_info=e,
                    )
                else:
                    removed_count += 1

        logger.info(
            "State reconciliation completed successfully.",
            extra={
                "new_feeds": new_count,
                "removed_feeds": removed_count,
                "changed_feeds": changed_count,
                "ready_feeds": len(ready_feeds),
            },
        )

        return ready_feeds
