"""State reconciliation for startup and configuration changes.

This module provides the StateReconciler class, which handles synchronization
between YAML configuration and database state during application startup and
when configuration changes are detected.
"""

from datetime import UTC, datetime
import logging
from pathlib import Path
from typing import Any

from .config import FeedConfig
from .data_coordinator import Pruner
from .db import DownloadDatabase
from .db.feed_db import FeedDatabase
from .db.types import DownloadStatus, Feed
from .exceptions import (
    DatabaseOperationError,
    PruneError,
    StateReconciliationError,
    YtdlpApiError,
)
from .metadata import merge_feed_metadata
from .ytdlp_wrapper import YtdlpWrapper

logger = logging.getLogger(__name__)
# YouTube's founding year
MIN_SYNC_DATE = datetime(2005, 1, 1, tzinfo=UTC)


class StateReconciler:
    """Manage state reconciliation between configuration and database.

    The StateReconciler handles synchronization of feed configuration with
    database state during startup and when configuration changes are detected.
    It ensures database consistency and applies configuration changes properly.

    Attributes:
        _feed_db: Database manager for feed record operations.
        _download_db: Database manager for download record operations.
        _pruner: Pruner for feed pruning on deletion.
        _ytdlp_wrapper: YtdlpWrapper for feed discovery operations.
    """

    def __init__(
        self,
        feed_db: FeedDatabase,
        download_db: DownloadDatabase,
        ytdlp_wrapper: YtdlpWrapper,
        pruner: Pruner,
    ):
        self._feed_db = feed_db
        self._download_db = download_db
        self._ytdlp_wrapper = ytdlp_wrapper
        self._pruner = pruner
        logger.debug("StateReconciler initialized.")

    async def _handle_new_feed(
        self, feed_id: str, feed_config: FeedConfig, cookies_path: Path | None = None
    ) -> None:
        """Handle a new feed by inserting it into the database.

        Args:
            feed_id: The feed identifier.
            feed_config: The FeedConfig from YAML.
            cookies_path: Optional path to cookies file for yt-dlp authentication.

        Raises:
            StateReconciliationError: If database operations fail.
        """
        log_params = {"feed_id": feed_id, "feed_url": feed_config.url}
        logger.info("Processing new feed.", extra=log_params)

        # Set initial sync timestamp to 'since' if provided, otherwise use min
        initial_sync = feed_config.since if feed_config.since else MIN_SYNC_DATE

        # Discover feed properties (source type and resolved URL)
        try:
            (
                source_type,
                resolved_url,
            ) = await self._ytdlp_wrapper.discover_feed_properties(
                feed_id, feed_config.url
            )
        except YtdlpApiError as e:
            raise StateReconciliationError(
                "Failed to discover feed properties during feed creation.",
                feed_id=feed_id,
            ) from e
        else:
            logger.debug(
                "Feed discovery completed.",
                extra={
                    **log_params,
                    "discovered_source_type": source_type.value,
                    "discovered_resolved_url": resolved_url,
                },
            )

        # Fetch and merge feed metadata
        try:
            fetched_feed, _ = await self._ytdlp_wrapper.fetch_metadata(
                feed_id=feed_id,
                source_type=source_type,
                source_url=feed_config.url,
                resolved_url=resolved_url,
                user_yt_cli_args=feed_config.yt_args,
                fetch_since_date=feed_config.since,
                fetch_until_date=None,
                keep_last=feed_config.keep_last,
                cookies_path=cookies_path,
                metadata_only=True,
            )
        except YtdlpApiError as e:
            raise StateReconciliationError(
                "Failed to fetch and merge feed metadata during feed creation.",
                feed_id=feed_id,
            ) from e

        merged_metadata = merge_feed_metadata(fetched_feed, feed_config)
        new_feed = Feed(
            id=feed_id,
            is_enabled=feed_config.enabled,
            source_type=fetched_feed.source_type,
            source_url=feed_config.url,
            resolved_url=resolved_url,
            last_successful_sync=initial_sync,
            since=feed_config.since,
            keep_last=feed_config.keep_last,
            **merged_metadata,
        )
        try:
            await self._feed_db.upsert_feed(new_feed)
        except (DatabaseOperationError, ValueError) as e:
            raise StateReconciliationError(
                "Failed to insert new feed into database.",
                feed_id=feed_id,
            ) from e

    async def _handle_pruning_changes(
        self,
        feed_id: str,
        config_since: datetime | None,
        config_keep_last: int | None,
        db_feed: Feed,
        log_params: dict[str, Any],
    ) -> bool:
        """Handle changes to retention policies (since and keep_last) together.

        This method considers both retention policies simultaneously to determine
        the set of downloads to restore from archived status.

        Args:
            feed_id: The feed identifier.
            config_since: The 'since' value from config (or None).
            config_keep_last: The 'keep_last' value from config (or None).
            db_feed: The existing Feed from database.
            log_params: Logging parameters for context.

        Returns:
            True if changes were applied.

        Raises:
            StateReconciliationError: If database operations fail.
        """
        # Check if either retention policy has changed
        since_changed = config_since != db_feed.since
        keep_last_changed = config_keep_last != db_feed.keep_last

        if not since_changed and not keep_last_changed:
            return False

        match (db_feed.since, config_since):
            case (None, None):
                should_restore = False
                restore_filter_date = None
            case (None, config_since):
                # Adding 'since' filter - no restoration needed (making filter stricter)
                # Pruner will handle this case automatically
                logger.debug(
                    f"'since' filter added ({config_since}), no restoration needed.",
                    extra={
                        **log_params,
                        "old_since": None,
                        "new_since": config_since,
                    },
                )
                should_restore = False
                restore_filter_date = None
            case (db_since, None):
                # Removing 'since' filter - potentially restore all archived downloads
                logger.info(
                    f"'since' filter removed (was {db_since}), considering all archived downloads for restoration.",
                    extra={**log_params, "old_since": db_since, "new_since": None},
                )
                should_restore = True
                restore_filter_date = None
            case (db_since, config_since) if config_since < db_since:
                # Expanding 'since' to earlier date - restore downloads between the dates
                logger.info(
                    f"'since' date expanded from {db_since} to {config_since}, considering downloads after {config_since} for restoration.",
                    extra={
                        **log_params,
                        "old_since": db_since,
                        "new_since": config_since,
                    },
                )
                should_restore = True
                restore_filter_date = config_since
            case (db_since, config_since):
                # Unchanged or `since` filter made stricter - no restoration needed
                # Pruner will handle this case automatically
                logger.debug(
                    f"'since' date made stricter from {db_since} to {config_since}, no restoration needed.",
                    extra={
                        **log_params,
                        "old_since": db_since,
                        "new_since": config_since,
                    },
                )
                should_restore = False
                restore_filter_date = None

        # Handle 'keep_last' changes and determine restoration limit
        match db_feed.keep_last, config_keep_last, db_feed.total_downloads:
            case (None, None, _):
                # No keep_last constraint, no contribution to restoration limit
                logger.debug(
                    "No 'keep_last' constraint, no contribution to restoration limit.",
                    extra=log_params,
                )
                restore_limit = -1
            case (_, config_keep_last, total_downloads) if (
                config_keep_last is not None and config_keep_last > total_downloads
            ):
                # Keep_last allows restoration - can restore up to the difference
                available_slots = config_keep_last - total_downloads
                logger.info(
                    f"'keep_last' limit allows restoration, can restore up to {available_slots} archived downloads.",
                    extra={
                        **log_params,
                        "old_keep_last": db_feed.keep_last,
                        "new_keep_last": config_keep_last,
                    },
                )
                should_restore = True
                restore_limit = available_slots
            case (db_keep_last, None, _):
                # Removing 'keep_last' filter - potentially restore all archived downloads
                logger.info(
                    f"'keep_last' filter removed (was {db_keep_last}), considering all archived downloads for restoration.",
                    extra={
                        **log_params,
                        "old_keep_last": db_keep_last,
                        "new_keep_last": None,
                    },
                )
                should_restore = True
                restore_limit = -1
            case (_, config_keep_last, _):
                # Keep_last exists and is less than total downloads - constrains restoration
                # This overrides any since expansion since we're at/above the limit
                logger.debug(
                    "'keep_last' limit constrains restoration.",
                    extra={
                        **log_params,
                        "old_keep_last": db_feed.keep_last,
                        "new_keep_last": config_keep_last,
                    },
                )
                should_restore = False
                restore_limit = -1

        # Check if we should restore based on the combined policies
        if not should_restore:
            return False

        # Find archived downloads that should be restored
        try:
            downloads_to_restore = await self._download_db.get_downloads_by_status(
                DownloadStatus.ARCHIVED,
                feed_id=feed_id,
                published_after=restore_filter_date,  # None means all downloads
                limit=restore_limit,
            )
        except DatabaseOperationError as e:
            raise StateReconciliationError(
                "Failed to fetch archived downloads for retention policy check.",
                feed_id=feed_id,
            ) from e

        if not downloads_to_restore:
            logger.debug(
                "No archived downloads to restore for retention policy changes.",
                extra=log_params,
            )
            return False

        # Log the restoration details
        restore_reason: list[str] = []
        if since_changed:
            restore_reason.append("'since' expansion")
        if keep_last_changed and config_keep_last is not None:
            restore_reason.append("'keep_last' increase")

        logger.info(
            f"Restoring {len(downloads_to_restore)} archived downloads due to {' and '.join(restore_reason)}.",
            extra={
                **log_params,
                "since_date": config_since.isoformat() if config_since else None,
                "keep_last": config_keep_last,
            },
        )

        # Restore downloads to QUEUED status in batch
        download_ids = [dl.id for dl in downloads_to_restore]
        try:
            count_restored = await self._download_db.requeue_downloads(
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

    async def _handle_existing_feed(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        db_feed: Feed,
        cookies_path: Path | None = None,
    ) -> bool:
        """Handle an existing feed by applying configuration changes.

        Args:
            feed_id: The feed identifier.
            feed_config: The FeedConfig from YAML.
            db_feed: The existing Feed from database.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

        Returns:
            True if any changes were applied.

        Raises:
            StateReconciliationError: If database operations fail.
        """
        log_params = {"feed_id": feed_id}

        updated_feed = db_feed.model_copy()

        match db_feed, feed_config:
            # Feed is being re-enabled or URL has changed, re-discover feed properties, reset stats
            case (
                Feed(is_enabled=old_enabled, source_url=old_url),
                FeedConfig(enabled=new_enabled, url=new_url),
            ) if (not old_enabled and new_enabled) or (old_url != new_url):
                if not old_enabled and new_enabled:
                    logger.info(
                        "Feed has been enabled.",
                        extra=log_params,
                    )
                elif old_url != new_url:
                    logger.info(
                        "Feed URL has changed.",
                        extra=log_params,
                    )
                try:
                    (
                        updated_source_type,
                        updated_resolved_url,
                    ) = await self._ytdlp_wrapper.discover_feed_properties(
                        feed_id, new_url
                    )
                except YtdlpApiError as e:
                    raise StateReconciliationError(
                        "Failed to re-discover feed properties when re-enabling feed.",
                        feed_id=feed_id,
                    ) from e
                updated_feed.is_enabled = new_enabled
                updated_feed.source_type = updated_source_type
                updated_feed.source_url = new_url
                updated_feed.resolved_url = updated_resolved_url
                updated_feed.last_successful_sync = MIN_SYNC_DATE
                updated_feed.last_failed_sync = None
                updated_feed.consecutive_failures = 0
            # Feed has been disabled
            case (Feed(is_enabled=True), FeedConfig(enabled=False)):
                logger.info(
                    "Feed has been disabled.",
                    extra=log_params,
                )
                updated_feed.is_enabled = False
            # Feed status has not changed
            case _:
                pass

        try:
            fetched_feed, _ = await self._ytdlp_wrapper.fetch_metadata(
                feed_id=feed_id,
                source_type=updated_feed.source_type,
                source_url=feed_config.url,
                resolved_url=updated_feed.resolved_url,
                user_yt_cli_args=feed_config.yt_args,
                fetch_since_date=feed_config.since,
                fetch_until_date=None,
                keep_last=feed_config.keep_last,
                cookies_path=cookies_path,
                metadata_only=True,
            )
        except YtdlpApiError as e:
            raise StateReconciliationError(
                "Failed to fetch fresh metadata for existing feed.",
                feed_id=feed_id,
            ) from e

        merged_metadata = merge_feed_metadata(fetched_feed, feed_config)
        updated_feed = updated_feed.model_copy(
            update={
                **merged_metadata,
                "since": feed_config.since,
                "keep_last": feed_config.keep_last,
            }
        )

        # Reset last_successful_sync to MIN_SYNC_DATE when 'since' filter is removed
        # This ensures the enqueuer will fetch all videos from the beginning of time
        if db_feed.since is not None and feed_config.since is None:
            logger.info(
                f"'since' filter removed (was {db_feed.since}), resetting last_successful_sync to allow re-fetching all videos.",
                extra={**log_params, "old_since": db_feed.since, "new_since": None},
            )
            updated_feed.last_successful_sync = MIN_SYNC_DATE

        await self._handle_pruning_changes(
            feed_id, feed_config.since, feed_config.keep_last, db_feed, log_params
        )

        if updated_feed != db_feed:
            logger.debug("Feed configuration changes applied.", extra=log_params)
            try:
                await self._feed_db.upsert_feed(updated_feed)
            except DatabaseOperationError as e:
                raise StateReconciliationError(
                    "Failed to update feed configuration.",
                    feed_id=feed_id,
                ) from e
            return True
        else:
            logger.debug("No feed configuration changes detected.", extra=log_params)
            return False

    async def _handle_removed_feed(self, feed_id: str) -> None:
        """Handle a removed feed by marking it as disabled in the database.

        Args:
            feed_id: The feed identifier.
            db_feed: The existing Feed from database.

        Raises:
            StateReconciliationError: If archive action fails.
        """
        try:
            await self._pruner.archive_feed(feed_id)
        except PruneError as e:
            raise StateReconciliationError(
                "Failed to archive feed.",
                feed_id=feed_id,
            ) from e

    async def reconcile_startup_state(
        self, config_feeds: dict[str, FeedConfig], cookies_path: Path | None = None
    ) -> list[str]:
        """Reconcile configuration feeds with database state on startup.

        Compares the current YAML configuration with database feeds and performs
        necessary updates:
        - New feeds: Insert into database with initial sync timestamp
        - Removed feeds: Mark as disabled in database (only if currently enabled)
        - Changed feeds: Update metadata and configuration
        - Paused feeds: Feeds disabled in config are kept but not scheduled

        Args:
            config_feeds: Dictionary mapping feed_id to FeedConfig from YAML.
            cookies_path: Optional path to cookies file for yt-dlp authentication.

        Returns:
            List of feed IDs that are ready for scheduling (enabled and valid).

        Raises:
            StateReconciliationError: If reconciliation fails for critical operations.
        """
        logger.debug(
            "Starting state reconciliation for startup.",
            extra={"config_feed_count": len(config_feeds)},
        )

        # Get all existing feeds from database
        try:
            db_feeds = await self._feed_db.get_feeds()
        except (DatabaseOperationError, ValueError) as e:
            raise StateReconciliationError(
                "Failed to fetch feeds from database.",
            ) from e

        db_feed_lookup = {feed.id: feed for feed in db_feeds}
        ready_feeds: list[str] = []
        new_count = 0
        changed_count = 0
        processed_feed_ids: set[str] = set()
        failed_feeds: dict[str, str] = {}  # Track feeds that failed with error summary

        # Process all feeds from configuration
        for feed_id, feed_config in config_feeds.items():
            processed_feed_ids.add(feed_id)
            db_feed = db_feed_lookup.get(feed_id)

            if db_feed is None:
                # New feed - add to database
                try:
                    await self._handle_new_feed(feed_id, feed_config, cookies_path)
                except StateReconciliationError as e:
                    error_summary = str(e)
                    failed_feeds[feed_id] = error_summary
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
                    if await self._handle_existing_feed(
                        feed_id, feed_config, db_feed, cookies_path
                    ):
                        changed_count += 1
                except StateReconciliationError as e:
                    error_summary = str(e)
                    failed_feeds[feed_id] = error_summary
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
                    await self._handle_removed_feed(feed_id)
                except StateReconciliationError as e:
                    logger.warning(
                        "Failed to disable removed feed, continuing with others.",
                        extra={"feed_id": feed_id},
                        exc_info=e,
                    )
                else:
                    removed_count += 1

        logger.debug(
            "State reconciliation completed successfully.",
            extra={
                "new_feeds": new_count,
                "removed_feeds": removed_count,
                "changed_feeds": changed_count,
                "ready_feeds": len(ready_feeds),
                "failed_feeds": len(failed_feeds),
            },
        )

        # If we have configured feeds but none are ready, include error details
        if config_feeds and not ready_feeds and failed_feeds:
            logger.error(
                "All configured feeds failed during reconciliation.",
                extra={
                    "configured_feeds": len(config_feeds),
                    "failed_feeds": failed_feeds,
                },
            )

        return ready_feeds
