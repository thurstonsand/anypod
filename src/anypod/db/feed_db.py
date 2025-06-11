"""Database management for Anypod feeds.

This module provides the Feed dataclass and related enums for feed-related
database operations.
"""

from dataclasses import asdict
from datetime import UTC, datetime
import logging
from pathlib import Path
from typing import Any

from ..exceptions import DatabaseOperationError, FeedNotFoundError, NotFoundError
from .sqlite_utils_core import SqliteUtilsCore
from .types import Feed

logger = logging.getLogger(__name__)


class FeedDatabase:
    """Manage all database operations for feeds.

    Handles database initialization, CRUD operations, and queries for feed
    records using SQLite as the backend.

    Attributes:
        _db_path: Path to the database file.
        _db: Core SQLite database wrapper.
        _feed_table_name: Name of the feeds table.
    """

    def __init__(self, db_path: Path | None, memory_name: str | None = None):
        self._db_path = db_path
        self._db = SqliteUtilsCore(db_path, memory_name)
        self._feed_table_name = "feeds"
        self._initialize_schema()
        logger.debug("FeedDatabase initialized.", extra={"db_path": str(db_path)})

    def _initialize_schema(self) -> None:
        """Initialize the feeds table schema with triggers for timestamp management."""
        # Create the feeds table
        self._db.create_table(
            self._feed_table_name,
            {
                "id": str,
                "is_enabled": bool,
                "source_type": str,  # from a SourceType,
                # time keeping
                "created_at": datetime,
                "updated_at": datetime,
                "last_successful_sync": datetime,
                "last_rss_generation": datetime,
                # error tracking
                "last_failed_sync": datetime,
                "consecutive_failures": int,
                "last_error": str,
                # download tracking
                "total_downloads": int,
                "downloads_since_last_rss": int,
                # feed metadata
                "title": str,
                "subtitle": str,
                "description": str,
                "language": str,
                "author": str,
                "image_url": str,
                "category": str,
                "explicit": str,
            },
            pk="id",
            not_null={
                "id",
                "is_enabled",
                "source_type",
                "created_at",
                "updated_at",
                "consecutive_failures",
                "total_downloads",
                "downloads_since_last_rss",
            },
            defaults={
                "created_at": "STRFTIME('%Y-%m-%dT%H:%M:%f+00:00','now')",
                "updated_at": "STRFTIME('%Y-%m-%dT%H:%M:%f+00:00','now')",
                "consecutive_failures": 0,
                "total_downloads": 0,
                "downloads_since_last_rss": 0,
            },
        )

        # Create trigger to set updated_at on UPDATE
        self._db.create_trigger(
            trigger_name=f"{self._feed_table_name}_update_timestamp",
            table_name=self._feed_table_name,
            trigger_event="AFTER UPDATE",
            exclude_columns=["created_at", "updated_at"],
            trigger_sql_body=f"""
                UPDATE {self._db.quote(self._feed_table_name)}
                SET updated_at = STRFTIME('%Y-%m-%dT%H:%M:%f+00:00','now')
                WHERE id = NEW.id;
            """,
        )

    def close(self) -> None:
        """Closes the database connection."""
        logger.info("Closing database connection.")
        self._db.close()

    # --- CRUD Operations ---

    def upsert_feed(self, feed: Feed) -> None:
        """Insert or update a feed in the feeds table.

        If a feed with the same id exists, it will be replaced.

        Args:
            feed: The Feed object to insert or update.

        Raises:
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {"feed_id": feed.id}
        logger.debug("Attempting to upsert feed record.", extra=log_params)
        try:
            # Convert feed to dict and exclude None values for timestamp fields
            # so database defaults can take effect
            feed_dict = asdict(feed)
            if feed_dict.get("created_at") is None:
                feed_dict.pop("created_at", None)
            if feed_dict.get("updated_at") is None:
                feed_dict.pop("updated_at", None)

            self._db.upsert(
                self._feed_table_name,
                feed_dict,
                pk="id",
                not_null={
                    "id",
                    "is_enabled",
                    "source_type",
                    "total_downloads",
                    "downloads_since_last_rss",
                    "consecutive_failures",
                },
            )
        except DatabaseOperationError as e:
            e.feed_id = feed.id
            raise e
        logger.debug("Upsert feed record execution complete.", extra=log_params)

    def get_feed_by_id(self, feed_id: str) -> Feed:
        """Retrieve a specific feed by ID.

        Args:
            feed_id: The feed identifier.

        Returns:
            Feed object for the specified ID.

        Raises:
            FeedNotFoundError: If the feed is not found.
            DatabaseOperationError: If the database operation fails.
            ValueError: If unable to parse row into a Feed object.
        """
        log_params = {"feed_id": feed_id}
        logger.debug("Attempting to get feed by ID.", extra=log_params)
        try:
            row = self._db.get(self._feed_table_name, feed_id)
        except NotFoundError as e:
            raise FeedNotFoundError("Feed not found.", feed_id=feed_id) from e
        except DatabaseOperationError as e:
            e.feed_id = feed_id
            raise e
        return Feed.from_row(row)

    def get_feeds(self, enabled: bool | None = None) -> list[Feed]:
        """Get all feeds, or filter by enabled status if provided.

        Args:
            enabled: Optional filter by enabled status. If None, returns all feeds.

        Returns:
            List of Feed objects matching the criteria.

        Raises:
            DatabaseOperationError: If the database query fails.
            ValueError: If unable to parse a row into a Feed object.
        """
        log_params = {"enabled_filter": enabled or "no_filter"}
        logger.debug("Attempting to get feeds.", extra=log_params)

        if enabled is None:
            where_clause = None
            where_args = None
        else:
            where_clause = "is_enabled = :enabled"
            where_args = {"enabled": enabled}

        try:
            rows = self._db.rows_where(
                self._feed_table_name,
                where_clause,
                where_args=where_args,
                order_by="id ASC",
            )
        except DatabaseOperationError as e:
            raise e
        return [Feed.from_row(row) for row in rows]

    def mark_sync_success(self, feed_id: str) -> None:
        """Set last_successful_sync to current timestamp, reset consecutive_failures to 0, clear last_error.

        Args:
            feed_id: The feed identifier.

        Raises:
            FeedNotFoundError: If the feed is not found.
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {"feed_id": feed_id}
        logger.debug("Attempting to mark sync success for feed.", extra=log_params)
        try:
            self._db.update(
                self._feed_table_name,
                feed_id,
                {
                    "last_successful_sync": datetime.now(UTC),
                    "consecutive_failures": 0,
                    "last_error": None,
                },
            )
        except NotFoundError as e:
            raise FeedNotFoundError("Feed not found.", feed_id=feed_id) from e
        except DatabaseOperationError as e:
            e.feed_id = feed_id
            raise e
        logger.info("Feed sync success marked.", extra=log_params)

    def mark_sync_failure(self, feed_id: str, error_message: str) -> None:
        """Set last_failed_sync to current timestamp, increment consecutive_failures, set last_error.

        Args:
            feed_id: The feed identifier.
            error_message: The error message to record.

        Raises:
            FeedNotFoundError: If the feed is not found.
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {"feed_id": feed_id, "error_message": error_message}
        logger.debug("Attempting to mark sync failure for feed.", extra=log_params)

        updates = {
            "last_failed_sync": datetime.now(UTC),
            "consecutive_failures": 1,
            "last_error": error_message,
        }
        conversions = {"consecutive_failures": "[consecutive_failures] + ?"}

        try:
            self._db.update(
                self._feed_table_name,
                feed_id,
                updates,
                conversions=conversions,
            )
        except NotFoundError as e:
            raise FeedNotFoundError("Feed not found.", feed_id=feed_id) from e
        except DatabaseOperationError as e:
            e.feed_id = feed_id
            raise e
        logger.warning("Feed sync failure marked.", extra=log_params)

    def mark_rss_generated(self, feed_id: str, new_downloads_count: int) -> None:
        """Set last_rss_generation to current timestamp, increment total_downloads by new_downloads_count, set downloads_since_last_rss to new_downloads_count.

        Args:
            feed_id: The feed identifier.
            new_downloads_count: Number of new downloads since last RSS generation.

        Raises:
            FeedNotFoundError: If the feed is not found.
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {
            "feed_id": feed_id,
            "new_downloads_count": new_downloads_count,
        }
        logger.debug("Attempting to mark RSS generated for feed.", extra=log_params)

        try:
            self._db.update(
                self._feed_table_name,
                feed_id,
                {
                    "last_rss_generation": datetime.now(UTC),
                    "total_downloads": new_downloads_count,
                    "downloads_since_last_rss": new_downloads_count,
                },
                conversions={
                    "total_downloads": "[total_downloads] + ?",
                },
            )
        except NotFoundError as e:
            raise FeedNotFoundError("Feed not found.", feed_id=feed_id) from e
        except DatabaseOperationError as e:
            e.feed_id = feed_id
            raise e
        logger.info("RSS generation marked for feed.", extra=log_params)

    def set_feed_enabled(self, feed_id: str, enabled: bool) -> None:
        """Set is_enabled to the provided value.

        Args:
            feed_id: The feed identifier.
            enabled: Whether the feed should be enabled.

        Raises:
            FeedNotFoundError: If the feed is not found.
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {"feed_id": feed_id, "enabled": enabled}
        logger.debug("Attempting to set feed enabled status.", extra=log_params)
        try:
            self._db.update(
                self._feed_table_name,
                feed_id,
                {"is_enabled": enabled},
            )
        except NotFoundError as e:
            raise FeedNotFoundError("Feed not found.", feed_id=feed_id) from e
        except DatabaseOperationError as e:
            e.feed_id = feed_id
            raise e
        logger.info("Feed enabled status updated.", extra=log_params)

    def update_feed_metadata(
        self,
        feed_id: str,
        *,
        title: str | None = None,
        subtitle: str | None = None,
        description: str | None = None,
        language: str | None = None,
        author: str | None = None,
        image_url: str | None = None,
        category: str | None = None,
        explicit: str | None = None,
    ) -> None:
        """Update feed metadata fields; no-op if all metadata fields are None.

        Args:
            feed_id: The feed identifier.
            title: Optional new title.
            subtitle: Optional new subtitle.
            description: Optional new description.
            language: Optional new language.
            author: Optional new author.
            image_url: Optional new image URL.
            category: Optional new category.
            explicit: Optional new explicit flag.

        Raises:
            FeedNotFoundError: If the feed is not found.
            DatabaseOperationError: If the database operation fails.
        """
        # Build update dictionary with only non-None values
        updates: dict[str, Any] = {}
        if title is not None:
            updates["title"] = title
        if subtitle is not None:
            updates["subtitle"] = subtitle
        if description is not None:
            updates["description"] = description
        if language is not None:
            updates["language"] = language
        if author is not None:
            updates["author"] = author
        if image_url is not None:
            updates["image_url"] = image_url
        if category is not None:
            updates["category"] = category
        if explicit is not None:
            updates["explicit"] = explicit

        # No-op if all fields are None
        if not updates:
            logger.debug(
                "No metadata fields provided for update, skipping.",
                extra={"feed_id": feed_id},
            )
            return

        log_params = {"feed_id": feed_id, "updated_fields": list(updates.keys())}
        logger.debug("Attempting to update feed metadata.", extra=log_params)
        try:
            self._db.update(
                self._feed_table_name,
                feed_id,
                updates,
            )
        except NotFoundError as e:
            raise FeedNotFoundError("Feed not found.", feed_id=feed_id) from e
        except DatabaseOperationError as e:
            e.feed_id = feed_id
            raise e
        logger.info("Feed metadata updated.", extra=log_params)
