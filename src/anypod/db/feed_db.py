"""Database management for Anypod feeds.

This module provides the Feed dataclass and related enums for feed-related
database operations.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
import logging
from typing import Any

from sqlalchemy import update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col, select

from ..config.types import PodcastCategories, PodcastExplicit
from ..exceptions import FeedNotFoundError, NotFoundError
from .decorators import handle_db_errors, handle_feed_db_errors
from .sqlalchemy_core import SqlalchemyCore
from .types import Feed, SourceType

logger = logging.getLogger(__name__)


class FeedDatabase:
    """Manage all database operations for feeds.

    Handles database initialization, CRUD operations, and queries for feed
    records using SQLAlchemy as the backend.

    Attributes:
        _db: Core SQLAlchemy database manager.
    """

    def __init__(self, db_core: SqlalchemyCore):
        self._db = db_core

    # --- Transaction Support ---
    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession]:
        """Provide a transactional session.

        This is a passthrough to the core SQLAlchemy session manager.
        Use as an async context manager for database transactions.

        Yields:
            An active, transactional AsyncSession.
        """
        async with self._db.session() as session:
            yield session

    # --- CRUD Operations ---
    @handle_feed_db_errors("upsert feed", feed_id_from="feed.id")
    async def upsert_feed(self, feed: Feed) -> None:
        """Insert or update a feed in the feeds table.

        If a feed with the same id exists, it will be updated.

        Args:
            feed: The Feed object to insert or update.

        Raises:
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {"feed_id": feed.id}
        logger.debug("Attempting to upsert feed record.", extra=log_params)
        async with self._db.session() as session:
            data = feed.model_dump_for_insert()

            stmt = insert(Feed).values(**data)

            # Don't include primary keys in the update
            data.pop("id", None)
            stmt = stmt.on_conflict_do_update(index_elements=["id"], set_=data)
            await session.execute(stmt)
            await session.commit()
        logger.debug("Upsert feed record execution complete.", extra=log_params)

    @handle_feed_db_errors("get feed by ID")
    async def get_feed_by_id(self, feed_id: str) -> Feed:
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
        async with self._db.session() as session:
            feed = await session.get(Feed, feed_id)
            if not feed:
                raise FeedNotFoundError("Feed not found.", feed_id=feed_id)
            return feed

    @handle_db_errors("get feeds")
    async def get_feeds(self, enabled: bool | None = None) -> list[Feed]:
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

        async with self._db.session() as session:
            stmt = select(Feed)
            if enabled is not None:
                stmt = stmt.where(col(Feed.is_enabled) == enabled)
            stmt = stmt.order_by(col(Feed.id))

            result = await session.execute(stmt)
            return list(result.scalars().all())

    @handle_feed_db_errors("mark sync success")
    async def mark_sync_success(
        self, feed_id: str, sync_time: datetime | None = None
    ) -> None:
        """Set last_successful_sync to current timestamp, reset consecutive_failures to 0.

        Args:
            feed_id: The feed identifier.
            sync_time: The time to set the last_successful_sync to. If None, the current time is used.

        Raises:
            FeedNotFoundError: If the feed is not found.
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {"feed_id": feed_id}
        logger.debug("Attempting to mark sync success for feed.", extra=log_params)
        async with self._db.session() as session:
            stmt = (
                update(Feed)
                .where(col(Feed.id) == feed_id)
                .values(
                    last_successful_sync=sync_time or datetime.now(UTC),
                    consecutive_failures=0,
                )
            )
            try:
                self._db.assert_exactly_one_row_affected(
                    await session.execute(stmt), feed_id=feed_id
                )
            except NotFoundError as e:
                raise FeedNotFoundError("Feed not found.", feed_id=feed_id) from e
            await session.commit()
        logger.debug("Feed sync success marked.", extra=log_params)

    @handle_feed_db_errors("mark sync failure")
    async def mark_sync_failure(
        self, feed_id: str, sync_time: datetime | None = None
    ) -> None:
        """Set last_failed_sync to current timestamp, increment consecutive_failures.

        Args:
            feed_id: The feed identifier.
            sync_time: The time to set the last_failed_sync to. If None, the current time is used.

        Raises:
            FeedNotFoundError: If the feed is not found.
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {"feed_id": feed_id}
        logger.debug("Attempting to mark sync failure for feed.", extra=log_params)
        async with self._db.session() as session:
            stmt = (
                update(Feed)
                .where(col(Feed.id) == feed_id)
                .values(
                    last_failed_sync=sync_time or datetime.now(UTC),
                    consecutive_failures=col(Feed.consecutive_failures) + 1,
                )
            )

            try:
                self._db.assert_exactly_one_row_affected(
                    await session.execute(stmt), feed_id=feed_id
                )
            except NotFoundError as e:
                raise FeedNotFoundError("Feed not found.", feed_id=feed_id) from e
            await session.commit()
        logger.warning("Feed sync failure marked.", extra=log_params)

    @handle_feed_db_errors("mark RSS generated")
    async def mark_rss_generated(self, feed_id: str) -> None:
        """Set last_rss_generation to the current timestamp.

        Args:
            feed_id: The feed identifier.

        Raises:
            FeedNotFoundError: If the feed is not found.
            DatabaseOperationError: If the database operation fails.
        """
        log_params = {"feed_id": feed_id}
        logger.debug("Attempting to mark RSS generated for feed.", extra=log_params)
        async with self._db.session() as session:
            stmt = (
                update(Feed)
                .where(col(Feed.id) == feed_id)
                .values(
                    last_rss_generation=datetime.now(UTC),
                )
            )
            try:
                self._db.assert_exactly_one_row_affected(
                    await session.execute(stmt), feed_id=feed_id
                )
            except NotFoundError as e:
                raise FeedNotFoundError("Feed not found.", feed_id=feed_id) from e
            await session.commit()
        logger.debug("RSS generation marked for feed.", extra=log_params)

    @handle_feed_db_errors("set feed enabled")
    async def set_feed_enabled(self, feed_id: str, enabled: bool) -> None:
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
        async with self._db.session() as session:
            stmt = (
                update(Feed).where(col(Feed.id) == feed_id).values(is_enabled=enabled)
            )
            try:
                self._db.assert_exactly_one_row_affected(
                    await session.execute(stmt), feed_id=feed_id
                )
            except NotFoundError as e:
                raise FeedNotFoundError("Feed not found.", feed_id=feed_id) from e
            await session.commit()
        logger.debug("Feed enabled status updated.", extra=log_params)

    @handle_feed_db_errors("update feed metadata")
    async def update_feed_metadata(
        self,
        feed_id: str,
        *,
        source_type: SourceType | None = None,
        title: str | None = None,
        subtitle: str | None = None,
        description: str | None = None,
        language: str | None = None,
        author: str | None = None,
        image_url: str | None = None,
        category: PodcastCategories | None = None,
        explicit: PodcastExplicit | None = None,
        since: datetime | None = None,
        keep_last: int | None = None,
    ) -> None:
        """Update feed metadata fields; no-op if all metadata fields are None.

        Args:
            feed_id: The feed identifier.
            source_type: Optional new source type.
            title: Optional new title.
            subtitle: Optional new subtitle.
            description: Optional new description.
            language: Optional new language.
            author: Optional new author.
            image_url: Optional new image URL.
            category: Optional new category.
            explicit: Optional new explicit flag.
            since: Optional new since date.
            keep_last: Optional new keep_last value.

        Raises:
            FeedNotFoundError: If the feed is not found.
            DatabaseOperationError: If the database operation fails.
        """
        # Build update dictionary with only non-None values
        updates: dict[str, Any] = {}
        if source_type is not None:
            updates["source_type"] = source_type
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
        if since is not None:
            updates["since"] = since
        if keep_last is not None:
            updates["keep_last"] = keep_last

        # No-op if all fields are None
        if not updates:
            logger.debug(
                "No metadata fields provided for update, skipping.",
                extra={"feed_id": feed_id},
            )
            return

        log_params = {"feed_id": feed_id, "updated_fields": list(updates.keys())}
        logger.debug("Attempting to update feed metadata.", extra=log_params)
        async with self._db.session() as session:
            stmt = update(Feed).where(col(Feed.id) == feed_id).values(**updates)
            try:
                self._db.assert_exactly_one_row_affected(
                    await session.execute(stmt), feed_id=feed_id
                )
            except NotFoundError as e:
                raise FeedNotFoundError("Feed not found.", feed_id=feed_id) from e
            await session.commit()
        logger.debug("Feed metadata updated.", extra=log_params)
