# pyright: reportPrivateUsage=false

"""Tests for SqlalchemyCore functionality."""

from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path

from helpers.alembic import run_migrations
import pytest
import pytest_asyncio
from sqlalchemy import text

from anypod.db.sqlalchemy_core import SqlalchemyCore
from anypod.db.types import Download, DownloadStatus, Feed, SourceType


@pytest_asyncio.fixture
async def db_core(tmp_path: Path) -> AsyncGenerator[SqlalchemyCore]:
    """Provides a SqlalchemyCore instance."""
    # Run Alembic migrations to set up the database schema
    db_path = tmp_path / "anypod.db"
    run_migrations(db_path)

    # Create SqlalchemyCore instance
    core = SqlalchemyCore(db_dir=tmp_path)
    yield core
    await core.close()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_db_and_tables_creates_expected_schema(db_core: SqlalchemyCore):
    """Test that all expected tables and indexes are created."""
    # Verify tables exist
    async with db_core.engine.begin() as conn:
        # Get table names
        result = await conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        )
        tables = [row[0] for row in result]

    assert "download" in tables, "download table should exist"
    assert "feed" in tables, "feed table should exist"

    # Verify indexes exist
    async with db_core.engine.begin() as conn:
        # Get index names
        result = await conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='index' ORDER BY name")
        )
        indexes = [row[0] for row in result]

    # Check for expected indexes (from Download model)
    assert any("idx_feed_status" in idx for idx in indexes), (
        "idx_feed_status should exist"
    )
    assert any("idx_feed_published" in idx for idx in indexes), (
        "idx_feed_published should exist"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_sqlite_triggers_are_created(
    db_core: SqlalchemyCore, subtests: pytest.Subtests
):
    """Test that SQLite triggers for timestamp management are created."""
    async with db_core.engine.begin() as conn:
        result = await conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='trigger' ORDER BY name")
        )
        triggers = [row[0] for row in result]

    expected_triggers = [
        "downloads_update_updated_at",
        "downloads_update_downloaded_at",
        "feeds_update_updated_at",
        "downloads_after_insert_total_downloads",
        "downloads_after_delete_total_downloads",
        "downloads_status_to_downloaded_total_downloads",
        "downloads_status_from_downloaded_total_downloads",
    ]

    for trigger in expected_triggers:
        with subtests.test(msg=f"trigger {trigger} exists"):
            assert trigger in triggers


@pytest.mark.unit
@pytest.mark.asyncio
async def test_event_listener_sets_wal_mode(db_core: SqlalchemyCore):
    """Test that the connect event listener enables WAL mode."""
    # Check journal mode
    async with db_core.engine.begin() as conn:
        result = await conn.execute(text("PRAGMA journal_mode"))
        journal_mode = result.scalar()

    assert journal_mode == "wal", "Journal mode should be WAL"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_event_listener_sets_foreign_keys(db_core: SqlalchemyCore):
    """Test that the connect event listener enables foreign key constraints."""
    # Insert a feed first
    async with db_core.session() as session:
        feed = Feed(
            id="test_feed",
            is_enabled=True,
            source_type=SourceType.CHANNEL,
            source_url="http://example.com",
            last_successful_sync=datetime.now(UTC),
        )
        session.add(feed)
        await session.commit()

    # Try to insert download with invalid feed_id - should fail due to FK constraint
    async with db_core.session() as session:
        download = Download(
            feed_id="invalid_feed",  # This feed doesn't exist
            id="test_download",
            source_url="http://example.com/video",
            title="Test",
            published=datetime.now(UTC),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=60,
            status=DownloadStatus.QUEUED,
        )
        session.add(download)

        # Should raise IntegrityError due to foreign key constraint
        with pytest.raises(Exception) as exc_info:
            await session.commit()

        # SQLAlchemy wraps the sqlite3.IntegrityError
        assert "FOREIGN KEY constraint failed" in str(exc_info.value)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_event_listener_runs_on_each_connection(db_core: SqlalchemyCore):
    """Test that event listener runs on each new connection."""
    # Get multiple connections and verify settings
    for _ in range(3):
        async with db_core.engine.begin() as conn:
            # Check journal mode
            result = await conn.execute(text("PRAGMA journal_mode"))
            assert result.scalar() == "wal"

            # Check foreign keys
            result = await conn.execute(text("PRAGMA foreign_keys"))
            assert result.scalar() == 1
