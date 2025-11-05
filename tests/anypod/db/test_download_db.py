# pyright: reportPrivateUsage=false

"""Tests for the DownloadDatabase and Download model functionality."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from pathlib import Path

from helpers.alembic import run_migrations
import pytest
import pytest_asyncio
from sqlalchemy import update
from sqlmodel import col

from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.sqlalchemy_core import SqlalchemyCore
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.exceptions import DatabaseOperationError, DownloadNotFoundError

# --- Fixtures ---


@pytest_asyncio.fixture
async def db_core(tmp_path: Path) -> AsyncGenerator[SqlalchemyCore]:
    """Provides a SqlalchemyCore instance for testing."""
    # Run Alembic migrations to set up the database schema
    db_path = tmp_path / "anypod.db"
    run_migrations(db_path)

    # Create SqlalchemyCore instance
    core = SqlalchemyCore(db_dir=tmp_path)
    yield core
    await core.close()


@pytest_asyncio.fixture
async def feed_db(db_core: SqlalchemyCore) -> FeedDatabase:
    """Provides a FeedDatabase instance for testing."""
    return FeedDatabase(db_core)


@pytest_asyncio.fixture
async def download_db(db_core: SqlalchemyCore) -> DownloadDatabase:
    """Provides a DownloadDatabase instance for testing."""
    return DownloadDatabase(db_core)


@pytest_asyncio.fixture
async def test_feed(feed_db: FeedDatabase) -> Feed:
    """Provides a test feed that sample downloads can reference."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    feed = Feed(
        id="test_feed",
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="http://example.com/channel",
        last_successful_sync=base_time,
    )
    await feed_db.upsert_feed(feed)
    return feed


@pytest.fixture
def sample_download_queued(test_feed: Feed) -> Download:
    """Provides a sample Download instance for adding to the DB."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    return Download(
        feed_id=test_feed.id,
        id="test_id_1",
        source_url="http://example.com/video1",
        title="Test Video 1",
        published=base_time,
        ext="mp4",
        mime_type="video/mp4",
        duration=120,
        status=DownloadStatus.QUEUED,
        remote_thumbnail_url="http://example.com/thumb1.jpg",
        description="Test video description",
        filesize=0,  # 0 for queued items
        retries=0,
        last_error=None,
    )


@pytest.fixture
def sample_download_upcoming(test_feed: Feed) -> Download:
    """Provides a sample Download instance with UPCOMING status for testing."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    return Download(
        feed_id=test_feed.id,
        id="test_id_upcoming",
        source_url="http://example.com/video_upcoming",
        title="Test Video Upcoming",
        published=base_time,
        ext="mp4",
        mime_type="video/mp4",
        duration=120,
        status=DownloadStatus.UPCOMING,
        remote_thumbnail_url="http://example.com/thumb_upcoming.jpg",
        description="Upcoming video description",
        filesize=0,
        retries=0,
        last_error=None,
    )


# --- Tests ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_add_and_get_download(
    download_db: DownloadDatabase, sample_download_queued: Download
):
    """Test adding a new download and then retrieving it."""
    await download_db.upsert_download(sample_download_queued)

    retrieved_download = await download_db.get_download_by_id(
        feed_id=sample_download_queued.feed_id,
        download_id=sample_download_queued.id,
    )

    assert retrieved_download is not None, "Download should be found in DB"
    assert retrieved_download.feed_id == sample_download_queued.feed_id
    assert retrieved_download.id == sample_download_queued.id
    assert retrieved_download.title == sample_download_queued.title
    assert retrieved_download.published == sample_download_queued.published
    assert retrieved_download.ext == sample_download_queued.ext
    assert retrieved_download.duration == sample_download_queued.duration
    assert (
        retrieved_download.remote_thumbnail_url
        == sample_download_queued.remote_thumbnail_url
    )
    assert retrieved_download.status == sample_download_queued.status
    assert retrieved_download.retries == 0, (
        "Retries should be 0 for a new download from fixture"
    )
    assert retrieved_download.last_error is None, (
        "Last_error should be None for a new download from fixture"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_upsert_download_updates_existing(
    download_db: DownloadDatabase, sample_download_queued: Download
):
    """Test that upsert_download updates an existing download instead of raising an error."""
    # Add initial download
    await download_db.upsert_download(sample_download_queued)

    # Create a modified version with the same (feed, id)
    modified_download = Download(
        feed_id=sample_download_queued.feed_id,
        id=sample_download_queued.id,
        source_url="http://example.com/video/v123_updated",
        title="Updated Test Video Title",
        published=sample_download_queued.published + timedelta(hours=1),
        ext="mkv",  # Changed ext
        mime_type="video/x-matroska",  # Changed mime_type
        duration=150,  # Changed duration
        remote_thumbnail_url="http://example.com/thumb/v123_updated.jpg",
        description="Updated description",
        filesize=4096,  # Changed filesize
        status=DownloadStatus.DOWNLOADED,  # Changed status
        retries=1,  # Changed retries
        last_error="An old error",  # Changed last_error
    )

    # Perform upsert with the modified download
    await download_db.upsert_download(
        modified_download
    )  # Should not raise IntegrityError

    # Retrieve and verify
    retrieved_download = await download_db.get_download_by_id(
        feed_id=sample_download_queued.feed_id,
        download_id=sample_download_queued.id,
    )

    assert retrieved_download is not None, "Download should still be found"
    assert retrieved_download.title == modified_download.title
    assert retrieved_download.source_url == modified_download.source_url
    assert retrieved_download.published == modified_download.published
    assert retrieved_download.ext == modified_download.ext
    assert retrieved_download.duration == modified_download.duration
    assert (
        retrieved_download.remote_thumbnail_url
        == modified_download.remote_thumbnail_url
    )
    assert retrieved_download.status == modified_download.status
    assert retrieved_download.retries == modified_download.retries
    assert retrieved_download.last_error == modified_download.last_error


@pytest.mark.unit
@pytest.mark.asyncio
async def test_status_transitions(
    download_db: DownloadDatabase,
    sample_download_queued: Download,
    sample_download_upcoming: Download,
):
    """Test various status transition methods."""
    # Start with an UPCOMING download
    await download_db.upsert_download(sample_download_upcoming)
    feed_id = sample_download_upcoming.feed_id
    dl_id = sample_download_upcoming.id

    # UPCOMING -> QUEUED
    await download_db.mark_as_queued_from_upcoming(feed_id, dl_id)
    download = await download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.QUEUED
    assert download.retries == 0  # Preserved from initial UPCOMING
    assert download.last_error is None  # Preserved from initial UPCOMING

    # QUEUED -> DOWNLOADED
    await download_db.mark_as_downloaded(feed_id, dl_id, "mp4", 1024)
    download = await download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.DOWNLOADED
    assert download.retries == 0, "Retries should be reset on DOWNLOADED"
    assert download.last_error is None, "Error should be cleared on DOWNLOADED"

    # Attempt to bump retries on DOWNLOADED: should increment retries, set last_error, but NOT change status to ERROR
    await download_db.bump_retries(
        feed_id, dl_id, "Simulated error on downloaded item", 1
    )  # max_errors = 1
    download = await download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.DOWNLOADED, (
        "Status should remain DOWNLOADED"
    )
    assert download.retries == 1, (
        "Retries should increment even if status doesn't change"
    )
    assert download.last_error == "Simulated error on downloaded item"

    # DOWNLOADED (with error info) -> REQUEUED
    await download_db.requeue_downloads(feed_id, dl_id)
    download = await download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.QUEUED
    assert download.retries == 0, "Retries should be reset on REQUEUE"
    assert download.last_error is None, "Error should be cleared on REQUEUE"

    # QUEUED -> SKIPPED
    # To test preservation of error/retries, let's set them via bump_retries first
    # (though skip_download itself preserves whatever is there)
    await download_db.bump_retries(feed_id, dl_id, "Error before skip", 3)
    await download_db.skip_download(feed_id, dl_id)
    download = await download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.SKIPPED
    assert download.retries == 1, (
        "Retries should be preserved on SKIP"
    )  # from bump_retries
    assert download.last_error == "Error before skip", (
        "Error should be preserved on SKIP"
    )

    # SKIPPED -> UNSKIP (which re-queues)
    await download_db.requeue_downloads(
        feed_id, dl_id, from_status=DownloadStatus.SKIPPED
    )
    download = await download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.QUEUED
    assert download.retries == 0, "Retries should be reset on UNSKIP (via REQUEUE)"
    assert download.last_error is None, (
        "Error should be cleared on UNSKIP (via REQUEUE)"
    )

    # Set thumbnail_ext before archiving to test it gets cleared
    await download_db.set_thumbnail_extension(feed_id, dl_id, "jpg")
    before_archive = await download_db.get_download_by_id(feed_id, dl_id)
    assert before_archive.thumbnail_ext == "jpg"

    # QUEUED -> ARCHIVED (from a clean QUEUED state)
    await download_db.archive_download(feed_id, dl_id)
    download = await download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.ARCHIVED
    assert download.retries == 0  # Preserved from last requeue
    assert download.last_error is None  # Preserved from last requeue
    assert download.thumbnail_ext is None  # Should be cleared when archived

    # Re-insert a fresh download to test archiving from an ERROR state
    sample_download_queued.status = DownloadStatus.QUEUED
    sample_download_queued.retries = 0
    sample_download_queued.last_error = None
    await download_db.upsert_download(sample_download_queued)
    q_feed_id = sample_download_queued.feed_id
    q_dl_id = sample_download_queued.id

    # Transition to ERROR
    _, _, _ = await download_db.bump_retries(
        q_feed_id, q_dl_id, "Maxed out errors", 1
    )  # Max errors = 1
    download_error_state = await download_db.get_download_by_id(q_feed_id, q_dl_id)
    assert download_error_state.status == DownloadStatus.ERROR
    assert download_error_state.retries == 1
    assert download_error_state.last_error == "Maxed out errors"

    # Set thumbnail_ext before archiving from ERROR state
    await download_db.set_thumbnail_extension(q_feed_id, q_dl_id, "png")
    before_error_archive = await download_db.get_download_by_id(q_feed_id, q_dl_id)
    assert before_error_archive.thumbnail_ext == "png"

    # ERROR -> ARCHIVED
    await download_db.archive_download(q_feed_id, q_dl_id)
    download_archived_from_error = await download_db.get_download_by_id(
        q_feed_id, q_dl_id
    )
    assert download_archived_from_error.status == DownloadStatus.ARCHIVED
    assert download_archived_from_error.retries == 1, "Retries should be preserved"
    assert download_archived_from_error.thumbnail_ext is None  # Should be cleared
    assert download_archived_from_error.last_error == "Maxed out errors", (
        "Error should be preserved"
    )

    # Test non_existent_download for each relevant method
    with pytest.raises(DownloadNotFoundError):
        await download_db.mark_as_queued_from_upcoming("bad", "bad")
    with pytest.raises(DatabaseOperationError):
        await download_db.requeue_downloads("bad", "bad")
    with pytest.raises(DownloadNotFoundError):
        await download_db.mark_as_downloaded("bad", "bad", "mp4", 0)
    with pytest.raises(DownloadNotFoundError):
        await download_db.skip_download("bad", "bad")
    with pytest.raises(DatabaseOperationError):
        await download_db.requeue_downloads(
            "bad", "bad", from_status=DownloadStatus.SKIPPED
        )
    with pytest.raises(DownloadNotFoundError):
        await download_db.archive_download("bad", "bad")

    # Test mark_as_downloaded from a non-QUEUED state (e.g., UPCOMING)
    await download_db.upsert_download(sample_download_upcoming)  # dl_id is now UPCOMING
    with pytest.raises(DownloadNotFoundError):
        await download_db.mark_as_downloaded(
            feed_id, sample_download_upcoming.id, "mp4", 1024
        )

    # Test mark_as_downloaded from ERROR state
    # First, set an item to ERROR
    await download_db.upsert_download(sample_download_queued)  # dl_id is now QUEUED
    await download_db.bump_retries(
        sample_download_queued.feed_id,
        sample_download_queued.id,
        "Error to test from",
        1,
    )  # max_errors = 1, so it becomes ERROR
    error_download = await download_db.get_download_by_id(
        sample_download_queued.feed_id, sample_download_queued.id
    )
    assert error_download.status == DownloadStatus.ERROR

    with pytest.raises(DownloadNotFoundError):
        await download_db.mark_as_downloaded(
            sample_download_queued.feed_id,
            sample_download_queued.id,
            "mp4",
            1024,
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_set_thumbnail_extension_sets_and_clears(
    download_db: DownloadDatabase, sample_download_queued: Download
):
    """Verify setting and clearing thumbnail_ext works as expected."""
    # Insert base download (thumbnail_ext defaults to None)
    await download_db.upsert_download(sample_download_queued)

    # Confirm initial state
    before = await download_db.get_download_by_id(
        sample_download_queued.feed_id, sample_download_queued.id
    )
    assert before.thumbnail_ext is None

    # Set to jpg
    await download_db.set_thumbnail_extension(
        sample_download_queued.feed_id, sample_download_queued.id, "jpg"
    )
    after_set = await download_db.get_download_by_id(
        sample_download_queued.feed_id, sample_download_queued.id
    )
    assert after_set.thumbnail_ext == "jpg"

    # Clear back to None
    await download_db.set_thumbnail_extension(
        sample_download_queued.feed_id, sample_download_queued.id, None
    )
    after_clear = await download_db.get_download_by_id(
        sample_download_queued.feed_id, sample_download_queued.id
    )
    assert after_clear.thumbnail_ext is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_set_thumbnail_extension_updates_existing_value(
    download_db: DownloadDatabase, sample_download_queued: Download
):
    """Setting thumbnail_ext again should overwrite the existing value."""
    await download_db.upsert_download(sample_download_queued)

    await download_db.set_thumbnail_extension(
        sample_download_queued.feed_id, sample_download_queued.id, "jpg"
    )
    await download_db.set_thumbnail_extension(
        sample_download_queued.feed_id, sample_download_queued.id, "png"
    )

    updated = await download_db.get_download_by_id(
        sample_download_queued.feed_id, sample_download_queued.id
    )
    assert updated.thumbnail_ext == "png"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_set_thumbnail_extension_nonexistent_download(
    download_db: DownloadDatabase,
):
    """Setting thumbnail_ext on a nonexistent download raises DownloadNotFoundError."""
    with pytest.raises(DownloadNotFoundError):
        await download_db.set_thumbnail_extension("missing_feed", "missing_id", "jpg")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_set_download_logs_persists_value(
    download_db: DownloadDatabase, sample_download_queued: Download
):
    """set_download_logs stores the provided log content for a download."""
    await download_db.upsert_download(sample_download_queued)

    logs = "yt-dlp log output"
    await download_db.set_download_logs(
        sample_download_queued.feed_id,
        sample_download_queued.id,
        logs,
    )

    stored = await download_db.get_download_by_id(
        sample_download_queued.feed_id,
        sample_download_queued.id,
    )
    assert stored.download_logs == logs


@pytest.mark.unit
@pytest.mark.asyncio
async def test_set_download_logs_overwrites_existing_value(
    download_db: DownloadDatabase, sample_download_queued: Download
):
    """set_download_logs replaces previously stored log content."""
    await download_db.upsert_download(sample_download_queued)

    logs1 = "initial logs"
    logs2 = "updated logs"
    await download_db.set_download_logs(
        sample_download_queued.feed_id,
        sample_download_queued.id,
        logs1,
    )
    await download_db.set_download_logs(
        sample_download_queued.feed_id,
        sample_download_queued.id,
        logs2,
    )

    stored = await download_db.get_download_by_id(
        sample_download_queued.feed_id,
        sample_download_queued.id,
    )
    assert stored.download_logs == logs2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_set_download_logs_nonexistent_download_raises(
    download_db: DownloadDatabase,
):
    """set_download_logs raises DownloadNotFoundError when the row is missing."""
    with pytest.raises(DownloadNotFoundError):
        await download_db.set_download_logs("missing_feed", "missing_id", "logs")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_requeue_downloads_multi(
    feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Test requeue_downloads method with single/multiple downloads."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    feed_id = "requeue_test_feed"

    # Create the feed that downloads will reference
    feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url=f"http://example.com/{feed_id}",
        last_successful_sync=base_time,
    )
    await feed_db.upsert_feed(feed)

    # Create multiple downloads in different states
    downloads = [
        Download(
            feed_id=feed_id,
            id="error1",
            published=base_time,
            status=DownloadStatus.ERROR,
            source_url="url",
            title="Error 1",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
            retries=3,
            last_error="Some error",
        ),
        Download(
            feed_id=feed_id,
            id="error2",
            published=base_time,
            status=DownloadStatus.ERROR,
            source_url="url",
            title="Error 2",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
            retries=2,
            last_error="Another error",
        ),
        Download(
            feed_id=feed_id,
            id="skipped1",
            published=base_time,
            status=DownloadStatus.SKIPPED,
            source_url="url",
            title="Skipped 1",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
            retries=1,
            last_error="Skip error",
        ),
        Download(
            feed_id=feed_id,
            id="archived1",
            published=base_time,
            status=DownloadStatus.ARCHIVED,
            source_url="url",
            title="Archived 1",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
            retries=0,
        ),
    ]

    for dl in downloads:
        await download_db.upsert_download(dl)

    # Test requeue_downloads with single string download ID
    count = await download_db.requeue_downloads(feed_id, "error1")
    assert count == 1

    error1 = await download_db.get_download_by_id(feed_id, "error1")
    assert error1.status == DownloadStatus.QUEUED
    assert error1.retries == 0
    assert error1.last_error is None

    # Test requeue_downloads with single download ID in list
    count = await download_db.requeue_downloads(feed_id, ["error2"])
    assert count == 1

    error2 = await download_db.get_download_by_id(feed_id, "error2")
    assert error2.status == DownloadStatus.QUEUED
    assert error2.retries == 0
    assert error2.last_error is None

    # Test requeue_downloads with multiple download IDs
    count = await download_db.requeue_downloads(feed_id, ["skipped1", "archived1"])
    assert count == 2

    skipped1 = await download_db.get_download_by_id(feed_id, "skipped1")
    assert skipped1.status == DownloadStatus.QUEUED
    assert skipped1.retries == 0
    assert skipped1.last_error is None

    archived1 = await download_db.get_download_by_id(feed_id, "archived1")
    assert archived1.status == DownloadStatus.QUEUED
    assert archived1.retries == 0
    assert archived1.last_error is None

    # Test with from_status filter
    # First reset both downloads to SKIPPED
    await download_db.skip_download(feed_id, "error1")
    await download_db.skip_download(feed_id, "error2")

    # Requeue only SKIPPED ones
    count = await download_db.requeue_downloads(
        feed_id, ["error1", "error2"], from_status=DownloadStatus.SKIPPED
    )
    assert count == 2  # Both should be requeued

    error1_recheck = await download_db.get_download_by_id(feed_id, "error1")
    assert error1_recheck.status == DownloadStatus.QUEUED

    error2_recheck = await download_db.get_download_by_id(feed_id, "error2")
    assert error2_recheck.status == DownloadStatus.QUEUED

    # Test empty list
    count = await download_db.requeue_downloads(feed_id, [])
    assert count == 0

    # Test nonexistent download ID without from_status - should fail
    with pytest.raises(DatabaseOperationError):
        await download_db.requeue_downloads(feed_id, ["nonexistent"])


@pytest.mark.unit
@pytest.mark.asyncio
async def test_bulk_requeue_by_status_only_updates_matching(
    feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Bulk requeue applies to all downloads with a given status for a feed."""
    base_time = datetime(2023, 2, 1, 12, 0, 0, tzinfo=UTC)
    feed_id = "bulk_requeue_feed"

    # Create feed
    await feed_db.upsert_feed(
        Feed(
            id=feed_id,
            is_enabled=True,
            source_type=SourceType.CHANNEL,
            source_url=f"http://example.com/{feed_id}",
            last_successful_sync=base_time,
        )
    )

    # Add downloads: two ERROR, one SKIPPED, one QUEUED
    d_error_a = Download(
        feed_id=feed_id,
        id="err_a",
        published=base_time,
        status=DownloadStatus.ERROR,
        source_url="url",
        title="err a",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1,
        duration=1,
        retries=2,
        last_error="boom",
    )
    d_error_b = d_error_a.model_copy(update={"id": "err_b", "retries": 5})
    d_skipped = d_error_a.model_copy(
        update={"id": "skip_1", "status": DownloadStatus.SKIPPED, "last_error": None}
    )
    d_queued = d_error_a.model_copy(
        update={"id": "q_1", "status": DownloadStatus.QUEUED, "last_error": None}
    )

    for dl in (d_error_a, d_error_b, d_skipped, d_queued):
        await download_db.upsert_download(dl)

    # Bulk requeue only ERROR rows
    count = await download_db.requeue_downloads(
        feed_id, None, from_status=DownloadStatus.ERROR
    )
    assert count == 2

    # Verify ERROR rows are now QUEUED and cleared, others unchanged
    err_a = await download_db.get_download_by_id(feed_id, "err_a")
    err_b = await download_db.get_download_by_id(feed_id, "err_b")
    skip_1 = await download_db.get_download_by_id(feed_id, "skip_1")
    q_1 = await download_db.get_download_by_id(feed_id, "q_1")

    for row in (err_a, err_b):
        assert row.status == DownloadStatus.QUEUED
        assert row.retries == 0
        assert row.last_error is None

    assert skip_1.status == DownloadStatus.SKIPPED  # unaffected
    assert q_1.status == DownloadStatus.QUEUED  # unaffected

    # Re-running should be idempotent (no ERROR left)
    count_second = await download_db.requeue_downloads(
        feed_id, None, from_status=DownloadStatus.ERROR
    )
    assert count_second == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_bulk_requeue_requires_status(
    feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Bulk requeue without from_status should raise an error for safety."""
    base_time = datetime(2023, 2, 2, 12, 0, 0, tzinfo=UTC)
    feed_id = "bulk_require_status"

    await feed_db.upsert_feed(
        Feed(
            id=feed_id,
            is_enabled=True,
            source_type=SourceType.CHANNEL,
            source_url=f"http://example.com/{feed_id}",
            last_successful_sync=base_time,
        )
    )

    # Add one download just to have rows in the table
    await download_db.upsert_download(
        Download(
            feed_id=feed_id,
            id="x",
            published=base_time,
            status=DownloadStatus.ERROR,
            source_url="url",
            title="x",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1,
            duration=1,
        )
    )

    with pytest.raises(DatabaseOperationError):
        await download_db.requeue_downloads(feed_id, None)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_downloads_to_prune_by_keep_last(
    feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Test fetching downloads to prune based on 'keep_last'."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    feed1_name = "prune_feed1"

    # Create the feeds that downloads will reference
    for feed_id in [feed1_name, "prune_feed2"]:
        feed = Feed(
            id=feed_id,
            is_enabled=True,
            source_type=SourceType.CHANNEL,
            source_url=f"http://example.com/{feed_id}",
            last_successful_sync=base_time,
        )
        await feed_db.upsert_feed(feed)

    # Mix of statuses and published dates
    dl_f1v1_dl_oldest = Download(
        feed_id=feed1_name,
        id="f1v1_dl_oldest",
        published=base_time - timedelta(days=5),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t1",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )
    dl_f1v2_err_mid1 = Download(
        feed_id=feed1_name,
        id="f1v2_err_mid1",
        published=base_time - timedelta(days=4),
        status=DownloadStatus.ERROR,
        source_url="url",
        title="t2",
        ext="mkv",
        mime_type="video/x-matroska",
        filesize=1024,
        duration=1,
    )
    dl_f1v3_q_mid2 = Download(
        feed_id=feed1_name,
        id="f1v3_q_mid2",
        published=base_time - timedelta(days=3),
        status=DownloadStatus.QUEUED,
        source_url="url",
        title="t3",
        ext="webm",
        mime_type="video/webm",
        filesize=1024,
        duration=1,
    )
    dl_f1v4_dl_newest = Download(
        feed_id=feed1_name,
        id="f1v4_dl_newest",
        published=base_time - timedelta(days=2),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t4",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )
    dl_f1v5_arch = Download(
        feed_id=feed1_name,
        id="f1v5_arch",
        published=base_time - timedelta(days=1),
        status=DownloadStatus.ARCHIVED,
        source_url="url",
        title="t5",
        ext="mp3",
        mime_type="audio/mpeg",
        filesize=1024,
        duration=1,
    )
    dl_f1v6_upcoming_older = Download(  # New UPCOMING download, older
        feed_id=feed1_name,
        id="f1v6_upcoming_older",
        published=base_time - timedelta(days=6),  # Older than f1v1_dl_oldest
        status=DownloadStatus.UPCOMING,
        source_url="url",
        title="t6_upcoming",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )
    dl_f1v7_skipped = Download(
        feed_id=feed1_name,
        id="f1v7_skipped",
        published=base_time - timedelta(days=7),  # Very old
        status=DownloadStatus.SKIPPED,
        source_url="url",
        title="t7_skipped",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )
    # download for another feed, should be ignored
    dl_f2v1_dl = Download(
        feed_id="prune_feed2",
        id="f2v1_dl",
        published=base_time - timedelta(days=3),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t_f2",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    downloads_to_add = [
        dl_f1v1_dl_oldest,
        dl_f1v2_err_mid1,
        dl_f1v3_q_mid2,
        dl_f1v4_dl_newest,
        dl_f1v5_arch,
        dl_f2v1_dl,
        dl_f1v6_upcoming_older,
        dl_f1v7_skipped,
    ]
    for dl in downloads_to_add:
        await download_db.upsert_download(dl)

    # Keep 2: f1v4_dl_newest (kept), f1v3_q_mid2 (kept)
    # Pruned: f1v2_err_mid1, f1v1_dl_oldest, f1v6_upcoming_older
    # Ignored from pruning: f1v5_arch, f1v7_skipped
    prune_keep2 = await download_db.get_downloads_to_prune_by_keep_last(
        feed_id=feed1_name, keep_last=2
    )
    assert len(prune_keep2) == 3, (
        "Should identify 3 downloads to prune (f1v1_dl_oldest, f1v2_err_mid1, f1v6_upcoming_older)"
    )
    pruned_ids_keep2 = sorted([row.id for row in prune_keep2])
    assert "f1v7_skipped" not in pruned_ids_keep2, (
        "SKIPPED download f1v7_skipped should NOT be in the prune list"
    )
    assert (
        sorted(["f1v1_dl_oldest", "f1v2_err_mid1", "f1v6_upcoming_older"])
        == pruned_ids_keep2
    )

    # Keep 5: All non-ARCHIVED and non-SKIPPED downloads are kept
    # (f1v4_dl_newest, f1v3_q_mid2, f1v2_err_mid1, f1v1_dl_oldest, f1v6_upcoming_older)
    # There are 5 such downloads. f1v5_arch and f1v7_skipped are ignored.
    prune_keep5 = await download_db.get_downloads_to_prune_by_keep_last(
        feed_id=feed1_name, keep_last=5
    )
    assert len(prune_keep5) == 0, (
        "Should identify 0 if keep_last >= total non-ARCHIVED/non-SKIPPED downloads for feed"
    )

    prune_keep0 = await download_db.get_downloads_to_prune_by_keep_last(
        feed_id=feed1_name, keep_last=0
    )
    assert len(prune_keep0) == 0, "Should return 0 if keep_last is 0"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_downloads_to_prune_by_since(
    feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Test fetching downloads to prune by 'since' date."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    feed_id = "prune_since_feed"
    other_feed_id = "other_feed"

    # Create the feeds that downloads will reference
    for feed_name in [feed_id, other_feed_id]:
        feed = Feed(
            id=feed_name,
            is_enabled=True,
            source_type=SourceType.CHANNEL,
            source_url=f"http://example.com/{feed_name}",
            last_successful_sync=base_time,
        )
        await feed_db.upsert_feed(feed)

    dl_ps_v1_older_dl = Download(
        feed_id=feed_id,
        id="ps_v1_older_dl",
        published=base_time - timedelta(days=5),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t1",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )
    dl_ps_v2_mid_err = Download(
        feed_id=feed_id,
        id="ps_v2_mid_err",
        published=base_time - timedelta(days=2),
        status=DownloadStatus.ERROR,
        source_url="url",
        title="t2",
        ext="mkv",
        mime_type="video/x-matroska",
        filesize=1024,
        duration=1,
    )
    dl_ps_v3_newer_q = Download(
        feed_id=feed_id,
        id="ps_v3_newer_q",
        published=base_time + timedelta(days=1),
        status=DownloadStatus.QUEUED,
        source_url="url",
        title="t3",
        ext="webm",
        mime_type="video/webm",
        filesize=1024,
        duration=1,
    )
    dl_ps_v4_arch = Download(
        feed_id=feed_id,
        id="ps_v4_arch",
        published=base_time,
        status=DownloadStatus.ARCHIVED,
        source_url="url",
        title="t4",
        ext="mp3",
        mime_type="audio/mpeg",
        filesize=1024,
        duration=1,
    )
    dl_ps_v5_upcoming_ancient = Download(  # New UPCOMING download, very old
        feed_id=feed_id,
        id="ps_v5_upcoming_ancient",
        published=base_time - timedelta(days=10),  # Much older than cutoff
        status=DownloadStatus.UPCOMING,
        source_url="url",
        title="t5_upcoming",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )
    dl_ps_v6_skipped_ancient = Download(
        feed_id=feed_id,
        id="ps_v6_skipped_ancient",
        published=base_time - timedelta(days=12),  # Much older than cutoff
        status=DownloadStatus.SKIPPED,
        source_url="url",
        title="t6_skipped",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )

    # download for another feed
    dl_other_v1_older_dl = Download(
        feed_id=other_feed_id,
        id="other_v1_older_dl",
        published=base_time - timedelta(days=5),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t_other",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )
    downloads_to_add = [
        dl_ps_v1_older_dl,
        dl_ps_v2_mid_err,
        dl_ps_v3_newer_q,
        dl_ps_v4_arch,
        dl_ps_v5_upcoming_ancient,
        dl_ps_v6_skipped_ancient,
        dl_other_v1_older_dl,
    ]
    for dl in downloads_to_add:
        await download_db.upsert_download(dl)

    # Prune downloads older than 'base_time - 3 days' for feed_id
    # Candidates for pruning (ignoring ARCHIVED and SKIPPED):
    # - ps_v1_older_dl (day -5) -> YES
    # - ps_v2_mid_err (day -2) -> NO (not older than day -3)
    # - ps_v3_newer_q (day +1) -> NO
    # - ps_v4_arch (day 0) -> NO (archived)
    # - ps_v5_upcoming_ancient (day -10) -> YES
    # - ps_v6_skipped_ancient (day -12) -> NO
    since_cutoff_1 = base_time - timedelta(days=3)
    pruned_1 = await download_db.get_downloads_to_prune_by_since(
        feed_id=feed_id, since=since_cutoff_1
    )
    assert len(pruned_1) == 2
    pruned_ids_1 = sorted([row.id for row in pruned_1])
    assert pruned_ids_1 == sorted(["ps_v1_older_dl", "ps_v5_upcoming_ancient"])
    assert "ps_v6_skipped_ancient" not in pruned_ids_1, (
        "SKIPPED download should not be pruned by since_cutoff_1"
    )

    # Prune downloads older than 'base_time + 2 days' for feed_id
    # Candidates for pruning (ignoring ARCHIVED and SKIPPED):
    # - ps_v1_older_dl (day -5) -> YES
    # - ps_v2_mid_err (day -2) -> YES
    # - ps_v3_newer_q (day +1) -> YES
    # - ps_v4_arch (day 0) -> NO (archived)
    # - ps_v5_upcoming_ancient (day -10) -> YES
    # - ps_v6_skipped_ancient (day -12) -> NO
    since_cutoff_2 = base_time + timedelta(days=2)
    pruned_2 = await download_db.get_downloads_to_prune_by_since(
        feed_id=feed_id, since=since_cutoff_2
    )
    pruned_ids_2 = sorted([row.id for row in pruned_2])
    assert len(pruned_2) == 4
    assert pruned_ids_2 == sorted(
        ["ps_v1_older_dl", "ps_v2_mid_err", "ps_v3_newer_q", "ps_v5_upcoming_ancient"]
    )
    assert "ps_v6_skipped_ancient" not in pruned_ids_2, (
        "SKIPPED download should not be pruned by since_cutoff_2"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_downloads_by_status(
    feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Test fetching downloads by various statuses, including offset and limit."""
    base_time = datetime(2023, 1, 15, 12, 0, 0, tzinfo=UTC)
    feed1 = "status_feed1"
    feed2 = "status_feed2"
    feed3 = "feed3_no_match"

    # Create the feeds that downloads will reference
    for feed_id in [feed1, feed2, feed3]:
        feed = Feed(
            id=feed_id,
            is_enabled=True,
            source_type=SourceType.CHANNEL,
            source_url=f"http://example.com/{feed_id}",
            last_successful_sync=base_time,
        )
        await feed_db.upsert_feed(feed)

    # oldest, feed2, ERROR
    dl_f2e1 = Download(
        feed_id=feed2,
        id="f2e1",
        published=base_time - timedelta(days=3),
        status=DownloadStatus.ERROR,
        last_error="Feed 2 error",
        source_url="url",
        title="t_err_f2",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )
    # middle, feed1, ERROR
    dl_f1e1_old = Download(
        feed_id=feed1,
        id="f1e1_old",
        published=base_time - timedelta(days=2),
        status=DownloadStatus.ERROR,
        last_error="Old error 1",
        source_url="url",
        title="t_err_f1_old",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )
    # newest, feed1, ERROR
    dl_f1e2_new = Download(
        feed_id=feed1,
        id="f1e2_new",
        published=base_time - timedelta(days=1),
        status=DownloadStatus.ERROR,
        last_error="New error 1",
        source_url="url",
        title="t_err_f1_new",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )
    # Other status downloads for noise and testing other statuses
    dl_f1q1 = Download(
        feed_id=feed1,
        id="f1q1",
        published=base_time,
        status=DownloadStatus.QUEUED,
        source_url="url",
        title="t_q_f1",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )
    dl_f3d1 = Download(
        feed_id=feed3,
        id="f3d1",
        published=base_time,
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t_d_f3",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )
    dl_f1_upcoming = Download(
        feed_id=feed1,
        id="f1upcoming",
        published=base_time - timedelta(days=4),  # Older than errors
        status=DownloadStatus.UPCOMING,
        source_url="url",
        title="t_up_f1",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )
    dl_f2_upcoming = Download(
        feed_id=feed2,
        id="f2upcoming",
        published=base_time - timedelta(days=5),
        status=DownloadStatus.UPCOMING,
        source_url="url",
        title="t_up_f2",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
    )
    downloads_to_add = [
        dl_f2e1,  # ERROR
        dl_f1e1_old,  # ERROR
        dl_f1e2_new,  # ERROR
        dl_f1q1,  # QUEUED
        dl_f3d1,  # DOWNLOADED
        dl_f1_upcoming,  # UPCOMING
        dl_f2_upcoming,  # UPCOMING
    ]
    for dl_data in downloads_to_add:
        await download_db.upsert_download(dl_data)

    # Expected order for all errors: f2e1 (oldest), f1e1_old, f1e2_new (newest)
    all_errors = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR
    )
    assert len(all_errors) == 3, "Should fetch all 3 ERROR downloads"
    assert [row.id for row in all_errors] == ["f1e2_new", "f1e1_old", "f2e1"]
    for row in all_errors:
        assert row.status == DownloadStatus.ERROR

    feed1_errors = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR, feed_id=feed1
    )
    assert len(feed1_errors) == 2, "Should fetch 2 ERROR downloads for feed1"
    assert [row.id for row in feed1_errors] == ["f1e2_new", "f1e1_old"]

    limited_errors = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR, limit=1, offset=0
    )
    assert len(limited_errors) == 1, "Should fetch only 1 error with limit=1"
    assert limited_errors[0].id == "f1e2_new", "Should be the newest overall error"

    # --- Test UPCOMING status ---
    # Expected order for all UPCOMING: f2upcoming (oldest), f1upcoming (newest)
    all_upcoming = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.UPCOMING
    )
    assert len(all_upcoming) == 2, "Should fetch all 2 UPCOMING downloads"
    assert [row.id for row in all_upcoming] == ["f1upcoming", "f2upcoming"]
    for row in all_upcoming:
        assert row.status == DownloadStatus.UPCOMING

    feed1_upcoming = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.UPCOMING, feed_id=feed1
    )
    assert len(feed1_upcoming) == 1, "Should fetch 1 UPCOMING download for feed1"
    assert feed1_upcoming[0].id == "f1upcoming"

    # --- Test QUEUED status (feed1 has one) ---
    feed1_queued = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.QUEUED, feed_id=feed1
    )
    assert len(feed1_queued) == 1, "Should fetch 1 QUEUED download for feed1"
    assert feed1_queued[0].id == "f1q1"

    # --- Test DOWNLOADED status (feed3_no_match has one) ---
    downloaded_f3 = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.DOWNLOADED, feed_id=feed3
    )
    assert len(downloaded_f3) == 1, "Should fetch 1 DOWNLOADED for feed3_no_match"
    assert downloaded_f3[0].id == "f3d1"

    # --- Test with offset and limit for UPCOMING ---
    upcoming_limit1_offset1 = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.UPCOMING, limit=1, offset=1
    )  # Skips f1upcoming, gets f2upcoming
    assert len(upcoming_limit1_offset1) == 1
    assert upcoming_limit1_offset1[0].id == "f2upcoming"

    # --- Test no downloads for a status/feed combination ---
    no_feed2_queued = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.QUEUED, feed_id=feed2
    )
    assert len(no_feed2_queued) == 0

    no_skipped_any_feed = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.SKIPPED
    )
    assert len(no_skipped_any_feed) == 0

    # --- Test offset greater than number of downloads ---
    offset_too_high_error = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR, limit=100, offset=5
    )
    assert len(offset_too_high_error) == 0

    # --- Test no downloads at all (after updating existing downloads to a different status) ---
    await download_db.requeue_downloads(feed1, "f1e1_old")  # ERROR -> QUEUED
    await download_db.requeue_downloads(feed1, "f1e2_new")  # ERROR -> QUEUED
    await download_db.skip_download(
        feed_id=feed2, download_id="f2e1"
    )  # ERROR -> SKIPPED

    all_errors_cleared = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR
    )
    assert len(all_errors_cleared) == 0, (
        "Should return empty list when all ERROR downloads are cleared"
    )

    # Test original upcoming downloads are also gone if we query for them after updates
    await download_db.mark_as_queued_from_upcoming(
        feed_id=feed1, download_id="f1upcoming"
    )  # UPCOMING -> QUEUED
    await download_db.mark_as_queued_from_upcoming(
        feed_id=feed2, download_id="f2upcoming"
    )  # UPCOMING -> QUEUED
    all_upcoming_cleared = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.UPCOMING
    )
    assert len(all_upcoming_cleared) == 0, (
        "Should return empty list when all UPCOMING downloads are cleared"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_downloads_by_status_date_filtering(
    feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Test get_downloads_by_status with published_after and published_before date filtering."""
    base_time = datetime(2023, 6, 15, 12, 0, 0, tzinfo=UTC)
    feed_id = "date_filter_feed"

    # Create the feed that downloads will reference
    feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url=f"http://example.com/{feed_id}",
        last_successful_sync=base_time,
    )
    await feed_db.upsert_feed(feed)

    # Create downloads with different published dates
    downloads = [
        Download(
            feed_id=feed_id,
            id="old_queued",
            published=base_time - timedelta(days=10),  # Very old
            status=DownloadStatus.QUEUED,
            source_url="url",
            title="Old Queued",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
        ),
        Download(
            feed_id=feed_id,
            id="mid_queued",
            published=base_time - timedelta(days=5),  # Middle
            status=DownloadStatus.QUEUED,
            source_url="url",
            title="Mid Queued",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
        ),
        Download(
            feed_id=feed_id,
            id="recent_queued",
            published=base_time - timedelta(days=1),  # Recent
            status=DownloadStatus.QUEUED,
            source_url="url",
            title="Recent Queued",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
        ),
        Download(
            feed_id=feed_id,
            id="future_queued",
            published=base_time + timedelta(days=2),  # Future
            status=DownloadStatus.QUEUED,
            source_url="url",
            title="Future Queued",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
        ),
        Download(
            feed_id=feed_id,
            id="old_downloaded",
            published=base_time - timedelta(days=8),  # Old, different status
            status=DownloadStatus.DOWNLOADED,
            source_url="url",
            title="Old Downloaded",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
        ),
    ]

    for dl in downloads:
        await download_db.upsert_download(dl)

    # Test published_after filtering (inclusive)
    after_cutoff = base_time - timedelta(days=6)
    after_filtered = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.QUEUED,
        feed_id=feed_id,
        published_after=after_cutoff,
    )
    after_ids = [dl.id for dl in after_filtered]
    # Should include: mid_queued (day -5), recent_queued (day -1), future_queued (day +2)
    # Should exclude: old_queued (day -10)
    assert len(after_filtered) == 3
    assert set(after_ids) == {"mid_queued", "recent_queued", "future_queued"}

    # Test published_before filtering (exclusive)
    before_cutoff = base_time - timedelta(days=2)
    before_filtered = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.QUEUED,
        feed_id=feed_id,
        published_before=before_cutoff,
    )
    before_ids = [dl.id for dl in before_filtered]
    # Should include: old_queued (day -10), mid_queued (day -5)
    # Should exclude: recent_queued (day -1), future_queued (day +2)
    assert len(before_filtered) == 2
    assert set(before_ids) == {"old_queued", "mid_queued"}

    # Test both published_after and published_before together (date range)
    range_after = base_time - timedelta(days=6)
    range_before = base_time - timedelta(days=2)
    range_filtered = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.QUEUED,
        feed_id=feed_id,
        published_after=range_after,
        published_before=range_before,
    )
    range_ids = [dl.id for dl in range_filtered]
    # Should include: mid_queued (day -5)
    # Should exclude: old_queued (day -10, before range), recent_queued (day -1, after range), future_queued (day +2, after range)
    assert len(range_filtered) == 1
    assert range_ids == ["mid_queued"]

    # Test date filtering with different status
    old_downloaded_filtered = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.DOWNLOADED,
        feed_id=feed_id,
        published_before=base_time - timedelta(days=7),
    )
    assert len(old_downloaded_filtered) == 1
    assert old_downloaded_filtered[0].id == "old_downloaded"

    # Test date filtering that excludes everything
    far_future_after = base_time + timedelta(days=10)
    empty_filtered = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.QUEUED,
        feed_id=feed_id,
        published_after=far_future_after,
    )
    assert len(empty_filtered) == 0

    # Test date filtering that includes everything
    far_past_after = base_time - timedelta(days=20)
    all_filtered = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.QUEUED,
        feed_id=feed_id,
        published_after=far_past_after,
    )
    assert len(all_filtered) == 4  # All 4 QUEUED downloads

    # Test date filtering works correctly with ordering (newest first)
    ordered_filtered = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.QUEUED,
        feed_id=feed_id,
        published_after=base_time - timedelta(days=6),
    )
    ordered_ids = [dl.id for dl in ordered_filtered]
    # Should be ordered newest first: future_queued, recent_queued, mid_queued
    assert ordered_ids == ["future_queued", "recent_queued", "mid_queued"]

    # Test date filtering with limit and offset
    limited_filtered = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.QUEUED,
        feed_id=feed_id,
        published_after=base_time - timedelta(days=6),
        limit=2,
        offset=1,
    )
    limited_ids = [dl.id for dl in limited_filtered]
    # Should skip first (future_queued) and get next 2: recent_queued, mid_queued
    assert limited_ids == ["recent_queued", "mid_queued"]


# --- Tests for DownloadDatabase.count_downloads_by_status ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_count_downloads_by_status(
    feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Test counting downloads by status with and without feed filtering."""
    base_time = datetime(2023, 1, 15, 12, 0, 0, tzinfo=UTC)
    feed1 = "count_feed1"
    feed2 = "count_feed2"

    # Create the feeds that downloads will reference
    for feed_id in [feed1, feed2]:
        feed = Feed(
            id=feed_id,
            is_enabled=True,
            source_type=SourceType.CHANNEL,
            source_url=f"http://example.com/{feed_id}",
            last_successful_sync=base_time,
        )
        await feed_db.upsert_feed(feed)

    # Add downloads with various statuses
    downloads = [
        Download(
            feed_id=feed1,
            id="q1",
            published=base_time,
            status=DownloadStatus.QUEUED,
            source_url="url",
            title="queued1",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
        ),
        Download(
            feed_id=feed1,
            id="q2",
            published=base_time,
            status=DownloadStatus.QUEUED,
            source_url="url",
            title="queued2",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
        ),
        Download(
            feed_id=feed1,
            id="d1",
            published=base_time,
            status=DownloadStatus.DOWNLOADED,
            source_url="url",
            title="downloaded1",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
        ),
        Download(
            feed_id=feed1,
            id="u1",
            published=base_time,
            status=DownloadStatus.UPCOMING,
            source_url="url",
            title="upcoming1",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
        ),
        Download(
            feed_id=feed2,
            id="q3",
            published=base_time,
            status=DownloadStatus.QUEUED,
            source_url="url",
            title="queued3",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
        ),
        Download(
            feed_id=feed2,
            id="e1",
            published=base_time,
            status=DownloadStatus.ERROR,
            source_url="url",
            title="error1",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
        ),
        Download(
            feed_id=feed2,
            id="u2",
            published=base_time,
            status=DownloadStatus.UPCOMING,
            source_url="url",
            title="upcoming2",
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=1,
        ),
    ]

    for dl in downloads:
        await download_db.upsert_download(dl)

    # Test counting single status across all feeds
    queued_count = await download_db.count_downloads_by_status(DownloadStatus.QUEUED)
    assert queued_count == 3

    downloaded_count = await download_db.count_downloads_by_status(
        DownloadStatus.DOWNLOADED
    )
    assert downloaded_count == 1

    error_count = await download_db.count_downloads_by_status(DownloadStatus.ERROR)
    assert error_count == 1

    upcoming_count = await download_db.count_downloads_by_status(
        DownloadStatus.UPCOMING
    )
    assert upcoming_count == 2

    # Test counting single status with feed filter
    feed1_queued = await download_db.count_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed1
    )
    assert feed1_queued == 2

    feed2_queued = await download_db.count_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed2
    )
    assert feed2_queued == 1

    feed1_downloaded = await download_db.count_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed1
    )
    assert feed1_downloaded == 1

    feed2_downloaded = await download_db.count_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed2
    )
    assert feed2_downloaded == 0

    # Test counting multiple statuses across all feeds
    active_count = await download_db.count_downloads_by_status(
        [DownloadStatus.QUEUED, DownloadStatus.UPCOMING]
    )
    assert active_count == 5  # 3 QUEUED + 2 UPCOMING

    processing_count = await download_db.count_downloads_by_status(
        [DownloadStatus.QUEUED, DownloadStatus.DOWNLOADED, DownloadStatus.ERROR]
    )
    assert processing_count == 5  # 3 QUEUED + 1 DOWNLOADED + 1 ERROR

    # Test counting multiple statuses with feed filter
    feed1_active = await download_db.count_downloads_by_status(
        [DownloadStatus.QUEUED, DownloadStatus.UPCOMING], feed_id=feed1
    )
    assert feed1_active == 3  # 2 QUEUED + 1 UPCOMING

    feed2_active = await download_db.count_downloads_by_status(
        [DownloadStatus.QUEUED, DownloadStatus.UPCOMING], feed_id=feed2
    )
    assert feed2_active == 2  # 1 QUEUED + 1 UPCOMING

    # Test counting status that doesn't exist
    archived_count = await download_db.count_downloads_by_status(
        DownloadStatus.ARCHIVED
    )
    assert archived_count == 0

    # Test counting multiple statuses where none exist
    empty_count = await download_db.count_downloads_by_status(
        [DownloadStatus.ARCHIVED, DownloadStatus.SKIPPED]
    )
    assert empty_count == 0

    # Test with empty list (edge case)
    empty_list_count = await download_db.count_downloads_by_status([])
    assert empty_list_count == 0


# --- Tests for Download.from_row ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_upsert_download_with_default_timestamps(
    feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Test that SQLModel default_factory sets timestamps when not explicitly provided."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)

    # Create the feed that downloads will reference
    feed = Feed(
        id="test_feed",
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="http://example.com/test_feed",
        last_successful_sync=base_time,
    )
    await feed_db.upsert_feed(feed)

    # Create a download without explicitly setting timestamp fields
    # This allows SQLModel default_factory to set them
    download_with_defaults = Download(
        feed_id=feed.id,
        id="test_default_timestamps",
        source_url="http://example.com/video1",
        title="Test Video with Default Timestamps",
        published=base_time,
        ext="mp4",
        mime_type="video/mp4",
        duration=120,
        status=DownloadStatus.QUEUED,
        filesize=1024,
        retries=0,
    )

    # Insert the download
    await download_db.upsert_download(download_with_defaults)

    # Retrieve and verify timestamps were set by SQLModel default_factory
    retrieved = await download_db.get_download_by_id(feed.id, "test_default_timestamps")

    assert retrieved.discovered_at is not None, (
        "discovered_at should be set by SQLModel default_factory"
    )
    assert retrieved.updated_at is not None, (
        "updated_at should be set by SQLModel default_factory"
    )

    # Verify the timestamps are reasonable (within a few seconds of now)
    current_time = datetime.now(UTC)
    time_diff_discovered = abs((current_time - retrieved.discovered_at).total_seconds())
    time_diff_updated = abs((current_time - retrieved.updated_at).total_seconds())

    assert time_diff_discovered < 5, "discovered_at should be close to current time"
    assert time_diff_updated < 5, "updated_at should be close to current time"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_database_triggers_update_timestamps(
    feed_db: FeedDatabase, download_db: DownloadDatabase
):
    """Test that database triggers correctly update timestamps with proper timezone format."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)

    # First create the feed
    feed = Feed(
        id="test_feed",
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="http://example.com/channel",
        last_successful_sync=base_time,
    )
    await feed_db.upsert_feed(feed)

    # Create a download with explicit timestamps
    download = Download(
        feed_id="test_feed",
        id="test_triggers",
        source_url="http://example.com/video1",
        title="Test Video for Triggers",
        published=base_time,
        ext="mp4",
        mime_type="video/mp4",
        duration=120,
        status=DownloadStatus.QUEUED,
        filesize=1024,
        retries=0,
    )

    # Insert the download
    await download_db.upsert_download(download)

    # Record the initial timestamps
    initial_retrieved = await download_db.get_download_by_id(
        "test_feed", "test_triggers"
    )
    initial_updated_at = initial_retrieved.updated_at
    initial_downloaded_at = initial_retrieved.downloaded_at

    # Verify initial state
    assert initial_downloaded_at is None, "downloaded_at should be None initially"

    # Wait a moment to ensure timestamp differences
    await asyncio.sleep(1.2)

    # Update the download to trigger the updated_at trigger
    # Change the title to trigger UPDATE (title is not in exclude_columns)
    async with download_db._db.session() as session:
        stmt = (
            update(Download)
            .where(
                col(Download.feed_id) == "test_feed",
                col(Download.id) == "test_triggers",
            )
            .values(title="Updated Title")
        )
        await session.execute(stmt)
        await session.commit()

    # Check that updated_at was changed by trigger
    after_update = await download_db.get_download_by_id("test_feed", "test_triggers")
    assert after_update.updated_at is not None, "updated_at should not be None"
    assert after_update.updated_at != initial_updated_at, (
        "updated_at should be changed by trigger"
    )
    assert after_update.updated_at.tzinfo == UTC, "updated_at should have UTC timezone"
    assert after_update.downloaded_at is None, "downloaded_at should still be None"

    # Wait a moment to ensure timestamp differences
    await asyncio.sleep(1.2)

    # Mark as downloaded to trigger the downloaded_at trigger
    await download_db.mark_as_downloaded("test_feed", "test_triggers", "mp4", 2048)

    # Check that both updated_at and downloaded_at were set by triggers
    final_retrieved = await download_db.get_download_by_id("test_feed", "test_triggers")

    # Both should be set and have proper timezone
    assert final_retrieved.updated_at is not None, "updated_at should not be None"
    assert final_retrieved.updated_at != after_update.updated_at, (
        "updated_at should be updated again"
    )
    assert final_retrieved.updated_at.tzinfo == UTC, (
        "updated_at should have UTC timezone"
    )
    assert final_retrieved.downloaded_at is not None, (
        "downloaded_at should be set by trigger"
    )
    assert final_retrieved.downloaded_at.tzinfo == UTC, (
        "downloaded_at should have UTC timezone"
    )

    # Verify timestamps are reasonable (within a few seconds of now)
    current_time = datetime.now(UTC)
    updated_diff = abs((current_time - final_retrieved.updated_at).total_seconds())
    downloaded_diff = abs(
        (current_time - final_retrieved.downloaded_at).total_seconds()
    )

    assert updated_diff < 5, "updated_at should be close to current time"
    assert downloaded_diff < 5, "downloaded_at should be close to current time"


@pytest.mark.asyncio
async def test_bump_retries_non_existent_download(download_db: DownloadDatabase):
    """Test bumping retries for a download that doesn't exist."""
    with pytest.raises(DownloadNotFoundError) as e:
        await download_db.bump_retries(
            feed_id="non_existent_feed",
            download_id="non_existent_id",
            error_message="Test error",
            max_allowed_errors=3,
        )
    assert e.value.feed_id == "non_existent_feed"
    assert e.value.download_id == "non_existent_id"


@pytest.mark.asyncio
async def test_bump_retries_below_max(
    download_db: DownloadDatabase, sample_download_upcoming: Download
):
    """Test bumping retries when new count is below max_allowed_errors."""
    await download_db.upsert_download(sample_download_upcoming)

    new_retries, final_status, did_transition = await download_db.bump_retries(
        feed_id=sample_download_upcoming.feed_id,
        download_id=sample_download_upcoming.id,
        error_message="First error",
        max_allowed_errors=3,
    )

    assert new_retries == 1
    assert final_status == DownloadStatus.UPCOMING
    assert not did_transition

    updated_row = await download_db.get_download_by_id(
        sample_download_upcoming.feed_id, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 1
    assert updated_row.status == DownloadStatus.UPCOMING
    assert updated_row.last_error == "First error"


@pytest.mark.asyncio
async def test_bump_retries_reaches_max(
    download_db: DownloadDatabase, sample_download_upcoming: Download
):
    """Test bumping retries when new count reaches max_allowed_errors."""
    sample_download_upcoming.retries = 2
    await download_db.upsert_download(sample_download_upcoming)

    new_retries, final_status, did_transition = await download_db.bump_retries(
        feed_id=sample_download_upcoming.feed_id,
        download_id=sample_download_upcoming.id,
        error_message="Third error - reaching max",
        max_allowed_errors=3,
    )

    assert new_retries == 3
    assert final_status == DownloadStatus.ERROR
    assert did_transition

    updated_row = await download_db.get_download_by_id(
        sample_download_upcoming.feed_id, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 3
    assert updated_row.status == DownloadStatus.ERROR
    assert updated_row.last_error == "Third error - reaching max"


@pytest.mark.asyncio
async def test_bump_retries_exceeds_max(
    download_db: DownloadDatabase, sample_download_upcoming: Download
):
    """Test bumping retries when new count would exceed max_allowed_errors."""
    sample_download_upcoming.retries = 3
    await download_db.upsert_download(sample_download_upcoming)

    new_retries, final_status, did_transition = await download_db.bump_retries(
        feed_id=sample_download_upcoming.feed_id,
        download_id=sample_download_upcoming.id,
        error_message="Fourth error - exceeds max from upcoming",
        max_allowed_errors=3,
    )

    assert new_retries == 4
    assert final_status == DownloadStatus.ERROR
    assert did_transition

    updated_row = await download_db.get_download_by_id(
        sample_download_upcoming.feed_id, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 4
    assert updated_row.status == DownloadStatus.ERROR
    assert updated_row.last_error == "Fourth error - exceeds max from upcoming"


@pytest.mark.asyncio
async def test_bump_retries_already_error_status(
    download_db: DownloadDatabase, sample_download_upcoming: Download
):
    """Test bumping retries when the item is already in ERROR state."""
    # Modify fixture to be in ERROR state initially
    sample_download_upcoming.status = DownloadStatus.ERROR
    sample_download_upcoming.retries = 5
    sample_download_upcoming.last_error = "Previous major error"
    await download_db.upsert_download(sample_download_upcoming)

    (
        new_retries,
        final_status,
        did_transition_to_error_state,
    ) = await download_db.bump_retries(
        feed_id=sample_download_upcoming.feed_id,
        download_id=sample_download_upcoming.id,
        error_message="Another error while already in ERROR state",
        max_allowed_errors=3,
    )

    assert new_retries == 6
    assert final_status == DownloadStatus.ERROR
    assert not did_transition_to_error_state  # Should be False as it was already ERROR

    updated_row = await download_db.get_download_by_id(
        sample_download_upcoming.feed_id, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 6
    assert updated_row.status == DownloadStatus.ERROR
    assert updated_row.last_error == "Another error while already in ERROR state"


@pytest.mark.asyncio
async def test_bump_retries_max_errors_is_one(
    download_db: DownloadDatabase, sample_download_upcoming: Download
):
    """Test bumping retries transitions to ERROR immediately if max_allowed_errors is 1."""
    await download_db.upsert_download(sample_download_upcoming)

    new_retries, final_status, did_transition = await download_db.bump_retries(
        feed_id=sample_download_upcoming.feed_id,
        download_id=sample_download_upcoming.id,
        error_message="First and only error allowed",
        max_allowed_errors=1,
    )

    assert new_retries == 1
    assert final_status == DownloadStatus.ERROR
    assert did_transition

    updated_row = await download_db.get_download_by_id(
        sample_download_upcoming.feed_id, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 1
    assert updated_row.status == DownloadStatus.ERROR
    assert updated_row.last_error == "First and only error allowed"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_bump_retries_downloaded_item_does_not_become_error(
    download_db: DownloadDatabase,
    sample_download_queued: Download,
):
    """Test that bumping retries on a DOWNLOADED item does not change its status to ERROR, even if retries reach max."""
    # Setup: Insert a download and mark it as DOWNLOADED
    feed_id = sample_download_queued.feed_id
    dl_id = sample_download_queued.id
    await download_db.upsert_download(sample_download_queued)  # Initially QUEUED
    await download_db.mark_as_downloaded(feed_id, dl_id, "mp4", 1024)

    download = await download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.DOWNLOADED
    assert download.retries == 0

    # Bump retries enough times to exceed max_allowed_errors
    max_errors = 2
    new_retries, final_status, did_transition = await download_db.bump_retries(
        feed_id, dl_id, "Error 1 on downloaded", max_errors
    )
    assert new_retries == 1
    assert final_status == DownloadStatus.DOWNLOADED
    assert not did_transition

    new_retries, final_status, did_transition = await download_db.bump_retries(
        feed_id, dl_id, "Error 2 on downloaded (max reached)", max_errors
    )
    assert new_retries == 2
    assert final_status == DownloadStatus.DOWNLOADED  # Should still be DOWNLOADED
    assert not did_transition  # Should not transition to ERROR

    updated_download = await download_db.get_download_by_id(feed_id, dl_id)
    assert updated_download.status == DownloadStatus.DOWNLOADED
    assert updated_download.retries == 2
    assert updated_download.last_error == "Error 2 on downloaded (max reached)"

    # One more bump, still should not change status
    new_retries, final_status, did_transition = await download_db.bump_retries(
        feed_id, dl_id, "Error 3 on downloaded (exceeds max)", max_errors
    )
    assert new_retries == 3
    assert final_status == DownloadStatus.DOWNLOADED
    assert not did_transition

    updated_download = await download_db.get_download_by_id(feed_id, dl_id)
    assert updated_download.status == DownloadStatus.DOWNLOADED
    assert updated_download.retries == 3


# --- Tests validating computed total_downloads column ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_total_downloads_computed_column(
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
):
    """Verify that total_downloads is computed correctly based on DOWNLOADED status."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)

    feed = Feed(
        id="feed_computed_test",
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="http://example.com/channel",
        last_successful_sync=base_time,
    )
    await feed_db.upsert_feed(feed)

    # Initially should be 0
    assert (await feed_db.get_feed_by_id(feed.id)).total_downloads == 0

    # Add a QUEUED download - should not count
    queued_download = Download(
        feed_id=feed.id,
        id="queued_video",
        source_url="http://example.com/queued_video",
        title="Queued Video",
        published=base_time,
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=60,
        status=DownloadStatus.QUEUED,
    )
    await download_db.upsert_download(queued_download)
    assert (await feed_db.get_feed_by_id(feed.id)).total_downloads == 0

    # Add a DOWNLOADED download - should count
    downloaded_download = Download(
        feed_id=feed.id,
        id="downloaded_video",
        source_url="http://example.com/downloaded_video",
        title="Downloaded Video",
        published=base_time,
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=60,
        status=DownloadStatus.DOWNLOADED,
    )
    await download_db.upsert_download(downloaded_download)
    assert (await feed_db.get_feed_by_id(feed.id)).total_downloads == 1

    # Add another DOWNLOADED download - should count
    downloaded_download2 = Download(
        feed_id=feed.id,
        id="downloaded_video2",
        source_url="http://example.com/downloaded_video2",
        title="Downloaded Video 2",
        published=base_time,
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=60,
        status=DownloadStatus.DOWNLOADED,
    )
    await download_db.upsert_download(downloaded_download2)
    assert (await feed_db.get_feed_by_id(feed.id)).total_downloads == 2

    # Archive one downloaded item - should reduce count
    await download_db.archive_download(feed.id, "downloaded_video")
    assert (await feed_db.get_feed_by_id(feed.id)).total_downloads == 1
