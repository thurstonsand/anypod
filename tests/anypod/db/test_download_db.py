# pyright: reportPrivateUsage=false

"""Tests for the DownloadDatabase and Download model functionality."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
import sqlite3
from typing import Any

import pytest

from anypod.db import DownloadDatabase
from anypod.db.download import Download
from anypod.db.download_status import DownloadStatus
from anypod.exceptions import DatabaseOperationError, DownloadNotFoundError

# --- Fixtures ---


@pytest.fixture
def download_db() -> Iterator[DownloadDatabase]:
    """Provides a DownloadDatabase instance for testing with a temporary in-memory database."""
    # db_path = tmp_path / "test.db"
    download_db = DownloadDatabase(db_path=None, memory_name="test_db")
    yield download_db
    download_db.close()  # Ensure connection is closed after test


@pytest.fixture
def sample_download_queued() -> Download:
    """Provides a sample Download instance for adding to the DB."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    return Download(
        feed="test_feed",
        id="test_id_1",
        source_url="http://example.com/video1",
        title="Test Video 1",
        published=base_time,
        ext="mp4",
        mime_type="video/mp4",
        duration=120,
        status=DownloadStatus.QUEUED,
        thumbnail="http://example.com/thumb1.jpg",
        description="Test video description",
        filesize=0,  # 0 for queued items
        retries=0,
        last_error=None,
        discovered_at=base_time,
        updated_at=base_time,
    )


@pytest.fixture
def sample_download_row_data() -> dict[str, Any]:
    """Provides raw data for a sample Download object, simulating a DB row."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    return {
        "feed": "test_feed",
        "id": "test_id_123",
        "source_url": "http://example.com/video/123",
        "title": "Test Video Title",
        "published": base_time.isoformat(),  # Stored as ISO string in DB
        "ext": "mp4",
        "mime_type": "video/mp4",
        "duration": 120,
        "thumbnail": "http://example.com/thumb/123.jpg",
        "description": "Test video description from DB",
        "filesize": 0,
        "status": str(DownloadStatus.QUEUED),
        "discovered_at": base_time.isoformat(),
        "updated_at": base_time.isoformat(),
        "retries": 0,
        "last_error": None,
    }


@pytest.fixture
def sample_download_upcoming() -> Download:
    """Provides a sample Download instance with UPCOMING status for testing."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    return Download(
        feed="test_feed",
        id="test_id_upcoming",
        source_url="http://example.com/video_upcoming",
        title="Test Video Upcoming",
        published=base_time,
        ext="mp4",
        mime_type="video/mp4",
        duration=120,
        status=DownloadStatus.UPCOMING,
        thumbnail="http://example.com/thumb_upcoming.jpg",
        description="Upcoming video description",
        filesize=0,
        retries=0,
        last_error=None,
        discovered_at=base_time,
        updated_at=base_time,
    )


# --- Tests ---


def test_download_status_enum():
    """Test DownloadStatus enum values and string conversion."""
    assert str(DownloadStatus.UPCOMING) == "upcoming"
    assert str(DownloadStatus.QUEUED) == "queued"
    assert str(DownloadStatus.DOWNLOADED) == "downloaded"
    assert str(DownloadStatus.ERROR) == "error"
    assert str(DownloadStatus.SKIPPED) == "skipped"
    assert str(DownloadStatus.ARCHIVED) == "archived"

    # Test enum creation from string
    assert DownloadStatus("upcoming") == DownloadStatus.UPCOMING
    assert DownloadStatus("queued") == DownloadStatus.QUEUED
    assert DownloadStatus("downloaded") == DownloadStatus.DOWNLOADED
    assert DownloadStatus("error") == DownloadStatus.ERROR
    assert DownloadStatus("skipped") == DownloadStatus.SKIPPED
    assert DownloadStatus("archived") == DownloadStatus.ARCHIVED


@pytest.mark.unit
def test_download_equality_and_hash(sample_download_queued: Download):
    """Test equality and hashability of Download objects."""
    download1_v1 = sample_download_queued
    download2_v1_same_key = Download(
        feed=sample_download_queued.feed,  # Use same feed from fixture
        id=sample_download_queued.id,  # Use same ID from fixture
        source_url="http://example.com/video/v123_alt",
        title="Test Video Title One Alt",
        published=datetime(2023, 1, 1, 13, 0, 0, tzinfo=UTC),
        ext="mkv",
        mime_type="video/x-matroska",
        duration=130,
        thumbnail="http://example.com/thumb/v123_alt.jpg",
        description="Alt description",
        filesize=2048,
        status=DownloadStatus.DOWNLOADED,  # Different status
        retries=1,  # Different retries
        last_error="some error",  # Different error
        discovered_at=datetime(2023, 1, 1, 13, 5, 0, tzinfo=UTC),
        updated_at=datetime(2023, 1, 1, 13, 5, 0, tzinfo=UTC),
    )
    download3_v2_diff_id = Download(
        feed=sample_download_queued.feed,
        id="v456",  # Different id
        source_url="http://example.com/video/v456",
        title="Test Video Title Two",
        published=datetime(2023, 1, 2, 12, 0, 0, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        duration=180,
        thumbnail="http://example.com/thumb/v456.jpg",
        description="Another description",
        filesize=0,
        status=DownloadStatus.QUEUED,
        discovered_at=datetime(2023, 1, 2, 12, 5, 0, tzinfo=UTC),
        updated_at=datetime(2023, 1, 2, 12, 5, 0, tzinfo=UTC),
    )
    download4_feed2_v1_diff_feed = Download(
        feed="another_feed",  # Different feed
        id="v123",
        source_url="http://example.com/video/another_v123",
        title="Another Feed Video",
        published=datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        duration=120,
        thumbnail="http://example.com/thumb/another_v123.jpg",
        description="Different feed description",
        filesize=0,
        status=DownloadStatus.QUEUED,
        discovered_at=datetime(2023, 1, 1, 12, 5, 0, tzinfo=UTC),
        updated_at=datetime(2023, 1, 1, 12, 5, 0, tzinfo=UTC),
    )

    # Test equality
    assert download1_v1 == download2_v1_same_key, (
        "Downloads with same feed/id should be equal"
    )
    assert download1_v1 != download3_v2_diff_id, (
        "Downloads with different id should not be equal"
    )
    assert download1_v1 != download4_feed2_v1_diff_feed, (
        "Downloads with different feed should not be equal"
    )

    # Test hashability (equal objects must have equal hashes)
    assert hash(download1_v1) == hash(download2_v1_same_key), (
        "Hashes of equal Download objects should be equal"
    )

    # Test usage in a set
    download_set = {
        download1_v1,
        download2_v1_same_key,
        download3_v2_diff_id,
        download4_feed2_v1_diff_feed,
    }
    # Should contain 3 unique downloads based on (feed, id)
    assert len(download_set) == 3, (
        "Set should contain 3 unique downloads based on (feed,id)"
    )
    assert download1_v1 in download_set
    assert download2_v1_same_key in download_set  # Treated as same as download1_v1
    assert download3_v2_diff_id in download_set
    assert download4_feed2_v1_diff_feed in download_set


@pytest.mark.unit
def test_download_db_initialization_and_schema(download_db: DownloadDatabase):
    """Test that the schema (tables and indices) is created upon first DB interaction."""
    conn: sqlite3.Connection = download_db._db.db.conn  # type: ignore
    assert conn is not None, "Connection should have been established"

    cursor: sqlite3.Cursor | None = None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='downloads';"
        )
        table = cursor.fetchone()
        assert table is not None, "'downloads' table should have been created"
        assert table[0] == "downloads"

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_feed_status';"
        )
        index = cursor.fetchone()
        assert index is not None, "'idx_feed_status' index should have been created"
        assert index[0] == "idx_feed_status"
    finally:
        if cursor:
            cursor.close()


@pytest.mark.unit
def test_add_and_get_download(
    download_db: DownloadDatabase, sample_download_queued: Download
):
    """Test adding a new download and then retrieving it."""
    download_db.upsert_download(sample_download_queued)

    retrieved_download = download_db.get_download_by_id(
        feed=sample_download_queued.feed,
        id=sample_download_queued.id,
    )

    assert retrieved_download is not None, "Download should be found in DB"
    assert retrieved_download.feed == sample_download_queued.feed
    assert retrieved_download.id == sample_download_queued.id
    assert retrieved_download.title == sample_download_queued.title
    assert retrieved_download.published == sample_download_queued.published
    assert retrieved_download.ext == sample_download_queued.ext
    assert retrieved_download.duration == sample_download_queued.duration
    assert retrieved_download.thumbnail == sample_download_queued.thumbnail
    assert retrieved_download.status == sample_download_queued.status
    assert retrieved_download.retries == 0, (
        "Retries should be 0 for a new download from fixture"
    )
    assert retrieved_download.last_error is None, (
        "Last_error should be None for a new download from fixture"
    )


@pytest.mark.unit
def test_upsert_download_updates_existing(
    download_db: DownloadDatabase, sample_download_queued: Download
):
    """Test that upsert_download updates an existing download instead of raising an error."""
    # Add initial download
    download_db.upsert_download(sample_download_queued)

    # Create a modified version with the same (feed, id)
    modified_download = Download(
        feed=sample_download_queued.feed,
        id=sample_download_queued.id,
        source_url="http://example.com/video/v123_updated",
        title="Updated Test Video Title",
        published=sample_download_queued.published + timedelta(hours=1),
        ext="mkv",  # Changed ext
        mime_type="video/x-matroska",  # Changed mime_type
        duration=150,  # Changed duration
        thumbnail="http://example.com/thumb/v123_updated.jpg",
        description="Updated description",
        filesize=4096,  # Changed filesize
        status=DownloadStatus.DOWNLOADED,  # Changed status
        retries=1,  # Changed retries
        last_error="An old error",  # Changed last_error
        discovered_at=sample_download_queued.published + timedelta(hours=1, minutes=5),
        updated_at=sample_download_queued.published + timedelta(hours=1, minutes=5),
    )

    # Perform upsert with the modified download
    download_db.upsert_download(modified_download)  # Should not raise IntegrityError

    # Retrieve and verify
    retrieved_download = download_db.get_download_by_id(
        feed=sample_download_queued.feed,
        id=sample_download_queued.id,
    )

    assert retrieved_download is not None, "Download should still be found"
    assert retrieved_download.title == modified_download.title
    assert retrieved_download.source_url == modified_download.source_url
    assert retrieved_download.published == modified_download.published
    assert retrieved_download.ext == modified_download.ext
    assert retrieved_download.duration == modified_download.duration
    assert retrieved_download.thumbnail == modified_download.thumbnail
    assert retrieved_download.status == modified_download.status
    assert retrieved_download.retries == modified_download.retries
    assert retrieved_download.last_error == modified_download.last_error


@pytest.mark.unit
def test_status_transitions(
    download_db: DownloadDatabase,
    sample_download_queued: Download,
    sample_download_upcoming: Download,
):
    """Test various status transition methods."""
    # Start with an UPCOMING download
    download_db.upsert_download(sample_download_upcoming)
    feed_id = sample_download_upcoming.feed
    dl_id = sample_download_upcoming.id

    # UPCOMING -> QUEUED
    download_db.mark_as_queued_from_upcoming(feed_id, dl_id)
    download = download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.QUEUED
    assert download.retries == 0  # Preserved from initial UPCOMING
    assert download.last_error is None  # Preserved from initial UPCOMING

    # QUEUED -> DOWNLOADED
    download_db.mark_as_downloaded(feed_id, dl_id, "mp4", 1024)
    download = download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.DOWNLOADED
    assert download.retries == 0, "Retries should be reset on DOWNLOADED"
    assert download.last_error is None, "Error should be cleared on DOWNLOADED"

    # Attempt to bump retries on DOWNLOADED: should increment retries, set last_error, but NOT change status to ERROR
    download_db.bump_retries(
        feed_id, dl_id, "Simulated error on downloaded item", 1
    )  # max_errors = 1
    download = download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.DOWNLOADED, (
        "Status should remain DOWNLOADED"
    )
    assert download.retries == 1, (
        "Retries should increment even if status doesn't change"
    )
    assert download.last_error == "Simulated error on downloaded item"

    # DOWNLOADED (with error info) -> REQUEUED
    download_db.requeue_download(feed_id, dl_id)
    download = download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.QUEUED
    assert download.retries == 0, "Retries should be reset on REQUEUE"
    assert download.last_error is None, "Error should be cleared on REQUEUE"

    # QUEUED -> SKIPPED
    # To test preservation of error/retries, let's set them via bump_retries first
    # (though skip_download itself preserves whatever is there)
    download_db.bump_retries(feed_id, dl_id, "Error before skip", 3)
    download_db.skip_download(feed_id, dl_id)
    download = download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.SKIPPED
    assert download.retries == 1, (
        "Retries should be preserved on SKIP"
    )  # from bump_retries
    assert download.last_error == "Error before skip", (
        "Error should be preserved on SKIP"
    )

    # SKIPPED -> UNSKIP (which re-queues)
    returned_status = download_db.unskip_download(feed_id, dl_id)
    assert returned_status == DownloadStatus.QUEUED
    download = download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.QUEUED
    assert download.retries == 0, "Retries should be reset on UNSKIP (via REQUEUE)"
    assert download.last_error is None, (
        "Error should be cleared on UNSKIP (via REQUEUE)"
    )

    # QUEUED -> ARCHIVED (from a clean QUEUED state)
    download_db.archive_download(feed_id, dl_id)
    download = download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.ARCHIVED
    assert download.retries == 0  # Preserved from last requeue
    assert download.last_error is None  # Preserved from last requeue

    # Re-insert a fresh download to test archiving from an ERROR state
    sample_download_queued.status = DownloadStatus.QUEUED
    sample_download_queued.retries = 0
    sample_download_queued.last_error = None
    download_db.upsert_download(sample_download_queued)
    q_feed_id = sample_download_queued.feed
    q_dl_id = sample_download_queued.id

    # Transition to ERROR
    _, _, _ = download_db.bump_retries(
        q_feed_id, q_dl_id, "Maxed out errors", 1
    )  # Max errors = 1
    download_error_state = download_db.get_download_by_id(q_feed_id, q_dl_id)
    assert download_error_state.status == DownloadStatus.ERROR
    assert download_error_state.retries == 1
    assert download_error_state.last_error == "Maxed out errors"

    # ERROR -> ARCHIVED
    download_db.archive_download(q_feed_id, q_dl_id)
    download_archived_from_error = download_db.get_download_by_id(q_feed_id, q_dl_id)
    assert download_archived_from_error.status == DownloadStatus.ARCHIVED
    assert download_archived_from_error.retries == 1, "Retries should be preserved"
    assert download_archived_from_error.last_error == "Maxed out errors", (
        "Error should be preserved"
    )

    # Test non_existent_download for each relevant method
    with pytest.raises(DownloadNotFoundError):
        download_db.mark_as_queued_from_upcoming("bad", "bad")
    with pytest.raises(DownloadNotFoundError):
        download_db.requeue_download("bad", "bad")
    with pytest.raises(DownloadNotFoundError):
        download_db.mark_as_downloaded("bad", "bad", "mp4", 0)
    with pytest.raises(DownloadNotFoundError):
        download_db.skip_download("bad", "bad")
    with pytest.raises(DownloadNotFoundError):
        download_db.unskip_download("bad", "bad")
    with pytest.raises(DownloadNotFoundError):
        download_db.archive_download("bad", "bad")

    # Test mark_as_downloaded from a non-QUEUED state (e.g., UPCOMING)
    download_db.upsert_download(sample_download_upcoming)  # dl_id is now UPCOMING
    with pytest.raises(DatabaseOperationError, match="Download status is not QUEUED"):
        download_db.mark_as_downloaded(
            feed_id, sample_download_upcoming.id, "mp4", 1024
        )

    # Test mark_as_downloaded from ERROR state
    # First, set an item to ERROR
    download_db.upsert_download(sample_download_queued)  # dl_id is now QUEUED
    download_db.bump_retries(
        sample_download_queued.feed,
        sample_download_queued.id,
        "Error to test from",
        1,
    )  # max_errors = 1, so it becomes ERROR
    error_download = download_db.get_download_by_id(
        sample_download_queued.feed, sample_download_queued.id
    )
    assert error_download.status == DownloadStatus.ERROR

    with pytest.raises(DatabaseOperationError, match="Download status is not QUEUED"):
        download_db.mark_as_downloaded(
            sample_download_queued.feed,
            sample_download_queued.id,
            "mp4",
            1024,
        )


@pytest.mark.unit
def test_get_downloads_to_prune_by_keep_last(download_db: DownloadDatabase):
    """Test fetching downloads to prune based on 'keep_last'."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    feed1_name = "prune_feed1"

    # Mix of statuses and published dates
    dl_f1v1_dl_oldest = Download(
        feed=feed1_name,
        id="f1v1_dl_oldest",
        published=base_time - timedelta(days=5),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t1",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    dl_f1v2_err_mid1 = Download(
        feed=feed1_name,
        id="f1v2_err_mid1",
        published=base_time - timedelta(days=4),
        status=DownloadStatus.ERROR,
        source_url="url",
        title="t2",
        ext="mkv",
        mime_type="video/x-matroska",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    dl_f1v3_q_mid2 = Download(
        feed=feed1_name,
        id="f1v3_q_mid2",
        published=base_time - timedelta(days=3),
        status=DownloadStatus.QUEUED,
        source_url="url",
        title="t3",
        ext="webm",
        mime_type="video/webm",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    dl_f1v4_dl_newest = Download(
        feed=feed1_name,
        id="f1v4_dl_newest",
        published=base_time - timedelta(days=2),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t4",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    dl_f1v5_arch = Download(
        feed=feed1_name,
        id="f1v5_arch",
        published=base_time - timedelta(days=1),
        status=DownloadStatus.ARCHIVED,
        source_url="url",
        title="t5",
        ext="mp3",
        mime_type="audio/mpeg",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    dl_f1v6_upcoming_older = Download(  # New UPCOMING download, older
        feed=feed1_name,
        id="f1v6_upcoming_older",
        published=base_time - timedelta(days=6),  # Older than f1v1_dl_oldest
        status=DownloadStatus.UPCOMING,
        source_url="url",
        title="t6_upcoming",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    dl_f1v7_skipped = Download(
        feed=feed1_name,
        id="f1v7_skipped",
        published=base_time - timedelta(days=7),  # Very old
        status=DownloadStatus.SKIPPED,
        source_url="url",
        title="t7_skipped",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    # download for another feed, should be ignored
    dl_f2v1_dl = Download(
        feed="prune_feed2",
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
        download_db.upsert_download(dl)

    # Keep 2: f1v4_dl_newest (kept), f1v3_q_mid2 (kept)
    # Pruned: f1v2_err_mid1, f1v1_dl_oldest, f1v6_upcoming_older
    # Ignored from pruning: f1v5_arch, f1v7_skipped
    prune_keep2 = download_db.get_downloads_to_prune_by_keep_last(
        feed=feed1_name, keep_last=2
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
    prune_keep5 = download_db.get_downloads_to_prune_by_keep_last(
        feed=feed1_name, keep_last=5
    )
    assert len(prune_keep5) == 0, (
        "Should identify 0 if keep_last >= total non-ARCHIVED/non-SKIPPED downloads for feed"
    )

    prune_keep0 = download_db.get_downloads_to_prune_by_keep_last(
        feed=feed1_name, keep_last=0
    )
    assert len(prune_keep0) == 0, "Should return 0 if keep_last is 0"


@pytest.mark.unit
def test_get_downloads_to_prune_by_since(download_db: DownloadDatabase):
    """Test fetching downloads to prune by 'since' date."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    feed_id = "prune_since_feed"

    dl_ps_v1_older_dl = Download(
        feed=feed_id,
        id="ps_v1_older_dl",
        published=base_time - timedelta(days=5),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t1",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    dl_ps_v2_mid_err = Download(
        feed=feed_id,
        id="ps_v2_mid_err",
        published=base_time - timedelta(days=2),
        status=DownloadStatus.ERROR,
        source_url="url",
        title="t2",
        ext="mkv",
        mime_type="video/x-matroska",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    dl_ps_v3_newer_q = Download(
        feed=feed_id,
        id="ps_v3_newer_q",
        published=base_time + timedelta(days=1),
        status=DownloadStatus.QUEUED,
        source_url="url",
        title="t3",
        ext="webm",
        mime_type="video/webm",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    dl_ps_v4_arch = Download(
        feed=feed_id,
        id="ps_v4_arch",
        published=base_time,
        status=DownloadStatus.ARCHIVED,
        source_url="url",
        title="t4",
        ext="mp3",
        mime_type="audio/mpeg",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    dl_ps_v5_upcoming_ancient = Download(  # New UPCOMING download, very old
        feed=feed_id,
        id="ps_v5_upcoming_ancient",
        published=base_time - timedelta(days=10),  # Much older than cutoff
        status=DownloadStatus.UPCOMING,
        source_url="url",
        title="t5_upcoming",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    dl_ps_v6_skipped_ancient = Download(
        feed=feed_id,
        id="ps_v6_skipped_ancient",
        published=base_time - timedelta(days=12),  # Much older than cutoff
        status=DownloadStatus.SKIPPED,
        source_url="url",
        title="t6_skipped",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )

    # download for another feed
    dl_other_v1_older_dl = Download(
        feed="other_feed",
        id="other_v1_older_dl",
        published=base_time - timedelta(days=5),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t_other",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
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
        download_db.upsert_download(dl)

    # Prune downloads older than 'base_time - 3 days' for feed_id
    # Candidates for pruning (ignoring ARCHIVED and SKIPPED):
    # - ps_v1_older_dl (day -5) -> YES
    # - ps_v2_mid_err (day -2) -> NO (not older than day -3)
    # - ps_v3_newer_q (day +1) -> NO
    # - ps_v4_arch (day 0) -> NO (archived)
    # - ps_v5_upcoming_ancient (day -10) -> YES
    # - ps_v6_skipped_ancient (day -12) -> NO
    since_cutoff_1 = base_time - timedelta(days=3)
    pruned_1 = download_db.get_downloads_to_prune_by_since(
        feed=feed_id, since=since_cutoff_1
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
    pruned_2 = download_db.get_downloads_to_prune_by_since(
        feed=feed_id, since=since_cutoff_2
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
def test_get_downloads_by_status(download_db: DownloadDatabase):
    """Test fetching downloads by various statuses, including offset and limit."""
    base_time = datetime(2023, 1, 15, 12, 0, 0, tzinfo=UTC)
    feed1 = "status_feed1"
    feed2 = "status_feed2"

    # oldest, feed2, ERROR
    dl_f2e1 = Download(
        feed=feed2,
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
        discovered_at=base_time,
        updated_at=base_time,
    )
    # middle, feed1, ERROR
    dl_f1e1_old = Download(
        feed=feed1,
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
        discovered_at=base_time,
        updated_at=base_time,
    )
    # newest, feed1, ERROR
    dl_f1e2_new = Download(
        feed=feed1,
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
        discovered_at=base_time,
        updated_at=base_time,
    )
    # Other status downloads for noise and testing other statuses
    dl_f1q1 = Download(
        feed=feed1,
        id="f1q1",
        published=base_time,
        status=DownloadStatus.QUEUED,
        source_url="url",
        title="t_q_f1",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    dl_f3d1 = Download(
        feed="feed3_no_match",
        id="f3d1",
        published=base_time,
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t_d_f3",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    dl_f1_upcoming = Download(
        feed=feed1,
        id="f1upcoming",
        published=base_time - timedelta(days=4),  # Older than errors
        status=DownloadStatus.UPCOMING,
        source_url="url",
        title="t_up_f1",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
    )
    dl_f2_upcoming = Download(
        feed=feed2,
        id="f2upcoming",
        published=base_time - timedelta(days=5),
        status=DownloadStatus.UPCOMING,
        source_url="url",
        title="t_up_f2",
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024,
        duration=1,
        discovered_at=base_time,
        updated_at=base_time,
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
        download_db.upsert_download(dl_data)

    # Expected order for all errors: f2e1 (oldest), f1e1_old, f1e2_new (newest)
    all_errors = download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR
    )
    assert len(all_errors) == 3, "Should fetch all 3 ERROR downloads"
    assert [row.id for row in all_errors] == ["f1e2_new", "f1e1_old", "f2e1"]
    for row in all_errors:
        assert row.status == DownloadStatus.ERROR

    feed1_errors = download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR, feed=feed1
    )
    assert len(feed1_errors) == 2, "Should fetch 2 ERROR downloads for feed1"
    assert [row.id for row in feed1_errors] == ["f1e2_new", "f1e1_old"]

    limited_errors = download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR, limit=1, offset=0
    )
    assert len(limited_errors) == 1, "Should fetch only 1 error with limit=1"
    assert limited_errors[0].id == "f1e2_new", "Should be the newest overall error"

    # --- Test UPCOMING status ---
    # Expected order for all UPCOMING: f2upcoming (oldest), f1upcoming (newest)
    all_upcoming = download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.UPCOMING
    )
    assert len(all_upcoming) == 2, "Should fetch all 2 UPCOMING downloads"
    assert [row.id for row in all_upcoming] == ["f1upcoming", "f2upcoming"]
    for row in all_upcoming:
        assert row.status == DownloadStatus.UPCOMING

    feed1_upcoming = download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.UPCOMING, feed=feed1
    )
    assert len(feed1_upcoming) == 1, "Should fetch 1 UPCOMING download for feed1"
    assert feed1_upcoming[0].id == "f1upcoming"

    # --- Test QUEUED status (feed1 has one) ---
    feed1_queued = download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.QUEUED, feed=feed1
    )
    assert len(feed1_queued) == 1, "Should fetch 1 QUEUED download for feed1"
    assert feed1_queued[0].id == "f1q1"

    # --- Test DOWNLOADED status (feed3_no_match has one) ---
    downloaded_f3 = download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.DOWNLOADED, feed="feed3_no_match"
    )
    assert len(downloaded_f3) == 1, "Should fetch 1 DOWNLOADED for feed3_no_match"
    assert downloaded_f3[0].id == "f3d1"

    # --- Test with offset and limit for UPCOMING ---
    upcoming_limit1_offset1 = download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.UPCOMING, limit=1, offset=1
    )  # Skips f1upcoming, gets f2upcoming
    assert len(upcoming_limit1_offset1) == 1
    assert upcoming_limit1_offset1[0].id == "f2upcoming"

    # --- Test no downloads for a status/feed combination ---
    no_feed2_queued = download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.QUEUED, feed=feed2
    )
    assert len(no_feed2_queued) == 0

    no_skipped_any_feed = download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.SKIPPED
    )
    assert len(no_skipped_any_feed) == 0

    # --- Test offset greater than number of downloads ---
    offset_too_high_error = download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR, limit=100, offset=5
    )
    assert len(offset_too_high_error) == 0

    # --- Test no downloads at all (after updating existing downloads to a different status) ---
    download_db.requeue_download(feed=feed1, id="f1e1_old")  # ERROR -> QUEUED
    download_db.requeue_download(feed=feed1, id="f1e2_new")  # ERROR -> QUEUED
    download_db.skip_download(feed=feed2, id="f2e1")  # ERROR -> SKIPPED

    all_errors_cleared = download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR
    )
    assert len(all_errors_cleared) == 0, (
        "Should return empty list when all ERROR downloads are cleared"
    )

    # Test original upcoming downloads are also gone if we query for them after updates
    download_db.mark_as_queued_from_upcoming(
        feed=feed1, id="f1upcoming"
    )  # UPCOMING -> QUEUED
    download_db.mark_as_queued_from_upcoming(
        feed=feed2, id="f2upcoming"
    )  # UPCOMING -> QUEUED
    all_upcoming_cleared = download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.UPCOMING
    )
    assert len(all_upcoming_cleared) == 0, (
        "Should return empty list when all UPCOMING downloads are cleared"
    )


# --- Tests for Download.from_row ---


@pytest.mark.unit
def test_upsert_download_with_none_timestamps(download_db: DownloadDatabase):
    """Test that database defaults are applied when discovered_at/updated_at are None."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)

    # Create a download with None timestamp fields
    download_with_none_timestamps = Download(
        feed="test_feed",
        id="test_none_timestamps",
        source_url="http://example.com/video1",
        title="Test Video with None Timestamps",
        published=base_time,
        ext="mp4",
        mime_type="video/mp4",
        duration=120,
        status=DownloadStatus.QUEUED,
        filesize=1024,
        retries=0,
        # These should be None, triggering database defaults
        discovered_at=None,
        updated_at=None,
    )

    # Insert the download
    download_db.upsert_download(download_with_none_timestamps)

    # Retrieve and verify timestamps were set by database
    retrieved = download_db.get_download_by_id("test_feed", "test_none_timestamps")

    assert retrieved.discovered_at is not None, (
        "discovered_at should be set by database default"
    )
    assert retrieved.updated_at is not None, (
        "updated_at should be set by database default"
    )

    # Verify the timestamps are reasonable (within a few seconds of now)
    current_time = datetime.now(UTC)
    time_diff_discovered = abs((current_time - retrieved.discovered_at).total_seconds())
    time_diff_updated = abs((current_time - retrieved.updated_at).total_seconds())

    assert time_diff_discovered < 5, "discovered_at should be close to current time"
    assert time_diff_updated < 5, "updated_at should be close to current time"


@pytest.mark.unit
def test_database_triggers_update_timestamps(download_db: DownloadDatabase):
    """Test that database triggers correctly update timestamps with proper timezone format."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)

    # Create a download with explicit timestamps
    download = Download(
        feed="test_feed",
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
        discovered_at=base_time,
        updated_at=base_time,
    )

    # Insert the download
    download_db.upsert_download(download)

    # Record the initial timestamps
    initial_retrieved = download_db.get_download_by_id("test_feed", "test_triggers")
    initial_updated_at = initial_retrieved.updated_at
    initial_downloaded_at = initial_retrieved.downloaded_at

    # Verify initial state
    assert initial_downloaded_at is None, "downloaded_at should be None initially"

    # Wait a moment to ensure timestamp differences
    import time

    time.sleep(0.1)

    # Update the download to trigger the updated_at trigger
    # Change the title to trigger UPDATE (title is not in exclude_columns)
    download_db._db.update(
        "downloads", ("test_feed", "test_triggers"), {"title": "Updated Title"}
    )

    # Check that updated_at was changed by trigger
    after_update = download_db.get_download_by_id("test_feed", "test_triggers")
    assert after_update.updated_at is not None, "updated_at should not be None"
    assert after_update.updated_at != initial_updated_at, (
        "updated_at should be changed by trigger"
    )
    assert after_update.updated_at.tzinfo == UTC, "updated_at should have UTC timezone"
    assert after_update.downloaded_at is None, "downloaded_at should still be None"

    # Wait a moment to ensure timestamp differences
    time.sleep(0.1)

    # Mark as downloaded to trigger the downloaded_at trigger
    download_db.mark_as_downloaded("test_feed", "test_triggers", "mp4", 2048)

    # Check that both updated_at and downloaded_at were set by triggers
    final_retrieved = download_db.get_download_by_id("test_feed", "test_triggers")

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


@pytest.mark.unit
def test_download_from_row_success(sample_download_row_data: dict[str, Any]):
    """Test successful conversion of a valid row dictionary to a Download object."""
    # Simulate sqlite3.Row by using a dictionary. Access by string keys is what sqlite3.Row provides.
    mock_row = sample_download_row_data

    # Expected Download object based on the row data
    expected_published_dt = datetime.fromisoformat(mock_row["published"])
    expected_status_enum = DownloadStatus(mock_row["status"])
    expected_download = Download(
        feed=mock_row["feed"],
        id=mock_row["id"],
        source_url=mock_row["source_url"],
        title=mock_row["title"],
        published=expected_published_dt,
        ext=mock_row["ext"],
        mime_type=mock_row["mime_type"],
        filesize=mock_row["filesize"],
        duration=int(mock_row["duration"]),
        thumbnail=mock_row["thumbnail"],
        status=expected_status_enum,
        retries=int(mock_row["retries"]),
        last_error=mock_row["last_error"],
        description=mock_row["description"],
    )

    converted_download = Download.from_row(mock_row)
    assert converted_download == expected_download
    assert converted_download.published == expected_published_dt
    assert converted_download.status == expected_status_enum
    assert converted_download.duration == int(mock_row["duration"])


@pytest.mark.unit
@pytest.mark.parametrize(
    "malformed_field, malformed_value",
    [
        ("published", "not-a-date-string"),
        ("published", None),
        ("status", "unknown_status"),
        ("duration", "not-a-float"),
    ],
)
def test_download_from_row_malformed_data(
    sample_download_row_data: dict[str, Any],
    malformed_field: str,
    malformed_value: Any,
):
    """Test that Download.from_row raises ValueError for malformed data."""
    corrupted_row_data = sample_download_row_data.copy()
    corrupted_row_data[malformed_field] = malformed_value

    with pytest.raises(ValueError):
        Download.from_row(corrupted_row_data)


def test_bump_retries_non_existent_download(download_db: DownloadDatabase):
    """Test bumping retries for a download that doesn't exist."""
    with pytest.raises(DownloadNotFoundError) as e:
        download_db.bump_retries(
            feed_id="non_existent_feed",
            download_id="non_existent_id",
            error_message="Test error",
            max_allowed_errors=3,
        )
    assert e.value.feed_id == "non_existent_feed"
    assert e.value.download_id == "non_existent_id"


def test_bump_retries_below_max(
    download_db: DownloadDatabase, sample_download_upcoming: Download
):
    """Test bumping retries when new count is below max_allowed_errors."""
    download_db.upsert_download(sample_download_upcoming)

    new_retries, final_status, did_transition = download_db.bump_retries(
        feed_id=sample_download_upcoming.feed,
        download_id=sample_download_upcoming.id,
        error_message="First error",
        max_allowed_errors=3,
    )

    assert new_retries == 1
    assert final_status == DownloadStatus.UPCOMING
    assert not did_transition

    updated_row = download_db.get_download_by_id(
        sample_download_upcoming.feed, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 1
    assert updated_row.status == DownloadStatus.UPCOMING
    assert updated_row.last_error == "First error"


def test_bump_retries_reaches_max(
    download_db: DownloadDatabase, sample_download_upcoming: Download
):
    """Test bumping retries when new count reaches max_allowed_errors."""
    sample_download_upcoming.retries = 2
    download_db.upsert_download(sample_download_upcoming)

    new_retries, final_status, did_transition = download_db.bump_retries(
        feed_id=sample_download_upcoming.feed,
        download_id=sample_download_upcoming.id,
        error_message="Third error - reaching max",
        max_allowed_errors=3,
    )

    assert new_retries == 3
    assert final_status == DownloadStatus.ERROR
    assert did_transition

    updated_row = download_db.get_download_by_id(
        sample_download_upcoming.feed, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 3
    assert updated_row.status == DownloadStatus.ERROR
    assert updated_row.last_error == "Third error - reaching max"


def test_bump_retries_exceeds_max(
    download_db: DownloadDatabase, sample_download_upcoming: Download
):
    """Test bumping retries when new count would exceed max_allowed_errors."""
    sample_download_upcoming.retries = 3
    download_db.upsert_download(sample_download_upcoming)

    new_retries, final_status, did_transition = download_db.bump_retries(
        feed_id=sample_download_upcoming.feed,
        download_id=sample_download_upcoming.id,
        error_message="Fourth error - exceeds max from upcoming",
        max_allowed_errors=3,
    )

    assert new_retries == 4
    assert final_status == DownloadStatus.ERROR
    assert did_transition

    updated_row = download_db.get_download_by_id(
        sample_download_upcoming.feed, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 4
    assert updated_row.status == DownloadStatus.ERROR
    assert updated_row.last_error == "Fourth error - exceeds max from upcoming"


def test_bump_retries_already_error_status(
    download_db: DownloadDatabase, sample_download_upcoming: Download
):
    """Test bumping retries when the item is already in ERROR state."""
    # Modify fixture to be in ERROR state initially
    sample_download_upcoming.status = DownloadStatus.ERROR
    sample_download_upcoming.retries = 5
    sample_download_upcoming.last_error = "Previous major error"
    download_db.upsert_download(sample_download_upcoming)

    new_retries, final_status, did_transition_to_error_state = download_db.bump_retries(
        feed_id=sample_download_upcoming.feed,
        download_id=sample_download_upcoming.id,
        error_message="Another error while already in ERROR state",
        max_allowed_errors=3,
    )

    assert new_retries == 6
    assert final_status == DownloadStatus.ERROR
    assert not did_transition_to_error_state  # Should be False as it was already ERROR

    updated_row = download_db.get_download_by_id(
        sample_download_upcoming.feed, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 6
    assert updated_row.status == DownloadStatus.ERROR
    assert updated_row.last_error == "Another error while already in ERROR state"


def test_bump_retries_max_errors_is_one(
    download_db: DownloadDatabase, sample_download_upcoming: Download
):
    """Test bumping retries transitions to ERROR immediately if max_allowed_errors is 1."""
    download_db.upsert_download(sample_download_upcoming)

    new_retries, final_status, did_transition = download_db.bump_retries(
        feed_id=sample_download_upcoming.feed,
        download_id=sample_download_upcoming.id,
        error_message="First and only error allowed",
        max_allowed_errors=1,
    )

    assert new_retries == 1
    assert final_status == DownloadStatus.ERROR
    assert did_transition

    updated_row = download_db.get_download_by_id(
        sample_download_upcoming.feed, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 1
    assert updated_row.status == DownloadStatus.ERROR
    assert updated_row.last_error == "First and only error allowed"


@pytest.mark.unit
def test_bump_retries_downloaded_item_does_not_become_error(
    download_db: DownloadDatabase, sample_download_queued: Download
):
    """Test that bumping retries on a DOWNLOADED item does not change its status to ERROR, even if retries reach max."""
    # Setup: Insert a download and mark it as DOWNLOADED
    feed_id = sample_download_queued.feed
    dl_id = sample_download_queued.id
    download_db.upsert_download(sample_download_queued)  # Initially QUEUED
    download_db.mark_as_downloaded(feed_id, dl_id, "mp4", 1024)

    download = download_db.get_download_by_id(feed_id, dl_id)
    assert download.status == DownloadStatus.DOWNLOADED
    assert download.retries == 0

    # Bump retries enough times to exceed max_allowed_errors
    max_errors = 2
    new_retries, final_status, did_transition = download_db.bump_retries(
        feed_id, dl_id, "Error 1 on downloaded", max_errors
    )
    assert new_retries == 1
    assert final_status == DownloadStatus.DOWNLOADED
    assert not did_transition

    new_retries, final_status, did_transition = download_db.bump_retries(
        feed_id, dl_id, "Error 2 on downloaded (max reached)", max_errors
    )
    assert new_retries == 2
    assert final_status == DownloadStatus.DOWNLOADED  # Should still be DOWNLOADED
    assert not did_transition  # Should not transition to ERROR

    updated_download = download_db.get_download_by_id(feed_id, dl_id)
    assert updated_download.status == DownloadStatus.DOWNLOADED
    assert updated_download.retries == 2
    assert updated_download.last_error == "Error 2 on downloaded (max reached)"

    # One more bump, still should not change status
    new_retries, final_status, did_transition = download_db.bump_retries(
        feed_id, dl_id, "Error 3 on downloaded (exceeds max)", max_errors
    )
    assert new_retries == 3
    assert final_status == DownloadStatus.DOWNLOADED
    assert not did_transition

    updated_download = download_db.get_download_by_id(feed_id, dl_id)
    assert updated_download.status == DownloadStatus.DOWNLOADED
    assert updated_download.retries == 3
