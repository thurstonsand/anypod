from collections.abc import Iterator
import datetime
from pathlib import Path
import sqlite3
from typing import Any

import pytest

from anypod.db import DatabaseManager, Download, DownloadStatus
from anypod.exceptions import DownloadNotFoundError

# --- Fixtures ---


@pytest.fixture
def db_manager(tmp_path: Path) -> Iterator[DatabaseManager]:
    # db_path = tmp_path / "test.db"
    manager = DatabaseManager(db_path=None, memory_name="test_db")
    yield manager
    manager.close()  # Ensure connection is closed after test


@pytest.fixture
def sample_download_queued() -> Download:
    """Provides a sample Download instance for adding to the DB."""
    return Download(
        feed="test_feed",
        id="test_id_1",
        source_url="http://example.com/video1",
        title="Test Video 1",
        published=datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC),
        ext="mp4",
        duration=120.0,
        status=DownloadStatus.QUEUED,
        thumbnail="http://example.com/thumb1.jpg",
        retries=0,
        last_error=None,
    )


@pytest.fixture
def sample_download_row_data() -> dict[str, Any]:
    """Provides raw data for a sample Download object, simulating a DB row."""
    return {
        "feed": "test_feed",
        "id": "test_id_123",
        "source_url": "http://example.com/video/123",
        "title": "Test Video Title",
        "published": datetime.datetime(
            2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC
        ).isoformat(),  # Stored as ISO string in DB
        "ext": "mp4",
        "duration": 120.0,
        "thumbnail": "http://example.com/thumb/123.jpg",
        "status": str(DownloadStatus.QUEUED),
        "retries": 0,
        "last_error": None,
    }


@pytest.fixture
def sample_download_upcoming() -> Download:
    return Download(
        feed="test_feed",
        id="test_id_upcoming",
        source_url="http://example.com/video_upcoming",
        title="Test Video Upcoming",
        published=datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC),
        ext="mp4",
        duration=120.0,
        status=DownloadStatus.UPCOMING,
        thumbnail="http://example.com/thumb_upcoming.jpg",
        retries=0,
        last_error=None,
    )


# --- Tests ---


@pytest.mark.unit
def test_download_equality_and_hash(sample_download_queued: Download):
    """Test equality and hashability of Download objects."""
    download1_v1 = sample_download_queued
    download2_v1_same_key = Download(
        feed=sample_download_queued.feed,  # Use same feed from fixture
        id=sample_download_queued.id,  # Use same ID from fixture
        source_url="http://example.com/video/v123_alt",
        title="Test Video Title One Alt",
        published=datetime.datetime(2023, 1, 1, 13, 0, 0, tzinfo=datetime.UTC),
        ext="mkv",
        duration=130.5,
        thumbnail="http://example.com/thumb/v123_alt.jpg",
        status=DownloadStatus.DOWNLOADED,  # Different status
        retries=1,  # Different retries
        last_error="some error",  # Different error
    )
    download3_v2_diff_id = Download(
        feed=sample_download_queued.feed,
        id="v456",  # Different id
        source_url="http://example.com/video/v456",
        title="Test Video Title Two",
        published=datetime.datetime(2023, 1, 2, 12, 0, 0, tzinfo=datetime.UTC),
        ext="mp4",
        duration=180,
        thumbnail="http://example.com/thumb/v456.jpg",
        status=DownloadStatus.QUEUED,
    )
    download4_feed2_v1_diff_feed = Download(
        feed="another_feed",  # Different feed
        id="v123",
        source_url="http://example.com/video/another_v123",
        title="Another Feed Video",
        published=datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC),
        ext="mp4",
        duration=120.5,
        thumbnail="http://example.com/thumb/another_v123.jpg",
        status=DownloadStatus.QUEUED,
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
def test_db_manager_initialization_and_schema(db_manager: DatabaseManager):
    """Test that the schema (tables and indices) is created upon first DB interaction."""
    conn: sqlite3.Connection = db_manager._db.db.conn  # type: ignore
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
    db_manager: DatabaseManager, sample_download_queued: Download
):
    """Test adding a new download and then retrieving it."""
    db_manager.upsert_download(sample_download_queued)

    # assert 1 == 0, f"{sqlite3.sqlite_version}, {sqlite3.sqlite_version_info}"
    # assert 1 == 0, f"rows: {list(db_manager._db.db['downloads'].rows)}"
    retrieved_download = db_manager.get_download_by_id(
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
    db_manager: DatabaseManager, sample_download_queued: Download
):
    """Test that upsert_download updates an existing download instead of raising an error."""
    # Add initial download
    db_manager.upsert_download(sample_download_queued)

    # Create a modified version with the same (feed, id)
    modified_download = Download(
        feed=sample_download_queued.feed,
        id=sample_download_queued.id,
        source_url="http://example.com/video/v123_updated",
        title="Updated Test Video Title",
        published=sample_download_queued.published + datetime.timedelta(hours=1),
        ext="mkv",  # Changed ext
        duration=150.0,  # Changed duration
        thumbnail="http://example.com/thumb/v123_updated.jpg",
        status=DownloadStatus.DOWNLOADED,  # Changed status
        retries=1,  # Changed retries
        last_error="An old error",  # Changed last_error
    )

    # Perform upsert with the modified download
    db_manager.upsert_download(modified_download)  # Should not raise IntegrityError

    # Retrieve and verify
    retrieved_download = db_manager.get_download_by_id(
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
def test_update_status(db_manager: DatabaseManager, sample_download_queued: Download):
    """Test updating the status of a download, including new re-queue logic."""
    db_manager.upsert_download(sample_download_queued)  # Initial status is QUEUED

    # Test: QUEUED -> DOWNLOADED
    db_manager.update_status(
        feed=sample_download_queued.feed,
        id=sample_download_queued.id,
        status=DownloadStatus.DOWNLOADED,
    )
    download = db_manager.get_download_by_id(
        sample_download_queued.feed, sample_download_queued.id
    )
    assert download is not None
    assert download.status == DownloadStatus.DOWNLOADED
    assert download.retries == 0, "Retries should be reset on DOWNLOADED"
    assert download.last_error is None, "Error should be cleared on DOWNLOADED"

    # Test: DOWNLOADED -> ERROR
    error_message = "Download failed: Network issue"
    db_manager.update_status(
        feed=sample_download_queued.feed,
        id=sample_download_queued.id,
        status=DownloadStatus.ERROR,
        last_error=error_message,
    )
    download_error = db_manager.get_download_by_id(
        sample_download_queued.feed, sample_download_queued.id
    )
    assert download_error is not None
    assert download_error.status == DownloadStatus.ERROR
    assert download_error.last_error == error_message
    assert download_error.retries == 1, "Retries should be incremented on first ERROR"

    # Test: ERROR -> ERROR (increment retries)
    error_message_2 = "Download failed again"
    db_manager.update_status(
        feed=sample_download_queued.feed,
        id=sample_download_queued.id,
        status=DownloadStatus.ERROR,
        last_error=error_message_2,
    )
    download_error_2 = db_manager.get_download_by_id(
        sample_download_queued.feed, sample_download_queued.id
    )
    assert download_error_2 is not None
    assert download_error_2.retries == 2, "Retries should increment on subsequent ERROR"
    assert download_error_2.last_error == error_message_2

    # Test: ERROR -> UPCOMING (retries and error persist)
    db_manager.update_status(
        feed=sample_download_queued.feed,
        id=sample_download_queued.id,
        status=DownloadStatus.UPCOMING,
    )
    download_upcoming_from_error = db_manager.get_download_by_id(
        sample_download_queued.feed, sample_download_queued.id
    )
    assert download_upcoming_from_error is not None
    assert download_upcoming_from_error.status == DownloadStatus.UPCOMING
    assert download_upcoming_from_error.last_error == error_message_2, (
        "Error message should persist when transitioning to UPCOMING from ERROR"
    )
    assert download_upcoming_from_error.retries == 2, (
        "Retries should persist when transitioning to UPCOMING from ERROR"
    )

    # Test: UPCOMING -> QUEUED (retries and error persist)
    db_manager.update_status(
        feed=sample_download_queued.feed,
        id=sample_download_queued.id,
        status=DownloadStatus.QUEUED,
    )
    download_requeued_from_upcoming = db_manager.get_download_by_id(
        sample_download_queued.feed, sample_download_queued.id
    )
    assert download_requeued_from_upcoming is not None
    assert download_requeued_from_upcoming.status == DownloadStatus.QUEUED
    assert download_requeued_from_upcoming.last_error == error_message_2, (
        "Error message should persist when transitioning to QUEUED from UPCOMING"
    )
    assert download_requeued_from_upcoming.retries == 2, (
        "Retries should persist when transitioning to QUEUED from UPCOMING"
    )

    # Test: QUEUED -> SKIPPED (retries and error persist)
    db_manager.update_status(
        feed=sample_download_queued.feed,
        id=sample_download_queued.id,
        status=DownloadStatus.SKIPPED,
    )
    download_skipped = db_manager.get_download_by_id(
        sample_download_queued.feed, sample_download_queued.id
    )
    assert download_skipped is not None
    assert download_skipped.status == DownloadStatus.SKIPPED
    assert download_skipped.last_error == error_message_2, (
        "Error message should persist on SKIPPED"
    )
    assert download_skipped.retries == 2, "Retries should persist on SKIPPED"

    # Test: SKIPPED -> QUEUED (error and retries persist)
    # Re-using download_requeued variable name for clarity of flow
    db_manager.update_status(
        feed=sample_download_queued.feed,
        id=sample_download_queued.id,
        status=DownloadStatus.QUEUED,
    )
    download_requeued = db_manager.get_download_by_id(
        sample_download_queued.feed, sample_download_queued.id
    )
    assert download_requeued is not None
    assert download_requeued.status == DownloadStatus.QUEUED
    assert download_requeued.last_error == error_message_2, (
        "Error message should persist on re-QUEUE"
    )
    assert download_requeued.retries == 2, "Retries should persist on re-QUEUE"

    # Test transitioning to ARCHIVED from an ERROR state
    db_manager.update_status(
        feed=sample_download_queued.feed,
        id=sample_download_queued.id,
        status=DownloadStatus.ARCHIVED,
    )
    download_archived_from_error = db_manager.get_download_by_id(
        sample_download_queued.feed, sample_download_queued.id
    )
    assert download_archived_from_error is not None
    assert download_archived_from_error.status == DownloadStatus.ARCHIVED
    assert download_archived_from_error.last_error == error_message_2, (
        "Error message should persist when archiving from ERROR state"
    )
    assert download_archived_from_error.retries == 2, (
        "Retries should persist when archiving from ERROR state"
    )

    # Test transitioning to ARCHIVED from a DOWNLOADED state (error/retries should be null/0)
    db_manager.update_status(
        feed=sample_download_queued.feed,
        id=sample_download_queued.id,
        status=DownloadStatus.DOWNLOADED,  # First set to DOWNLOADED to clear errors/retries
    )
    db_manager.update_status(
        feed=sample_download_queued.feed,
        id=sample_download_queued.id,
        status=DownloadStatus.ARCHIVED,
    )
    download_archived_from_downloaded = db_manager.get_download_by_id(
        sample_download_queued.feed, sample_download_queued.id
    )
    assert download_archived_from_downloaded is not None
    assert download_archived_from_downloaded.status == DownloadStatus.ARCHIVED
    assert download_archived_from_downloaded.last_error is None, (
        "Error message should be None when archiving from DOWNLOADED state"
    )
    assert download_archived_from_downloaded.retries == 0, (
        "Retries should be 0 when archiving from DOWNLOADED state"
    )

    non_existent_update = db_manager.update_status(
        feed="other_feed", id="non_existent_id", status=DownloadStatus.DOWNLOADED
    )
    assert non_existent_update is False, (
        "Updating non-existent download should return False"
    )


@pytest.mark.unit
def test_get_downloads_to_prune_by_keep_last(db_manager: DatabaseManager):
    """Test fetching downloads to prune based on 'keep_last'."""
    base_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    feed1_name = "prune_feed1"

    # Mix of statuses and published dates
    dl_f1v1_dl_oldest = Download(
        feed=feed1_name,
        id="f1v1_dl_oldest",
        published=base_time - datetime.timedelta(days=5),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t1",
        ext="mp4",
        duration=1,
    )
    dl_f1v2_err_mid1 = Download(
        feed=feed1_name,
        id="f1v2_err_mid1",
        published=base_time - datetime.timedelta(days=4),
        status=DownloadStatus.ERROR,
        source_url="url",
        title="t2",
        ext="mkv",
        duration=1,
    )
    dl_f1v3_q_mid2 = Download(
        feed=feed1_name,
        id="f1v3_q_mid2",
        published=base_time - datetime.timedelta(days=3),
        status=DownloadStatus.QUEUED,
        source_url="url",
        title="t3",
        ext="webm",
        duration=1,
    )
    dl_f1v4_dl_newest = Download(
        feed=feed1_name,
        id="f1v4_dl_newest",
        published=base_time - datetime.timedelta(days=2),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t4",
        ext="mp4",
        duration=1,
    )
    dl_f1v5_arch = Download(
        feed=feed1_name,
        id="f1v5_arch",
        published=base_time - datetime.timedelta(days=1),
        status=DownloadStatus.ARCHIVED,
        source_url="url",
        title="t5",
        ext="mp3",
        duration=1,
    )
    dl_f1v6_upcoming_older = Download(  # New UPCOMING download, older
        feed=feed1_name,
        id="f1v6_upcoming_older",
        published=base_time - datetime.timedelta(days=6),  # Older than f1v1_dl_oldest
        status=DownloadStatus.UPCOMING,
        source_url="url",
        title="t6_upcoming",
        ext="mp4",
        duration=1,
    )

    # download for another feed, should be ignored
    dl_f2v1_dl = Download(
        feed="prune_feed2",
        id="f2v1_dl",
        published=base_time - datetime.timedelta(days=3),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t_f2",
        ext="mp4",
        duration=1,
    )

    downloads_to_add = [
        dl_f1v1_dl_oldest,
        dl_f1v2_err_mid1,
        dl_f1v3_q_mid2,
        dl_f1v4_dl_newest,
        dl_f1v5_arch,
        dl_f2v1_dl,
        dl_f1v6_upcoming_older,  # Add to list
    ]
    for dl in downloads_to_add:
        db_manager.upsert_download(dl)

    # Keep 2: f1v4_dl_newest (kept), f1v3_q_mid2 (kept)
    # Pruned: f1v2_err_mid1, f1v1_dl_oldest
    # Ignored from pruning: f1v5_arch, f1v6_upcoming_older
    prune_keep2 = db_manager.get_downloads_to_prune_by_keep_last(
        feed=feed1_name, keep_last=2
    )
    assert len(prune_keep2) == 2, (
        "Should identify 2 downloads to prune (f1v1_dl_oldest, f1v2_err_mid1)"
    )
    pruned_ids_keep2 = sorted([row.id for row in prune_keep2])
    assert "f1v6_upcoming_older" not in pruned_ids_keep2, (
        "UPCOMING download f1v6_upcoming_older should NOT be in the prune list"
    )
    assert sorted(["f1v1_dl_oldest", "f1v2_err_mid1"]) == pruned_ids_keep2

    # Keep 5: All non-ARCHIVED and non-UPCOMING downloads are kept
    # (f1v4_dl_newest, f1v3_q_mid2, f1v2_err_mid1, f1v1_dl_oldest)
    # There are 4 such downloads. f1v5_arch and f1v6_upcoming_older are ignored.
    prune_keep5 = db_manager.get_downloads_to_prune_by_keep_last(
        feed=feed1_name, keep_last=5
    )
    assert len(prune_keep5) == 0, (
        "Should identify 0 if keep_last >= total non-ARCHIVED/non-UPCOMING downloads for feed"
    )

    prune_keep0 = db_manager.get_downloads_to_prune_by_keep_last(
        feed=feed1_name, keep_last=0
    )
    assert len(prune_keep0) == 0, "Should return 0 if keep_last is 0"


@pytest.mark.unit
def test_get_downloads_to_prune_by_since(db_manager: DatabaseManager):
    """Test fetching downloads to prune by 'since' date."""
    base_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    feed_id = "prune_since_feed"

    dl_ps_v1_older_dl = Download(
        feed=feed_id,
        id="ps_v1_older_dl",
        published=base_time - datetime.timedelta(days=5),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t1",
        ext="mp4",
        duration=1,
    )
    dl_ps_v2_mid_err = Download(
        feed=feed_id,
        id="ps_v2_mid_err",
        published=base_time - datetime.timedelta(days=2),
        status=DownloadStatus.ERROR,
        source_url="url",
        title="t2",
        ext="mkv",
        duration=1,
    )
    dl_ps_v3_newer_q = Download(
        feed=feed_id,
        id="ps_v3_newer_q",
        published=base_time + datetime.timedelta(days=1),
        status=DownloadStatus.QUEUED,
        source_url="url",
        title="t3",
        ext="webm",
        duration=1,
    )
    dl_ps_v4_arch = Download(
        feed=feed_id,
        id="ps_v4_arch",
        published=base_time,
        status=DownloadStatus.ARCHIVED,
        source_url="url",
        title="t4",
        ext="mp3",
        duration=1,
    )
    dl_ps_v5_upcoming_ancient = Download(  # New UPCOMING download, very old
        feed=feed_id,
        id="ps_v5_upcoming_ancient",
        published=base_time - datetime.timedelta(days=10),  # Much older than cutoff
        status=DownloadStatus.UPCOMING,
        source_url="url",
        title="t5_upcoming",
        ext="mp4",
        duration=1,
    )

    # download for another feed
    dl_other_v1_older_dl = Download(
        feed="other_feed",
        id="other_v1_older_dl",
        published=base_time - datetime.timedelta(days=5),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t_other",
        ext="mp4",
        duration=1,
    )

    downloads_to_add = [
        dl_ps_v1_older_dl,
        dl_ps_v2_mid_err,
        dl_ps_v3_newer_q,
        dl_ps_v4_arch,
        dl_ps_v5_upcoming_ancient,  # Add to list
        dl_other_v1_older_dl,
    ]
    for dl in downloads_to_add:
        db_manager.upsert_download(dl)

    # Prune downloads older than 'base_time - 3 days' for feed_id
    # Candidates for pruning (ignoring ARCHIVED and UPCOMING):
    # - ps_v1_older_dl (day -5) -> YES
    # - ps_v2_mid_err (day -2) -> NO (not older than day -3)
    # - ps_v3_newer_q (day +1) -> NO
    # - ps_v4_arch (day 0) -> NO (archived)
    # - ps_v5_upcoming_ancient (day -10) -> NO (upcoming, due to db.py change)
    since_cutoff_1 = base_time - datetime.timedelta(days=3)
    pruned_1 = db_manager.get_downloads_to_prune_by_since(
        feed=feed_id, since=since_cutoff_1
    )
    assert len(pruned_1) == 1
    assert pruned_1[0].id == "ps_v1_older_dl"
    # Verify that ps_v5_upcoming_ancient was not pruned
    pruned_ids_1 = [row.id for row in pruned_1]
    assert "ps_v5_upcoming_ancient" not in pruned_ids_1, (
        "UPCOMING download should not be pruned by since_cutoff_1"
    )

    # Prune downloads older than 'base_time + 2 days' for feed_id
    # Candidates for pruning (ignoring ARCHIVED and UPCOMING):
    # - ps_v1_older_dl (day -5) -> YES
    # - ps_v2_mid_err (day -2) -> YES
    # - ps_v3_newer_q (day +1) -> YES
    # - ps_v4_arch (day 0) -> NO (archived)
    # - ps_v5_upcoming_ancient (day -10) -> NO (upcoming, due to db.py change)
    since_cutoff_2 = base_time + datetime.timedelta(days=2)
    pruned_2 = db_manager.get_downloads_to_prune_by_since(
        feed=feed_id, since=since_cutoff_2
    )
    pruned_ids_2 = sorted([row.id for row in pruned_2])
    assert len(pruned_2) == 3
    assert pruned_ids_2 == sorted(["ps_v1_older_dl", "ps_v2_mid_err", "ps_v3_newer_q"])
    assert "ps_v5_upcoming_ancient" not in pruned_ids_2, (
        "UPCOMING download should not be pruned by since_cutoff_2"
    )


@pytest.mark.unit
def test_get_downloads_by_status(db_manager: DatabaseManager):
    """Test fetching downloads by various statuses, including offset and limit."""
    base_time = datetime.datetime(2023, 1, 15, 12, 0, 0, tzinfo=datetime.UTC)
    feed1 = "status_feed1"
    feed2 = "status_feed2"

    # oldest, feed2, ERROR
    dl_f2e1 = Download(
        feed=feed2,
        id="f2e1",
        published=base_time - datetime.timedelta(days=3),
        status=DownloadStatus.ERROR,
        last_error="Feed 2 error",
        source_url="url",
        title="t_err_f2",
        ext="mp4",
        duration=1,
    )
    # middle, feed1, ERROR
    dl_f1e1_old = Download(
        feed=feed1,
        id="f1e1_old",
        published=base_time - datetime.timedelta(days=2),
        status=DownloadStatus.ERROR,
        last_error="Old error 1",
        source_url="url",
        title="t_err_f1_old",
        ext="mp4",
        duration=1,
    )
    # newest, feed1, ERROR
    dl_f1e2_new = Download(
        feed=feed1,
        id="f1e2_new",
        published=base_time - datetime.timedelta(days=1),
        status=DownloadStatus.ERROR,
        last_error="New error 1",
        source_url="url",
        title="t_err_f1_new",
        ext="mp4",
        duration=1,
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
        duration=1,
    )
    dl_f3d1 = Download(
        feed="feed3_no_match",
        id="f3d1",
        published=base_time,
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t_d_f3",
        ext="mp4",
        duration=1,
    )
    dl_f1_upcoming = Download(
        feed=feed1,
        id="f1upcoming",
        published=base_time - datetime.timedelta(days=4),  # Older than errors
        status=DownloadStatus.UPCOMING,
        source_url="url",
        title="t_up_f1",
        ext="mp4",
        duration=1,
    )
    dl_f2_upcoming = Download(
        feed=feed2,
        id="f2upcoming",
        published=base_time - datetime.timedelta(days=5),
        status=DownloadStatus.UPCOMING,
        source_url="url",
        title="t_up_f2",
        ext="mp4",
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
        db_manager.upsert_download(dl_data)

    # Expected order for all errors: f2e1 (oldest), f1e1_old, f1e2_new (newest)
    all_errors = db_manager.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR
    )
    assert len(all_errors) == 3, "Should fetch all 3 ERROR downloads"
    assert [row.id for row in all_errors] == ["f2e1", "f1e1_old", "f1e2_new"]
    for row in all_errors:
        assert row.status == DownloadStatus.ERROR

    feed1_errors = db_manager.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR, feed=feed1
    )
    assert len(feed1_errors) == 2, "Should fetch 2 ERROR downloads for feed1"
    assert [row.id for row in feed1_errors] == ["f1e1_old", "f1e2_new"]

    limited_errors = db_manager.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR, limit=1, offset=0
    )
    assert len(limited_errors) == 1, "Should fetch only 1 error with limit=1"
    assert limited_errors[0].id == "f2e1", "Should be the oldest overall error"

    # --- Test UPCOMING status ---
    # Expected order for all UPCOMING: f2upcoming (oldest), f1upcoming (newest)
    all_upcoming = db_manager.get_downloads_by_status(
        status_to_filter=DownloadStatus.UPCOMING
    )
    assert len(all_upcoming) == 2, "Should fetch all 2 UPCOMING downloads"
    assert [row.id for row in all_upcoming] == ["f2upcoming", "f1upcoming"]
    for row in all_upcoming:
        assert row.status == DownloadStatus.UPCOMING

    feed1_upcoming = db_manager.get_downloads_by_status(
        status_to_filter=DownloadStatus.UPCOMING, feed=feed1
    )
    assert len(feed1_upcoming) == 1, "Should fetch 1 UPCOMING download for feed1"
    assert feed1_upcoming[0].id == "f1upcoming"

    # --- Test QUEUED status (feed1 has one) ---
    feed1_queued = db_manager.get_downloads_by_status(
        status_to_filter=DownloadStatus.QUEUED, feed=feed1
    )
    assert len(feed1_queued) == 1, "Should fetch 1 QUEUED download for feed1"
    assert feed1_queued[0].id == "f1q1"

    # --- Test DOWNLOADED status (feed3_no_match has one) ---
    downloaded_f3 = db_manager.get_downloads_by_status(
        status_to_filter=DownloadStatus.DOWNLOADED, feed="feed3_no_match"
    )
    assert len(downloaded_f3) == 1, "Should fetch 1 DOWNLOADED for feed3_no_match"
    assert downloaded_f3[0].id == "f3d1"

    # --- Test with offset and limit for UPCOMING ---
    upcoming_limit1_offset1 = db_manager.get_downloads_by_status(
        status_to_filter=DownloadStatus.UPCOMING, limit=1, offset=1
    )  # Skips f2upcoming, gets f1upcoming
    assert len(upcoming_limit1_offset1) == 1
    assert upcoming_limit1_offset1[0].id == "f1upcoming"

    # --- Test no downloads for a status/feed combination ---
    no_feed2_queued = db_manager.get_downloads_by_status(
        status_to_filter=DownloadStatus.QUEUED, feed=feed2
    )
    assert len(no_feed2_queued) == 0

    no_skipped_any_feed = db_manager.get_downloads_by_status(
        status_to_filter=DownloadStatus.SKIPPED
    )
    assert len(no_skipped_any_feed) == 0

    # --- Test offset greater than number of downloads ---
    offset_too_high_error = db_manager.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR, limit=100, offset=5
    )
    assert len(offset_too_high_error) == 0

    # --- Test no downloads at all (after updating existing downloads to a different status) ---
    db_manager.update_status(feed=feed1, id="f1e1_old", status=DownloadStatus.QUEUED)
    db_manager.update_status(
        feed=feed1, id="f1e2_new", status=DownloadStatus.DOWNLOADED
    )
    db_manager.update_status(feed=feed2, id="f2e1", status=DownloadStatus.SKIPPED)
    all_errors_cleared = db_manager.get_downloads_by_status(
        status_to_filter=DownloadStatus.ERROR
    )
    assert len(all_errors_cleared) == 0, (
        "Should return empty list when all ERROR downloads are cleared"
    )

    # Test original upcoming downloads are also gone if we query for them after updates
    db_manager.update_status(feed=feed1, id="f1upcoming", status=DownloadStatus.QUEUED)
    db_manager.update_status(feed=feed2, id="f2upcoming", status=DownloadStatus.QUEUED)
    all_upcoming_cleared = db_manager.get_downloads_by_status(
        status_to_filter=DownloadStatus.UPCOMING
    )
    assert len(all_upcoming_cleared) == 0, (
        "Should return empty list when all UPCOMING downloads are cleared"
    )


# --- Tests for Download.from_row ---


@pytest.mark.unit
def test_download_from_row_success(sample_download_row_data: dict[str, Any]):
    """Test successful conversion of a valid row dictionary to a Download object."""
    # Simulate sqlite3.Row by using a dictionary. Access by string keys is what sqlite3.Row provides.
    mock_row = sample_download_row_data

    # Expected Download object based on the row data
    expected_published_dt = datetime.datetime.fromisoformat(mock_row["published"])
    expected_status_enum = DownloadStatus(mock_row["status"])
    expected_download = Download(
        feed=mock_row["feed"],
        id=mock_row["id"],
        source_url=mock_row["source_url"],
        title=mock_row["title"],
        published=expected_published_dt,
        ext=mock_row["ext"],
        duration=float(mock_row["duration"]),
        thumbnail=mock_row["thumbnail"],
        status=expected_status_enum,
        retries=int(mock_row["retries"]),
        last_error=mock_row["last_error"],
    )

    converted_download = Download.from_row(mock_row)  # type: ignore[arg-type] # dict approximates sqlite3.Row
    assert converted_download == expected_download
    assert converted_download.published == expected_published_dt
    assert converted_download.status == expected_status_enum
    assert converted_download.duration == float(mock_row["duration"])


@pytest.mark.unit
@pytest.mark.parametrize(
    "malformed_field, malformed_value, expected_error_message_part",
    [
        ("published", "not-a-date-string", "Invalid date format"),
        ("published", None, "Invalid date format"),
        ("status", "unknown_status", "Invalid status value"),
        (
            "duration",
            "not-a-float",
            "could not convert string to float",
        ),  # Assuming direct float conversion error
    ],
)
def test_download_from_row_malformed_data(
    sample_download_row_data: dict[str, Any],
    malformed_field: str,
    malformed_value: Any,
    expected_error_message_part: str,
):
    """Test ValueError is raised for malformed data fields during Download.from_row()."""
    corrupted_row_data = sample_download_row_data.copy()
    corrupted_row_data[malformed_field] = malformed_value

    with pytest.raises(ValueError):
        Download.from_row(corrupted_row_data)  # type: ignore[arg-type]


def test_bump_retries_non_existent_download(db_manager: DatabaseManager):
    """Test bumping retries for a download that doesn't exist."""
    with pytest.raises(DownloadNotFoundError) as e:
        db_manager.bump_retries(
            feed_id="non_existent_feed",
            download_id="non_existent_id",
            error_message="Test error",
            max_allowed_errors=3,
        )
    assert e.value.feed_id == "non_existent_feed"
    assert e.value.download_id == "non_existent_id"


def test_bump_retries_below_max(
    db_manager: DatabaseManager, sample_download_upcoming: Download
):
    """Test bumping retries when new count is below max_allowed_errors."""
    db_manager.upsert_download(sample_download_upcoming)

    new_retries, final_status, did_transition = db_manager.bump_retries(
        feed_id=sample_download_upcoming.feed,
        download_id=sample_download_upcoming.id,
        error_message="First error",
        max_allowed_errors=3,
    )

    assert new_retries == 1
    assert final_status == DownloadStatus.UPCOMING
    assert not did_transition

    updated_row = db_manager.get_download_by_id(
        sample_download_upcoming.feed, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 1
    assert updated_row.status == DownloadStatus.UPCOMING
    assert updated_row.last_error == "First error"


def test_bump_retries_reaches_max(
    db_manager: DatabaseManager, sample_download_upcoming: Download
):
    """Test bumping retries when new count reaches max_allowed_errors."""
    sample_download_upcoming.retries = 2
    db_manager.upsert_download(sample_download_upcoming)

    new_retries, final_status, did_transition = db_manager.bump_retries(
        feed_id=sample_download_upcoming.feed,
        download_id=sample_download_upcoming.id,
        error_message="Third error - reaching max",
        max_allowed_errors=3,
    )

    assert new_retries == 3
    assert final_status == DownloadStatus.ERROR
    assert did_transition

    updated_row = db_manager.get_download_by_id(
        sample_download_upcoming.feed, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 3
    assert updated_row.status == DownloadStatus.ERROR
    assert updated_row.last_error == "Third error - reaching max"


def test_bump_retries_exceeds_max(
    db_manager: DatabaseManager, sample_download_upcoming: Download
):
    """Test bumping retries when new count would exceed max_allowed_errors."""
    sample_download_upcoming.retries = 3
    db_manager.upsert_download(sample_download_upcoming)

    new_retries, final_status, did_transition = db_manager.bump_retries(
        feed_id=sample_download_upcoming.feed,
        download_id=sample_download_upcoming.id,
        error_message="Fourth error - exceeds max from upcoming",
        max_allowed_errors=3,
    )

    assert new_retries == 4
    assert final_status == DownloadStatus.ERROR
    assert did_transition

    updated_row = db_manager.get_download_by_id(
        sample_download_upcoming.feed, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 4
    assert updated_row.status == DownloadStatus.ERROR
    assert updated_row.last_error == "Fourth error - exceeds max from upcoming"


def test_bump_retries_already_error_status(
    db_manager: DatabaseManager, sample_download_upcoming: Download
):
    """Test bumping retries when the item is already in ERROR state."""
    sample_download_upcoming.status = DownloadStatus.ERROR
    sample_download_upcoming.retries = 5
    sample_download_upcoming.last_error = "Previous major error"
    db_manager.upsert_download(sample_download_upcoming)

    new_retries, final_status, is_error_after_bump = db_manager.bump_retries(
        feed_id=sample_download_upcoming.feed,
        download_id=sample_download_upcoming.id,
        error_message="Another error while already in ERROR state",
        max_allowed_errors=3,
    )

    assert new_retries == 6
    assert final_status == DownloadStatus.ERROR
    assert is_error_after_bump

    updated_row = db_manager.get_download_by_id(
        sample_download_upcoming.feed, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 6
    assert updated_row.status == DownloadStatus.ERROR
    assert updated_row.last_error == "Another error while already in ERROR state"


def test_bump_retries_max_errors_is_one(
    db_manager: DatabaseManager, sample_download_upcoming: Download
):
    """Test bumping retries transitions to ERROR immediately if max_allowed_errors is 1."""
    db_manager.upsert_download(sample_download_upcoming)

    new_retries, final_status, did_transition = db_manager.bump_retries(
        feed_id=sample_download_upcoming.feed,
        download_id=sample_download_upcoming.id,
        error_message="First and only error allowed",
        max_allowed_errors=1,
    )

    assert new_retries == 1
    assert final_status == DownloadStatus.ERROR
    assert did_transition

    updated_row = db_manager.get_download_by_id(
        sample_download_upcoming.feed, sample_download_upcoming.id
    )
    assert updated_row is not None
    assert updated_row.retries == 1
    assert updated_row.status == DownloadStatus.ERROR
    assert updated_row.last_error == "First and only error allowed"
