from collections.abc import Iterator
import datetime
import sqlite3
from typing import Any

import pytest

from anypod.db import DatabaseManager, Download, DownloadStatus

# --- Fixtures ---


@pytest.fixture
def db_manager(tmp_path_factory: pytest.TempPathFactory) -> Iterator[DatabaseManager]:
    """Provides a DatabaseManager instance with a temporary file-based SQLite database."""
    temp_dir = tmp_path_factory.mktemp("test_db_data")
    db_file = temp_dir / "test_anypod.db"
    manager = DatabaseManager(db_path=db_file)
    yield manager  # Test will run here
    manager.close()


@pytest.fixture
def sample_download() -> Download:
    """Provides a sample Download instance for adding to the DB."""
    return Download(
        feed="test_feed",
        id="v123",
        source_url="http://example.com/video/v123",
        title="Test Video Title One",
        published=datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC),
        ext="mp4",
        duration=120.5,
        thumbnail="http://example.com/thumb/v123.jpg",
        status=DownloadStatus.QUEUED,
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


# --- Tests ---


@pytest.mark.unit
def test_download_equality_and_hash(sample_download: Download):
    """Test equality and hashability of Download objects."""
    download1_v1 = sample_download
    download2_v1_same_key = Download(
        feed="test_feed",
        id="v123",  # Same feed and id as sample_download
        source_url="http://example.com/video/v123_alt",
        title="Test Video Title One Alt",
        published=datetime.datetime(2023, 1, 1, 13, 0, 0, tzinfo=datetime.UTC),
        ext="mkv",
        duration=130.5,
        thumbnail="http://example.com/thumb/v123_alt.jpg",
        status=DownloadStatus.DOWNLOADED,  # Different status
    )
    download3_v2_diff_id = Download(
        feed="test_feed",
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
    # Should contain 3 unique items based on (feed, id)
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
    conn = db_manager._get_connection()  # type: ignore
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
def test_add_and_get_download(db_manager: DatabaseManager, sample_download: Download):
    """Test adding a new download and then retrieving it."""
    db_manager.upsert_download(sample_download)

    retrieved_download = db_manager.get_download_by_id(
        feed=sample_download.feed,
        id=sample_download.id,
    )

    assert retrieved_download is not None, "Download should be found in DB"
    assert retrieved_download["feed"] == sample_download.feed
    assert retrieved_download["id"] == sample_download.id
    assert retrieved_download["title"] == sample_download.title
    assert retrieved_download["published"] == sample_download.published.isoformat()
    assert retrieved_download["ext"] == sample_download.ext
    assert retrieved_download["duration"] == sample_download.duration
    assert retrieved_download["thumbnail"] == sample_download.thumbnail
    assert retrieved_download["status"] == str(sample_download.status)
    assert retrieved_download["retries"] == 0, (
        "Retries should be 0 for a new download from fixture"
    )
    assert retrieved_download["last_error"] is None, (
        "Last_error should be None for a new download from fixture"
    )


@pytest.mark.unit
def test_upsert_download_updates_existing(
    db_manager: DatabaseManager, sample_download: Download
):
    """Test that upsert_download updates an existing download instead of raising an error."""
    # Add initial download
    db_manager.upsert_download(sample_download)

    # Create a modified version with the same (feed, id)
    modified_download = Download(
        feed=sample_download.feed,
        id=sample_download.id,
        source_url="http://example.com/video/v123_updated",
        title="Updated Test Video Title",
        published=sample_download.published + datetime.timedelta(hours=1),
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
        feed=sample_download.feed,
        id=sample_download.id,
    )

    assert retrieved_download is not None, "Download should still be found"
    assert retrieved_download["title"] == modified_download.title
    assert retrieved_download["source_url"] == modified_download.source_url
    assert retrieved_download["published"] == modified_download.published.isoformat()
    assert retrieved_download["ext"] == modified_download.ext
    assert retrieved_download["duration"] == modified_download.duration
    assert retrieved_download["thumbnail"] == modified_download.thumbnail
    assert retrieved_download["status"] == str(modified_download.status)
    assert retrieved_download["retries"] == modified_download.retries
    assert retrieved_download["last_error"] == modified_download.last_error


@pytest.mark.unit
def test_update_status(db_manager: DatabaseManager, sample_download: Download):
    """Test updating the status of a download, including new re-queue logic."""
    db_manager.upsert_download(sample_download)

    db_manager.update_status(
        feed=sample_download.feed,
        id=sample_download.id,
        status=DownloadStatus.DOWNLOADED,
    )
    download = db_manager.get_download_by_id(sample_download.feed, sample_download.id)
    assert download is not None
    assert download["status"] == str(DownloadStatus.DOWNLOADED)
    assert download["retries"] == 0, "Retries should be reset on DOWNLOADED"
    assert download["last_error"] is None, "Error should be cleared on DOWNLOADED"

    error_message = "Download failed: Network issue"
    db_manager.update_status(
        feed=sample_download.feed,
        id=sample_download.id,
        status=DownloadStatus.ERROR,
        last_error=error_message,
    )
    download_error = db_manager.get_download_by_id(
        sample_download.feed, sample_download.id
    )
    assert download_error is not None
    assert download_error["status"] == str(DownloadStatus.ERROR)
    assert download_error["last_error"] == error_message
    assert download_error["retries"] == 1, (
        "Retries should be incremented on first ERROR"
    )

    error_message_2 = "Download failed again"
    db_manager.update_status(
        feed=sample_download.feed,
        id=sample_download.id,
        status=DownloadStatus.ERROR,
        last_error=error_message_2,
    )
    download_error_2 = db_manager.get_download_by_id(
        sample_download.feed, sample_download.id
    )
    assert download_error_2 is not None
    assert download_error_2["retries"] == 2, (
        "Retries should increment on subsequent ERROR"
    )
    assert download_error_2["last_error"] == error_message_2

    db_manager.update_status(
        feed=sample_download.feed,
        id=sample_download.id,
        status=DownloadStatus.SKIPPED,
    )
    download_skipped = db_manager.get_download_by_id(
        sample_download.feed, sample_download.id
    )
    assert download_skipped is not None
    assert download_skipped["status"] == str(DownloadStatus.SKIPPED)
    assert download_skipped["last_error"] == error_message_2, (
        "Error message should persist on SKIPPED"
    )
    assert download_skipped["retries"] == 2, "Retries should persist on SKIPPED"

    db_manager.update_status(
        feed=sample_download.feed,
        id=sample_download.id,
        status=DownloadStatus.QUEUED,
    )
    download_requeued = db_manager.get_download_by_id(
        sample_download.feed, sample_download.id
    )
    assert download_requeued is not None
    assert download_requeued["status"] == str(DownloadStatus.QUEUED)
    assert download_requeued["last_error"] == error_message_2, (
        "Error message should persist on re-QUEUE"
    )
    assert download_requeued["retries"] == 2, "Retries should persist on re-QUEUE"

    # Test transitioning to ARCHIVED from an ERROR state
    db_manager.update_status(
        feed=sample_download.feed,
        id=sample_download.id,
        status=DownloadStatus.ARCHIVED,
    )
    download_archived_from_error = db_manager.get_download_by_id(
        sample_download.feed, sample_download.id
    )
    assert download_archived_from_error is not None
    assert download_archived_from_error["status"] == str(DownloadStatus.ARCHIVED)
    assert download_archived_from_error["last_error"] == error_message_2, (
        "Error message should persist when archiving from ERROR state"
    )
    assert download_archived_from_error["retries"] == 2, (
        "Retries should persist when archiving from ERROR state"
    )

    # Test transitioning to ARCHIVED from a DOWNLOADED state (error/retries should be null/0)
    db_manager.update_status(
        feed=sample_download.feed,
        id=sample_download.id,
        status=DownloadStatus.DOWNLOADED,  # First set to DOWNLOADED to clear errors/retries
    )
    db_manager.update_status(
        feed=sample_download.feed,
        id=sample_download.id,
        status=DownloadStatus.ARCHIVED,
    )
    download_archived_from_downloaded = db_manager.get_download_by_id(
        sample_download.feed, sample_download.id
    )
    assert download_archived_from_downloaded is not None
    assert download_archived_from_downloaded["status"] == str(DownloadStatus.ARCHIVED)
    assert download_archived_from_downloaded["last_error"] is None, (
        "Error message should be None when archiving from DOWNLOADED state"
    )
    assert download_archived_from_downloaded["retries"] == 0, (
        "Retries should be 0 when archiving from DOWNLOADED state"
    )

    non_existent_update = db_manager.update_status(
        feed="other_feed", id="non_existent_id", status=DownloadStatus.DOWNLOADED
    )
    assert non_existent_update is False, (
        "Updating non-existent download should return False"
    )


@pytest.mark.unit
def test_next_queued_downloads(db_manager: DatabaseManager):
    """Test fetching the next queued downloads for a feed."""
    base_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)

    download1_feed1 = Download(
        feed="feed1",
        id="v001",
        source_url="url1",
        title="title1_old",
        published=base_time - datetime.timedelta(hours=2),
        ext="mp4",
        duration=10,
        status=DownloadStatus.QUEUED,
    )
    download2_feed1 = Download(
        feed="feed1",
        id="v002",
        source_url="url2",
        title="title2_new",
        published=base_time - datetime.timedelta(hours=1),
        ext="mp4",
        duration=10,
        status=DownloadStatus.QUEUED,
    )
    download3_feed1_downloaded = Download(
        feed="feed1",
        id="v003",
        source_url="url3",
        title="title3_downloaded",
        published=base_time,
        ext="mp4",
        duration=10,
        status=DownloadStatus.DOWNLOADED,
    )
    download4_feed1_error = Download(
        feed="feed1",
        id="v004",
        source_url="url4",
        title="title4_error",
        published=base_time + datetime.timedelta(hours=1),
        ext="mp4",
        duration=10,
        status=DownloadStatus.ERROR,
    )
    download5_feed1_queued_newest = Download(
        feed="feed1",
        id="v005",
        source_url="url5",
        title="title5_newest",
        published=base_time + datetime.timedelta(hours=2),
        ext="mp4",
        duration=10,
        status=DownloadStatus.QUEUED,
    )
    download6_feed2_queued = Download(
        feed="feed2",
        id="v006",
        source_url="url6",
        title="title6_feed2",
        published=base_time,
        ext="mp4",
        duration=10,
        status=DownloadStatus.QUEUED,
    )

    all_downloads = [
        download1_feed1,
        download2_feed1,
        download3_feed1_downloaded,
        download4_feed1_error,
        download5_feed1_queued_newest,
        download6_feed2_queued,
    ]
    for dl in all_downloads:
        db_manager.upsert_download(dl)

    # Test case 1: Get next 2 for feed1 (should be item1 and item2 in that order)
    queued_feed1_limit2 = db_manager.next_queued_downloads(feed="feed1", limit=2)
    assert len(queued_feed1_limit2) == 2
    assert queued_feed1_limit2[0]["id"] == "v001", (
        "Oldest queued download for feed1 should be first"
    )
    assert queued_feed1_limit2[1]["id"] == "v002", (
        "Second oldest queued download for feed1 should be second"
    )

    # Test case 2: Get all for feed1 (should be item1, item2, item5)
    queued_feed1_all = db_manager.next_queued_downloads(feed="feed1", limit=10)
    assert len(queued_feed1_all) == 3
    assert [dl["id"] for dl in queued_feed1_all] == [
        "v001",
        "v002",
        "v005",
    ], "Should return all queued for feed1 in order"

    # Test case 3: Get for feed2 (should only be item6)
    queued_feed2 = db_manager.next_queued_downloads(feed="feed2", limit=10)
    assert len(queued_feed2) == 1
    assert queued_feed2[0]["id"] == "v006", "Should return only download for feed2"

    # Test case 4: Limit 0
    queued_feed1_limit0 = db_manager.next_queued_downloads(feed="feed1", limit=0)
    assert len(queued_feed1_limit0) == 0, "Limit 0 should return no downloads"

    # Test case 5: No queued items for a non-existent feed
    queued_feed_nonexistent = db_manager.next_queued_downloads(
        feed="feed_non_existent", limit=10
    )
    assert len(queued_feed_nonexistent) == 0, (
        "Non-existent feed should return no downloads"
    )

    # Test case 6: Feed exists but has no QUEUED items (after updating one)
    db_manager.update_status(
        feed="feed2",
        id="v006",
        status=DownloadStatus.DOWNLOADED,
    )
    queued_feed2_none_left = db_manager.next_queued_downloads(feed="feed2", limit=10)
    assert len(queued_feed2_none_left) == 0, (
        "Feed2 should have no queued downloads left"
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

    # Item for another feed, should be ignored
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
    ]
    for dl in downloads_to_add:
        db_manager.upsert_download(dl)

    prune_keep2 = db_manager.get_downloads_to_prune_by_keep_last(
        feed=feed1_name, keep_last=2
    )
    assert len(prune_keep2) == 2, (
        "Should identify 2 downloads to prune (f1v1_dl_oldest, f1v2_err_mid1)"
    )
    pruned_ids_keep2 = sorted([row["id"] for row in prune_keep2])
    assert pruned_ids_keep2 == sorted(["f1v1_dl_oldest", "f1v2_err_mid1"])
    # Check if full rows are returned - access key directly
    assert prune_keep2[0]["title"] is not None, "Row should contain 'title' key"
    assert prune_keep2[0]["feed"] == feed1_name

    prune_keep5 = db_manager.get_downloads_to_prune_by_keep_last(
        feed=feed1_name, keep_last=5
    )  # Keep all
    assert len(prune_keep5) == 0, (
        "Should identify 0 if keep_last >= total items for feed"
    )

    prune_keep0 = db_manager.get_downloads_to_prune_by_keep_last(
        feed=feed1_name, keep_last=0
    )
    assert len(prune_keep0) == 0, "Should return 0 if keep_last is 0"


@pytest.mark.unit
def test_get_downloads_to_prune_by_since(db_manager: DatabaseManager):
    """Test fetching downloads to prune by 'since' date."""
    base_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    feed_name = "prune_since_feed"

    dl_ps_v1_older_dl = Download(
        feed=feed_name,
        id="ps_v1_older_dl",
        published=base_time - datetime.timedelta(days=5),
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t1",
        ext="mp4",
        duration=1,
    )
    dl_ps_v2_mid_err = Download(
        feed=feed_name,
        id="ps_v2_mid_err",
        published=base_time - datetime.timedelta(days=2),
        status=DownloadStatus.ERROR,
        source_url="url",
        title="t2",
        ext="mkv",
        duration=1,
    )
    dl_ps_v3_newer_q = Download(
        feed=feed_name,
        id="ps_v3_newer_q",
        published=base_time + datetime.timedelta(days=1),
        status=DownloadStatus.QUEUED,
        source_url="url",
        title="t3",
        ext="webm",
        duration=1,
    )
    dl_ps_v4_arch = Download(
        feed=feed_name,
        id="ps_v4_arch",
        published=base_time,
        status=DownloadStatus.ARCHIVED,
        source_url="url",
        title="t4",
        ext="mp3",
        duration=1,
    )

    # Item for another feed
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
        dl_other_v1_older_dl,
    ]
    for dl in downloads_to_add:
        db_manager.upsert_download(dl)

    # Prune items older than 'base_time - 3 days' for feed_name
    # Candidates: ps_v1_older_dl (day -5)
    since_cutoff_1 = base_time - datetime.timedelta(days=3)
    pruned_1 = db_manager.get_downloads_to_prune_by_since(
        feed=feed_name, since=since_cutoff_1
    )
    assert len(pruned_1) == 1
    assert pruned_1[0]["id"] == "ps_v1_older_dl"
    if pruned_1:  # Check full row
        # Access key directly
        assert pruned_1[0]["title"] is not None, "Row should contain 'title' key"
        assert pruned_1[0]["feed"] == feed_name
        assert pruned_1[0]["status"] == str(DownloadStatus.DOWNLOADED)

    # Prune items older than 'base_time + 2 days' for feed_name
    # Candidates: ps_v1_older_dl (day -5), ps_v2_mid_err (day -2), ps_v4_arch (day 0), ps_v3_newer_q (day +1)
    since_cutoff_2 = base_time + datetime.timedelta(days=2)
    pruned_2 = db_manager.get_downloads_to_prune_by_since(
        feed=feed_name, since=since_cutoff_2
    )
    pruned_ids_2 = sorted([row["id"] for row in pruned_2])
    assert len(pruned_2) == 3
    assert pruned_ids_2 == sorted(["ps_v1_older_dl", "ps_v2_mid_err", "ps_v3_newer_q"])


@pytest.mark.unit
def test_get_errors(db_manager: DatabaseManager):
    """Test fetching downloads with 'error' status, including offset and limit."""
    base_time = datetime.datetime(2023, 1, 15, 12, 0, 0, tzinfo=datetime.UTC)
    feed1 = "error_feed1"
    feed2 = "error_feed2"

    # oldest, feed2
    dl_f2e1 = Download(
        feed=feed2,
        id="f2e1",
        published=base_time - datetime.timedelta(days=3),
        status=DownloadStatus.ERROR,
        last_error="Feed 2 error",
        source_url="url",
        title="t",
        ext="mp4",
        duration=1,
    )
    # middle, feed1
    dl_f1e1_old = Download(
        feed=feed1,
        id="f1e1_old",
        published=base_time - datetime.timedelta(days=2),
        status=DownloadStatus.ERROR,
        last_error="Old error 1",
        source_url="url",
        title="t",
        ext="mp4",
        duration=1,
    )
    # newest, feed1
    dl_f1e2_new = Download(
        feed=feed1,
        id="f1e2_new",
        published=base_time - datetime.timedelta(days=1),
        status=DownloadStatus.ERROR,
        last_error="New error 1",
        source_url="url",
        title="t",
        ext="mp4",
        duration=1,
    )
    # Other status items for noise
    dl_f1q1 = Download(
        feed=feed1,
        id="f1q1",
        published=base_time,
        status=DownloadStatus.QUEUED,
        source_url="url",
        title="t",
        ext="mp4",
        duration=1,
    )
    dl_f3d1 = Download(
        feed="feed3_no_errors",
        id="f3d1",
        published=base_time,
        status=DownloadStatus.DOWNLOADED,
        source_url="url",
        title="t",
        ext="mp4",
        duration=1,
    )

    downloads_to_add = [dl_f2e1, dl_f1e1_old, dl_f1e2_new, dl_f1q1, dl_f3d1]
    for dl_data in downloads_to_add:
        db_manager.upsert_download(dl_data)

    # Expected order for all errors: f2e1 (oldest), f1e1_old, f1e2_new (newest)

    # Test case 1: Get all errors (default limit 100, default offset 0)
    all_errors = db_manager.get_errors()
    assert len(all_errors) == 3, "Should fetch all 3 error downloads"
    assert [row["id"] for row in all_errors] == ["f2e1", "f1e1_old", "f1e2_new"]
    for row in all_errors:
        assert row["status"] == str(DownloadStatus.ERROR)

    # Test case 2: Get errors for feed1 (default limit, default offset)
    # Expected order for feed1: f1e1_old, f1e2_new
    feed1_errors = db_manager.get_errors(feed=feed1)
    assert len(feed1_errors) == 2, "Should fetch 2 errors for feed1"
    assert [row["id"] for row in feed1_errors] == ["f1e1_old", "f1e2_new"]

    # Test case 3: Get errors with limit
    limited_errors = db_manager.get_errors(limit=1, offset=0)
    assert len(limited_errors) == 1, "Should fetch only 1 error with limit=1"
    assert limited_errors[0]["id"] == "f2e1", "Should be the oldest overall error"

    limited_errors_feed1 = db_manager.get_errors(feed=feed1, limit=1, offset=0)
    assert len(limited_errors_feed1) == 1
    assert limited_errors_feed1[0]["id"] == "f1e1_old", "Should be oldest for feed1"

    # Test case 4: Get errors with offset
    offset_errors = db_manager.get_errors(limit=100, offset=1)  # Skip 1, get the rest
    assert len(offset_errors) == 2, "Should fetch 2 errors with offset=1"
    assert [row["id"] for row in offset_errors] == ["f1e1_old", "f1e2_new"]

    offset_errors_feed1 = db_manager.get_errors(
        feed=feed1, limit=100, offset=1
    )  # Skip 1 from feed1 errors
    assert len(offset_errors_feed1) == 1, "Should fetch 1 error for feed1 with offset=1"
    assert offset_errors_feed1[0]["id"] == "f1e2_new"

    # Test case 5: Get errors with limit and offset
    limit_offset_errors = db_manager.get_errors(
        limit=1, offset=1
    )  # Skip f2e1, get f1e1_old
    assert len(limit_offset_errors) == 1, "Should fetch 1 error with limit=1, offset=1"
    assert limit_offset_errors[0]["id"] == "f1e1_old"

    # Test case 6: Offset greater than number of items
    offset_too_high = db_manager.get_errors(limit=100, offset=5)
    assert len(offset_too_high) == 0, "Should return empty list if offset is too high"

    offset_too_high_feed1 = db_manager.get_errors(feed=feed1, limit=100, offset=3)
    assert len(offset_too_high_feed1) == 0, (
        "Should return empty for feed1 if offset is too high"
    )

    # Test case 7: No errors for a specific feed
    no_errors_feed3 = db_manager.get_errors(feed="feed3_no_errors")
    assert len(no_errors_feed3) == 0, "Should return empty list for feed with no errors"

    # Test case 8: No errors at all (after updating existing errors)
    db_manager.update_status(feed=feed1, id="f1e1_old", status=DownloadStatus.QUEUED)
    db_manager.update_status(
        feed=feed1, id="f1e2_new", status=DownloadStatus.DOWNLOADED
    )
    db_manager.update_status(feed=feed2, id="f2e1", status=DownloadStatus.SKIPPED)
    all_errors_cleared = db_manager.get_errors()
    assert len(all_errors_cleared) == 0, (
        "Should return empty list when all errors are cleared"
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
