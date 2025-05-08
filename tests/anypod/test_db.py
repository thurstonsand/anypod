from collections.abc import Iterator
import datetime
import sqlite3

import pytest

from anypod.db import DatabaseManager, DownloadItem, DownloadStatus

# --- Fixtures ---


@pytest.fixture
def db_manager(tmp_path_factory: pytest.TempPathFactory) -> Iterator[DatabaseManager]:
    """Provides a DatabaseManager instance with a temporary file-based SQLite database."""
    # Using tmp_path_factory to create a unique temporary directory for the test run
    temp_dir = tmp_path_factory.mktemp("test_db_data")
    db_file = temp_dir / "test_anypod.db"
    manager = DatabaseManager(db_path=db_file)
    yield manager  # Test will run here
    manager.close()  # Ensure DB connection is closed


@pytest.fixture
def sample_item() -> DownloadItem:
    """Provides a sample DownloadItem instance for adding to the DB."""
    return DownloadItem(
        feed="test_feed",
        video_id="v123",
        source_url="http://example.com/video/v123",
        title="Test Video Title One",
        published=datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC),
        ext="mp4",
        duration=120.5,
        thumbnail="http://example.com/thumb/v123.jpg",
        status=DownloadStatus.QUEUED,
        # path, retries, last_error will use defaults from DownloadItem (None, 0, None)
    )


# --- Tests ---


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
def test_add_and_get_item(db_manager: DatabaseManager, sample_item: DownloadItem):
    """Test adding a new item and then retrieving it."""
    db_manager.add_item(sample_item)

    retrieved_item = db_manager.get_item_by_video_id(
        feed=sample_item.feed,
        video_id=sample_item.video_id,
    )

    assert retrieved_item is not None, "Item should be found in DB"
    assert retrieved_item["feed"] == sample_item.feed
    assert retrieved_item["video_id"] == sample_item.video_id
    assert retrieved_item["title"] == sample_item.title
    assert retrieved_item["published"] == sample_item.published.isoformat()
    assert retrieved_item["ext"] == sample_item.ext
    assert retrieved_item["duration"] == sample_item.duration
    assert retrieved_item["thumbnail"] == sample_item.thumbnail
    assert retrieved_item["status"] == str(sample_item.status)
    assert retrieved_item["retries"] == 0, (
        "Retries should be 0 for a new item from fixture"
    )
    assert retrieved_item["last_error"] is None, (
        "Last_error should be None for a new item from fixture"
    )
    assert retrieved_item["path"] is None, (
        "Path should be None for a new item from fixture"
    )


@pytest.mark.unit
def test_add_item_conflict(db_manager: DatabaseManager, sample_item: DownloadItem):
    """Test that adding an item with a conflicting (feed, video_id) raises IntegrityError."""
    db_manager.add_item(sample_item)  # Add the first item

    # Try to add the same item again
    with pytest.raises(sqlite3.IntegrityError):
        db_manager.add_item(sample_item)


@pytest.mark.unit
def test_update_status(db_manager: DatabaseManager, sample_item: DownloadItem):
    """Test updating the status of a download item, including new re-queue logic."""
    # 1. Add an initial item (status QUEUED, retries 0, last_error None)
    db_manager.add_item(sample_item)

    # 2. Update to DOWNLOADED
    download_path = "/media/test_feed/test_video.mp4"
    db_manager.update_status(
        feed=sample_item.feed,
        video_id=sample_item.video_id,
        status=DownloadStatus.DOWNLOADED,
        path=download_path,
    )
    item_downloaded = db_manager.get_item_by_video_id(
        sample_item.feed, sample_item.video_id
    )
    assert item_downloaded is not None
    assert item_downloaded["status"] == str(DownloadStatus.DOWNLOADED)
    assert item_downloaded["path"] == download_path
    assert item_downloaded["retries"] == 0, "Retries should be reset on DOWNLOADED"
    assert item_downloaded["last_error"] is None, (
        "Error should be cleared on DOWNLOADED"
    )

    # 3. Update to ERROR
    error_message = "Download failed: Network issue"
    db_manager.update_status(
        feed=sample_item.feed,
        video_id=sample_item.video_id,
        status=DownloadStatus.ERROR,
        last_error=error_message,
    )
    item_error = db_manager.get_item_by_video_id(sample_item.feed, sample_item.video_id)
    assert item_error is not None
    assert item_error["status"] == str(DownloadStatus.ERROR)
    assert item_error["last_error"] == error_message
    assert item_error["retries"] == 1, "Retries should be incremented on first ERROR"

    # 4. Update to ERROR again (check retry increment)
    error_message_2 = "Download failed again"
    db_manager.update_status(
        feed=sample_item.feed,
        video_id=sample_item.video_id,
        status=DownloadStatus.ERROR,
        last_error=error_message_2,
    )
    item_error_2 = db_manager.get_item_by_video_id(
        sample_item.feed, sample_item.video_id
    )
    assert item_error_2 is not None
    assert item_error_2["retries"] == 2, "Retries should increment on subsequent ERROR"
    assert item_error_2["last_error"] == error_message_2

    # 5. Update to SKIPPED (retries and last_error should persist, path also persists)
    db_manager.update_status(
        feed=sample_item.feed,
        video_id=sample_item.video_id,
        status=DownloadStatus.SKIPPED,
    )
    item_skipped = db_manager.get_item_by_video_id(
        sample_item.feed, sample_item.video_id
    )
    assert item_skipped is not None
    assert item_skipped["status"] == str(DownloadStatus.SKIPPED)
    assert item_skipped["last_error"] == error_message_2, (
        "Error message should persist on SKIPPED"
    )
    assert item_skipped["retries"] == 2, "Retries should persist on SKIPPED"
    assert item_skipped["path"] == download_path, "Path should persist on SKIPPED"

    # 6. Update back to QUEUED (retries and last_error should persist, path becomes NULL)
    db_manager.update_status(
        feed=sample_item.feed,
        video_id=sample_item.video_id,
        status=DownloadStatus.QUEUED,
    )
    item_requeued = db_manager.get_item_by_video_id(
        sample_item.feed, sample_item.video_id
    )
    assert item_requeued is not None
    assert item_requeued["status"] == str(DownloadStatus.QUEUED)
    assert item_requeued["path"] is None, "Path should be NULL when re-queued"
    assert item_requeued["last_error"] == error_message_2, (
        "Error message should persist on re-QUEUE"
    )
    assert item_requeued["retries"] == 2, "Retries should persist on re-QUEUE"

    # 7. Test updating a non-existent item
    non_existent_update = db_manager.update_status(
        feed="other_feed", video_id="non_existent_id", status=DownloadStatus.DOWNLOADED
    )
    assert non_existent_update is False, (
        "Updating non-existent item should return False"
    )


@pytest.mark.unit
def test_next_queued_items(db_manager: DatabaseManager, sample_item: DownloadItem):
    """Test fetching the next queued items for a feed."""
    base_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)

    # Items for feed1
    item1_feed1 = DownloadItem(
        feed="feed1",
        video_id="v001",
        source_url="url1",
        title="title1_old",
        published=base_time - datetime.timedelta(hours=2),
        ext="mp4",
        duration=10,
        status=DownloadStatus.QUEUED,
    )
    item2_feed1 = DownloadItem(
        feed="feed1",
        video_id="v002",
        source_url="url2",
        title="title2_new",
        published=base_time - datetime.timedelta(hours=1),
        ext="mp4",
        duration=10,
        status=DownloadStatus.QUEUED,
    )
    item3_feed1_downloaded = DownloadItem(
        feed="feed1",
        video_id="v003",
        source_url="url3",
        title="title3_downloaded",
        published=base_time,
        ext="mp4",
        duration=10,
        status=DownloadStatus.DOWNLOADED,
    )
    item4_feed1_error = DownloadItem(
        feed="feed1",
        video_id="v004",
        source_url="url4",
        title="title4_error",
        published=base_time + datetime.timedelta(hours=1),
        ext="mp4",
        duration=10,
        status=DownloadStatus.ERROR,
    )
    item5_feed1_queued_newest = DownloadItem(
        feed="feed1",
        video_id="v005",
        source_url="url5",
        title="title5_newest",
        published=base_time + datetime.timedelta(hours=2),
        ext="mp4",
        duration=10,
        status=DownloadStatus.QUEUED,
    )

    # Item for feed2
    item6_feed2_queued = DownloadItem(
        feed="feed2",
        video_id="v006",
        source_url="url6",
        title="title6_feed2",
        published=base_time,
        ext="mp4",
        duration=10,
        status=DownloadStatus.QUEUED,
    )

    all_items = [
        item1_feed1,
        item2_feed1,
        item3_feed1_downloaded,
        item4_feed1_error,
        item5_feed1_queued_newest,
        item6_feed2_queued,
    ]
    for item in all_items:
        db_manager.add_item(item)

    # Test case 1: Get next 2 for feed1 (should be item1 and item2 in that order)
    queued_feed1_limit2 = db_manager.next_queued_items(feed="feed1", limit=2)
    assert len(queued_feed1_limit2) == 2
    assert queued_feed1_limit2[0]["video_id"] == "v001", (
        "Oldest queued item for feed1 should be first"
    )
    assert queued_feed1_limit2[1]["video_id"] == "v002", (
        "Second oldest queued item for feed1 should be second"
    )

    # Test case 2: Get all for feed1 (should be item1, item2, item5)
    queued_feed1_all = db_manager.next_queued_items(
        feed="feed1", limit=10
    )  # High limit
    assert len(queued_feed1_all) == 3
    assert [item["video_id"] for item in queued_feed1_all] == [
        "v001",
        "v002",
        "v005",
    ], "Should return all queued for feed1 in order"

    # Test case 3: Get for feed2 (should only be item6)
    queued_feed2 = db_manager.next_queued_items(feed="feed2", limit=10)
    assert len(queued_feed2) == 1
    assert queued_feed2[0]["video_id"] == "v006", "Should return only item for feed2"

    # Test case 4: Limit 0
    queued_feed1_limit0 = db_manager.next_queued_items(feed="feed1", limit=0)
    assert len(queued_feed1_limit0) == 0, "Limit 0 should return no items"

    # Test case 5: No queued items for a non-existent feed
    queued_feed_nonexistent = db_manager.next_queued_items(
        feed="feed_non_existent", limit=10
    )
    assert len(queued_feed_nonexistent) == 0, "Non-existent feed should return no items"

    # Test case 6: Feed exists but has no QUEUED items (after updating one)
    db_manager.update_status(
        feed="feed2",
        video_id="v006",
        status=DownloadStatus.DOWNLOADED,
        path="/some/path",
    )
    queued_feed2_none_left = db_manager.next_queued_items(feed="feed2", limit=10)
    assert len(queued_feed2_none_left) == 0, "Feed2 should have no queued items left"


@pytest.mark.unit
def test_get_items_to_prune_by_keep_last(db_manager: DatabaseManager):
    """Test fetching items to prune based on the 'keep_last' rule."""
    base_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    feed1_name = "prune_feed1"

    items_to_add = [
        # Feed 1: Downloaded items - these are candidates for pruning
        DownloadItem(
            feed=feed1_name,
            video_id="f1v1_dl_oldest",
            published=base_time - datetime.timedelta(days=5),
            status=DownloadStatus.DOWNLOADED,
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
            path="/path/f1v1",
        ),
        DownloadItem(
            feed=feed1_name,
            video_id="f1v2_dl_mid1",
            published=base_time - datetime.timedelta(days=4),
            status=DownloadStatus.DOWNLOADED,
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
            path="/path/f1v2",
        ),
        DownloadItem(
            feed=feed1_name,
            video_id="f1v3_dl_mid2",
            published=base_time - datetime.timedelta(days=3),
            status=DownloadStatus.DOWNLOADED,
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
            path="/path/f1v3",
        ),
        DownloadItem(
            feed=feed1_name,
            video_id="f1v4_dl_newest",
            published=base_time - datetime.timedelta(days=2),
            status=DownloadStatus.DOWNLOADED,
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
            path="/path/f1v4",
        ),
        # Feed 1: Non-downloaded items - should be ignored by this pruning logic
        DownloadItem(
            feed=feed1_name,
            video_id="f1v5_queued",
            published=base_time - datetime.timedelta(days=1),
            status=DownloadStatus.QUEUED,
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
        ),
        DownloadItem(
            feed=feed1_name,
            video_id="f1v6_error",
            published=base_time,
            status=DownloadStatus.ERROR,
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
        ),
        # Feed 2: Downloaded item - should be ignored as it's for a different feed
        DownloadItem(
            feed="prune_feed2",
            video_id="f2v1_dl",
            published=base_time - datetime.timedelta(days=3),
            status=DownloadStatus.DOWNLOADED,
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
            path="/path/f2v1",
        ),
    ]
    for item in items_to_add:
        db_manager.add_item(item)

    # Test case 1: keep_last = 2 (should prune f1v1, f1v2)
    # Newest are f1v4, f1v3. To be pruned: f1v2, f1v1 (offset 2 means skip 2 newest)
    # SQL is ORDER BY published DESC. So newest first. OFFSET 2 skips f1v4, f1v3.
    # It then selects f1v2, f1v1.
    # The `get_items_to_prune_by_keep_last` already orders by published DESC in its query.
    # The items returned by the function will be the ones *to be pruned*, sorted newest-among-the-pruned first (due to DESC order).
    # So, if we keep 2 (f1v4, f1v3), then f1v2 and f1v1 are pruned. Sqlite OFFSET 2 skips the newest 2.
    # Result from query will be f1v2, then f1v1.
    prune_keep2 = db_manager.get_items_to_prune_by_keep_last(
        feed=feed1_name, keep_last=2
    )
    assert len(prune_keep2) == 2, "Should identify 2 items to prune when keeping 2/4"
    pruned_ids_keep2 = sorted(
        [row["video_id"] for row in prune_keep2]
    )  # Sort for consistent comparison
    assert pruned_ids_keep2 == sorted(["f1v1_dl_oldest", "f1v2_dl_mid1"])
    for row in prune_keep2:
        assert row["path"] is not None, "Pruned items should have a path"

    # Test case 2: keep_last = 4 (all items for feed1 are kept, so nothing to prune)
    prune_keep4 = db_manager.get_items_to_prune_by_keep_last(
        feed=feed1_name, keep_last=4
    )
    assert len(prune_keep4) == 0, "Should identify 0 items to prune when keeping all 4"

    # Test case 3: keep_last = 5 (more than available, nothing to prune)
    prune_keep5 = db_manager.get_items_to_prune_by_keep_last(
        feed=feed1_name, keep_last=5
    )
    assert len(prune_keep5) == 0, (
        "Should identify 0 items to prune when keep_last > available"
    )

    # Test case 4: keep_last = 0 (invalid, should return empty as per function guard)
    prune_keep0 = db_manager.get_items_to_prune_by_keep_last(
        feed=feed1_name, keep_last=0
    )
    assert len(prune_keep0) == 0, "Should return 0 items if keep_last is 0"

    # Test case 5: keep_last = 1 (should prune f1v1, f1v2, f1v3)
    # Keeps f1v4. Prunes f1v3, f1v2, f1v1. (Offset 1 skips f1v4)
    prune_keep1 = db_manager.get_items_to_prune_by_keep_last(
        feed=feed1_name, keep_last=1
    )
    assert len(prune_keep1) == 3, "Should identify 3 items to prune when keeping 1/4"
    pruned_ids_keep1 = sorted([row["video_id"] for row in prune_keep1])
    assert pruned_ids_keep1 == sorted(
        ["f1v1_dl_oldest", "f1v2_dl_mid1", "f1v3_dl_mid2"]
    )

    # Test case 6: Non-existent feed
    prune_non_existent_feed = db_manager.get_items_to_prune_by_keep_last(
        feed="non_existent_feed", keep_last=1
    )
    assert len(prune_non_existent_feed) == 0, "Should return 0 for non-existent feed"


@pytest.mark.unit
def test_get_items_to_prune_by_since(db_manager: DatabaseManager):
    """Test fetching items to prune based on the 'since' date rule."""
    base_time = datetime.datetime(2023, 1, 10, 12, 0, 0, tzinfo=datetime.UTC)
    feed_name = "prune_since_feed"

    items_to_add = [
        DownloadItem(
            feed=feed_name,
            video_id="ps_v1_older",
            published=base_time - datetime.timedelta(days=5),
            status=DownloadStatus.DOWNLOADED,
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
            path="/path/ps_v1",
        ),  # Should be pruned by a since of base_time-3d
        DownloadItem(
            feed=feed_name,
            video_id="ps_v2_mid",
            published=base_time - datetime.timedelta(days=2),
            status=DownloadStatus.DOWNLOADED,
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
            path="/path/ps_v2",
        ),  # Should NOT be pruned by a since of base_time-3d
        DownloadItem(
            feed=feed_name,
            video_id="ps_v3_newer",
            published=base_time + datetime.timedelta(days=1),
            status=DownloadStatus.DOWNLOADED,
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
            path="/path/ps_v3",
        ),  # Should NOT be pruned
        # Feed: Non-downloaded - should be ignored
        DownloadItem(
            feed=feed_name,
            video_id="ps_v4_queued",
            published=base_time - datetime.timedelta(days=6),
            status=DownloadStatus.QUEUED,
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
        ),
        # Different Feed: Downloaded item - should be ignored
        DownloadItem(
            feed="other_feed",
            video_id="other_v1_older_dl",
            published=base_time - datetime.timedelta(days=5),
            status=DownloadStatus.DOWNLOADED,
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
            path="/path/other_v1",
        ),
    ]
    for item in items_to_add:
        db_manager.add_item(item)

    # Test case 1: Prune items older than 'base_time - 3 days' (i.e., ps_v1_older)
    since_cutoff_1 = base_time - datetime.timedelta(days=3)
    pruned_items_1 = db_manager.get_items_to_prune_by_since(
        feed=feed_name, since=since_cutoff_1
    )
    assert len(pruned_items_1) == 1, "Should identify 1 item older than since_cutoff_1"
    assert pruned_items_1[0]["video_id"] == "ps_v1_older"
    assert pruned_items_1[0]["path"] is not None

    # Test case 2: Prune items older than 'base_time + 2 days' (should prune ps_v1, ps_v2, ps_v3)
    since_cutoff_2 = base_time + datetime.timedelta(days=2)
    pruned_items_2 = db_manager.get_items_to_prune_by_since(
        feed=feed_name, since=since_cutoff_2
    )
    pruned_ids_2 = sorted([row["video_id"] for row in pruned_items_2])
    assert len(pruned_items_2) == 3, "Should identify 3 items older than since_cutoff_2"
    assert pruned_ids_2 == sorted(["ps_v1_older", "ps_v2_mid", "ps_v3_newer"])

    # Test case 3: Prune items older than 'base_time - 10 days' (no items this old)
    since_cutoff_3 = base_time - datetime.timedelta(days=10)
    pruned_items_3 = db_manager.get_items_to_prune_by_since(
        feed=feed_name, since=since_cutoff_3
    )
    assert len(pruned_items_3) == 0, (
        "Should identify 0 items older than a very early since_cutoff_3"
    )

    # Test case 4: Non-existent feed
    prune_non_existent_feed = db_manager.get_items_to_prune_by_since(
        feed="non_existent_feed", since=base_time
    )
    assert len(prune_non_existent_feed) == 0, "Should return 0 for non-existent feed"


@pytest.mark.unit
def test_remove_pruned_items(db_manager: DatabaseManager, sample_item: DownloadItem):
    """Test removing multiple items by their video_ids for a specific feed."""
    feed1_name = "remove_feed1"
    feed2_name = "remove_feed2"

    # Items to add
    item_f1_v1 = DownloadItem(
        feed=feed1_name,
        video_id="f1v1",
        source_url=sample_item.source_url,
        title="t1",
        published=sample_item.published,
        ext="mp4",
        duration=10,
        status=DownloadStatus.DOWNLOADED,
        path="/path/f1v1",
    )
    item_f1_v2 = DownloadItem(
        feed=feed1_name,
        video_id="f1v2",
        source_url=sample_item.source_url,
        title="t2",
        published=sample_item.published,
        ext="mp4",
        duration=10,
        status=DownloadStatus.DOWNLOADED,
        path="/path/f1v2",
    )
    item_f1_v3 = DownloadItem(
        feed=feed1_name,
        video_id="f1v3",
        source_url=sample_item.source_url,
        title="t3",
        published=sample_item.published,
        ext="mp4",
        duration=10,
        status=DownloadStatus.DOWNLOADED,
        path="/path/f1v3",
    )
    item_f2_v1 = DownloadItem(
        feed=feed2_name,
        video_id="f2v1",
        source_url=sample_item.source_url,
        title="t_f2",
        published=sample_item.published,
        ext="mp4",
        duration=10,
        status=DownloadStatus.DOWNLOADED,
        path="/path/f2v1",
    )

    items_to_add = [item_f1_v1, item_f1_v2, item_f1_v3, item_f2_v1]
    for item in items_to_add:
        db_manager.add_item(item)

    # Test case 1: Remove a subset of items from feed1
    ids_to_remove_f1 = ["f1v1", "f1v3"]
    deleted_count = db_manager.remove_pruned_items(
        feed=feed1_name, video_ids=ids_to_remove_f1
    )
    assert deleted_count == 2, "Should report 2 items deleted from feed1"
    assert db_manager.get_item_by_video_id(feed1_name, "f1v1") is None, (
        "f1v1 should be deleted"
    )
    assert db_manager.get_item_by_video_id(feed1_name, "f1v3") is None, (
        "f1v3 should be deleted"
    )
    assert db_manager.get_item_by_video_id(feed1_name, "f1v2") is not None, (
        "f1v2 should still exist"
    )
    assert db_manager.get_item_by_video_id(feed2_name, "f2v1") is not None, (
        "Item from feed2 should not be affected"
    )

    # Test case 2: Try to remove already deleted items and non-existent items for feed1
    ids_to_remove_again_f1 = ["f1v1", "non_existent_id"]
    deleted_count_again = db_manager.remove_pruned_items(
        feed=feed1_name, video_ids=ids_to_remove_again_f1
    )
    assert deleted_count_again == 0, (
        "Should report 0 items deleted if they don't exist or already gone"
    )
    assert db_manager.get_item_by_video_id(feed1_name, "f1v2") is not None, (
        "f1v2 should still exist after trying to delete others"
    )

    # Test case 3: Remove remaining item from feed1
    deleted_count_last_f1 = db_manager.remove_pruned_items(
        feed=feed1_name, video_ids=["f1v2"]
    )
    assert deleted_count_last_f1 == 1, "Should report 1 item deleted"
    assert db_manager.get_item_by_video_id(feed1_name, "f1v2") is None, (
        "f1v2 should now be deleted"
    )

    # Test case 4: Empty video_ids list
    deleted_count_empty = db_manager.remove_pruned_items(feed=feed2_name, video_ids=[])
    assert deleted_count_empty == 0, "Should report 0 items deleted for empty ID list"
    assert db_manager.get_item_by_video_id(feed2_name, "f2v1") is not None, (
        "Item f2v1 should still exist"
    )

    # Test case 5: Removing from a non-existent feed
    deleted_count_bad_feed = db_manager.remove_pruned_items(
        feed="non_existent_feed", video_ids=["f2v1"]
    )
    assert deleted_count_bad_feed == 0, (
        "Should report 0 items deleted for non-existent feed"
    )
    assert db_manager.get_item_by_video_id(feed2_name, "f2v1") is not None, (
        "Item f2v1 should still exist after trying to delete from wrong feed"
    )


@pytest.mark.unit
def test_get_errors(db_manager: DatabaseManager, sample_item: DownloadItem):
    """Test fetching items with 'error' status."""
    base_time = datetime.datetime(2023, 1, 15, 12, 0, 0, tzinfo=datetime.UTC)
    feed1 = "error_feed1"
    feed2 = "error_feed2"

    items_to_add = [
        # Feed 1 errors
        DownloadItem(
            feed=feed1,
            video_id="f1e1_old",
            published=base_time - datetime.timedelta(days=2),
            status=DownloadStatus.ERROR,
            last_error="Old error 1",
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
        ),
        DownloadItem(
            feed=feed1,
            video_id="f1e2_new",
            published=base_time - datetime.timedelta(days=1),
            status=DownloadStatus.ERROR,
            last_error="New error 1",
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
        ),
        # Feed 1 non-errors
        DownloadItem(
            feed=feed1,
            video_id="f1q1",
            published=base_time,
            status=DownloadStatus.QUEUED,
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
        ),
        # Feed 2 errors
        DownloadItem(
            feed=feed2,
            video_id="f2e1",
            published=base_time - datetime.timedelta(days=3),
            status=DownloadStatus.ERROR,
            last_error="Feed 2 error",
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
        ),
        # Feed 3 no errors, just one downloaded item
        DownloadItem(
            feed="feed3_no_errors",
            video_id="f3d1",
            published=base_time,
            status=DownloadStatus.DOWNLOADED,
            source_url="url",
            title="t",
            ext="mp4",
            duration=1,
            path="/path/f3d1",
        ),
    ]
    for item_data in items_to_add:
        # Create new DownloadItem instances for each entry if sample_item is used as a base
        # Or instantiate directly as done above.
        db_manager.add_item(item_data)

    # Test case 1: Get all errors (default limit 100)
    all_errors = db_manager.get_errors()
    assert len(all_errors) == 3, "Should fetch all 3 error items"
    # Check order (newest first overall: f1e2_new, f1e1_old, f2e1)
    assert all_errors[0]["video_id"] == "f1e2_new"
    assert all_errors[1]["video_id"] == "f1e1_old"
    assert all_errors[2]["video_id"] == "f2e1"
    for row in all_errors:
        assert row["status"] == str(DownloadStatus.ERROR)

    # Test case 2: Get errors for feed1
    feed1_errors = db_manager.get_errors(feed=feed1)
    assert len(feed1_errors) == 2, "Should fetch 2 errors for feed1"
    # Check order (newest first for feed1: f1e2_new, f1e1_old)
    assert feed1_errors[0]["video_id"] == "f1e2_new"
    assert feed1_errors[1]["video_id"] == "f1e1_old"

    # Test case 3: Get errors with limit
    limited_errors = db_manager.get_errors(limit=1)
    assert len(limited_errors) == 1, "Should fetch only 1 error with limit=1"
    assert limited_errors[0]["video_id"] == "f1e2_new", (
        "Should be the newest overall error"
    )

    limited_errors_feed1 = db_manager.get_errors(feed=feed1, limit=1)
    assert len(limited_errors_feed1) == 1
    assert limited_errors_feed1[0]["video_id"] == "f1e2_new", (
        "Should be newest for feed1"
    )

    # Test case 4: No errors for a specific feed
    no_errors_feed3 = db_manager.get_errors(feed="feed3_no_errors")
    assert len(no_errors_feed3) == 0, "Should return empty list for feed with no errors"

    # Test case 5: No errors at all (after updating existing errors)
    db_manager.update_status(
        feed=feed1, video_id="f1e1_old", status=DownloadStatus.QUEUED
    )
    db_manager.update_status(
        feed=feed1, video_id="f1e2_new", status=DownloadStatus.DOWNLOADED, path="/p"
    )
    db_manager.update_status(feed=feed2, video_id="f2e1", status=DownloadStatus.SKIPPED)
    all_errors_cleared = db_manager.get_errors()
    assert len(all_errors_cleared) == 0, (
        "Should return empty list when all errors are cleared"
    )
