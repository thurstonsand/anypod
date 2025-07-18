# pyright: reportPrivateUsage=false

"""Integration tests for Pruner with real database and file system operations."""

from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio

from anypod.data_coordinator.pruner import Pruner
from anypod.db import DownloadDatabase
from anypod.db.feed_db import FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.exceptions import PruneError
from anypod.file_manager import FileManager

# Test data constants
TEST_FEED_ID = "test_feed"
BASE_PUBLISH_DATE = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

SAMPLE_DOWNLOADS = [
    # Downloaded items (older to newer by published date)
    Download(
        feed_id=TEST_FEED_ID,
        id="downloaded_old_1",
        source_url="https://example.com/video1",
        title="Old Downloaded Video 1",
        published=BASE_PUBLISH_DATE - timedelta(days=10),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024 * 1024,  # 1MB
        duration=120,
        status=DownloadStatus.DOWNLOADED,
        retries=0,
        discovered_at=BASE_PUBLISH_DATE - timedelta(days=9),
        updated_at=BASE_PUBLISH_DATE - timedelta(days=9),
    ),
    Download(
        feed_id=TEST_FEED_ID,
        id="downloaded_old_2",
        source_url="https://example.com/video2",
        title="Old Downloaded Video 2",
        published=BASE_PUBLISH_DATE - timedelta(days=8),
        ext="webm",
        mime_type="video/webm",
        filesize=2 * 1024 * 1024,  # 2MB
        duration=180,
        status=DownloadStatus.DOWNLOADED,
        retries=0,
        discovered_at=BASE_PUBLISH_DATE - timedelta(days=7),
        updated_at=BASE_PUBLISH_DATE - timedelta(days=7),
    ),
    Download(
        feed_id=TEST_FEED_ID,
        id="downloaded_mid",
        source_url="https://example.com/video3",
        title="Mid Downloaded Video",
        published=BASE_PUBLISH_DATE - timedelta(days=5),
        ext="mp4",
        mime_type="video/mp4",
        filesize=3 * 1024 * 1024,  # 3MB
        duration=200,
        status=DownloadStatus.DOWNLOADED,
        retries=0,
        discovered_at=BASE_PUBLISH_DATE - timedelta(days=4),
        updated_at=BASE_PUBLISH_DATE - timedelta(days=4),
    ),
    Download(
        feed_id=TEST_FEED_ID,
        id="downloaded_recent_1",
        source_url="https://example.com/video4",
        title="Recent Downloaded Video 1",
        published=BASE_PUBLISH_DATE - timedelta(days=2),
        ext="mp4",
        mime_type="video/mp4",
        filesize=int(1.5 * 1024 * 1024),  # 1.5MB
        duration=150,
        status=DownloadStatus.DOWNLOADED,
        retries=0,
        discovered_at=BASE_PUBLISH_DATE - timedelta(days=1),
        updated_at=BASE_PUBLISH_DATE - timedelta(days=1),
    ),
    Download(
        feed_id=TEST_FEED_ID,
        id="downloaded_recent_2",
        source_url="https://example.com/video5",
        title="Recent Downloaded Video 2",
        published=BASE_PUBLISH_DATE - timedelta(days=1),
        ext="mkv",
        mime_type="video/mkv",
        filesize=4 * 1024 * 1024,  # 4MB
        duration=240,
        status=DownloadStatus.DOWNLOADED,
        retries=0,
        discovered_at=BASE_PUBLISH_DATE - timedelta(hours=12),
        updated_at=BASE_PUBLISH_DATE - timedelta(hours=12),
    ),
    # Non-downloaded items (should not have files deleted but can be archived)
    Download(
        feed_id=TEST_FEED_ID,
        id="queued_old",
        source_url="https://example.com/video6",
        title="Old Queued Video",
        published=BASE_PUBLISH_DATE - timedelta(days=9),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024 * 1024,  # 1MB
        duration=100,
        status=DownloadStatus.QUEUED,
        retries=0,
        discovered_at=BASE_PUBLISH_DATE - timedelta(days=8),
        updated_at=BASE_PUBLISH_DATE - timedelta(days=8),
    ),
    Download(
        feed_id=TEST_FEED_ID,
        id="error_old",
        source_url="https://example.com/video7",
        title="Old Error Video",
        published=BASE_PUBLISH_DATE - timedelta(days=7),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024 * 1024,  # 1MB
        duration=90,
        status=DownloadStatus.ERROR,
        retries=3,
        last_error="Failed to download",
        discovered_at=BASE_PUBLISH_DATE - timedelta(days=6),
        updated_at=BASE_PUBLISH_DATE - timedelta(days=6),
    ),
    Download(
        feed_id=TEST_FEED_ID,
        id="skipped_old",
        source_url="https://example.com/video8",
        title="Old Skipped Video",
        published=BASE_PUBLISH_DATE - timedelta(days=6),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024 * 1024,  # 1MB
        duration=110,
        status=DownloadStatus.SKIPPED,
        retries=0,
        discovered_at=BASE_PUBLISH_DATE - timedelta(days=5),
        updated_at=BASE_PUBLISH_DATE - timedelta(days=5),
    ),
    # Items that should be excluded from pruning or have special handling
    Download(
        feed_id=TEST_FEED_ID,
        id="upcoming_recent",
        source_url="https://example.com/video9",
        title="Recent Upcoming Video",
        published=BASE_PUBLISH_DATE - timedelta(days=3),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024 * 1024,  # 1MB
        duration=130,
        status=DownloadStatus.UPCOMING,
        retries=0,
        discovered_at=BASE_PUBLISH_DATE - timedelta(days=2),
        updated_at=BASE_PUBLISH_DATE - timedelta(days=2),
    ),
    Download(
        feed_id=TEST_FEED_ID,
        id="archived_old",
        source_url="https://example.com/video10",
        title="Old Archived Video",
        published=BASE_PUBLISH_DATE - timedelta(days=15),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1024 * 1024,  # 1MB
        duration=140,
        status=DownloadStatus.ARCHIVED,
        retries=0,
        discovered_at=BASE_PUBLISH_DATE - timedelta(days=14),
        updated_at=BASE_PUBLISH_DATE - timedelta(days=14),
    ),
]

# --- Tests for Pruner.prune_feed_downloads ---


def get_downloads_by_status(
    downloads: list[Download], status: DownloadStatus
) -> list[Download]:
    """Helper function to get downloads by status."""
    return [dl for dl in downloads if dl.status == status]


def get_downloads_by_published_order(
    downloads: list[Download], status: DownloadStatus, reverse: bool = False
) -> list[Download]:
    """Helper function to get downloads by status sorted by published date."""
    filtered = get_downloads_by_status(downloads, status)
    return sorted(filtered, key=lambda dl: dl.published, reverse=reverse)


async def create_dummy_file(file_manager: FileManager, download: Download) -> Path:
    """Create a dummy file for a download on the filesystem.

    Args:
        file_manager: FileManager instance for path resolution.
        download: Download object to create file for.

    Returns:
        Path to the created file.
    """
    feed_dir = await file_manager._paths.feed_data_dir(download.feed_id)

    file_name = f"{download.id}.{download.ext}"
    file_path = feed_dir / file_name

    # Create dummy content with appropriate size
    dummy_content = b"dummy video content " * (
        download.filesize // 20 if download.filesize else 50
    )
    if download.filesize:
        # Pad or truncate to match expected filesize
        dummy_content = dummy_content[: download.filesize]
        if len(dummy_content) < download.filesize:
            dummy_content += b"0" * (download.filesize - len(dummy_content))

    file_path.write_bytes(dummy_content)
    return file_path


@pytest_asyncio.fixture
async def populated_test_data(
    download_db: DownloadDatabase, feed_db: FeedDatabase, file_manager: FileManager
) -> AsyncGenerator[list[Download]]:
    """Populate database with test downloads and create corresponding files for DOWNLOADED items."""
    # Create the feed record first (required for pruner's recalculate_total_downloads)
    test_feed = Feed(
        id=TEST_FEED_ID,
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://www.youtube.com/@testchannel",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title="Test Feed",
        description="Test feed for integration tests",
    )
    await feed_db.upsert_feed(test_feed)

    # Insert all downloads into database
    for download in SAMPLE_DOWNLOADS:
        await download_db.upsert_download(download)

    # Create files only for DOWNLOADED items
    created_files: list[Path] = []
    for download in SAMPLE_DOWNLOADS:
        if download.status == DownloadStatus.DOWNLOADED:
            file_path: Path = await create_dummy_file(file_manager, download)
            created_files.append(file_path)

    yield SAMPLE_DOWNLOADS

    # Cleanup: files should be deleted by tests, but clean up any remaining
    for file_path in created_files:
        if file_path.exists():
            file_path.unlink()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_prune_feed_downloads_keep_last_success(
    pruner: Pruner,
    download_db: DownloadDatabase,
    file_manager: FileManager,
    populated_test_data: list[Download],
):
    """Tests successful pruning with keep_last rule."""
    keep_last = 2

    # Get initial counts
    initial_downloaded = get_downloads_by_status(
        populated_test_data, DownloadStatus.DOWNLOADED
    )
    initial_downloaded_count = len(initial_downloaded)
    assert initial_downloaded_count == 5  # Should have 5 DOWNLOADED items

    # Verify files exist for all DOWNLOADED items
    for download in initial_downloaded:
        assert await file_manager.download_exists(
            TEST_FEED_ID, download.id, download.ext
        )

    # Run pruner with keep_last=2
    archived_count, files_deleted_count = await pruner.prune_feed_downloads(
        feed_id=TEST_FEED_ID,
        keep_last=keep_last,
        prune_before_date=None,
    )

    # Expected to be pruned (based on published date order, keeping 2 most recent):
    # Prunable statuses: DOWNLOADED, QUEUED, ERROR, UPCOMING (but NOT SKIPPED or ARCHIVED)
    # Total prunable: 5 DOWNLOADED + 1 QUEUED + 1 ERROR + 1 UPCOMING = 8 items
    # Keep 2 most recent: Keep the 2 most recent by published date
    # Prune: 8 - 2 = 6 archived items
    expected_archived = 6
    assert archived_count == expected_archived

    # Should delete files only for DOWNLOADED items that were pruned
    # The 2 most recent downloads by published date are:
    # - downloaded_recent_2 (day -1), downloaded_recent_1 (day -2)
    # So 3 DOWNLOADED items should be pruned and files deleted: downloaded_old_1, downloaded_old_2, downloaded_mid
    expected_files_deleted = 3
    assert files_deleted_count == expected_files_deleted

    # Verify remaining downloads in database
    remaining_downloaded = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=TEST_FEED_ID
    )
    assert len(remaining_downloaded) == 2  # Should keep 2 most recent

    # Verify the correct downloads were kept (most recent by published date)
    remaining_ids = {dl.id for dl in remaining_downloaded}
    most_recent_downloaded = get_downloads_by_published_order(
        populated_test_data, DownloadStatus.DOWNLOADED, reverse=True
    )[:2]
    expected_remaining_ids = {dl.id for dl in most_recent_downloaded}
    assert remaining_ids == expected_remaining_ids

    # Verify files were deleted for pruned DOWNLOADED items
    kept_downloaded_ids = {dl.id for dl in most_recent_downloaded}
    for download in initial_downloaded:
        if download.id in kept_downloaded_ids:
            assert await file_manager.download_exists(
                TEST_FEED_ID, download.id, download.ext
            )
        else:
            assert not await file_manager.download_exists(
                TEST_FEED_ID, download.id, download.ext
            )

    # Verify archived downloads increased
    archived_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.ARCHIVED, feed_id=TEST_FEED_ID
    )
    # Should have the original archived item plus the newly archived ones
    assert (
        len(archived_downloads) == expected_archived + 1
    )  # +1 for pre-existing archived item

    skipped_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.SKIPPED, feed_id=TEST_FEED_ID
    )
    assert len(skipped_downloads) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_prune_feed_downloads_since_date_success(
    pruner: Pruner,
    download_db: DownloadDatabase,
    file_manager: FileManager,
    populated_test_data: list[Download],
):
    """Tests successful pruning with prune_before_date rule."""
    # Prune everything older than 4 days
    prune_before_date = BASE_PUBLISH_DATE - timedelta(days=4)

    # Run pruner with date filter
    archived_count, files_deleted_count = await pruner.prune_feed_downloads(
        feed_id=TEST_FEED_ID,
        keep_last=None,
        prune_before_date=prune_before_date,
    )

    # Should archive items published before the cutoff (older than 4 days):
    # Items older than cutoff and prunable (NOT SKIPPED or ARCHIVED):
    # - downloaded_old_1 (-10 days), downloaded_old_2 (-8 days), downloaded_mid (-5 days)
    # - queued_old (-9 days), error_old (-7 days)
    # SKIPPED items are ignored, UPCOMING items can be pruned if they meet date criteria
    # upcoming_recent is at -3 days (newer than cutoff), so not pruned
    # Total: 5 items archived
    expected_archived = 5
    assert archived_count == expected_archived

    # Should delete files for 3 DOWNLOADED items (downloaded_old_1, downloaded_old_2, downloaded_mid)
    expected_files_deleted = 3
    assert files_deleted_count == expected_files_deleted

    # Verify remaining DOWNLOADED items (only items newer than cutoff)
    remaining_downloaded = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=TEST_FEED_ID
    )
    assert len(remaining_downloaded) == 2

    # Find expected remaining downloads (DOWNLOADED items newer than cutoff)
    expected_remaining = [
        dl
        for dl in get_downloads_by_status(
            populated_test_data, DownloadStatus.DOWNLOADED
        )
        if dl.published >= prune_before_date
    ]
    remaining_ids = {dl.id for dl in remaining_downloaded}
    expected_remaining_ids = {dl.id for dl in expected_remaining}
    assert remaining_ids == expected_remaining_ids

    # Verify files exist for remaining downloads
    for download in remaining_downloaded:
        assert await file_manager.download_exists(
            TEST_FEED_ID, download.id, download.ext
        )

    # Verify SKIPPED items were not affected (should still be SKIPPED)
    skipped_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.SKIPPED, feed_id=TEST_FEED_ID
    )
    assert len(skipped_downloads) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_prune_feed_downloads_combined_rules(
    pruner: Pruner,
    populated_test_data: list[Download],
):
    """Tests pruning with both keep_last and prune_before_date rules combined."""
    keep_last = 3
    prune_before_date = BASE_PUBLISH_DATE - timedelta(days=6)

    # Run pruner with both rules
    archived_count, files_deleted_count = await pruner.prune_feed_downloads(
        feed_id=TEST_FEED_ID,
        keep_last=keep_last,
        prune_before_date=prune_before_date,
    )

    # Union of candidates for pruning:
    # By keep_last=3: All prunable downloads except the 3 most recent by published date
    # By date (before -6 days): All prunable downloads older than the cutoff
    # Prunable statuses: DOWNLOADED, QUEUED, ERROR, UPCOMING (NOT SKIPPED or ARCHIVED)

    # Count prunable downloads
    prunable_downloads: list[Download] = []
    for status in [
        DownloadStatus.DOWNLOADED,
        DownloadStatus.QUEUED,
        DownloadStatus.ERROR,
        DownloadStatus.UPCOMING,
    ]:
        prunable_downloads.extend(get_downloads_by_status(populated_test_data, status))

    # For keep_last=3: prune all but 3 most recent
    # For date: prune those older than cutoff
    # Union of both conditions

    # Items older than cutoff (6 days): downloaded_old_1 (-10), downloaded_old_2 (-8), queued_old (-9), error_old (-7)
    # Items by keep_last (all but 3 most recent): depends on sorting all prunable by date

    # Most recent 3 prunable by published date are likely:
    # downloaded_recent_2 (-1), downloaded_recent_1 (-2), upcoming_recent (-3)

    # Expected archived: Union of keep_last and date rules
    # This is complex to calculate exactly, but we can verify the counts
    expected_archived = 5  # Based on actual behavior
    assert archived_count == expected_archived

    # Files deleted for DOWNLOADED items that were pruned
    # Union of both rules will prune: downloaded_mid, downloaded_old_2, downloaded_old_1 (3 DOWNLOADED items)
    expected_files_deleted = 3
    assert files_deleted_count == expected_files_deleted


@pytest.mark.integration
@pytest.mark.asyncio
async def test_prune_feed_downloads_no_candidates(
    pruner: Pruner,
    download_db: DownloadDatabase,
    populated_test_data: list[Download],
):
    """Tests pruning when no downloads match the criteria."""
    # Use very restrictive rules that match nothing
    keep_last = 100  # Keep more than we have
    prune_before_date = BASE_PUBLISH_DATE - timedelta(days=30)  # Very old date

    archived_count, files_deleted_count = await pruner.prune_feed_downloads(
        feed_id=TEST_FEED_ID,
        keep_last=keep_last,
        prune_before_date=prune_before_date,
    )

    assert archived_count == 0
    assert files_deleted_count == 0

    # Verify nothing changed
    downloaded_items = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=TEST_FEED_ID
    )
    assert len(downloaded_items) == 5


@pytest.mark.integration
@pytest.mark.asyncio
async def test_prune_feed_downloads_missing_files(
    pruner: Pruner,
    download_db: DownloadDatabase,
    file_manager: FileManager,
    populated_test_data: list[Download],
):
    """Tests pruning when DOWNLOADED items have missing files."""
    # Manually delete one of the files
    missing_download = next(
        dl
        for dl in populated_test_data
        if dl.status == DownloadStatus.DOWNLOADED and dl.id == "downloaded_old_1"
    )
    await file_manager.delete_download_file(
        TEST_FEED_ID, missing_download.id, missing_download.ext
    )

    # Verify file is gone
    assert not await file_manager.download_exists(
        TEST_FEED_ID, missing_download.id, missing_download.ext
    )

    # Run pruner to remove old items
    archived_count, files_deleted_count = await pruner.prune_feed_downloads(
        feed_id=TEST_FEED_ID,
        keep_last=2,
        prune_before_date=None,
    )

    # Should still archive all expected items
    expected_archived = 6
    assert archived_count == expected_archived

    # Should delete 2 files (3 DOWNLOADED items targeted, but 1 file was already missing)
    expected_files_deleted = 2
    assert files_deleted_count == expected_files_deleted

    # Verify the download was still archived despite missing file
    archived_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.ARCHIVED, feed_id=TEST_FEED_ID
    )
    archived_ids = {dl.id for dl in archived_downloads}
    assert "downloaded_old_1" in archived_ids


@pytest.mark.integration
@pytest.mark.asyncio
async def test_prune_feed_downloads_empty_feed(
    pruner: Pruner,
    feed_db: FeedDatabase,
):
    """Tests pruning an empty feed."""
    empty_feed_id = "empty_feed"

    # Create the feed record first
    test_feed = Feed(
        id=empty_feed_id,
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://www.youtube.com/@emptyfeed",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title="Empty Feed",
        description="Test feed with no downloads",
    )
    await feed_db.upsert_feed(test_feed)

    archived_count, files_deleted_count = await pruner.prune_feed_downloads(
        feed_id=empty_feed_id,
        keep_last=5,
        prune_before_date=datetime.now(UTC),
    )

    assert archived_count == 0
    assert files_deleted_count == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_prune_feed_downloads_only_excluded_statuses(
    pruner: Pruner,
    download_db: DownloadDatabase,
    feed_db: FeedDatabase,
):
    """Tests pruning a feed with only SKIPPED and ARCHIVED items."""
    feed_id = "excluded_only_feed"

    # Create the feed record first
    test_feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://www.youtube.com/@excludedfeed",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title="Excluded Only Feed",
        description="Test feed with only excluded statuses",
    )
    await feed_db.upsert_feed(test_feed)

    # Create downloads with only excluded statuses
    excluded_downloads = [
        Download(
            feed_id=feed_id,
            id="skipped_1",
            source_url="https://example.com/skipped1",
            title="Skipped Video 1",
            published=BASE_PUBLISH_DATE - timedelta(days=10),
            ext="mp4",
            mime_type="video/mp4",
            filesize=12345,
            duration=120,
            status=DownloadStatus.SKIPPED,
            retries=0,
            discovered_at=BASE_PUBLISH_DATE,
            updated_at=BASE_PUBLISH_DATE,
        ),
        Download(
            feed_id=feed_id,
            id="archived_1",
            source_url="https://example.com/archived1",
            title="Archived Video 1",
            published=BASE_PUBLISH_DATE - timedelta(days=15),
            ext="mp4",
            mime_type="video/mp4",
            filesize=12345,
            duration=150,
            status=DownloadStatus.ARCHIVED,
            retries=0,
            discovered_at=BASE_PUBLISH_DATE,
            updated_at=BASE_PUBLISH_DATE,
        ),
    ]

    for download in excluded_downloads:
        await download_db.upsert_download(download)

    archived_count, files_deleted_count = await pruner.prune_feed_downloads(
        feed_id=feed_id,
        keep_last=1,
        prune_before_date=datetime.now(UTC),
    )

    # Nothing should be pruned since SKIPPED and ARCHIVED are excluded
    assert archived_count == 0
    assert files_deleted_count == 0

    # Verify items remain in their original status
    skipped_items = await download_db.get_downloads_by_status(
        DownloadStatus.SKIPPED, feed_id=feed_id
    )
    assert len(skipped_items) == 1

    archived_items = await download_db.get_downloads_by_status(
        DownloadStatus.ARCHIVED, feed_id=feed_id
    )
    assert len(archived_items) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_prune_feed_downloads_large_dataset(
    pruner: Pruner,
    download_db: DownloadDatabase,
    feed_db: FeedDatabase,
    file_manager: FileManager,
):
    """Tests pruning with a larger dataset to verify performance and correctness."""
    feed_id = "large_dataset_feed"

    # Create the feed record first
    test_feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://www.youtube.com/@largefeed",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title="Large Dataset Feed",
        description="Test feed with large dataset",
    )
    await feed_db.upsert_feed(test_feed)
    num_downloads = 50

    # Create many downloads
    large_downloads: list[Download] = []
    for i in range(num_downloads):
        download = Download(
            feed_id=feed_id,
            id=f"download_{i:03d}",
            source_url=f"https://example.com/video{i}",
            title=f"Video {i}",
            published=BASE_PUBLISH_DATE - timedelta(days=i),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024 * 1024,  # 1MB
            duration=120,
            status=DownloadStatus.DOWNLOADED if i % 2 == 0 else DownloadStatus.QUEUED,
            retries=0,
            discovered_at=BASE_PUBLISH_DATE - timedelta(days=i),
            updated_at=BASE_PUBLISH_DATE - timedelta(days=i),
        )
        large_downloads.append(download)
        await download_db.upsert_download(download)

        # Create files for DOWNLOADED items
        if download.status == DownloadStatus.DOWNLOADED:
            await create_dummy_file(file_manager, download)

    # Prune keeping only the 10 most recent
    archived_count, files_deleted_count = await pruner.prune_feed_downloads(
        feed_id=feed_id,
        keep_last=10,
        prune_before_date=None,
    )

    # Should archive 40 items (50 - 10 kept)
    assert archived_count == 40

    # Should delete 20 files (half were DOWNLOADED)
    assert files_deleted_count == 20

    # Verify 10 most recent remain non-archived
    remaining_downloaded = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    remaining_queued = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )

    total_remaining = len(remaining_downloaded) + len(remaining_queued)
    assert total_remaining == 10


@pytest.mark.integration
@pytest.mark.asyncio
async def test_prune_feed_downloads_zero_keep_last(
    pruner: Pruner,
    populated_test_data: list[Download],
):
    """Tests pruning with keep_last=0 (should prune nothing)."""
    archived_count, files_deleted_count = await pruner.prune_feed_downloads(
        feed_id=TEST_FEED_ID,
        keep_last=0,
        prune_before_date=None,
    )

    # Should prune nothing because keep_last=0 is treated as "ignore this rule"
    assert archived_count == 0
    assert files_deleted_count == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_prune_feed_downloads_future_date(
    pruner: Pruner,
    download_db: DownloadDatabase,
    populated_test_data: list[Download],
):
    """Tests pruning with a future prune_before_date (should prune everything prunable)."""
    future_date = BASE_PUBLISH_DATE + timedelta(days=10)

    archived_count, files_deleted_count = await pruner.prune_feed_downloads(
        feed_id=TEST_FEED_ID,
        keep_last=None,
        prune_before_date=future_date,
    )

    # Should archive all prunable items (NOT SKIPPED or ARCHIVED):
    # DOWNLOADED: 5, QUEUED: 1, ERROR: 1, UPCOMING: 1 = 8 total
    expected_archived = 8
    assert archived_count == expected_archived

    # Should delete files for all DOWNLOADED items (5 total)
    expected_files_deleted = 5
    assert files_deleted_count == expected_files_deleted

    # Verify only SKIPPED and pre-existing ARCHIVED items remain unchanged
    remaining_downloads: list[Download] = []
    for status in [
        DownloadStatus.DOWNLOADED,
        DownloadStatus.QUEUED,
        DownloadStatus.ERROR,
        DownloadStatus.UPCOMING,
    ]:
        remaining_downloads.extend(
            await download_db.get_downloads_by_status(status, feed_id=TEST_FEED_ID)
        )

    assert len(remaining_downloads) == 0

    # SKIPPED should remain
    skipped_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.SKIPPED, feed_id=TEST_FEED_ID
    )
    assert len(skipped_downloads) == 1

    # ARCHIVED should now include the newly archived items plus the original
    archived_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.ARCHIVED, feed_id=TEST_FEED_ID
    )
    assert (
        len(archived_downloads) == expected_archived + 1
    )  # +1 for pre-existing archived item


# --- Tests for Pruner.archive_feed ---


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archive_feed_success(
    pruner: Pruner,
    download_db: DownloadDatabase,
    feed_db: FeedDatabase,
    file_manager: FileManager,
    populated_test_data: list[Download],
):
    """Tests archive_feed successfully archives all non-terminal downloads and disables feed."""
    # Verify initial state
    initial_downloaded = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=TEST_FEED_ID
    )
    initial_queued = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=TEST_FEED_ID
    )
    initial_upcoming = await download_db.get_downloads_by_status(
        DownloadStatus.UPCOMING, feed_id=TEST_FEED_ID
    )
    initial_error = await download_db.get_downloads_by_status(
        DownloadStatus.ERROR, feed_id=TEST_FEED_ID
    )

    total_non_terminal = (
        len(initial_downloaded)
        + len(initial_queued)
        + len(initial_upcoming)
        + len(initial_error)
    )

    # Verify files exist for downloaded items
    for download in initial_downloaded:
        assert await file_manager.download_exists(
            TEST_FEED_ID, download.id, download.ext
        )

    # Archive the feed
    archived_count, files_deleted_count = await pruner.archive_feed(TEST_FEED_ID)

    # Verify results
    assert archived_count == total_non_terminal
    assert files_deleted_count == len(initial_downloaded)

    # Verify all non-terminal downloads are now archived
    for status in [
        DownloadStatus.DOWNLOADED,
        DownloadStatus.QUEUED,
        DownloadStatus.UPCOMING,
        DownloadStatus.ERROR,
    ]:
        remaining = await download_db.get_downloads_by_status(
            status, feed_id=TEST_FEED_ID
        )
        assert len(remaining) == 0

    # Verify SKIPPED downloads are unchanged
    skipped_downloads = await download_db.get_downloads_by_status(
        DownloadStatus.SKIPPED, feed_id=TEST_FEED_ID
    )
    assert len(skipped_downloads) == 1

    # Verify files are deleted for previously downloaded items
    for download in initial_downloaded:
        assert not await file_manager.download_exists(
            TEST_FEED_ID, download.id, download.ext
        )

    # Verify feed is disabled
    feed = await feed_db.get_feed_by_id(TEST_FEED_ID)
    assert feed.is_enabled is False

    # Verify total_downloads was recalculated to 0
    assert feed.total_downloads == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archive_feed_transaction_rollback_on_failure(
    pruner: Pruner,
    download_db: DownloadDatabase,
    feed_db: FeedDatabase,
    file_manager: FileManager,
):
    """Tests archive_feed transaction rollback when download archival fails."""
    feed_id = "rollback_test_feed"

    # Create test feed
    test_feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://www.youtube.com/@rollbackfeed",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title="Rollback Test Feed",
        description="Feed for testing transaction rollback",
    )
    await feed_db.upsert_feed(test_feed)

    # Create test downloads
    test_downloads = [
        Download(
            feed_id=feed_id,
            id="download_1",
            source_url="https://example.com/video1",
            title="Video 1",
            published=BASE_PUBLISH_DATE - timedelta(days=1),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=120,
            status=DownloadStatus.DOWNLOADED,
            retries=0,
            discovered_at=BASE_PUBLISH_DATE,
            updated_at=BASE_PUBLISH_DATE,
        ),
        Download(
            feed_id=feed_id,
            id="download_2",
            source_url="https://example.com/video2",
            title="Video 2",
            published=BASE_PUBLISH_DATE - timedelta(days=2),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=180,
            status=DownloadStatus.QUEUED,
            retries=0,
            discovered_at=BASE_PUBLISH_DATE,
            updated_at=BASE_PUBLISH_DATE,
        ),
    ]

    # Insert downloads and create file for downloaded item
    for download in test_downloads:
        await download_db.upsert_download(download)
        if download.status == DownloadStatus.DOWNLOADED:
            await create_dummy_file(file_manager, download)

    # Verify initial state
    initial_downloaded = await download_db.get_downloads_by_status(
        DownloadStatus.DOWNLOADED, feed_id=feed_id
    )
    initial_queued = await download_db.get_downloads_by_status(
        DownloadStatus.QUEUED, feed_id=feed_id
    )
    assert len(initial_downloaded) == 1
    assert len(initial_queued) == 1

    # Mock archive_download to fail partway through
    original_archive = download_db.archive_download

    async def failing_archive(feed_id: str, download_id: str) -> None:
        if download_id == "download_2":
            from anypod.exceptions import DatabaseOperationError

            raise DatabaseOperationError("Simulated archive failure")
        return await original_archive(feed_id, download_id)

    download_db.archive_download = failing_archive

    try:
        # Archive should fail and rollback
        with pytest.raises(PruneError):
            await pruner.archive_feed(feed_id)

        # Verify rollback occurred - downloads should still be in original states
        remaining_downloaded = await download_db.get_downloads_by_status(
            DownloadStatus.DOWNLOADED, feed_id=feed_id
        )
        remaining_queued = await download_db.get_downloads_by_status(
            DownloadStatus.QUEUED, feed_id=feed_id
        )

        # Transaction should have rolled back
        assert len(remaining_downloaded) == 1
        assert len(remaining_queued) == 1

        # Files should still exist for downloaded items
        for download in initial_downloaded:
            assert await file_manager.download_exists(
                feed_id, download.id, download.ext
            )

        # Feed should still be enabled (rollback)
        feed = await feed_db.get_feed_by_id(feed_id)
        assert feed.is_enabled is True

    finally:
        # Cleanup
        download_db.archive_download = original_archive
