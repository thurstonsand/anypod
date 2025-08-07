# pyright: reportPrivateUsage=false

"""Tests for the FeedDatabase and Feed model functionality."""

from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from pathlib import Path
import time

from helpers.alembic import run_migrations
import pytest
import pytest_asyncio

from anypod.config.types import PodcastCategories, PodcastExplicit
from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.sqlalchemy_core import SqlalchemyCore
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.exceptions import FeedNotFoundError

# --- Fixtures ---


@pytest_asyncio.fixture
async def db_core(tmp_path: Path) -> AsyncGenerator[SqlalchemyCore]:
    """Provides a SqlalchemyCore instance for testing."""
    # Run Alembic migrations to set up the database schema
    db_path = tmp_path / "anypod.db"
    run_migrations(db_path)

    # Create SqlalchemyCore instance
    core = SqlalchemyCore(tmp_path)
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


@pytest.fixture
def sample_feed() -> Feed:
    """Provides a sample Feed instance for testing."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    return Feed(
        id="test_feed",
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://www.youtube.com/@testchannel",
        created_at=base_time,
        updated_at=base_time,
        last_successful_sync=base_time + timedelta(hours=1),
        last_rss_generation=base_time + timedelta(hours=2),
        last_failed_sync=None,
        consecutive_failures=0,
        title="Test Feed Title",
        subtitle="Test Feed Subtitle",
        description="Test feed description",
        language="en",
        author="Test Author",
        image_url="https://example.com/image.jpg",
        category=PodcastCategories("Technology"),
        explicit=PodcastExplicit.NO,
    )


@pytest.fixture
def minimal_feed() -> Feed:
    """Provides a minimal Feed instance with only required fields."""
    return Feed(
        id="minimal_feed",
        is_enabled=False,
        source_type=SourceType.SINGLE_VIDEO,
        source_url="https://www.youtube.com/watch?v=test123",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        # All optional fields use defaults
    )


@pytest.fixture
def sample_downloads() -> list[Download]:
    """Provides sample Download instances for testing."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    return [
        Download(
            feed_id="test_feed",
            id="download1",
            source_url="https://www.youtube.com/watch?v=test1",
            title="Test Download 1",
            published=base_time,
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=60,
            status=DownloadStatus.DOWNLOADED,
        ),
        Download(
            feed_id="test_feed",
            id="download2",
            source_url="https://www.youtube.com/watch?v=test2",
            title="Test Download 2",
            published=base_time + timedelta(hours=1),
            ext="mp4",
            mime_type="video/mp4",
            filesize=2048,
            duration=120,
            status=DownloadStatus.DOWNLOADED,
        ),
        Download(
            feed_id="test_feed",
            id="download3",
            source_url="https://www.youtube.com/watch?v=test3",
            title="Test Download 3",
            published=base_time + timedelta(hours=2),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1536,
            duration=90,
            status=DownloadStatus.QUEUED,
        ),
    ]


# --- Tests for FeedDatabase schema initialization ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_feed_db_initialization_and_schema(feed_db: FeedDatabase):
    """Test that the schema (tables) is created upon initialization."""
    # The schema should already be created by the db_core fixture
    # Just verify we can interact with the database
    feeds = await feed_db.get_feeds()
    assert isinstance(feeds, list)
    assert len(feeds) == 0  # Should be empty initially


# --- Tests for FeedDatabase.upsert_feed ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_upsert_and_get_feed(feed_db: FeedDatabase, sample_feed: Feed):
    """Test adding a new feed and then retrieving it."""
    await feed_db.upsert_feed(sample_feed)

    retrieved_feed = await feed_db.get_feed_by_id(sample_feed.id)

    assert retrieved_feed is not None, "Feed should be found in DB"
    assert retrieved_feed.id == sample_feed.id
    assert retrieved_feed.is_enabled == sample_feed.is_enabled
    assert retrieved_feed.source_type == sample_feed.source_type
    assert retrieved_feed.title == sample_feed.title
    assert retrieved_feed.subtitle == sample_feed.subtitle
    assert retrieved_feed.description == sample_feed.description
    assert retrieved_feed.language == sample_feed.language
    assert retrieved_feed.author == sample_feed.author
    assert retrieved_feed.image_url == sample_feed.image_url
    assert str(retrieved_feed.category) == str(sample_feed.category)
    assert str(retrieved_feed.explicit) == str(sample_feed.explicit)
    assert retrieved_feed.consecutive_failures == sample_feed.consecutive_failures


@pytest.mark.unit
@pytest.mark.asyncio
async def test_upsert_feed_updates_existing(feed_db: FeedDatabase, sample_feed: Feed):
    """Test that upsert_feed updates an existing feed instead of raising an error."""
    # Add initial feed
    await feed_db.upsert_feed(sample_feed)

    # Create a modified version with the same id
    modified_feed = Feed(
        id=sample_feed.id,
        is_enabled=False,  # Changed
        source_type=SourceType.PLAYLIST,  # Changed
        source_url="https://www.youtube.com/playlist?list=PLupdated",  # Changed
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title="Updated Feed Title",  # Changed
        subtitle="Updated Subtitle",  # Changed
        description="Updated description",  # Changed
        language="es",  # Changed
        author="Updated Author",  # Changed
        image_url="https://example.com/updated.jpg",  # Changed
        category=PodcastCategories("Business"),  # Changed
        explicit=PodcastExplicit.YES,  # Changed
        consecutive_failures=2,  # Changed
    )

    # Perform upsert with the modified feed
    await feed_db.upsert_feed(modified_feed)  # Should not raise IntegrityError

    # Retrieve and verify
    retrieved_feed = await feed_db.get_feed_by_id(sample_feed.id)

    assert retrieved_feed is not None, "Feed should still be found"
    assert retrieved_feed.id == modified_feed.id
    assert retrieved_feed.is_enabled == modified_feed.is_enabled
    assert retrieved_feed.source_type == modified_feed.source_type
    assert retrieved_feed.title == modified_feed.title
    assert retrieved_feed.subtitle == modified_feed.subtitle
    assert retrieved_feed.description == modified_feed.description
    assert retrieved_feed.language == modified_feed.language
    assert retrieved_feed.author == modified_feed.author
    assert retrieved_feed.image_url == modified_feed.image_url
    assert str(retrieved_feed.category) == str(modified_feed.category)
    assert str(retrieved_feed.explicit) == str(modified_feed.explicit)
    assert retrieved_feed.consecutive_failures == modified_feed.consecutive_failures


@pytest.mark.unit
@pytest.mark.asyncio
async def test_upsert_feed_with_none_timestamps(
    feed_db: FeedDatabase, minimal_feed: Feed
):
    """Test that database defaults are applied when created_at/updated_at are None."""
    # Insert the feed with no timestamps
    await feed_db.upsert_feed(minimal_feed)

    # Retrieve and verify timestamps were set by database
    retrieved = await feed_db.get_feed_by_id(minimal_feed.id)

    assert retrieved.created_at is not None, (
        "created_at should be set by database default"
    )
    assert retrieved.updated_at is not None, (
        "updated_at should be set by database default"
    )

    # Verify the timestamps are reasonable (within a few seconds of now)
    current_time = datetime.now(UTC)
    time_diff_created = abs((current_time - retrieved.created_at).total_seconds())
    time_diff_updated = abs((current_time - retrieved.updated_at).total_seconds())

    assert time_diff_created < 5, "created_at should be close to current time"
    assert time_diff_updated < 5, "updated_at should be close to current time"


# --- Tests for FeedDatabase.get_feed_by_id ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_feed_by_id_not_found(feed_db: FeedDatabase):
    """Test that get_feed_by_id raises FeedNotFoundError for non-existent feed."""
    with pytest.raises(FeedNotFoundError) as exc_info:
        await feed_db.get_feed_by_id("non_existent_feed")

    assert exc_info.value.feed_id == "non_existent_feed"


# --- Tests for FeedDatabase.get_feeds ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_feeds_all_and_filtered(feed_db: FeedDatabase):
    """Test getting all feeds and filtering by enabled status."""
    # Create test feeds
    feed1 = Feed(
        id="feed1",
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url="https://www.youtube.com/@channel1",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
    )
    feed2 = Feed(
        id="feed2",
        is_enabled=False,
        source_type=SourceType.PLAYLIST,
        source_url="https://www.youtube.com/playlist?list=PLfeed2",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
    )
    feed3 = Feed(
        id="feed3",
        is_enabled=True,
        source_type=SourceType.SINGLE_VIDEO,
        source_url="https://www.youtube.com/watch?v=feed3",
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
    )

    # Insert feeds
    for feed in [feed1, feed2, feed3]:
        await feed_db.upsert_feed(feed)

    # Test get all feeds
    all_feeds = await feed_db.get_feeds()
    assert len(all_feeds) == 3
    feed_ids = [f.id for f in all_feeds]
    assert "feed1" in feed_ids
    assert "feed2" in feed_ids
    assert "feed3" in feed_ids
    # Test ordering (should be by id ASC)
    assert feed_ids == ["feed1", "feed2", "feed3"]

    # Test get enabled feeds only
    enabled_feeds = await feed_db.get_feeds(enabled=True)
    assert len(enabled_feeds) == 2
    enabled_ids = [f.id for f in enabled_feeds]
    assert "feed1" in enabled_ids
    assert "feed3" in enabled_ids
    assert "feed2" not in enabled_ids

    # Test get disabled feeds only
    disabled_feeds = await feed_db.get_feeds(enabled=False)
    assert len(disabled_feeds) == 1
    assert disabled_feeds[0].id == "feed2"


# --- Tests for FeedDatabase.mark_sync_success ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mark_sync_success(feed_db: FeedDatabase, sample_feed: Feed):
    """Test marking a feed sync as successful."""
    # Set up feed with some failures
    sample_feed.consecutive_failures = 3
    await feed_db.upsert_feed(sample_feed)

    # Mark sync success
    await feed_db.mark_sync_success(sample_feed.id)

    # Verify changes
    updated_feed = await feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed.last_successful_sync is not None
    assert updated_feed.consecutive_failures == 0

    # Verify timestamp is recent
    current_time = datetime.now(UTC)
    time_diff = abs((current_time - updated_feed.last_successful_sync).total_seconds())
    assert time_diff < 5, "last_successful_sync should be close to current time"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mark_sync_success_not_found(feed_db: FeedDatabase):
    """Test marking sync success for non-existent feed."""
    with pytest.raises(FeedNotFoundError) as exc_info:
        await feed_db.mark_sync_success("non_existent_feed")

    assert exc_info.value.feed_id == "non_existent_feed"


# --- Tests for FeedDatabase.mark_sync_failure ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mark_sync_failure(feed_db: FeedDatabase, sample_feed: Feed):
    """Test marking a feed sync as failed."""
    # Set up feed with no previous failures
    sample_feed.consecutive_failures = 0
    await feed_db.upsert_feed(sample_feed)

    # Mark first failure
    await feed_db.mark_sync_failure(sample_feed.id)

    # Verify changes
    updated_feed = await feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed.last_failed_sync is not None
    assert updated_feed.consecutive_failures == 1

    # Mark second failure
    await feed_db.mark_sync_failure(sample_feed.id)

    # Verify consecutive failures incremented
    updated_feed2 = await feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed2.consecutive_failures == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mark_sync_failure_not_found(feed_db: FeedDatabase):
    """Test marking sync failure for non-existent feed."""
    with pytest.raises(FeedNotFoundError) as exc_info:
        await feed_db.mark_sync_failure("non_existent_feed")

    assert exc_info.value.feed_id == "non_existent_feed"


# --- Tests for FeedDatabase.mark_rss_generated ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mark_rss_generated(feed_db: FeedDatabase, sample_feed: Feed):
    """Test marking RSS generation for a feed."""
    # Set up feed with initial values
    await feed_db.upsert_feed(sample_feed)

    # Mark RSS generated
    await feed_db.mark_rss_generated(sample_feed.id)

    # Verify changes
    updated_feed = await feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed.last_rss_generation is not None

    # Verify timestamp is recent
    current_time = datetime.now(UTC)
    time_diff = abs((current_time - updated_feed.last_rss_generation).total_seconds())
    assert time_diff < 5, "last_rss_generation should be close to current time"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mark_rss_generated_not_found(feed_db: FeedDatabase):
    """Test marking RSS generated for non-existent feed."""
    with pytest.raises(FeedNotFoundError) as exc_info:
        await feed_db.mark_rss_generated("non_existent_feed")

    assert exc_info.value.feed_id == "non_existent_feed"


# --- Tests for FeedDatabase.set_feed_enabled ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_set_feed_enabled(feed_db: FeedDatabase, sample_feed: Feed):
    """Test enabling and disabling a feed."""
    # Set up feed as enabled
    sample_feed.is_enabled = True
    await feed_db.upsert_feed(sample_feed)

    # Disable the feed
    await feed_db.set_feed_enabled(sample_feed.id, False)

    # Verify change
    updated_feed = await feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed.is_enabled is False

    # Re-enable the feed
    await feed_db.set_feed_enabled(sample_feed.id, True)

    # Verify change
    updated_feed2 = await feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed2.is_enabled is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_set_feed_enabled_not_found(feed_db: FeedDatabase):
    """Test setting enabled status for non-existent feed."""
    with pytest.raises(FeedNotFoundError) as exc_info:
        await feed_db.set_feed_enabled("non_existent_feed", True)

    assert exc_info.value.feed_id == "non_existent_feed"


# --- Tests for FeedDatabase.update_feed_metadata ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_feed_metadata(feed_db: FeedDatabase, sample_feed: Feed):
    """Test updating feed metadata fields."""
    await feed_db.upsert_feed(sample_feed)

    new_title = "New Title"
    new_description = "New Description"
    new_language = "de"
    new_category = PodcastCategories("News")
    new_explicit = PodcastExplicit.CLEAN

    # Update some metadata fields
    await feed_db.update_feed_metadata(
        sample_feed.id,
        title=new_title,
        description=new_description,
        language=new_language,
        category=new_category,
        explicit=new_explicit,
    )

    # Verify changes
    updated_feed = await feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed.title == new_title
    assert updated_feed.description == new_description
    assert updated_feed.language == new_language
    assert updated_feed.category == new_category
    assert updated_feed.explicit == new_explicit
    # Other fields should be unchanged
    assert updated_feed.subtitle == sample_feed.subtitle
    assert updated_feed.author == sample_feed.author
    assert updated_feed.image_url == sample_feed.image_url


@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_feed_metadata_no_op(feed_db: FeedDatabase, sample_feed: Feed):
    """Test that update_feed_metadata is no-op when all fields are None."""
    await feed_db.upsert_feed(sample_feed)

    # Get initial updated_at timestamp
    initial_feed = await feed_db.get_feed_by_id(sample_feed.id)
    initial_updated_at = initial_feed.updated_at

    # Call update with all None values
    await feed_db.update_feed_metadata(sample_feed.id)

    # Should be no change
    final_feed = await feed_db.get_feed_by_id(sample_feed.id)
    assert final_feed.title == sample_feed.title
    assert final_feed.subtitle == sample_feed.subtitle
    # updated_at should be unchanged since no actual update occurred
    assert final_feed.updated_at == initial_updated_at


@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_feed_metadata_not_found(feed_db: FeedDatabase):
    """Test updating metadata for non-existent feed."""
    with pytest.raises(FeedNotFoundError) as exc_info:
        await feed_db.update_feed_metadata("non_existent_feed", title="New Title")

    assert exc_info.value.feed_id == "non_existent_feed"


# --- Tests for FeedDatabase timestamp updates ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_database_update_timestamp(feed_db: FeedDatabase, sample_feed: Feed):
    """Test that database correctly updates timestamps."""
    # Insert feed
    await feed_db.upsert_feed(sample_feed)

    # Get initial timestamp
    initial_feed = await feed_db.get_feed_by_id(sample_feed.id)
    initial_updated_at = initial_feed.updated_at
    assert initial_updated_at is not None

    # Wait a moment to ensure timestamp differences
    time.sleep(0.1)

    # Update the feed to trigger the updated_at change
    await feed_db.update_feed_metadata(sample_feed.id, title="Trigger Updated Title")

    # Check that updated_at was changed
    after_update = await feed_db.get_feed_by_id(sample_feed.id)
    assert after_update.updated_at is not None
    assert after_update.updated_at != initial_updated_at
    assert after_update.updated_at.tzinfo == UTC
    assert after_update.updated_at > initial_updated_at


# --- Tests for Feed.total_downloads property ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_total_downloads_property(
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    sample_feed: Feed,
    sample_downloads: list[Download],
):
    """Test that the total_downloads property correctly counts downloaded items."""
    # Insert the feed
    await feed_db.upsert_feed(sample_feed)

    # Insert sample downloads
    for download in sample_downloads:
        await download_db.upsert_download(download)

    # Retrieve the feed and check total_downloads
    retrieved_feed = await feed_db.get_feed_by_id(sample_feed.id)

    # Should only count DOWNLOADED status downloads (2 out of 3)
    assert retrieved_feed.total_downloads == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_total_downloads_property_skip_downloads(
    feed_db: FeedDatabase, sample_feed: Feed
):
    """Test that the total_downloads property returns 0 when no downloads exist."""
    # Insert the feed
    await feed_db.upsert_feed(sample_feed)

    # Retrieve the feed and check total_downloads
    retrieved_feed = await feed_db.get_feed_by_id(sample_feed.id)

    # Should be 0 when no downloads exist
    assert retrieved_feed.total_downloads == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_total_downloads_property_mixed_statuses(
    feed_db: FeedDatabase, download_db: DownloadDatabase, sample_feed: Feed
):
    """Test that the total_downloads property only counts DOWNLOADED status."""
    # Insert the feed
    await feed_db.upsert_feed(sample_feed)

    # Create downloads with different statuses
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    downloads = [
        Download(
            feed_id="test_feed",
            id="downloaded1",
            source_url="https://www.youtube.com/watch?v=test1",
            title="Downloaded 1",
            published=base_time,
            ext="mp4",
            mime_type="video/mp4",
            filesize=1024,
            duration=60,
            status=DownloadStatus.DOWNLOADED,
        ),
        Download(
            feed_id="test_feed",
            id="downloaded2",
            source_url="https://www.youtube.com/watch?v=test2",
            title="Downloaded 2",
            published=base_time + timedelta(hours=1),
            ext="mp4",
            mime_type="video/mp4",
            filesize=2048,
            duration=120,
            status=DownloadStatus.DOWNLOADED,
        ),
        Download(
            feed_id="test_feed",
            id="queued1",
            source_url="https://www.youtube.com/watch?v=test3",
            title="Queued 1",
            published=base_time + timedelta(hours=2),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1536,
            duration=90,
            status=DownloadStatus.QUEUED,
        ),
        Download(
            feed_id="test_feed",
            id="error1",
            source_url="https://www.youtube.com/watch?v=test4",
            title="Error 1",
            published=base_time + timedelta(hours=3),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1200,
            duration=75,
            status=DownloadStatus.ERROR,
        ),
        Download(
            feed_id="test_feed",
            id="upcoming1",
            source_url="https://www.youtube.com/watch?v=test5",
            title="Upcoming 1",
            published=base_time + timedelta(hours=4),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1800,
            duration=105,
            status=DownloadStatus.UPCOMING,
        ),
    ]

    # Insert all downloads
    for download in downloads:
        await download_db.upsert_download(download)

    # Retrieve the feed and check total_downloads
    retrieved_feed = await feed_db.get_feed_by_id(sample_feed.id)

    # Should only count the 2 DOWNLOADED status downloads
    assert retrieved_feed.total_downloads == 2
