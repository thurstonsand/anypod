# pyright: reportPrivateUsage=false

"""Tests for the FeedDatabase and Feed model functionality."""

from collections.abc import Generator
from datetime import UTC, datetime, timedelta
import sqlite3
import time
from typing import Any

import pytest

from anypod.config.types import PodcastCategories, PodcastExplicit
from anypod.db import FeedDatabase
from anypod.db.types import Feed, SourceType
from anypod.exceptions import FeedNotFoundError

# --- Fixtures ---


@pytest.fixture
def feed_db() -> Generator[FeedDatabase]:
    """Provides a FeedDatabase instance for testing."""
    db = FeedDatabase(db_path=None, memory_name="test_feed_db")
    yield db
    db.close()


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
        total_downloads=5,
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
def sample_feed_row_data() -> dict[str, Any]:
    """Provides raw data for a sample Feed object, simulating a DB row."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    return {
        "id": "test_feed_123",
        "is_enabled": True,
        "source_type": str(SourceType.PLAYLIST),
        "source_url": "https://www.youtube.com/playlist?list=PLtest123",
        "created_at": base_time.isoformat(),
        "updated_at": base_time.isoformat(),
        "last_successful_sync": (base_time - timedelta(hours=1)).isoformat(),
        "last_rss_generation": (base_time - timedelta(hours=2)).isoformat(),
        "last_failed_sync": None,
        "consecutive_failures": 0,
        "total_downloads": 10,
        "title": "Test Playlist Feed",
        "subtitle": "Test Playlist Subtitle",
        "description": "Test playlist description from DB",
        "language": "fr",
        "author": "Test Playlist Author",
        "image_url": "https://example.com/playlist.jpg",
        "category": "Business",
        "explicit": "yes",
    }


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


# --- Tests ---


# --- Tests for SourceType enum ---


@pytest.mark.unit
def test_source_type_enum():
    """Test SourceType enum values and string conversion."""
    assert str(SourceType.CHANNEL) == "channel"
    assert str(SourceType.PLAYLIST) == "playlist"
    assert str(SourceType.SINGLE_VIDEO) == "single_video"
    assert str(SourceType.UNKNOWN) == "unknown"

    # Test enum creation from string
    assert SourceType("channel") == SourceType.CHANNEL
    assert SourceType("playlist") == SourceType.PLAYLIST
    assert SourceType("single_video") == SourceType.SINGLE_VIDEO
    assert SourceType("unknown") == SourceType.UNKNOWN


# --- Tests for Feed.from_row ---


@pytest.mark.unit
def test_feed_from_row_success(sample_feed_row_data: dict[str, Any]):
    """Test successful conversion of a valid row dictionary to a Feed object."""
    mock_row = sample_feed_row_data

    # Expected Feed object based on the row data
    expected_source_type = SourceType(mock_row["source_type"])
    converted_feed = Feed.from_row(mock_row)

    assert converted_feed.id == mock_row["id"]
    assert converted_feed.is_enabled == bool(mock_row["is_enabled"])
    assert converted_feed.source_type == expected_source_type
    assert converted_feed.title == mock_row["title"]
    assert converted_feed.subtitle == mock_row["subtitle"]
    assert converted_feed.description == mock_row["description"]
    assert converted_feed.language == mock_row["language"]
    assert converted_feed.author == mock_row["author"]
    assert converted_feed.image_url == mock_row["image_url"]
    assert str(converted_feed.category) == mock_row["category"]
    assert str(converted_feed.explicit) == mock_row["explicit"]
    assert converted_feed.total_downloads == mock_row["total_downloads"]
    assert converted_feed.consecutive_failures == mock_row["consecutive_failures"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "malformed_field, malformed_value",
    [
        ("created_at", "not-a-date-string"),
        ("updated_at", None),
        ("source_type", "unknown_source_type"),
        ("last_successful_sync", "invalid-date"),
    ],
)
def test_feed_from_row_malformed_data(
    sample_feed_row_data: dict[str, Any],
    malformed_field: str,
    malformed_value: Any,
):
    """Test that Feed.from_row raises ValueError for malformed data."""
    corrupted_row_data = sample_feed_row_data.copy()
    corrupted_row_data[malformed_field] = malformed_value

    with pytest.raises(ValueError):
        Feed.from_row(corrupted_row_data)


# --- Tests for FeedDatabase._initialize_schema ---


@pytest.mark.unit
def test_feed_db_initialization_and_schema(feed_db: FeedDatabase):
    """Test that the schema (tables and triggers) is created upon first DB interaction."""
    conn: sqlite3.Connection = feed_db._db.db.conn  # type: ignore
    assert conn is not None, "Connection should have been established"

    cursor: sqlite3.Cursor | None = None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='feeds';"
        )
        table = cursor.fetchone()
        assert table is not None, "'feeds' table should have been created"
        assert table[0] == "feeds"

        # Check for trigger
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name='feeds_update_timestamp';"
        )
        trigger = cursor.fetchone()
        assert trigger is not None, (
            "'feeds_update_timestamp' trigger should have been created"
        )
        assert trigger[0] == "feeds_update_timestamp"
    finally:
        if cursor:
            cursor.close()


# --- Tests for FeedDatabase.upsert_feed ---


@pytest.mark.unit
def test_upsert_and_get_feed(feed_db: FeedDatabase, sample_feed: Feed):
    """Test adding a new feed and then retrieving it."""
    feed_db.upsert_feed(sample_feed)

    retrieved_feed = feed_db.get_feed_by_id(sample_feed.id)

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
    assert retrieved_feed.total_downloads == sample_feed.total_downloads
    assert retrieved_feed.consecutive_failures == sample_feed.consecutive_failures


@pytest.mark.unit
def test_upsert_feed_updates_existing(feed_db: FeedDatabase, sample_feed: Feed):
    """Test that upsert_feed updates an existing feed instead of raising an error."""
    # Add initial feed
    feed_db.upsert_feed(sample_feed)

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
        total_downloads=15,  # Changed
        consecutive_failures=2,  # Changed
    )

    # Perform upsert with the modified feed
    feed_db.upsert_feed(modified_feed)  # Should not raise IntegrityError

    # Retrieve and verify
    retrieved_feed = feed_db.get_feed_by_id(sample_feed.id)

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
    assert retrieved_feed.total_downloads == modified_feed.total_downloads
    assert retrieved_feed.consecutive_failures == modified_feed.consecutive_failures


@pytest.mark.unit
def test_upsert_feed_with_none_timestamps(feed_db: FeedDatabase, minimal_feed: Feed):
    """Test that database defaults are applied when created_at/updated_at are None."""
    # Insert the feed with None timestamps
    feed_db.upsert_feed(minimal_feed)

    # Retrieve and verify timestamps were set by database
    retrieved = feed_db.get_feed_by_id(minimal_feed.id)

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
def test_get_feed_by_id_not_found(feed_db: FeedDatabase):
    """Test that get_feed_by_id raises FeedNotFoundError for non-existent feed."""
    with pytest.raises(FeedNotFoundError) as exc_info:
        feed_db.get_feed_by_id("non_existent_feed")

    assert exc_info.value.feed_id == "non_existent_feed"


# --- Tests for FeedDatabase.get_feeds ---


@pytest.mark.unit
def test_get_feeds_all_and_filtered(feed_db: FeedDatabase):
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
        feed_db.upsert_feed(feed)

    # Test get all feeds
    all_feeds = feed_db.get_feeds()
    assert len(all_feeds) == 3
    feed_ids = [f.id for f in all_feeds]
    assert "feed1" in feed_ids
    assert "feed2" in feed_ids
    assert "feed3" in feed_ids
    # Test ordering (should be by id ASC)
    assert feed_ids == ["feed1", "feed2", "feed3"]

    # Test get enabled feeds only
    enabled_feeds = feed_db.get_feeds(enabled=True)
    assert len(enabled_feeds) == 2
    enabled_ids = [f.id for f in enabled_feeds]
    assert "feed1" in enabled_ids
    assert "feed3" in enabled_ids
    assert "feed2" not in enabled_ids

    # Test get disabled feeds only
    disabled_feeds = feed_db.get_feeds(enabled=False)
    assert len(disabled_feeds) == 1
    assert disabled_feeds[0].id == "feed2"


# --- Tests for FeedDatabase.mark_sync_success ---


@pytest.mark.unit
def test_mark_sync_success(feed_db: FeedDatabase, sample_feed: Feed):
    """Test marking a feed sync as successful."""
    # Set up feed with some failures
    sample_feed.consecutive_failures = 3
    feed_db.upsert_feed(sample_feed)

    # Mark sync success
    feed_db.mark_sync_success(sample_feed.id)

    # Verify changes
    updated_feed = feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed.last_successful_sync is not None
    assert updated_feed.consecutive_failures == 0

    # Verify timestamp is recent
    current_time = datetime.now(UTC)
    time_diff = abs((current_time - updated_feed.last_successful_sync).total_seconds())
    assert time_diff < 5, "last_successful_sync should be close to current time"


@pytest.mark.unit
def test_mark_sync_success_not_found(feed_db: FeedDatabase):
    """Test marking sync success for non-existent feed."""
    with pytest.raises(FeedNotFoundError) as exc_info:
        feed_db.mark_sync_success("non_existent_feed")

    assert exc_info.value.feed_id == "non_existent_feed"


# --- Tests for FeedDatabase.mark_sync_failure ---


@pytest.mark.unit
def test_mark_sync_failure(feed_db: FeedDatabase, sample_feed: Feed):
    """Test marking a feed sync as failed."""
    # Set up feed with no previous failures
    sample_feed.consecutive_failures = 0
    feed_db.upsert_feed(sample_feed)

    # Mark first failure
    feed_db.mark_sync_failure(sample_feed.id)

    # Verify changes
    updated_feed = feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed.last_failed_sync is not None
    assert updated_feed.consecutive_failures == 1

    # Mark second failure
    feed_db.mark_sync_failure(sample_feed.id)

    # Verify consecutive failures incremented
    updated_feed2 = feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed2.consecutive_failures == 2


@pytest.mark.unit
def test_mark_sync_failure_not_found(feed_db: FeedDatabase):
    """Test marking sync failure for non-existent feed."""
    with pytest.raises(FeedNotFoundError) as exc_info:
        feed_db.mark_sync_failure("non_existent_feed")

    assert exc_info.value.feed_id == "non_existent_feed"


# --- Tests for FeedDatabase.mark_rss_generated ---


@pytest.mark.unit
def test_mark_rss_generated(feed_db: FeedDatabase, sample_feed: Feed):
    """Test marking RSS generation for a feed."""
    # Set up feed with initial values
    sample_feed.total_downloads = 10
    feed_db.upsert_feed(sample_feed)

    # Mark RSS generated
    feed_db.mark_rss_generated(sample_feed.id)

    # Verify changes
    updated_feed = feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed.last_rss_generation is not None
    assert updated_feed.total_downloads == sample_feed.total_downloads

    # Verify timestamp is recent
    current_time = datetime.now(UTC)
    time_diff = abs((current_time - updated_feed.last_rss_generation).total_seconds())
    assert time_diff < 5, "last_rss_generation should be close to current time"


@pytest.mark.unit
def test_mark_rss_generated_not_found(feed_db: FeedDatabase):
    """Test marking RSS generated for non-existent feed."""
    with pytest.raises(FeedNotFoundError) as exc_info:
        feed_db.mark_rss_generated("non_existent_feed")

    assert exc_info.value.feed_id == "non_existent_feed"


# --- Tests for FeedDatabase.update_total_downloads ---


@pytest.mark.unit
def test_update_total_downloads(feed_db: FeedDatabase, sample_feed: Feed):
    """Test updating total_downloads count."""
    # Insert feed
    feed_db.upsert_feed(sample_feed)

    # Verify initial count
    initial_feed = feed_db.get_feed_by_id(sample_feed.id)
    assert initial_feed.total_downloads == 5  # from sample_feed fixture

    # Update total downloads
    new_count = 42
    feed_db.update_total_downloads(sample_feed.id, new_count)

    # Verify the update
    updated_feed = feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed.total_downloads == new_count

    # Test with zero count
    feed_db.update_total_downloads(sample_feed.id, 0)
    zero_feed = feed_db.get_feed_by_id(sample_feed.id)
    assert zero_feed.total_downloads == 0


@pytest.mark.unit
def test_update_total_downloads_not_found(feed_db: FeedDatabase):
    """Test updating total_downloads for non-existent feed."""
    with pytest.raises(FeedNotFoundError) as exc_info:
        feed_db.update_total_downloads("non_existent_feed", 100)

    assert exc_info.value.feed_id == "non_existent_feed"


# --- Tests for FeedDatabase.set_feed_enabled ---


@pytest.mark.unit
def test_set_feed_enabled(feed_db: FeedDatabase, sample_feed: Feed):
    """Test enabling and disabling a feed."""
    # Set up feed as enabled
    sample_feed.is_enabled = True
    feed_db.upsert_feed(sample_feed)

    # Disable the feed
    feed_db.set_feed_enabled(sample_feed.id, False)

    # Verify change
    updated_feed = feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed.is_enabled is False

    # Re-enable the feed
    feed_db.set_feed_enabled(sample_feed.id, True)

    # Verify change
    updated_feed2 = feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed2.is_enabled is True


@pytest.mark.unit
def test_set_feed_enabled_not_found(feed_db: FeedDatabase):
    """Test setting enabled status for non-existent feed."""
    with pytest.raises(FeedNotFoundError) as exc_info:
        feed_db.set_feed_enabled("non_existent_feed", True)

    assert exc_info.value.feed_id == "non_existent_feed"


# --- Tests for FeedDatabase.update_feed_metadata ---


@pytest.mark.unit
def test_update_feed_metadata(feed_db: FeedDatabase, sample_feed: Feed):
    """Test updating feed metadata fields."""
    feed_db.upsert_feed(sample_feed)

    # Update some metadata fields
    feed_db.update_feed_metadata(
        sample_feed.id,
        title="New Title",
        description="New Description",
        language="de",
        category="News",
        explicit="clean",
    )

    # Verify changes
    updated_feed = feed_db.get_feed_by_id(sample_feed.id)
    assert updated_feed.title == "New Title"
    assert updated_feed.description == "New Description"
    assert updated_feed.language == "de"
    assert str(updated_feed.category) == "News"
    assert str(updated_feed.explicit) == "clean"
    # Other fields should be unchanged
    assert updated_feed.subtitle == sample_feed.subtitle
    assert updated_feed.author == sample_feed.author
    assert updated_feed.image_url == sample_feed.image_url


@pytest.mark.unit
def test_update_feed_metadata_no_op(feed_db: FeedDatabase, sample_feed: Feed):
    """Test that update_feed_metadata is no-op when all fields are None."""
    feed_db.upsert_feed(sample_feed)

    # Get initial updated_at timestamp
    initial_feed = feed_db.get_feed_by_id(sample_feed.id)
    initial_updated_at = initial_feed.updated_at

    # Call update with all None values
    feed_db.update_feed_metadata(sample_feed.id)

    # Should be no change
    final_feed = feed_db.get_feed_by_id(sample_feed.id)
    assert final_feed.title == sample_feed.title
    assert final_feed.subtitle == sample_feed.subtitle
    # updated_at should be unchanged since no actual update occurred
    assert final_feed.updated_at == initial_updated_at


@pytest.mark.unit
def test_update_feed_metadata_not_found(feed_db: FeedDatabase):
    """Test updating metadata for non-existent feed."""
    with pytest.raises(FeedNotFoundError) as exc_info:
        feed_db.update_feed_metadata("non_existent_feed", title="New Title")

    assert exc_info.value.feed_id == "non_existent_feed"


# --- Tests for FeedDatabase triggers ---


@pytest.mark.unit
def test_database_triggers_update_timestamp(feed_db: FeedDatabase, sample_feed: Feed):
    """Test that database triggers correctly update timestamps."""
    # Insert feed
    feed_db.upsert_feed(sample_feed)

    # Get initial timestamp
    initial_feed = feed_db.get_feed_by_id(sample_feed.id)
    initial_updated_at = initial_feed.updated_at
    assert initial_updated_at is not None

    # Wait a moment to ensure timestamp differences
    time.sleep(0.1)

    # Update the feed to trigger the updated_at trigger
    feed_db._db.update("feeds", sample_feed.id, {"title": "Trigger Updated Title"})

    # Check that updated_at was changed by trigger
    after_update = feed_db.get_feed_by_id(sample_feed.id)
    assert after_update.updated_at is not None
    assert after_update.updated_at != initial_updated_at
    assert after_update.updated_at.tzinfo == UTC
    assert after_update.updated_at > initial_updated_at
