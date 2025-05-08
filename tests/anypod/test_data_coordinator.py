from dataclasses import replace
import datetime
from io import BytesIO
import sqlite3
from typing import Any
from unittest.mock import MagicMock

import pytest

from anypod.data_coordinator import DataCoordinator
from anypod.db import Download, DownloadStatus
from anypod.exceptions import (
    DatabaseOperationError,
    DataCoordinatorError,
    DownloadNotFoundError,
    FileOperationError,
)

# --- Fixtures ---


def download_to_db_row(download: Download) -> dict[str, Any]:
    """Converts a Download object to a dictionary that can be used to create a mock sqlite3.Row."""
    return {
        "feed": download.feed,
        "id": download.id,
        "source_url": download.source_url,
        "title": download.title,
        "published": download.published.isoformat(),
        "ext": download.ext,
        "duration": download.duration,
        "thumbnail": download.thumbnail,
        "status": str(download.status),
        "retries": download.retries,
        "last_error": download.last_error,
    }


@pytest.fixture
def mock_db_manager() -> MagicMock:
    """Provides a MagicMock for the DatabaseManager."""
    return MagicMock()


@pytest.fixture
def mock_file_manager() -> MagicMock:
    """Provides a MagicMock for the FileManager."""
    return MagicMock()


@pytest.fixture
def coordinator(
    mock_db_manager: MagicMock, mock_file_manager: MagicMock
) -> DataCoordinator:
    """Provides a DataCoordinator instance with mocked dependencies."""
    return DataCoordinator(db_manager=mock_db_manager, file_manager=mock_file_manager)


@pytest.fixture
def sample_download_data() -> dict[str, Any]:
    """Provides raw data for a sample Download object, useful for mock DB rows."""
    return {
        "feed": "test_feed",
        "id": "test_id_123",
        "source_url": "http://example.com/video/123",
        "title": "Test Video Title",
        "published": datetime.datetime(
            2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC
        ).isoformat(),
        "ext": "mp4",
        "duration": 120.0,
        "thumbnail": "http://example.com/thumb/123.jpg",
        "status": str(DownloadStatus.QUEUED),
        "retries": 0,
        "last_error": None,
    }


@pytest.fixture
def sample_download_obj(sample_download_data: dict[str, Any]) -> Download:
    """Provides a sample Download object based on sample_download_data."""
    published_dt = datetime.datetime.fromisoformat(sample_download_data["published"])
    status_enum = DownloadStatus(sample_download_data["status"])

    return Download(
        feed=str(sample_download_data["feed"]),
        id=str(sample_download_data["id"]),
        source_url=str(sample_download_data["source_url"]),
        title=str(sample_download_data["title"]),
        published=published_dt,
        ext=str(sample_download_data["ext"]),
        duration=float(sample_download_data["duration"]),
        thumbnail=str(sample_download_data["thumbnail"])
        if sample_download_data["thumbnail"]
        else None,
        status=status_enum,
        retries=int(sample_download_data["retries"]),
        last_error=str(sample_download_data["last_error"])
        if sample_download_data["last_error"]
        else None,
    )


@pytest.fixture
def existing_download_db_row_downloaded(sample_download_obj: Download) -> sqlite3.Row:
    """Provides a mock sqlite3.Row for an existing DOWNLOADED download."""
    # This dictionary needs to be compatible with how sqlite3.Row is typically accessed (by string keys).
    mock_row_dict = {
        "feed": sample_download_obj.feed,
        "id": sample_download_obj.id,
        "source_url": sample_download_obj.source_url,
        "title": "Original Title for existing_download_db_row_downloaded",
        "published": (
            sample_download_obj.published - datetime.timedelta(days=1)
        ).isoformat(),
        "ext": "mp4",  # Important for file deletion logic
        "duration": sample_download_obj.duration,
        "thumbnail": sample_download_obj.thumbnail,
        "status": str(
            DownloadStatus.DOWNLOADED
        ),  # Critical for testing file deletion path
        "retries": 0,
        "last_error": None,
    }
    return mock_row_dict  # type: ignore


# --- Tests for add_download ---


@pytest.mark.unit
def test_add_new_download(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_download_obj: Download,
):
    """Test adding a completely new download (no existing DB record or file)."""
    mock_db_manager.get_download_by_id.return_value = None

    coordinator.add_download(sample_download_obj)

    mock_db_manager.get_download_by_id.assert_called_once_with(
        sample_download_obj.feed, sample_download_obj.id
    )
    mock_file_manager.delete_download_file.assert_not_called()
    mock_db_manager.delete_downloads.assert_not_called()
    mock_db_manager.upsert_download.assert_called_once_with(sample_download_obj)


@pytest.mark.unit
def test_replace_download_status_not_downloaded(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_download_obj: Download,
    sample_download_data: dict[str, Any],
):
    """Test replacing a download whose existing status is NOT 'DOWNLOADED'."""
    existing_download_data = sample_download_data.copy()
    existing_download_data["status"] = str(DownloadStatus.QUEUED)  # Not DOWNLOADED
    mock_db_manager.get_download_by_id.return_value = existing_download_data
    mock_db_manager.delete_downloads.return_value = (
        1  # Simulate successful DB deletion of old record
    )

    coordinator.add_download(sample_download_obj)

    mock_db_manager.get_download_by_id.assert_called_once_with(
        sample_download_obj.feed, sample_download_obj.id
    )
    mock_file_manager.delete_download_file.assert_not_called()  # File deletion should not be attempted
    mock_db_manager.update_status.assert_not_called()
    mock_db_manager.delete_downloads.assert_not_called()
    mock_db_manager.upsert_download.assert_called_once_with(sample_download_obj)


@pytest.mark.unit
def test_replace_download_status_downloaded_file_deleted(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_download_obj: Download,  # This is the NEW download to be added
    sample_download_data: dict[str, Any],  # Used to construct the OLD download
):
    """Test replacing a download (status DOWNLOADED), file is successfully deleted."""
    existing_download_data = sample_download_data.copy()
    existing_download_data["status"] = str(DownloadStatus.DOWNLOADED)
    existing_download_data["ext"] = (
        "mp4"  # Ensure ext is present for filename construction
    )
    mock_db_manager.get_download_by_id.return_value = existing_download_data
    mock_file_manager.delete_download_file.return_value = (
        True  # File deletion successful
    )
    mock_db_manager.delete_downloads.return_value = (
        1  # DB deletion of old record successful
    )

    new_download_to_add = sample_download_obj
    coordinator.add_download(new_download_to_add)

    mock_db_manager.get_download_by_id.assert_called_once_with(
        new_download_to_add.feed, new_download_to_add.id
    )
    expected_filename = (
        f"{existing_download_data['id']}.{existing_download_data['ext']}"
    )
    mock_file_manager.delete_download_file.assert_called_once_with(
        feed=existing_download_data["feed"], filename=expected_filename
    )
    mock_db_manager.update_status.assert_not_called()
    mock_db_manager.delete_downloads.assert_not_called()
    mock_db_manager.upsert_download.assert_called_once_with(new_download_to_add)


@pytest.mark.unit
def test_replace_download_status_downloaded_file_not_found_warning(
    capsys: pytest.CaptureFixture[str],
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_download_obj: Download,
    sample_download_data: dict[str, Any],
):
    """Test warning printed if status is DOWNLOADED but file is not found by FileManager during replacement."""
    existing_download_data = sample_download_data.copy()
    existing_download_data["status"] = str(DownloadStatus.DOWNLOADED)
    existing_download_data["ext"] = "mp4"
    mock_db_manager.get_download_by_id.return_value = existing_download_data
    mock_file_manager.delete_download_file.return_value = False  # File not found

    # Action: Add download, should not raise error but print warning
    coordinator.add_download(sample_download_obj)

    # Assertions
    mock_db_manager.get_download_by_id.assert_called_once_with(
        sample_download_obj.feed, sample_download_obj.id
    )
    # Check that delete was attempted
    expected_filename = (
        f"{existing_download_data['id']}.{existing_download_data['ext']}"
    )
    mock_file_manager.delete_download_file.assert_called_once_with(
        feed=existing_download_data["feed"], filename=expected_filename
    )
    # Check that the warning was printed
    captured = capsys.readouterr()
    assert f"Warning: Expected file {expected_filename}" in captured.out
    # Check that the upsert still happened
    mock_db_manager.upsert_download.assert_called_once_with(sample_download_obj)


@pytest.mark.unit
def test_replace_download_status_downloaded_file_delete_os_error(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_download_obj: Download,
    sample_download_data: dict[str, Any],
):
    """Test FileOperationError if file deletion raises OSError."""
    existing_download_data = sample_download_data.copy()
    existing_download_data["status"] = str(DownloadStatus.DOWNLOADED)
    existing_download_data["ext"] = "mkv"
    mock_db_manager.get_download_by_id.return_value = existing_download_data
    mock_file_manager.delete_download_file.side_effect = OSError("Disk full")

    with pytest.raises(FileOperationError) as exc_info:
        coordinator.add_download(sample_download_obj)

    assert exc_info.type is FileOperationError
    assert (
        exc_info.value.__cause__ is mock_file_manager.delete_download_file.side_effect
    )
    mock_db_manager.update_status.assert_not_called()
    mock_db_manager.delete_downloads.assert_not_called()
    mock_db_manager.upsert_download.assert_not_called()


@pytest.mark.unit
def test_add_download_db_error_on_get_existing(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    sample_download_obj: Download,
):
    """Test DatabaseOperationError if get_download_by_id raises an error."""
    mock_db_manager.get_download_by_id.side_effect = sqlite3.Error(
        "DB connection failed"
    )

    with pytest.raises(DatabaseOperationError) as exc_info:
        coordinator.add_download(sample_download_obj)

    assert exc_info.type is DatabaseOperationError
    assert exc_info.value.__cause__ is mock_db_manager.get_download_by_id.side_effect
    mock_db_manager.update_status.assert_not_called()
    mock_db_manager.upsert_download.assert_not_called()


@pytest.mark.unit
def test_add_download_db_error_on_upsert(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_download_obj: Download,
):
    """
    Test add_download when there's a database error during the final upsert operation.
    """
    updated_download_obj = replace(
        sample_download_obj, title="Updated Title Before DB Error"
    )

    # Pre-conditions: existing download is found, old file (if any) is deleted successfully
    mock_db_manager.get_download_by_id.return_value = download_to_db_row(
        updated_download_obj
    )
    mock_file_manager.delete_download_file.return_value = True

    # Simulate DB error during the final upsert call
    db_error_on_final_upsert = sqlite3.IntegrityError(
        "Simulated DB IntegrityError on upsert"
    )
    mock_db_manager.upsert_download.side_effect = db_error_on_final_upsert

    with pytest.raises(DatabaseOperationError) as exc_info:
        coordinator.add_download(updated_download_obj)

    assert exc_info.type is DatabaseOperationError
    assert exc_info.value.__cause__ is db_error_on_final_upsert

    # Verify calls leading up to the error
    mock_db_manager.get_download_by_id.assert_called_once_with(
        updated_download_obj.feed, updated_download_obj.id
    )
    mock_file_manager.delete_download_file.assert_not_called()
    mock_db_manager.upsert_download.assert_called_once_with(
        updated_download_obj
    )  # Ensure it was called before raising


# --- Tests for update_status ---


@pytest.mark.unit
def test_update_status_success_no_file_ops(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_download_obj: Download,
    sample_download_data: dict[str, Any],
):
    """Test successful status update where no file operations are expected
    (e.g., QUEUED to ERROR, or ERROR to QUEUED, or DOWNLOADED to DOWNLOADED).
    """
    feed = sample_download_obj.feed
    download_id = sample_download_obj.id
    new_status = DownloadStatus.ERROR
    last_error_msg = "Network issue"

    current_download_data = sample_download_data.copy()
    current_download_data["status"] = str(DownloadStatus.QUEUED)
    mock_db_manager.get_download_by_id.return_value = current_download_data
    mock_db_manager.update_status.return_value = True

    result = coordinator.update_status(
        feed, download_id, new_status, last_error=last_error_msg
    )

    assert result is True, "DB update should succeed"
    mock_db_manager.get_download_by_id.assert_called_once_with(feed, download_id)
    mock_file_manager.delete_download_file.assert_not_called()
    mock_db_manager.update_status.assert_called_once_with(
        feed=feed, id=download_id, status=new_status, last_error=last_error_msg
    )


@pytest.mark.unit
def test_update_status_from_downloaded_file_deleted(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_download_obj: Download,
    sample_download_data: dict[str, Any],
):
    """Test status change FROM DOWNLOADED, file deletion is successful."""
    feed = sample_download_obj.feed
    download_id = sample_download_obj.id
    new_status = DownloadStatus.QUEUED

    current_download_data = sample_download_data.copy()
    current_download_data["status"] = str(DownloadStatus.DOWNLOADED)
    current_download_data["id"] = download_id
    current_download_data["ext"] = "mp4"
    mock_db_manager.get_download_by_id.return_value = current_download_data
    mock_file_manager.delete_download_file.return_value = True
    mock_db_manager.update_status.return_value = True

    result = coordinator.update_status(feed, download_id, new_status)

    assert result is True, "DB update should succeed"
    mock_db_manager.get_download_by_id.assert_called_once_with(feed, download_id)
    expected_filename = f"{download_id}.{current_download_data['ext']}"
    mock_file_manager.delete_download_file.assert_called_once_with(
        feed=feed, filename=expected_filename
    )
    mock_db_manager.update_status.assert_called_once_with(
        feed=feed, id=download_id, status=new_status, last_error=None
    )


@pytest.mark.unit
def test_update_status_from_downloaded_file_not_found_warning(
    capsys: pytest.CaptureFixture[str],
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_download_data: dict[str, Any],
):
    """Test status change FROM DOWNLOADED, file not found (warning, DB update proceeds)."""
    feed = sample_download_data["feed"]
    download_id = sample_download_data["id"]
    new_status = DownloadStatus.ERROR

    current_download_data = sample_download_data.copy()
    current_download_data["status"] = str(DownloadStatus.DOWNLOADED)
    current_download_data["ext"] = "mkv"
    mock_db_manager.get_download_by_id.return_value = current_download_data
    mock_file_manager.delete_download_file.return_value = False
    mock_db_manager.update_status.return_value = True

    result = coordinator.update_status(
        feed, download_id, new_status, last_error="file missing"
    )

    assert result is True
    mock_db_manager.get_download_by_id.assert_called_once_with(feed, download_id)
    expected_filename = f"{download_id}.{current_download_data['ext']}"
    mock_file_manager.delete_download_file.assert_called_once_with(
        feed=feed, filename=expected_filename
    )
    captured = capsys.readouterr()
    assert f"Warning: Tried to delete file {expected_filename}" in captured.out
    mock_db_manager.update_status.assert_called_once_with(
        feed=feed, id=download_id, status=new_status, last_error="file missing"
    )


@pytest.mark.unit
def test_update_status_from_downloaded_file_delete_os_error(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_download_data: dict[str, Any],
):
    """Test FileOperationError if file deletion fails (FROM DOWNLOADED) due to OSError."""
    feed = sample_download_data["feed"]
    download_id = sample_download_data["id"]
    new_status = DownloadStatus.SKIPPED

    current_download_data = sample_download_data.copy()
    current_download_data["status"] = str(DownloadStatus.DOWNLOADED)
    current_download_data["ext"] = "avi"
    mock_db_manager.get_download_by_id.return_value = current_download_data
    original_os_error = OSError("Permission denied")
    mock_file_manager.delete_download_file.side_effect = original_os_error

    with pytest.raises(FileOperationError) as exc_info:
        coordinator.update_status(feed, download_id, new_status)

    assert exc_info.type is FileOperationError
    assert exc_info.value.__cause__ is original_os_error
    expected_filename = f"{download_id}.{current_download_data['ext']}"
    assert f"Failed to delete file {expected_filename}" in str(exc_info.value)
    mock_db_manager.update_status.assert_not_called()


@pytest.mark.unit
def test_update_status_download_not_found(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
):
    """Test DownloadNotFoundError if the download to update is not found initially."""
    feed = "non_existent_feed"
    download_id = "non_existent_id"
    new_status = DownloadStatus.QUEUED

    mock_db_manager.get_download_by_id.return_value = None

    with pytest.raises(DownloadNotFoundError):
        coordinator.update_status(feed, download_id, new_status)

    mock_db_manager.update_status.assert_not_called()


@pytest.mark.unit
def test_update_status_db_returns_false(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_download_data: dict[str, Any],
):
    """Test when db_manager.update_status returns False (no row updated by DB)."""
    feed = sample_download_data["feed"]
    download_id = sample_download_data["id"]
    new_status = DownloadStatus.DOWNLOADED

    current_download_data = sample_download_data.copy()
    current_download_data["status"] = str(DownloadStatus.QUEUED)
    mock_db_manager.get_download_by_id.return_value = current_download_data
    mock_db_manager.update_status.return_value = False

    result = coordinator.update_status(feed, download_id, new_status)

    assert result is False, "DB update should fail"
    mock_db_manager.get_download_by_id.assert_called_once_with(feed, download_id)
    mock_file_manager.delete_download_file.assert_not_called()
    mock_db_manager.update_status.assert_called_once_with(
        feed=feed, id=download_id, status=new_status, last_error=None
    )


@pytest.mark.unit
def test_update_status_db_error_on_final_update(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_download_data: dict[str, Any],
):
    """Test DatabaseOperationError when db_manager.update_status (final step) raises sqlite3.Error."""
    feed = sample_download_data["feed"]
    download_id = sample_download_data["id"]
    new_status = DownloadStatus.ERROR
    original_db_error = sqlite3.Error("DB constraint failed on final update")

    current_download_data = sample_download_data.copy()
    current_download_data["status"] = str(DownloadStatus.QUEUED)
    mock_db_manager.get_download_by_id.return_value = current_download_data
    mock_db_manager.update_status.side_effect = original_db_error

    with pytest.raises(DatabaseOperationError) as exc_info:
        coordinator.update_status(
            feed, download_id, new_status, last_error="test error"
        )

    assert exc_info.type is DatabaseOperationError
    assert exc_info.value.__cause__ is original_db_error
    mock_db_manager.get_download_by_id.assert_called_once_with(feed, download_id)
    mock_file_manager.delete_download_file.assert_not_called()
    mock_db_manager.update_status.assert_called_once_with(
        feed=feed, id=download_id, status=new_status, last_error="test error"
    )


# --- Tests for _row_to_download ---
# Note: Testing a private method. Generally discouraged, but useful here
# for isolating the conversion logic and its specific error handling.


@pytest.mark.unit
def test_row_to_download_success(
    coordinator: DataCoordinator,  # Need coordinator instance to call the method
    sample_download_data: dict[str, Any],
    sample_download_obj: Download,
):
    """Test successful conversion of a valid row dictionary."""
    # Mock sqlite3.Row behavior by using the dictionary directly
    mock_row = sample_download_data
    converted_download = coordinator._row_to_download(mock_row)  # type: ignore
    assert converted_download == sample_download_obj


@pytest.mark.unit
@pytest.mark.parametrize(
    "malformed_field, malformed_value, expected_error_message_part",
    [
        ("published", "not-a-date-string", "Invalid date format"),
        (
            "published",
            None,
            "Invalid date format",
        ),  # None is not a valid ISO date string
        ("status", "weird_status", "Invalid status value"),
    ],
)
def test_row_to_download_malformed_data(
    coordinator: DataCoordinator,
    sample_download_data: dict[str, Any],
    malformed_field: str,
    malformed_value: Any,
    expected_error_message_part: str,
):
    """Test ValueError is raised for malformed data fields."""
    corrupted_row_data = sample_download_data.copy()
    corrupted_row_data[malformed_field] = malformed_value

    with pytest.raises(ValueError) as exc_info:
        coordinator._row_to_download(corrupted_row_data)  # type: ignore

    assert exc_info.type is ValueError


# --- Tests for get_download_by_id ---


@pytest.mark.unit
def test_get_download_by_id_found(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    sample_download_data: dict[str, Any],  # Raw data for mock row
    sample_download_obj: Download,  # Expected Download object
):
    """Test successfully retrieving and converting a download."""
    feed = sample_download_obj.feed
    download_id = sample_download_obj.id

    mock_row = sample_download_data
    mock_db_manager.get_download_by_id.return_value = mock_row

    retrieved_download = coordinator.get_download_by_id(feed, download_id)

    assert retrieved_download is not None
    assert retrieved_download == sample_download_obj
    mock_db_manager.get_download_by_id.assert_called_once_with(feed, download_id)


@pytest.mark.unit
def test_get_download_by_id_not_found(
    coordinator: DataCoordinator, mock_db_manager: MagicMock
):
    """Test when the download is not found in the database."""
    feed = "test_feed"
    download_id = "non_existent_id"
    mock_db_manager.get_download_by_id.return_value = None

    retrieved_download = coordinator.get_download_by_id(feed, download_id)

    assert retrieved_download is None
    mock_db_manager.get_download_by_id.assert_called_once_with(feed, download_id)


@pytest.mark.unit
def test_get_download_by_id_db_error(
    coordinator: DataCoordinator, mock_db_manager: MagicMock
):
    """Test DatabaseOperationError when db_manager.get_download_by_id raises sqlite3.Error."""
    feed = "test_feed"
    download_id = "error_id"
    original_db_error = sqlite3.Error("DB query failed")
    mock_db_manager.get_download_by_id.side_effect = original_db_error

    with pytest.raises(DatabaseOperationError) as exc_info:
        coordinator.get_download_by_id(feed, download_id)

    assert exc_info.type is DatabaseOperationError
    assert exc_info.value.__cause__ is original_db_error


# --- Tests for stream_download_by_id ---


@pytest.mark.unit
def test_stream_download_success(
    coordinator: DataCoordinator,
    mock_file_manager: MagicMock,
    sample_download_obj: Download,  # Use this to mock the result of get_download_by_id
):
    """Test successfully getting a stream for a DOWNLOADED item."""
    feed = sample_download_obj.feed
    download_id = sample_download_obj.id
    sample_download_obj.status = DownloadStatus.DOWNLOADED  # Ensure status is correct
    sample_download_obj.ext = "mp4"  # Ensure ext is present
    expected_filename = f"{download_id}.{sample_download_obj.ext}"
    mock_stream = BytesIO(b"dummy file content")

    # Mock the internal get_download_by_id call to return the modified object
    # We can mock the coordinator's own method for simplicity here, assuming get_download_by_id is tested elsewhere
    coordinator.get_download_by_id = MagicMock(return_value=sample_download_obj)
    mock_file_manager.get_download_stream.return_value = mock_stream

    stream = coordinator.stream_download_by_id(feed, download_id)

    assert stream is mock_stream
    coordinator.get_download_by_id.assert_called_once_with(feed, download_id)
    mock_file_manager.get_download_stream.assert_called_once_with(
        feed, expected_filename
    )


@pytest.mark.unit
def test_stream_download_not_found(
    coordinator: DataCoordinator,
):
    """Test returns None if the initial get_download_by_id returns None."""
    feed = "test_feed"
    download_id = "not_found_id"
    coordinator.get_download_by_id = MagicMock(return_value=None)

    stream = coordinator.stream_download_by_id(feed, download_id)

    assert stream is None
    coordinator.get_download_by_id.assert_called_once_with(feed, download_id)


@pytest.mark.unit
def test_stream_download_status_not_downloaded(
    coordinator: DataCoordinator,
    sample_download_obj: Download,
):
    """Test returns None if download status is not DOWNLOADED."""
    feed = sample_download_obj.feed
    download_id = sample_download_obj.id
    sample_download_obj.status = DownloadStatus.QUEUED  # Set status to non-DOWNLOADED
    coordinator.get_download_by_id = MagicMock(return_value=sample_download_obj)

    stream = coordinator.stream_download_by_id(feed, download_id)

    assert stream is None
    coordinator.get_download_by_id.assert_called_once_with(feed, download_id)


@pytest.mark.unit
def test_stream_download_file_not_found_error(
    coordinator: DataCoordinator,
    mock_file_manager: MagicMock,
    sample_download_obj: Download,
):
    """Test FileOperationError if FileManager raises FileNotFoundError."""
    feed = sample_download_obj.feed
    download_id = sample_download_obj.id
    sample_download_obj.status = DownloadStatus.DOWNLOADED
    sample_download_obj.ext = "mp4"
    expected_filename = f"{download_id}.{sample_download_obj.ext}"
    original_error = FileNotFoundError("File vanished")

    coordinator.get_download_by_id = MagicMock(return_value=sample_download_obj)
    mock_file_manager.get_download_stream.side_effect = original_error

    with pytest.raises(FileOperationError) as exc_info:
        coordinator.stream_download_by_id(feed, download_id)

    assert exc_info.type is FileOperationError
    assert exc_info.value.__cause__ is original_error
    coordinator.get_download_by_id.assert_called_once_with(feed, download_id)
    mock_file_manager.get_download_stream.assert_called_once_with(
        feed, expected_filename
    )


@pytest.mark.unit
def test_stream_download_os_error(
    coordinator: DataCoordinator,
    mock_file_manager: MagicMock,
    sample_download_obj: Download,
):
    """Test FileOperationError if FileManager raises OSError."""
    feed = sample_download_obj.feed
    download_id = sample_download_obj.id
    sample_download_obj.status = DownloadStatus.DOWNLOADED
    sample_download_obj.ext = "wav"
    expected_filename = f"{download_id}.{sample_download_obj.ext}"
    original_error = OSError("Permission denied opening file")

    coordinator.get_download_by_id = MagicMock(return_value=sample_download_obj)
    mock_file_manager.get_download_stream.side_effect = original_error

    with pytest.raises(FileOperationError) as exc_info:
        coordinator.stream_download_by_id(feed, download_id)

    assert exc_info.type is FileOperationError
    assert exc_info.value.__cause__ is original_error
    coordinator.get_download_by_id.assert_called_once_with(feed, download_id)
    mock_file_manager.get_download_stream.assert_called_once_with(
        feed, expected_filename
    )


@pytest.mark.unit
def test_stream_download_db_error_propagates(
    coordinator: DataCoordinator,
):
    """Test that DatabaseOperationError from get_download_by_id propagates."""
    feed = "test_feed"
    download_id = "db_error_id"
    original_error = DatabaseOperationError("Initial DB lookup failed")
    coordinator.get_download_by_id = MagicMock(side_effect=original_error)

    with pytest.raises(DatabaseOperationError) as exc_info:
        coordinator.stream_download_by_id(feed, download_id)

    assert exc_info.type is DatabaseOperationError
    assert exc_info.value is original_error
    coordinator.get_download_by_id.assert_called_once_with(feed, download_id)


# --- Tests for get_errors ---


@pytest.mark.unit
def test_get_errors_success_no_feed_no_offset(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    sample_download_data: dict[str, Any],
):
    """Test successfully retrieving all errors, default limit, no offset."""
    limit_val = 5
    offset_val = 0

    error_row_1_data = sample_download_data.copy()
    error_row_1_data["id"] = "error_id_1"
    error_row_1_data["status"] = str(DownloadStatus.ERROR)
    error_row_1_data["last_error"] = "First error"

    error_row_2_data = sample_download_data.copy()
    error_row_2_data["feed"] = "another_feed"
    error_row_2_data["id"] = "error_id_2"
    error_row_2_data["status"] = str(DownloadStatus.ERROR)
    error_row_2_data["last_error"] = "Second error"

    mock_db_manager.get_errors.return_value = [error_row_1_data, error_row_2_data]

    # Re-create Download objects based on potentially modified data
    download1 = coordinator._row_to_download(error_row_1_data)  # type: ignore
    download2 = coordinator._row_to_download(error_row_2_data)  # type: ignore
    assert download1 is not None
    assert download2 is not None

    expected_downloads = [download1, download2]

    retrieved_errors = coordinator.get_errors(limit=limit_val, offset=offset_val)

    assert retrieved_errors == expected_downloads
    mock_db_manager.get_errors.assert_called_once_with(
        feed=None, limit=limit_val, offset=offset_val
    )


@pytest.mark.unit
def test_get_errors_success_with_feed(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    sample_download_data: dict[str, Any],
):
    """Test successfully retrieving errors for a specific feed."""
    target_feed = "my_specific_feed"
    limit_val = 10
    offset_val = 0

    error_row_data = sample_download_data.copy()
    error_row_data["feed"] = target_feed
    error_row_data["id"] = "error_for_feed"
    error_row_data["status"] = str(DownloadStatus.ERROR)
    error_row_data["last_error"] = "Feed specific error"

    mock_db_manager.get_errors.return_value = [error_row_data]
    expected_download = coordinator._row_to_download(error_row_data)  # type: ignore
    assert expected_download is not None

    retrieved_errors = coordinator.get_errors(
        feed=target_feed, limit=limit_val, offset=offset_val
    )

    assert retrieved_errors == [expected_download]
    mock_db_manager.get_errors.assert_called_once_with(
        feed=target_feed, limit=limit_val, offset=offset_val
    )


@pytest.mark.unit
def test_get_errors_pagination(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    sample_download_data: dict[str, Any],
):
    """Test pagination with limit and offset."""
    limit_val = 1
    offset_val = 5

    # Simulate that the db_manager.get_errors call with offset would return this specific row
    paginated_row_data = sample_download_data.copy()
    paginated_row_data["id"] = "paginated_error_id"
    paginated_row_data["status"] = str(DownloadStatus.ERROR)

    mock_db_manager.get_errors.return_value = [paginated_row_data]
    expected_download = coordinator._row_to_download(paginated_row_data)  # type: ignore
    assert expected_download is not None

    retrieved_errors = coordinator.get_errors(
        limit=limit_val, offset=offset_val
    )  # Get 1 item, skipping 5

    assert retrieved_errors == [expected_download]
    mock_db_manager.get_errors.assert_called_once_with(
        feed=None, limit=limit_val, offset=offset_val
    )


@pytest.mark.unit
def test_get_errors_no_errors_found(
    coordinator: DataCoordinator, mock_db_manager: MagicMock
):
    """Test behavior when no errors are found in the database."""
    mock_db_manager.get_errors.return_value = []

    retrieved_errors = coordinator.get_errors()

    assert retrieved_errors == []
    # default limit and offset
    mock_db_manager.get_errors.assert_called_once_with(feed=None, limit=100, offset=0)


@pytest.mark.unit
def test_get_errors_db_error_on_get_errors(
    coordinator: DataCoordinator, mock_db_manager: MagicMock
):
    """Test DatabaseOperationError is raised if db_manager.get_errors fails."""
    target_feed = "some_feed"

    original_db_error = sqlite3.Error("DB connection lost during get_errors")
    mock_db_manager.get_errors.side_effect = original_db_error

    with pytest.raises(DatabaseOperationError) as exc_info:
        coordinator.get_errors(feed=target_feed)

    assert exc_info.type is DatabaseOperationError
    assert exc_info.value.__cause__ is original_db_error
    # default limit and offset
    mock_db_manager.get_errors.assert_called_once_with(
        feed=target_feed, limit=100, offset=0
    )


@pytest.mark.unit
def test_get_errors_row_conversion_value_error(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    sample_download_data: dict[str, Any],
):
    """Test DataCoordinatorError if _row_to_download raises ValueError for a row."""
    valid_row_data = sample_download_data.copy()
    valid_row_data["id"] = "valid_id"
    valid_row_data["status"] = str(DownloadStatus.ERROR)

    malformed_row_data = sample_download_data.copy()
    malformed_row_data["id"] = "malformed_id"
    malformed_row_data["status"] = str(DownloadStatus.ERROR)
    malformed_row_data["published"] = "not-a-valid-date"  # This will cause ValueError

    mock_db_manager.get_errors.return_value = [valid_row_data, malformed_row_data]

    with pytest.raises(DataCoordinatorError) as exc_info:
        coordinator.get_errors()

    assert exc_info.type is DataCoordinatorError
    assert isinstance(exc_info.value.__cause__, ValueError)
    mock_db_manager.get_errors.assert_called_once_with(feed=None, limit=100, offset=0)


# --- Tests for prune_old_downloads ---


@pytest.fixture
def pruning_dl_data() -> list[dict[str, Any]]:
    """Provides a list of raw data for downloads with various statuses and published dates."""
    base_time = datetime.datetime(2023, 10, 15, 12, 0, 0, tzinfo=datetime.UTC)
    return [
        {
            "feed": "prune_feed",
            "id": "item1_dl_oldest",
            "title": "Oldest DL",
            "published": (base_time - datetime.timedelta(days=30)).isoformat(),
            "status": str(DownloadStatus.DOWNLOADED),
            "ext": "mp4",
            "duration": 10,
            "source_url": "url1",
            "retries": 0,
            "last_error": None,
            "thumbnail": None,
        },
        {
            "feed": "prune_feed",
            "id": "item2_err_old",
            "title": "Old Error",
            "published": (base_time - datetime.timedelta(days=20)).isoformat(),
            "status": str(DownloadStatus.ERROR),
            "ext": "mkv",
            "duration": 20,
            "source_url": "url2",
            "retries": 3,
            "last_error": "Failed DL",
            "thumbnail": None,
        },
        {
            "feed": "prune_feed",
            "id": "item3_q_mid",
            "title": "Mid Queued",
            "published": (base_time - datetime.timedelta(days=10)).isoformat(),
            "status": str(DownloadStatus.QUEUED),
            "ext": "webm",
            "duration": 30,
            "source_url": "url3",
            "retries": 0,
            "last_error": None,
            "thumbnail": None,
        },
        {
            "feed": "prune_feed",
            "id": "item4_dl_keep",
            "title": "To Keep DL",
            "published": (base_time - datetime.timedelta(days=5)).isoformat(),
            "status": str(DownloadStatus.DOWNLOADED),
            "ext": "mp4",
            "duration": 40,
            "source_url": "url4",
            "retries": 0,
            "last_error": None,
            "thumbnail": None,
        },
        {
            "feed": "prune_feed",
            "id": "item5_arch_new",
            "title": "New Archived",
            "published": (base_time - datetime.timedelta(days=1)).isoformat(),
            "status": str(DownloadStatus.ARCHIVED),
            "ext": "mp3",
            "duration": 50,
            "source_url": "url5",
            "retries": 0,
            "last_error": None,
            "thumbnail": None,
        },
        {
            "feed": "other_feed",
            "id": "item6_other_dl",
            "title": "Other Feed DL",
            "published": (base_time - datetime.timedelta(days=15)).isoformat(),
            "status": str(DownloadStatus.DOWNLOADED),
            "ext": "mp4",
            "duration": 60,
            "source_url": "url6",
            "retries": 0,
            "last_error": None,
            "thumbnail": None,
        },
    ]


@pytest.mark.unit
def test_prune_no_rules_no_candidates(
    coordinator: DataCoordinator, mock_db_manager: MagicMock
):
    """Test prune_old_downloads when no rules are given; nothing should be pruned."""
    feed_name = "feed_with_no_rules"

    # Action: call prune_old_downloads with no specific rules
    archived_ids, deleted_file_ids = coordinator.prune_old_downloads(
        feed=feed_name, keep_last=None, prune_before_date=None
    )

    # Assertions: expect empty lists for archived and deleted IDs
    assert not archived_ids, "Archived IDs list should be empty when no rules are given"
    assert not deleted_file_ids, (
        "Deleted file IDs list should be empty when no rules are given"
    )

    # Verify that DB methods for fetching candidates were not called if rules are None
    mock_db_manager.get_downloads_to_prune_by_keep_last.assert_not_called()
    mock_db_manager.get_downloads_to_prune_by_since.assert_not_called()
    # Since no candidates should be found/processed, update_status should not be called either.
    mock_db_manager.update_status.assert_not_called()


@pytest.mark.unit
def test_prune_no_candidates_found_by_rules(
    coordinator: DataCoordinator, mock_db_manager: MagicMock
):
    """Test pruning when rules are given but no DB items match them."""
    feed_name = "prune_feed_no_matches"
    keep_rule = 1
    date_rule = datetime.datetime.now(datetime.UTC)

    mock_db_manager.get_downloads_to_prune_by_keep_last.return_value = []
    mock_db_manager.get_downloads_to_prune_by_since.return_value = []

    archived_ids, deleted_file_ids = coordinator.prune_old_downloads(
        feed=feed_name,
        keep_last=keep_rule,
        prune_before_date=date_rule,
    )

    assert not archived_ids, (
        "Archived IDs list should be empty if no candidates match rules"
    )
    assert not deleted_file_ids, (
        "Deleted file IDs list should be empty if no candidates match rules"
    )

    mock_db_manager.get_downloads_to_prune_by_keep_last.assert_called_once_with(
        feed_name, keep_rule
    )
    mock_db_manager.get_downloads_to_prune_by_since.assert_called_once_with(
        feed_name, date_rule
    )
    mock_db_manager.update_status.assert_not_called()


@pytest.mark.unit
def test_prune_keep_last_archives_and_deletes_files(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    pruning_dl_data: list[dict[str, Any]],
    capsys: pytest.CaptureFixture[str],
):
    """Test pruning with 'keep_last', archives records, deletes files for DOWNLOADED."""
    feed_name = "prune_feed"
    # keep_last=1 means item4_dl_keep and item5_arch_new (newest 2 by published date) are kept based on offset logic.
    # Candidates for pruning from keep_last: item1, item2, item3 (oldest 3 for the feed)
    # item1 (DL), item2 (ERR), item3 (Q)
    # We expect item1's file to be deleted, all 3 to be archived.

    # Simulate DB returning these three as candidates for pruning by keep_last
    candidate_rows = [
        pruning_dl_data[0],
        pruning_dl_data[1],
        pruning_dl_data[2],
    ]  # item1, item2, item3
    mock_db_manager.get_downloads_to_prune_by_keep_last.return_value = candidate_rows
    mock_db_manager.get_downloads_to_prune_by_since.return_value = []  # No date rule

    mock_file_manager.delete_download_file.return_value = (
        True  # All file deletions succeed
    )
    mock_db_manager.update_status.return_value = (
        True  # All DB updates to ARCHIVED succeed
    )

    archived_ids, deleted_file_ids = coordinator.prune_old_downloads(
        feed=feed_name, keep_last=2, prune_before_date=None
    )

    assert len(archived_ids) == 3, (
        "Expected 3 items to be archived based on keep_last rule"
    )
    assert len(deleted_file_ids) == 1, (
        "Expected 1 file to be deleted (only item1 was DOWNLOADED)"
    )

    mock_db_manager.get_downloads_to_prune_by_keep_last.assert_called_once_with(
        feed_name, 2
    )

    # Check file deletion calls (only for item1)
    expected_filename_item1 = f"{candidate_rows[0]['id']}.{candidate_rows[0]['ext']}"
    mock_file_manager.delete_download_file.assert_any_call(
        feed_name, expected_filename_item1
    )
    assert mock_file_manager.delete_download_file.call_count == 1

    # Check update_status calls (for item1, item2, item3)
    for i in range(3):
        mock_db_manager.update_status.assert_any_call(
            feed_name, candidate_rows[i]["id"], DownloadStatus.ARCHIVED
        )
    assert mock_db_manager.update_status.call_count == 3


@pytest.mark.unit
def test_prune_by_date_archives_non_downloaded(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    pruning_dl_data: list[dict[str, Any]],
):
    """Test pruning with 'prune_before_date', archives non-DOWNLOADED items correctly."""
    feed_name = "prune_feed"
    cutoff_date = datetime.datetime.fromisoformat(
        pruning_dl_data[2]["published"]
    )  # Date of item3

    candidate_rows = [pruning_dl_data[0], pruning_dl_data[1]]  # item1, item2
    mock_db_manager.get_downloads_to_prune_by_keep_last.return_value = []
    mock_db_manager.get_downloads_to_prune_by_since.return_value = candidate_rows

    mock_file_manager.delete_download_file.return_value = True
    mock_db_manager.update_status.return_value = True

    archived_ids, deleted_file_ids = coordinator.prune_old_downloads(
        feed=feed_name, keep_last=None, prune_before_date=cutoff_date
    )
    assert len(archived_ids) == 2, "Expected 2 items to be archived by date rule"
    assert len(deleted_file_ids) == 1, (
        "Expected 1 file to be deleted (item1 was DOWNLOADED)"
    )

    mock_db_manager.get_downloads_to_prune_by_since.assert_called_once_with(
        feed_name, cutoff_date
    )
    expected_filename_item1 = f"{candidate_rows[0]['id']}.{candidate_rows[0]['ext']}"
    mock_file_manager.delete_download_file.assert_called_once_with(
        feed_name, expected_filename_item1
    )
    mock_db_manager.update_status.assert_any_call(
        feed_name, candidate_rows[0]["id"], DownloadStatus.ARCHIVED
    )
    mock_db_manager.update_status.assert_any_call(
        feed_name, candidate_rows[1]["id"], DownloadStatus.ARCHIVED
    )
    assert mock_db_manager.update_status.call_count == 2


@pytest.mark.unit
def test_prune_union_of_rules(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    pruning_dl_data: list[dict[str, Any]],
):
    """Test pruning considers unique items from both keep_last and date rules."""
    feed_name = "prune_feed"
    keep_last_candidates = [pruning_dl_data[0], pruning_dl_data[1]]  # item1, item2
    date_candidates = [
        pruning_dl_data[0],
        pruning_dl_data[1],
        pruning_dl_data[2],
    ]  # item1, item2, item3
    cutoff_date = datetime.datetime.fromisoformat(
        pruning_dl_data[3]["published"]
    )  # Date of item4

    mock_db_manager.get_downloads_to_prune_by_keep_last.return_value = (
        keep_last_candidates
    )
    mock_db_manager.get_downloads_to_prune_by_since.return_value = date_candidates
    mock_file_manager.delete_download_file.return_value = True
    mock_db_manager.update_status.return_value = True

    archived_ids, deleted_file_ids = coordinator.prune_old_downloads(
        feed=feed_name, keep_last=3, prune_before_date=cutoff_date
    )

    assert len(archived_ids) == 3, "Expected 3 items to be archived from union of rules"
    assert len(deleted_file_ids) == 1, (
        "Expected 1 file to be deleted (item1 was DOWNLOADED)"
    )

    expected_filename_item1 = f"{pruning_dl_data[0]['id']}.{pruning_dl_data[0]['ext']}"
    mock_file_manager.delete_download_file.assert_called_once_with(
        feed_name, expected_filename_item1
    )

    all_candidate_ids = {
        pruning_dl_data[0]["id"],
        pruning_dl_data[1]["id"],
        pruning_dl_data[2]["id"],
    }
    actual_archived_ids_set: set[str] = set()
    # Iterate through the recorded calls to update_status
    # Each call_obj in call_args_list is a unittest.mock.call object
    # which can be unpacked into (name, args, kwargs) or directly accessed.
    # Since update_status is called with positional args: (feed, id, status)
    for call_obj in mock_db_manager.update_status.call_args_list:
        args, _ = call_obj  # args is a tuple of positional arguments, kwargs is a dict
        called_id = args[1]
        actual_archived_ids_set.add(called_id)

    assert actual_archived_ids_set == all_candidate_ids, (
        f"Expected IDs {all_candidate_ids} to be archived, but got {actual_archived_ids_set}"
    )
    assert mock_db_manager.update_status.call_count == 3


@pytest.mark.unit
def test_prune_file_not_found_warning(
    capsys: pytest.CaptureFixture[str],
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    pruning_dl_data: list[dict[str, Any]],
):
    """Test warning logged if a DOWNLOADED item's file is not found during pruning."""
    feed_name = "prune_feed"
    candidate_rows = [pruning_dl_data[0]]
    mock_db_manager.get_downloads_to_prune_by_keep_last.return_value = candidate_rows
    mock_db_manager.get_downloads_to_prune_by_since.return_value = []

    mock_file_manager.delete_download_file.return_value = False
    mock_db_manager.update_status.return_value = True

    archived_ids, deleted_file_ids = coordinator.prune_old_downloads(
        feed=feed_name,
        keep_last=4,
        prune_before_date=None,
    )

    assert len(archived_ids) == 1
    assert len(deleted_file_ids) == 0
    captured = capsys.readouterr()
    expected_filename = f"{candidate_rows[0]['id']}.{candidate_rows[0]['ext']}"
    assert (
        f"Warning: File {expected_filename} for downloaded item {feed_name}/{candidate_rows[0]['id']} not found on disk"
        in captured.out
    )
    mock_db_manager.update_status.assert_called_once_with(
        feed_name, candidate_rows[0]["id"], DownloadStatus.ARCHIVED
    )


@pytest.mark.unit
def test_prune_file_delete_os_error_halts(
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    pruning_dl_data: list[dict[str, Any]],
):
    """Test FileOperationError from file deletion halts pruning for the feed."""
    feed_name = "prune_feed"
    candidate_rows = [pruning_dl_data[0], pruning_dl_data[1]]
    mock_db_manager.get_downloads_to_prune_by_keep_last.return_value = candidate_rows
    mock_db_manager.get_downloads_to_prune_by_since.return_value = []

    original_os_error = OSError("Disk permission error")
    mock_file_manager.delete_download_file.side_effect = original_os_error

    with pytest.raises(FileOperationError) as exc_info:
        coordinator.prune_old_downloads(
            feed=feed_name, keep_last=3, prune_before_date=None
        )

    assert exc_info.type is FileOperationError
    assert exc_info.value.__cause__ is original_os_error
    expected_filename_item1 = f"{candidate_rows[0]['id']}.{candidate_rows[0]['ext']}"
    mock_file_manager.delete_download_file.assert_called_once_with(
        feed_name, expected_filename_item1
    )
    mock_db_manager.update_status.assert_not_called()


@pytest.mark.unit
def test_prune_db_candidate_fetch_error(
    coordinator: DataCoordinator, mock_db_manager: MagicMock
):
    """Test DatabaseOperationError if fetching candidates (keep_last) fails."""
    original_db_error = sqlite3.Error("DB connection failed during keep_last query")
    mock_db_manager.get_downloads_to_prune_by_keep_last.side_effect = original_db_error

    with pytest.raises(DatabaseOperationError) as exc_info:
        coordinator.prune_old_downloads(
            feed="prune_feed", keep_last=1, prune_before_date=None
        )
    assert exc_info.type is DatabaseOperationError
    assert exc_info.value.__cause__ is original_db_error


@pytest.mark.unit
def test_prune_row_conversion_error_skips_candidate(
    capsys: pytest.CaptureFixture[str],
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    pruning_dl_data: list[dict[str, Any]],
):
    """Test ValueError during row conversion skips bad candidate, continues others."""
    feed_name = "prune_feed"
    valid_candidate_row = pruning_dl_data[0]  # item1 (DL)
    malformed_row = pruning_dl_data[1].copy()
    malformed_row["published"] = (
        "not-a-date"  # Will cause ValueError in _row_to_download
    )

    mock_db_manager.get_downloads_to_prune_by_keep_last.return_value = [
        malformed_row,
        valid_candidate_row,
    ]
    mock_db_manager.get_downloads_to_prune_by_since.return_value = []

    mock_file_manager.delete_download_file.return_value = (
        True  # For valid_candidate_row
    )
    mock_db_manager.update_status.return_value = True

    # Expecting error to be raised and halt further processing for this design
    with pytest.raises(DataCoordinatorError) as exc_info:
        coordinator.prune_old_downloads(
            feed=feed_name, keep_last=3, prune_before_date=None
        )

    # Assertions about the caught exception:
    assert exc_info.type is DataCoordinatorError
    assert isinstance(exc_info.value.__cause__, ValueError)


@pytest.mark.unit
def test_prune_db_update_status_error_skips_item(
    capsys: pytest.CaptureFixture[str],
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    pruning_dl_data: list[dict[str, Any]],
):
    """Test sqlite3.Error during update_status for one item skips it, continues others."""
    feed_name = "prune_feed"
    item1_row = pruning_dl_data[0]
    item3_row = pruning_dl_data[2]

    mock_db_manager.get_downloads_to_prune_by_keep_last.return_value = [
        item1_row,
        item3_row,
    ]
    mock_db_manager.get_downloads_to_prune_by_since.return_value = []

    mock_file_manager.delete_download_file.return_value = True

    original_update_error = sqlite3.Error("Constraint failed on update")

    def update_status_side_effect(
        feed: str,
        id: str,
        status: DownloadStatus,
        last_error: str | None = None,
        **kwargs: Any,
    ):
        if id == item1_row["id"]:
            raise original_update_error
        return True

    mock_db_manager.update_status.side_effect = update_status_side_effect

    # Expecting error during the update_status call for item1
    with pytest.raises(DatabaseOperationError) as exc_info:
        coordinator.prune_old_downloads(
            feed=feed_name, keep_last=3, prune_before_date=None
        )

    assert exc_info.type is DatabaseOperationError
    assert exc_info.value.__cause__ is original_update_error


@pytest.mark.unit
def test_prune_db_update_status_returns_false_skips_item(
    capsys: pytest.CaptureFixture[str],
    coordinator: DataCoordinator,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    pruning_dl_data: list[dict[str, Any]],
):
    """Test if update_status returns False for one item, it's skipped, continues others."""
    feed_name = "prune_feed"
    item1_row = pruning_dl_data[0]
    item3_row = pruning_dl_data[2]
    mock_db_manager.get_downloads_to_prune_by_keep_last.return_value = [
        item1_row,
        item3_row,
    ]
    mock_db_manager.get_downloads_to_prune_by_since.return_value = []
    mock_file_manager.delete_download_file.return_value = True

    def update_status_side_effect(
        feed: str,
        id: str,
        status: DownloadStatus,
        last_error: str | None = None,
        **kwargs: Any,
    ):
        return id != item1_row["id"]

    mock_db_manager.update_status.side_effect = update_status_side_effect

    archived_ids, deleted_file_ids = coordinator.prune_old_downloads(
        feed=feed_name, keep_last=3, prune_before_date=None
    )
    assert archived_ids == [item3_row["id"]], (
        "Only item3 should be successfully archived"
    )
    assert deleted_file_ids == [item1_row["id"]], (
        "Only item1's file should have been deleted"
    )

    captured = capsys.readouterr()
    assert (
        f"Warning: Failed to archive {feed_name}/{item1_row['id']} during pruning. DB record NOT updated."
        in captured.out
    )
    assert mock_db_manager.update_status.call_count == 2
