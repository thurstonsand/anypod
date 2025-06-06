# pyright: reportPrivateUsage=false

"""Tests for the Pruner service.

This module contains unit tests for the Pruner class, which is responsible
for identifying and removing old downloads according to configured retention
rules, including file deletion and database record archiving.
"""

import dataclasses
import datetime
from unittest.mock import MagicMock

import pytest

from anypod.data_coordinator.pruner import Pruner
from anypod.db import DatabaseManager, Download, DownloadStatus
from anypod.exceptions import (
    DatabaseOperationError,
    DownloadNotFoundError,
    FileOperationError,
    PruneError,
)
from anypod.file_manager import FileManager

# --- Fixtures ---


@pytest.fixture
def mock_db_manager() -> MagicMock:
    """Provides a mock DatabaseManager."""
    return MagicMock(spec=DatabaseManager)


@pytest.fixture
def mock_file_manager() -> MagicMock:
    """Provides a mock FileManager."""
    return MagicMock(spec=FileManager)


@pytest.fixture
def pruner(
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
) -> Pruner:
    """Provides a Pruner instance with mocked dependencies."""
    return Pruner(mock_db_manager, mock_file_manager)


@pytest.fixture
def sample_downloaded_item() -> Download:
    """Provides a sample Download object with DOWNLOADED status."""
    return Download(
        feed="test_feed",
        id="test_dl_id_1",
        source_url="http://example.com/video1",
        title="Test Video 1",
        published=datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC),
        ext="mp4",
        duration=120.0,
        status=DownloadStatus.DOWNLOADED,
    )


@pytest.fixture
def sample_queued_item() -> Download:
    """Provides a sample Download object with QUEUED status."""
    return Download(
        feed="test_feed",
        id="test_dl_id_2",
        source_url="http://example.com/video2",
        title="Test Video 2",
        published=datetime.datetime(2023, 1, 2, 12, 0, 0, tzinfo=datetime.UTC),
        ext="mp4",
        duration=180.0,
        status=DownloadStatus.QUEUED,
    )


# --- Tests for _identify_prune_candidates ---


@pytest.mark.unit
def test_identify_prune_candidates_keep_last_only(
    pruner: Pruner,
    mock_db_manager: MagicMock,
    sample_downloaded_item: Download,
):
    """Tests _identify_prune_candidates with only keep_last rule."""
    downloads_to_prune = [sample_downloaded_item]
    mock_db_manager.get_downloads_to_prune_by_keep_last.return_value = (
        downloads_to_prune
    )

    result = pruner._identify_prune_candidates(
        "test_feed", keep_last=5, prune_before_date=None
    )

    assert result == {sample_downloaded_item}
    mock_db_manager.get_downloads_to_prune_by_keep_last.assert_called_once_with(
        "test_feed", 5
    )
    mock_db_manager.get_downloads_to_prune_by_since.assert_not_called()


@pytest.mark.unit
def test_identify_prune_candidates_date_only(
    pruner: Pruner,
    mock_db_manager: MagicMock,
    sample_downloaded_item: Download,
):
    """Tests _identify_prune_candidates with only prune_before_date rule."""
    downloads_to_prune = [sample_downloaded_item]
    cutoff_date = datetime.datetime(2023, 6, 1, tzinfo=datetime.UTC)
    mock_db_manager.get_downloads_to_prune_by_since.return_value = downloads_to_prune

    result = pruner._identify_prune_candidates(
        "test_feed", keep_last=None, prune_before_date=cutoff_date
    )

    assert result == {sample_downloaded_item}
    mock_db_manager.get_downloads_to_prune_by_since.assert_called_once_with(
        "test_feed", cutoff_date
    )
    mock_db_manager.get_downloads_to_prune_by_keep_last.assert_not_called()


@pytest.mark.unit
def test_identify_prune_candidates_both_rules_union(
    pruner: Pruner,
    mock_db_manager: MagicMock,
    sample_downloaded_item: Download,
    sample_queued_item: Download,
):
    """Tests _identify_prune_candidates combines results from both rules."""
    keep_last_results = [sample_downloaded_item]
    date_results = [sample_downloaded_item, sample_queued_item]  # overlap + additional
    cutoff_date = datetime.datetime(2023, 6, 1, tzinfo=datetime.UTC)

    mock_db_manager.get_downloads_to_prune_by_keep_last.return_value = keep_last_results
    mock_db_manager.get_downloads_to_prune_by_since.return_value = date_results

    result = pruner._identify_prune_candidates(
        "test_feed", keep_last=3, prune_before_date=cutoff_date
    )

    # Should be union: {sample_downloaded_item, sample_queued_item}
    assert result == {sample_downloaded_item, sample_queued_item}
    mock_db_manager.get_downloads_to_prune_by_keep_last.assert_called_once_with(
        "test_feed", 3
    )
    mock_db_manager.get_downloads_to_prune_by_since.assert_called_once_with(
        "test_feed", cutoff_date
    )


@pytest.mark.unit
def test_identify_prune_candidates_no_rules_returns_empty(
    pruner: Pruner,
    mock_db_manager: MagicMock,
):
    """Tests _identify_prune_candidates returns empty set when no rules provided."""
    result = pruner._identify_prune_candidates(
        "test_feed", keep_last=None, prune_before_date=None
    )

    assert result == set()
    mock_db_manager.get_downloads_to_prune_by_keep_last.assert_not_called()
    mock_db_manager.get_downloads_to_prune_by_since.assert_not_called()


@pytest.mark.unit
def test_identify_prune_candidates_keep_last_db_error_raises_prune_error(
    pruner: Pruner,
    mock_db_manager: MagicMock,
):
    """Tests _identify_prune_candidates raises PruneError on DB error for keep_last."""
    db_error = DatabaseOperationError("DB fetch failed")
    mock_db_manager.get_downloads_to_prune_by_keep_last.side_effect = db_error

    with pytest.raises(PruneError) as exc_info:
        pruner._identify_prune_candidates(
            "test_feed", keep_last=5, prune_before_date=None
        )

    assert exc_info.value.feed_id == "test_feed"
    assert exc_info.value.__cause__ is db_error


# --- Tests for _handle_file_deletion ---


@pytest.mark.unit
def test_handle_file_deletion_success(
    pruner: Pruner,
    mock_file_manager: MagicMock,
    sample_downloaded_item: Download,
):
    """Tests _handle_file_deletion successfully deletes file."""
    pruner._handle_file_deletion(sample_downloaded_item, "test_feed")

    mock_file_manager.delete_download_file.assert_called_once_with(
        "test_feed", "test_dl_id_1.mp4"
    )


@pytest.mark.unit
def test_handle_file_deletion_file_operation_error_raises_prune_error(
    pruner: Pruner,
    mock_file_manager: MagicMock,
    sample_downloaded_item: Download,
):
    """Tests _handle_file_deletion raises PruneError on FileOperationError."""
    file_error = FileOperationError("Permission denied")
    mock_file_manager.delete_download_file.side_effect = file_error

    with pytest.raises(PruneError) as exc_info:
        pruner._handle_file_deletion(sample_downloaded_item, "test_feed")

    assert exc_info.value.feed_id == "test_feed"
    assert exc_info.value.download_id == sample_downloaded_item.id
    assert exc_info.value.__cause__ is file_error


# --- Tests for _archive_download_record ---


@pytest.mark.unit
def test_archive_download_success(
    pruner: Pruner,
    mock_db_manager: MagicMock,
    sample_downloaded_item: Download,
):
    """Tests _archive_download successfully archives download."""
    pruner._archive_download(sample_downloaded_item, "test_feed")

    mock_db_manager.archive_download.assert_called_once_with(
        "test_feed", sample_downloaded_item.id
    )


@pytest.mark.unit
def test_archive_download_db_error_raises_prune_error(
    pruner: Pruner,
    mock_db_manager: MagicMock,
    sample_downloaded_item: Download,
):
    """Tests _archive_download raises PruneError on DatabaseOperationError."""
    db_error = DatabaseOperationError("Archive failed")
    mock_db_manager.archive_download.side_effect = db_error

    with pytest.raises(PruneError) as exc_info:
        pruner._archive_download(sample_downloaded_item, "test_feed")

    assert exc_info.value.feed_id == "test_feed"
    assert exc_info.value.download_id == sample_downloaded_item.id
    assert exc_info.value.__cause__ is db_error


@pytest.mark.unit
def test_archive_download_not_found_error_raises_prune_error(
    pruner: Pruner,
    mock_db_manager: MagicMock,
    sample_downloaded_item: Download,
):
    """Tests _archive_download raises PruneError on DownloadNotFoundError."""
    not_found_error = DownloadNotFoundError("Download not found")
    mock_db_manager.archive_download.side_effect = not_found_error

    with pytest.raises(PruneError) as exc_info:
        pruner._archive_download(sample_downloaded_item, "test_feed")

    assert exc_info.value.feed_id == "test_feed"
    assert exc_info.value.download_id == sample_downloaded_item.id
    assert exc_info.value.__cause__ is not_found_error


# --- Tests for _process_single_download_for_pruning ---


@pytest.mark.unit
def test_process_single_download_downloaded_file_deleted_successfully(
    pruner: Pruner,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_downloaded_item: Download,
):
    """Tests _process_single_download_for_pruning returns True when file is deleted."""
    result = pruner._process_single_download_for_pruning(
        sample_downloaded_item, "test_feed"
    )

    assert result is True
    mock_file_manager.delete_download_file.assert_called_once_with(
        "test_feed", "test_dl_id_1.mp4"
    )
    mock_db_manager.archive_download.assert_called_once_with(
        "test_feed", sample_downloaded_item.id
    )


@pytest.mark.unit
def test_process_single_download_downloaded_file_not_found_returns_false(
    pruner: Pruner,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_downloaded_item: Download,
):
    """Tests _process_single_download_for_pruning returns False when file not found."""
    mock_file_manager.delete_download_file.side_effect = FileNotFoundError(
        "File not found"
    )

    result = pruner._process_single_download_for_pruning(
        sample_downloaded_item, "test_feed"
    )

    assert result is False
    mock_file_manager.delete_download_file.assert_called_once_with(
        "test_feed", "test_dl_id_1.mp4"
    )
    mock_db_manager.archive_download.assert_called_once_with(
        "test_feed", sample_downloaded_item.id
    )


@pytest.mark.unit
def test_process_single_download_non_downloaded_no_file_deletion(
    pruner: Pruner,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_queued_item: Download,
):
    """Tests _process_single_download_for_pruning skips file deletion for non-DOWNLOADED items."""
    result = pruner._process_single_download_for_pruning(
        sample_queued_item, "test_feed"
    )

    assert result is False
    mock_file_manager.delete_download_file.assert_not_called()
    mock_db_manager.archive_download.assert_called_once_with(
        "test_feed", sample_queued_item.id
    )


@pytest.mark.unit
def test_process_single_download_archive_error_raises_prune_error(
    pruner: Pruner,
    mock_db_manager: MagicMock,
    sample_queued_item: Download,
):
    """Tests _process_single_download_for_pruning raises PruneError on archive failure."""
    db_error = DatabaseOperationError("Archive failed")
    mock_db_manager.archive_download.side_effect = db_error

    with pytest.raises(PruneError) as exc_info:
        pruner._process_single_download_for_pruning(sample_queued_item, "test_feed")

    assert exc_info.value.__cause__ is db_error


# --- Tests for prune_feed_downloads ---


@pytest.mark.unit
def test_prune_feed_downloads_no_candidates_returns_zero_counts(
    pruner: Pruner,
    mock_db_manager: MagicMock,
):
    """Tests prune_feed_downloads returns (0,0) when no candidates are found."""
    mock_db_manager.get_downloads_to_prune_by_keep_last.return_value = []
    mock_db_manager.get_downloads_to_prune_by_since.return_value = []

    archived_count, files_deleted_count = pruner.prune_feed_downloads(
        "test_feed", keep_last=5, prune_before_date=None
    )

    assert archived_count == 0
    assert files_deleted_count == 0


@pytest.mark.unit
def test_prune_feed_downloads_processes_candidates_and_counts(
    pruner: Pruner,
    mock_db_manager: MagicMock,
    mock_file_manager: MagicMock,
    sample_downloaded_item: Download,
    sample_queued_item: Download,
):
    """Tests prune_feed_downloads processes candidates and returns correct counts."""
    candidates = [sample_downloaded_item, sample_queued_item]
    mock_db_manager.get_downloads_to_prune_by_keep_last.return_value = candidates

    archived_count, files_deleted_count = pruner.prune_feed_downloads(
        "test_feed", keep_last=1, prune_before_date=None
    )

    assert archived_count == 2  # Both items archived
    assert files_deleted_count == 1  # Only DOWNLOADED item had file deleted
    assert mock_file_manager.delete_download_file.call_count == 1
    assert mock_db_manager.archive_download.call_count == 2


@pytest.mark.unit
def test_prune_feed_downloads_candidate_identification_error_raises_prune_error(
    pruner: Pruner,
    mock_db_manager: MagicMock,
):
    """Tests prune_feed_downloads raises PruneError on candidate identification failure."""
    db_error = DatabaseOperationError("DB fetch failed")
    mock_db_manager.get_downloads_to_prune_by_keep_last.side_effect = db_error

    with pytest.raises(PruneError) as exc_info:
        pruner.prune_feed_downloads("test_feed", keep_last=5, prune_before_date=None)

    assert exc_info.value.feed_id == "test_feed"
    assert exc_info.value.__cause__ is db_error


@pytest.mark.unit
def test_prune_feed_downloads_individual_failure_continues_processing(
    pruner: Pruner,
    mock_db_manager: MagicMock,
    sample_downloaded_item: Download,
    sample_queued_item: Download,
):
    """Tests prune_feed_downloads continues processing other items when one fails."""
    dl1 = sample_downloaded_item
    dl2 = dataclasses.replace(sample_queued_item, id="fail_item")
    dl3 = dataclasses.replace(sample_downloaded_item, id="success_item")

    candidates = [dl1, dl2, dl3]
    mock_db_manager.get_downloads_to_prune_by_keep_last.return_value = candidates

    # Make dl2 fail during archival, others succeed
    def archive_side_effect(_feed_id: str, download_id: str) -> None:
        if download_id == "fail_item":
            raise DatabaseOperationError("Archive failed for fail_item")

    mock_db_manager.archive_download.side_effect = archive_side_effect

    archived_count, files_deleted_count = pruner.prune_feed_downloads(
        "test_feed", keep_last=1, prune_before_date=None
    )

    # dl1 and dl3 succeed (2 archived, 2 files deleted), dl2 fails (logged but processing continues)
    assert archived_count == 2
    assert files_deleted_count == 2
    assert mock_db_manager.archive_download.call_count == 3  # All attempts made
