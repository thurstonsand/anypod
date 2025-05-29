# pyright: reportPrivateUsage=false

"""Tests for the FileManager class and its file handling operations."""

from pathlib import Path
from unittest.mock import patch

import pytest

from anypod.file_manager import FileManager

# --- Fixtures ---


@pytest.fixture
def temp_base_download_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Creates a temporary base download directory for tests."""
    return tmp_path_factory.mktemp("test_download_base")


@pytest.fixture
def file_manager(temp_base_download_path: Path) -> FileManager:
    """Provides a FileManager instance initialized with a temporary base download path."""
    fm = FileManager(base_download_path=temp_base_download_path)
    return fm


def save_file(
    file_manager: FileManager, feed_id: str, file_name: str, file_content: bytes
) -> Path:
    """Saves a file. This is not built-in because yt-dlp does the saving."""
    file_path = file_manager.base_download_path / feed_id / file_name
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with Path.open(file_path, "wb") as f:
        f.write(file_content)
    return file_path


# --- Tests for delete_download_file ---


@pytest.mark.unit
def test_delete_download_file_success(file_manager: FileManager):
    """Tests successful deletion of an existing file."""
    feed_id = "delete_feed"
    file_name = "to_delete.txt"
    file_content = b"content"

    file_to_delete_path = save_file(file_manager, feed_id, file_name, file_content)

    assert file_to_delete_path.exists()

    file_manager.delete_download_file(feed_id, file_name)

    assert not file_to_delete_path.exists(), "File should be deleted from disk."


@pytest.mark.unit
def test_delete_download_file_not_found(file_manager: FileManager):
    """Tests delete_download_file returns False for a non-existent file."""
    feed_id = "delete_feed_not_found"
    file_name = "non_existent.txt"

    with pytest.raises(FileNotFoundError) as e:
        file_manager.delete_download_file(feed_id, file_name)

    assert file_name in str(e.value)


# --- Tests for download_exists ---


@pytest.mark.unit
def test_download_exists_true(file_manager: FileManager):
    """Tests download_exists returns True when a file exists."""
    feed_id = "exists_feed"
    file_name = "existing_file.mp3"

    save_file(file_manager, feed_id, file_name, b"dummy data")

    assert file_manager.download_exists(feed_id, file_name) is True


@pytest.mark.unit
def test_download_exists_false_not_found(file_manager: FileManager):
    """Tests download_exists returns False when a file does not exist."""
    feed_id = "exists_feed_false"
    file_name = "ghost_file.mp3"

    assert file_manager.download_exists(feed_id, file_name) is False


@pytest.mark.unit
def test_download_exists_false_is_directory(
    file_manager: FileManager, temp_base_download_path: Path
):
    """Tests download_exists returns False if the path is a directory, not a file."""
    feed_id = "exists_feed_dir"
    dir_as_file_name = "a_directory"

    # Setup: Create a directory where a file might be expected
    (temp_base_download_path / feed_id / dir_as_file_name).mkdir(
        parents=True, exist_ok=True
    )

    assert file_manager.download_exists(feed_id, dir_as_file_name) is False


# --- Tests for get_download_stream ---


@pytest.mark.unit
def test_get_download_stream_success(file_manager: FileManager):
    """Tests successfully getting a stream for an existing file and checks its content."""
    feed_id = "stream_feed"
    file_name = "stream_me.mp3"
    file_content = b"Test stream content."

    save_file(file_manager, feed_id, file_name, file_content)

    with file_manager.get_download_stream(feed_id, file_name) as stream:
        read_content = stream.read()

    assert read_content == file_content, (
        "Streamed content does not match original content."
    )


@pytest.mark.unit
def test_get_download_stream_file_not_found(file_manager: FileManager):
    """Tests get_download_stream raises FileNotFoundError for a non-existent file."""
    feed_id = "stream_feed_404"
    file_name = "no_such_file.mp3"

    with pytest.raises(FileNotFoundError):
        file_manager.get_download_stream(feed_id, file_name)


@pytest.mark.unit
def test_get_download_stream_path_is_directory(
    file_manager: FileManager, temp_base_download_path: Path
):
    """Tests get_download_stream raises FileNotFoundError if the path is a directory."""
    feed_id = "stream_feed_dir"
    dir_as_file_name = "i_am_a_dir"

    (temp_base_download_path / feed_id / dir_as_file_name).mkdir(
        parents=True, exist_ok=True
    )

    with pytest.raises(FileNotFoundError):
        file_manager.get_download_stream(feed_id, dir_as_file_name)


@pytest.mark.unit
def test_get_download_stream_file_operation_error(
    file_manager: FileManager, temp_base_download_path: Path
):
    """Tests get_download_stream raises FileOperationError for a file operation error."""
    feed_id = "stream_feed_error"
    file_name = "error_file.mp3"

    # Setup a dummy downloaded file
    feed_dir = temp_base_download_path / feed_id
    feed_dir.mkdir(parents=True, exist_ok=True)
    file_path = feed_dir / file_name
    with Path.open(file_path, "wb") as f:
        f.write(b"dummy content")

    # Patch Path.open to simulate a file operation error on read
    simulated_error = OSError("Simulated disk full error")
    with (
        patch.object(Path, "open", side_effect=simulated_error),
        pytest.raises(OSError) as exc_info,
    ):
        file_manager.get_download_stream(feed_id, file_name)

    # Verify the FileOperationError includes correct file_name and cause
    assert exc_info.value is simulated_error
