# pyright: reportPrivateUsage=false

"""Tests for the FileManager class and its file handling operations."""

from pathlib import Path
from unittest.mock import patch

import pytest

from anypod.exceptions import FileOperationError
from anypod.file_manager import FileManager
from anypod.path_manager import PathManager

# --- Fixtures ---


@pytest.fixture
def temp_base_download_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Creates a temporary base download directory for tests."""
    return tmp_path_factory.mktemp("test_download_base")


@pytest.fixture
def file_manager(temp_base_download_path: Path) -> FileManager:
    """Provides a FileManager instance initialized with a temporary base download path."""
    paths = PathManager(temp_base_download_path, "http://localhost")
    fm = FileManager(paths)
    return fm


def save_file(
    file_manager: FileManager, feed_id: str, file_name: str, file_content: bytes
) -> Path:
    """Saves a file. This is not built-in because yt-dlp does the saving."""
    file_path = file_manager._paths.base_data_dir / feed_id / file_name
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with Path.open(file_path, "wb") as f:
        f.write(file_content)
    return file_path


# --- Tests for delete_download_file ---


@pytest.mark.unit
def test_delete_download_file_success(file_manager: FileManager):
    """Tests successful deletion of an existing file."""
    feed_id = "delete_feed"
    download_id = "to_delete"
    ext = "txt"
    file_name = f"{download_id}.{ext}"
    file_content = b"content"

    file_to_delete_path = save_file(file_manager, feed_id, file_name, file_content)

    assert file_to_delete_path.exists()

    file_manager.delete_download_file(feed_id, download_id, ext)

    assert not file_to_delete_path.exists(), "File should be deleted from disk."


@pytest.mark.unit
def test_delete_download_file_not_found(file_manager: FileManager):
    """Tests delete_download_file raises FileNotFoundError for a non-existent file."""
    feed_id = "delete_feed_not_found"
    download_id = "non_existent"
    ext = "txt"

    with pytest.raises(FileNotFoundError):
        file_manager.delete_download_file(feed_id, download_id, ext)


# --- Tests for download_exists ---


@pytest.mark.unit
def test_download_exists_true(file_manager: FileManager):
    """Tests download_exists returns True when a file exists."""
    feed_id = "exists_feed"
    download_id = "existing_file"
    ext = "mp3"
    file_name = f"{download_id}.{ext}"

    save_file(file_manager, feed_id, file_name, b"dummy data")

    assert file_manager.download_exists(feed_id, download_id, ext) is True


@pytest.mark.unit
def test_download_exists_false_not_found(file_manager: FileManager):
    """Tests download_exists returns False when a file does not exist."""
    feed_id = "exists_feed_false"
    download_id = "ghost_file"
    ext = "mp3"

    assert file_manager.download_exists(feed_id, download_id, ext) is False


@pytest.mark.unit
def test_download_exists_false_is_directory(
    file_manager: FileManager, temp_base_download_path: Path
):
    """Tests download_exists returns False if the path is a directory, not a file."""
    feed_id = "exists_feed_dir"
    download_id = "a_directory"
    ext = "mp3"
    dir_as_file_name = f"{download_id}.{ext}"

    # Setup: Create a directory where a file might be expected
    (temp_base_download_path / feed_id / dir_as_file_name).mkdir(
        parents=True, exist_ok=True
    )

    assert file_manager.download_exists(feed_id, download_id, ext) is False


# --- Tests for get_download_stream ---


@pytest.mark.unit
def test_get_download_stream_success(file_manager: FileManager):
    """Tests successfully getting a stream for an existing file and checks its content."""
    feed_id = "stream_feed"
    download_id = "stream_me"
    ext = "mp3"
    file_name = f"{download_id}.{ext}"
    file_content = b"Test stream content."

    save_file(file_manager, feed_id, file_name, file_content)

    with file_manager.get_download_stream(feed_id, download_id, ext) as stream:
        read_content = stream.read()

    assert read_content == file_content, (
        "Streamed content does not match original content."
    )


@pytest.mark.unit
def test_get_download_stream_file_not_found(file_manager: FileManager):
    """Tests get_download_stream raises FileNotFoundError for a non-existent file."""
    feed_id = "stream_feed_404"
    download_id = "no_such_file"
    ext = "mp3"

    with pytest.raises(FileNotFoundError):
        file_manager.get_download_stream(feed_id, download_id, ext)


@pytest.mark.unit
def test_get_download_stream_path_is_directory(
    file_manager: FileManager, temp_base_download_path: Path
):
    """Tests get_download_stream raises FileNotFoundError if the path is a directory."""
    feed_id = "stream_feed_dir"
    download_id = "i_am_a_dir"
    ext = "mp3"
    dir_as_file_name = f"{download_id}.{ext}"

    (temp_base_download_path / feed_id / dir_as_file_name).mkdir(
        parents=True, exist_ok=True
    )

    with pytest.raises(FileNotFoundError):
        file_manager.get_download_stream(feed_id, download_id, ext)


@pytest.mark.unit
def test_get_download_stream_file_operation_error(file_manager: FileManager):
    """Tests get_download_stream raises FileOperationError for a file operation error."""
    feed_id = "stream_feed_error"
    download_id = "error_file"
    ext = "mp3"
    file_name = f"{download_id}.{ext}"

    # Setup a dummy downloaded file in the correct location (media subdirectory)
    file_path = file_manager._paths.media_file_path(feed_id, download_id, ext)
    with Path.open(file_path, "wb") as f:
        f.write(b"dummy content")

    # Patch Path.open to simulate a file operation error on read
    simulated_error = OSError("Simulated disk full error")
    with (
        patch.object(Path, "open", side_effect=simulated_error),
        pytest.raises(FileOperationError) as exc_info,
    ):
        file_manager.get_download_stream(feed_id, download_id, ext)

    # Verify the FileOperationError includes correct attributes and cause
    error = exc_info.value
    assert error.feed_id == feed_id
    assert error.download_id == download_id
    assert error.file_name == file_name
    assert error.__cause__ is simulated_error
