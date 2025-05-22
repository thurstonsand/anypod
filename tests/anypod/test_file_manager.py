import io
from pathlib import Path
from unittest.mock import patch

import pytest
from pytest_mock import MockerFixture

from anypod.exceptions import FileOperationError
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


# --- Tests for save_download_file ---


@pytest.mark.unit
def test_save_download_file_success(
    file_manager: FileManager, temp_base_download_path: Path
):
    """Tests successful saving of a download file.

    Verifies directory creation, .incomplete handling, final file
    content, and return path.
    """
    feed_id = "my_test_feed"
    file_name = "episode1.mp3"
    file_content = b"This is some test audio data."
    data_stream = io.BytesIO(file_content)

    saved_path = file_manager.save_download_file(
        feed=feed_id, file_name=file_name, data_stream=data_stream
    )

    expected_feed_dir = temp_base_download_path / feed_id
    expected_final_path = expected_feed_dir / file_name
    incomplete_file_path = expected_feed_dir / (file_name + ".incomplete")

    assert expected_feed_dir.exists(), "Feed directory should have been created."
    assert expected_feed_dir.is_dir(), "Feed directory should be a directory."

    assert saved_path == expected_final_path, (
        "Returned path should match expected final path."
    )
    assert expected_final_path.exists(), "Final download file should exist."
    assert expected_final_path.is_file(), "Final download file should be a file."
    assert not incomplete_file_path.exists(), (
        ".incomplete file should be removed after successful save."
    )

    with Path.open(expected_final_path, "rb") as f:
        content_on_disk = f.read()
    assert content_on_disk == file_content, (
        "Content of saved file does not match original content."
    )


@pytest.mark.unit
def test_save_download_file_creates_base_directory(
    tmp_path_factory: pytest.TempPathFactory,
):
    """Tests that FileManager creates the base_download_path if it doesn't exist."""
    non_existent_base_path = tmp_path_factory.mktemp("base") / "download_files"
    assert not non_existent_base_path.exists(), (
        "Ensure path doesn't exist before FileManager init"
    )

    fm = FileManager(base_download_path=non_existent_base_path)
    assert fm.base_download_path.exists(), (
        "Base download path should be created by FileManager constructor."
    )
    assert fm.base_download_path.is_dir(), "Base download path should be a directory."


@pytest.mark.unit
def test_save_download_file_overwrites_existing_file(
    file_manager: FileManager, temp_base_download_path: Path
):
    """Tests that save_download_file overwrites an existing file with the same name."""
    feed_id = "overwrite_feed"
    file_name = "track.mp3"
    initial_content = b"Initial version."
    new_content = b"New shiny version!"

    feed_dir = temp_base_download_path / feed_id
    feed_dir.mkdir(parents=True, exist_ok=True)
    existing_file_path = feed_dir / file_name

    # Create an initial file
    with Path.open(existing_file_path, "wb") as f:
        f.write(initial_content)
    assert existing_file_path.read_bytes() == initial_content

    # Act: Save new content to the same filname
    saved_path = file_manager.save_download_file(
        feed=feed_id, file_name=file_name, data_stream=io.BytesIO(new_content)
    )

    assert saved_path == existing_file_path, "Returned path should be the same."
    assert existing_file_path.exists(), "File should still exist."
    assert existing_file_path.read_bytes() == new_content, (
        "File content should be updated to the new content."
    )


@pytest.mark.unit
def test_save_download_file_overwrites_existing_incomplete_file(
    file_manager: FileManager, temp_base_download_path: Path
):
    """Tests that save_download_file correctly handles and overwrites an existing .incomplete file."""
    feed_id = "overwrite_incomplete_feed"
    file_name = "podcast.mp3"
    old_incomplete_content = b"Stale incomplete data."
    new_valid_content = b"Fresh and complete data!"

    feed_dir = temp_base_download_path / feed_id
    feed_dir.mkdir(parents=True, exist_ok=True)

    final_file_path = feed_dir / file_name
    incomplete_file_path = feed_dir / (file_name + ".incomplete")

    # Create an old .incomplete file
    with Path.open(incomplete_file_path, "wb") as f:
        f.write(old_incomplete_content)
    assert incomplete_file_path.read_bytes() == old_incomplete_content
    assert not final_file_path.exists()

    saved_path = file_manager.save_download_file(
        feed=feed_id, file_name=file_name, data_stream=io.BytesIO(new_valid_content)
    )

    assert saved_path == final_file_path, "Returned path should be the final path."
    assert final_file_path.exists(), "Final file should exist."
    assert final_file_path.read_bytes() == new_valid_content, (
        "Final file should have the new content."
    )
    assert not incomplete_file_path.exists(), "Old .incomplete file should be gone."


@pytest.mark.unit
def test_save_download_file_error_during_write_cleans_up(
    file_manager: FileManager, temp_base_download_path: Path, mocker: MockerFixture
):
    """Tests that if an error occurs during shutil.copyfileobj (writing to .incomplete), the .incomplete file is cleaned up."""
    feed_id = "error_write_feed"
    file_name = "broken_stream.dat"
    data_stream = io.BytesIO(b"Some data before error.")

    incomplete_file_path = (
        temp_base_download_path / feed_id / (file_name + ".incomplete")
    )

    mocked_copyfileobj = mocker.patch("shutil.copyfileobj")
    simulated_os_error = OSError("Simulated disk full error")
    mocked_copyfileobj.side_effect = simulated_os_error

    with pytest.raises(FileOperationError) as exc_info:
        file_manager.save_download_file(
            feed=feed_id, file_name=file_name, data_stream=data_stream
        )

    assert not incomplete_file_path.exists(), (
        ".incomplete file should be cleaned up after a write error."
    )

    assert (temp_base_download_path / feed_id).exists(), (
        "Feed directory should still exist as it's created before write attempt."
    )

    assert exc_info.value.__cause__ is simulated_os_error


# --- Tests for delete_download_file ---


@pytest.mark.unit
def test_delete_download_file_success(
    file_manager: FileManager, temp_base_download_path: Path
):
    """Tests successful deletion of an existing file."""
    feed_id = "delete_feed"
    file_name = "to_delete.txt"
    file_content = b"content"

    file_to_delete_path = file_manager.save_download_file(
        feed_id, file_name, io.BytesIO(file_content)
    )
    assert file_to_delete_path.exists()

    result = file_manager.delete_download_file(feed_id, file_name)

    assert result is True, "delete_download_file should return True on success."
    assert not file_to_delete_path.exists(), "File should be deleted from disk."


@pytest.mark.unit
def test_delete_download_file_not_found(file_manager: FileManager):
    """Tests delete_download_file returns False for a non-existent file."""
    feed_id = "delete_feed_not_found"
    file_name = "non_existent.txt"

    result = file_manager.delete_download_file(feed_id, file_name)

    assert result is False, (
        "delete_download_file should return False if file not found."
    )


# --- Tests for download_exists ---


@pytest.mark.unit
def test_download_exists_true(file_manager: FileManager):
    """Tests download_exists returns True when a file exists."""
    feed_id = "exists_feed"
    file_name = "existing_file.mp3"

    file_manager.save_download_file(feed_id, file_name, io.BytesIO(b"dummy data"))

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

    file_manager.save_download_file(feed_id, file_name, io.BytesIO(file_content))

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
