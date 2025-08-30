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
@pytest.mark.asyncio
async def test_delete_download_file_success(file_manager: FileManager):
    """Tests successful deletion of an existing file."""
    feed_id = "delete_feed"
    download_id = "to_delete"
    ext = "txt"
    file_name = f"{download_id}.{ext}"
    file_content = b"content"

    file_to_delete_path = save_file(file_manager, feed_id, file_name, file_content)

    assert file_to_delete_path.exists()

    await file_manager.delete_download_file(feed_id, download_id, ext)

    assert not file_to_delete_path.exists(), "File should be deleted from disk."


@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_download_file_not_found(file_manager: FileManager):
    """Tests delete_download_file raises FileNotFoundError for a non-existent file."""
    feed_id = "delete_feed_not_found"
    download_id = "non_existent"
    ext = "txt"

    with pytest.raises(FileNotFoundError):
        await file_manager.delete_download_file(feed_id, download_id, ext)


# --- Tests for download_exists ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_exists_true(file_manager: FileManager):
    """Tests download_exists returns True when a file exists."""
    feed_id = "exists_feed"
    download_id = "existing_file"
    ext = "mp3"
    file_name = f"{download_id}.{ext}"

    save_file(file_manager, feed_id, file_name, b"dummy data")

    assert await file_manager.download_exists(feed_id, download_id, ext) is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_exists_false_not_found(file_manager: FileManager):
    """Tests download_exists returns False when a file does not exist."""
    feed_id = "exists_feed_false"
    download_id = "ghost_file"
    ext = "mp3"

    assert await file_manager.download_exists(feed_id, download_id, ext) is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_exists_false_is_directory(
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

    assert await file_manager.download_exists(feed_id, download_id, ext) is False


# --- Tests for get_download_stream ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_download_stream_success(file_manager: FileManager):
    """Tests successfully getting a stream for an existing file and checks its content."""
    feed_id = "stream_feed"
    download_id = "stream_me"
    ext = "mp3"
    file_name = f"{download_id}.{ext}"
    file_content = b"Test stream content."

    save_file(file_manager, feed_id, file_name, file_content)

    download_stream = await file_manager.get_download_stream(feed_id, download_id, ext)
    read_content = b"".join([chunk async for chunk in download_stream])

    assert read_content == file_content, (
        "Streamed content does not match original content."
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_download_stream_file_not_found(file_manager: FileManager):
    """Tests get_download_stream raises FileNotFoundError for a non-existent file."""
    feed_id = "stream_feed_404"
    download_id = "no_such_file"
    ext = "mp3"

    with pytest.raises(FileNotFoundError):
        download_stream = await file_manager.get_download_stream(
            feed_id, download_id, ext
        )
        async for _ in download_stream:
            pass


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_download_stream_path_is_directory(
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
        download_stream = await file_manager.get_download_stream(
            feed_id, download_id, ext
        )
        async for _ in download_stream:
            pass


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_download_stream_file_operation_error(file_manager: FileManager):
    """Tests get_download_stream raises FileOperationError for a file operation error."""
    feed_id = "stream_feed_error"
    download_id = "error_file"
    ext = "mp3"
    file_name = f"{download_id}.{ext}"

    # Setup a dummy downloaded file in the correct location (media subdirectory)
    file_path = await file_manager._paths.media_file_path(feed_id, download_id, ext)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with Path.open(file_path, "wb") as f:
        f.write(b"dummy content")

    # Patch aiofiles.open to simulate a file operation error on read
    simulated_error = OSError("Simulated disk full error")
    with (
        patch("aiofiles.open", side_effect=simulated_error),
        pytest.raises(FileOperationError) as exc_info,
    ):
        download_stream = await file_manager.get_download_stream(
            feed_id, download_id, ext
        )
        async for _ in download_stream:
            pass

    # Verify the FileOperationError includes correct attributes and cause
    error = exc_info.value
    assert error.feed_id == feed_id
    assert error.download_id == download_id
    assert error.file_name == file_name
    assert error.__cause__ is simulated_error


# --- Tests for get_image_path ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_image_path_feed_level(file_manager: FileManager):
    """Tests get_image_path returns correct path for feed-level images."""
    feed_id = "image_feed"
    ext = "jpg"

    # Create the image file
    image_path = await file_manager._paths.image_path(feed_id, None, ext)
    image_path.write_bytes(b"dummy image content")

    result_path = await file_manager.get_image_path(feed_id, None, ext)

    assert result_path == image_path
    assert result_path.exists()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_image_path_download_level(file_manager: FileManager):
    """Tests get_image_path returns correct path for download-level images."""
    feed_id = "image_feed"
    download_id = "video_123"
    ext = "jpg"

    # Create the image file
    image_path = await file_manager._paths.image_path(feed_id, download_id, ext)
    image_path.write_bytes(b"dummy image content")

    result_path = await file_manager.get_image_path(feed_id, download_id, ext)

    assert result_path == image_path
    assert result_path.exists()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_image_path_not_found(file_manager: FileManager):
    """Tests get_image_path raises FileNotFoundError for non-existent files."""
    feed_id = "image_feed_404"
    download_id = "missing_image"
    ext = "jpg"

    with pytest.raises(FileNotFoundError):
        await file_manager.get_image_path(feed_id, download_id, ext)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_image_path_invalid_feed_id(file_manager: FileManager):
    """Tests get_image_path raises FileOperationError for invalid feed_id."""
    feed_id = ""
    download_id = "video_123"
    ext = "jpg"

    with pytest.raises(FileNotFoundError):
        await file_manager.get_image_path(feed_id, download_id, ext)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_image_path_invalid_download_id(file_manager: FileManager):
    """Tests get_image_path raises FileOperationError for invalid download_id."""
    feed_id = "valid_feed"
    download_id = ""
    ext = "jpg"

    with pytest.raises(FileNotFoundError):
        await file_manager.get_image_path(feed_id, download_id, ext)


# --- Tests for image_exists ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_image_exists_feed_level_true(file_manager: FileManager):
    """Tests image_exists returns True for existing feed-level images."""
    feed_id = "exists_image_feed"
    ext = "jpg"

    # Create the image file
    image_path = await file_manager._paths.image_path(feed_id, None, ext)
    image_path.write_bytes(b"dummy image content")

    assert await file_manager.image_exists(feed_id, None, ext)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_image_exists_download_level_true(file_manager: FileManager):
    """Tests image_exists returns True for existing download-level images."""
    feed_id = "exists_image_feed"
    download_id = "existing_image"
    ext = "jpg"

    # Create the image file
    image_path = await file_manager._paths.image_path(feed_id, download_id, ext)
    image_path.write_bytes(b"dummy image content")

    assert await file_manager.image_exists(feed_id, download_id, ext)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_image_exists_false_not_found(file_manager: FileManager):
    """Tests image_exists returns False for non-existent images."""
    feed_id = "exists_image_feed_false"
    download_id = "ghost_image"
    ext = "jpg"

    assert not await file_manager.image_exists(feed_id, download_id, ext)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_image_exists_false_invalid_feed_id(file_manager: FileManager):
    """Tests image_exists returns False for invalid feed_id."""
    feed_id = ""
    download_id = "video_123"
    ext = "jpg"

    assert not await file_manager.image_exists(feed_id, download_id, ext)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_image_exists_false_invalid_download_id(file_manager: FileManager):
    """Tests image_exists returns False for invalid download_id."""
    feed_id = "valid_feed"
    download_id = ""
    ext = "jpg"

    assert not await file_manager.image_exists(feed_id, download_id, ext)


# --- Tests for delete_image ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_image_feed_level_success(file_manager: FileManager):
    """Tests successful deletion of feed-level images."""
    feed_id = "delete_image_feed"
    ext = "jpg"

    # Create the image file
    image_path = await file_manager._paths.image_path(feed_id, None, ext)
    image_path.write_bytes(b"dummy image content")

    assert image_path.exists()
    await file_manager.delete_image(feed_id, None, ext)
    assert not image_path.exists()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_image_download_level_success(file_manager: FileManager):
    """Tests successful deletion of download-level images."""
    feed_id = "delete_image_feed"
    download_id = "to_delete"
    ext = "jpg"

    # Create the image file
    image_path = await file_manager._paths.image_path(feed_id, download_id, ext)
    image_path.write_bytes(b"dummy image content")

    assert image_path.exists()
    await file_manager.delete_image(feed_id, download_id, ext)
    assert not image_path.exists()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_image_not_found(file_manager: FileManager):
    """Tests delete_image raises FileNotFoundError for non-existent images."""
    feed_id = "delete_image_feed_404"
    download_id = "non_existent"
    ext = "jpg"

    with pytest.raises(FileNotFoundError):
        await file_manager.delete_image(feed_id, download_id, ext)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_image_invalid_feed_id(file_manager: FileManager):
    """Tests delete_image raises FileOperationError for invalid feed_id."""
    feed_id = ""
    download_id = "video_123"
    ext = "jpg"

    with pytest.raises(FileOperationError) as exc_info:
        await file_manager.delete_image(feed_id, download_id, ext)

    assert exc_info.value.feed_id == feed_id
    assert exc_info.value.download_id == download_id


@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_image_invalid_download_id(file_manager: FileManager):
    """Tests delete_image raises FileOperationError for invalid download_id."""
    feed_id = "valid_feed"
    download_id = ""
    ext = "jpg"

    with pytest.raises(FileOperationError) as exc_info:
        await file_manager.delete_image(feed_id, download_id, ext)

    assert exc_info.value.feed_id == feed_id
    assert exc_info.value.download_id == download_id


@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_image_os_error(file_manager: FileManager):
    """Tests delete_image raises FileOperationError for OS-level errors."""
    feed_id = "error_feed"
    download_id = "error_image"
    ext = "jpg"

    # Create the image file
    image_path = await file_manager._paths.image_path(feed_id, download_id, ext)
    image_path.write_bytes(b"dummy image content")

    # Remove the file after creating it to simulate a race condition
    image_path.unlink()

    # Now try to delete it again - this should raise FileNotFoundError which gets converted to FileOperationError
    with pytest.raises(FileNotFoundError):
        await file_manager.delete_image(feed_id, download_id, ext)
