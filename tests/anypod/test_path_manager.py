# pyright: reportPrivateUsage=false

"""Tests for the PathManager class and its path/URL generation functionality."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from anypod.exceptions import FileOperationError
from anypod.path_manager import PathManager

# --- Fixtures ---


@pytest.fixture
def data_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Creates temporary data directory for tests."""
    return tmp_path_factory.mktemp("test_data")


@pytest.fixture
def path_manager(data_dir: Path) -> PathManager:
    """Provides a PathManager instance with temporary directories."""
    return PathManager(data_dir, "http://localhost:8024")


# --- Tests for initialization and properties ---


@pytest.mark.unit
def test_init_normalizes_paths(tmp_path_factory: pytest.TempPathFactory):
    """Tests that PathManager normalizes and resolves provided paths."""
    data_dir = tmp_path_factory.mktemp("data")
    base_url = "http://example.com/"

    # Test with relative paths and trailing slash in URL
    path_manager = PathManager(data_dir / "..", base_url)

    assert path_manager.base_data_dir == data_dir.parent.resolve() / "media"
    assert path_manager.base_tmp_dir == data_dir.parent.resolve() / "tmp"
    assert path_manager.base_url == "http://example.com"


@pytest.mark.unit
def test_properties_return_correct_values(path_manager: PathManager, data_dir: Path):
    """Tests that properties return the initialized values."""
    assert path_manager.base_data_dir == data_dir.resolve() / "media"
    assert path_manager.base_tmp_dir == data_dir.resolve() / "tmp"
    assert path_manager.base_images_dir == data_dir.resolve() / "images"
    assert path_manager.base_url == "http://localhost:8024"


# --- Tests for feed_data_dir ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_feed_data_dir_creates_directory(path_manager: PathManager):
    """Tests that feed_data_dir creates the directory if it doesn't exist."""
    feed_id = "test_feed"

    feed_dir = await path_manager.feed_data_dir(feed_id)

    assert feed_dir.exists()
    assert feed_dir.is_dir()
    assert feed_dir.name == feed_id
    assert feed_dir.parent == path_manager.base_data_dir


@pytest.mark.unit
@pytest.mark.asyncio
async def test_feed_data_dir_idempotent(path_manager: PathManager):
    """Tests that calling feed_data_dir multiple times is safe."""
    feed_id = "idempotent_feed"

    first_call = await path_manager.feed_data_dir(feed_id)
    second_call = await path_manager.feed_data_dir(feed_id)

    assert first_call == second_call
    assert first_call.exists()


@pytest.mark.unit
@pytest.mark.asyncio
@patch(
    "aiofiles.os.makedirs",
    new_callable=AsyncMock,
    side_effect=OSError("Permission denied"),
)
async def test_feed_data_dir_handles_mkdir_error(
    mock_makedirs: AsyncMock, path_manager: PathManager
):
    """Tests that feed_data_dir handles directory creation errors properly."""
    feed_id = "error_feed"

    with pytest.raises(FileOperationError) as exc_info:
        await path_manager.feed_data_dir(feed_id)

    assert exc_info.value.file_name is not None
    assert feed_id in exc_info.value.file_name


# --- Tests for feed_tmp_dir ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_feed_tmp_dir_creates_directory(path_manager: PathManager):
    """Tests that feed_tmp_dir creates the directory if it doesn't exist."""
    feed_id = "temp_feed"

    tmp_dir = await path_manager.feed_tmp_dir(feed_id)

    assert tmp_dir.exists()
    assert tmp_dir.is_dir()
    assert tmp_dir.name == feed_id
    assert tmp_dir.parent == path_manager.base_tmp_dir


@pytest.mark.unit
@pytest.mark.asyncio
async def test_feed_tmp_dir_idempotent(path_manager: PathManager):
    """Tests that calling feed_tmp_dir multiple times is safe."""
    feed_id = "idempotent_tmp"

    first_call = await path_manager.feed_tmp_dir(feed_id)
    second_call = await path_manager.feed_tmp_dir(feed_id)

    assert first_call == second_call
    assert first_call.exists()


@pytest.mark.unit
@pytest.mark.asyncio
@patch("aiofiles.os.makedirs", new_callable=AsyncMock, side_effect=OSError("Disk full"))
async def test_feed_tmp_dir_handles_mkdir_error(
    mock_makedirs: AsyncMock, path_manager: PathManager
):
    """Tests that feed_tmp_dir handles directory creation errors properly."""
    feed_id = "tmp_error_feed"

    with pytest.raises(FileOperationError) as exc_info:
        await path_manager.feed_tmp_dir(feed_id)

    assert exc_info.value.file_name is not None
    assert feed_id in exc_info.value.file_name


# --- Tests for feed_images_dir ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_feed_images_dir_creates_directory(path_manager: PathManager):
    """Tests that feed_images_dir creates the directory if it doesn't exist."""
    feed_id = "test_image_feed"

    feed_dir = await path_manager.feed_images_dir(feed_id)

    assert feed_dir.exists()
    assert feed_dir.is_dir()
    assert feed_dir.name == feed_id
    assert feed_dir.parent == path_manager.base_images_dir


@pytest.mark.unit
@pytest.mark.asyncio
async def test_feed_images_dir_idempotent(path_manager: PathManager):
    """Tests that calling feed_images_dir multiple times is safe."""
    feed_id = "idempotent_image_feed"

    first_call = await path_manager.feed_images_dir(feed_id)
    second_call = await path_manager.feed_images_dir(feed_id)

    assert first_call == second_call
    assert first_call.exists()


@pytest.mark.unit
@pytest.mark.asyncio
@patch(
    "aiofiles.os.makedirs",
    new_callable=AsyncMock,
    side_effect=OSError("Permission denied"),
)
async def test_feed_images_dir_handles_mkdir_error(
    _mock_makedirs: AsyncMock, path_manager: PathManager
):
    """Tests that feed_images_dir handles directory creation errors properly."""
    feed_id = "image_error_feed"

    with pytest.raises(FileOperationError) as exc_info:
        await path_manager.feed_images_dir(feed_id)

    assert exc_info.value.file_name is not None
    assert feed_id in exc_info.value.file_name


# --- Tests for download_images_dir ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_images_dir_creates_directory(path_manager: PathManager):
    """Tests that download_images_dir creates the downloads directory under the feed images dir."""
    feed_id = "test_image_feed"

    downloads_dir = await path_manager.download_images_dir(feed_id)

    assert downloads_dir.exists()
    assert downloads_dir.is_dir()
    assert downloads_dir.name == "downloads"
    assert downloads_dir.parent.name == feed_id
    assert downloads_dir.parent.parent == path_manager.base_images_dir


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_images_dir_idempotent(path_manager: PathManager):
    """Tests that calling download_images_dir multiple times is safe and returns the same path."""
    feed_id = "idempotent_image_feed"

    first_call = await path_manager.download_images_dir(feed_id)
    second_call = await path_manager.download_images_dir(feed_id)

    assert first_call == second_call
    assert first_call.exists()


# --- Tests for URL generation methods ---


@pytest.mark.unit
def test_feed_url_generation(path_manager: PathManager):
    """Tests that feed_url generates correct RSS feed URLs."""
    feed_id = "my_podcast"

    url = path_manager.feed_url(feed_id)

    assert url == f"{path_manager.base_url}/feeds/{feed_id}.xml"


@pytest.mark.unit
def test_feed_url_special_characters(path_manager: PathManager):
    """Tests feed_url with special characters in feed_id."""
    feed_id = "feed-with_special.chars"

    url = path_manager.feed_url(feed_id)

    assert url == f"{path_manager.base_url}/feeds/{feed_id}.xml"


@pytest.mark.unit
def test_feed_media_url_generation(path_manager: PathManager):
    """Tests that feed_media_url generates correct base media URLs."""
    feed_id = "video_feed"

    url = path_manager.feed_media_url(feed_id)

    assert url == f"{path_manager.base_url}/media/{feed_id}/"


@pytest.mark.unit
def test_media_file_url_generation(path_manager: PathManager):
    """Tests that media_file_url generates correct individual file URLs."""
    feed_id = "content_feed"
    download_id = "video_123"
    ext = "mp4"

    url = path_manager.media_file_url(feed_id, download_id, ext)

    assert url == f"{path_manager.feed_media_url(feed_id)}{download_id}.{ext}"


@pytest.mark.unit
def test_media_file_url_different_extensions(path_manager: PathManager):
    """Tests media_file_url with various file extensions."""
    feed_id = "multi_format"
    download_id = "item_456"

    # Test different extensions
    mp4_url = path_manager.media_file_url(feed_id, download_id, "mp4")
    webm_url = path_manager.media_file_url(feed_id, download_id, "webm")
    m4a_url = path_manager.media_file_url(feed_id, download_id, "m4a")

    assert mp4_url == f"{path_manager.feed_media_url(feed_id)}{download_id}.mp4"
    assert webm_url == f"{path_manager.feed_media_url(feed_id)}{download_id}.webm"
    assert m4a_url == f"{path_manager.feed_media_url(feed_id)}{download_id}.m4a"


@pytest.mark.unit
def test_image_url_feed_level(path_manager: PathManager):
    """Tests that image_url generates correct feed-level image URLs."""
    feed_id = "my_podcast"
    ext = "jpg"

    url = path_manager.image_url(feed_id, None, ext)

    assert url == f"{path_manager.base_url}/images/{feed_id}.{ext}"


@pytest.mark.unit
def test_image_url_download_level(path_manager: PathManager):
    """Tests that image_url generates correct download-level image URLs."""
    feed_id = "content_feed"
    download_id = "video_123"
    ext = "jpg"

    url = path_manager.image_url(feed_id, download_id, ext)

    assert url == f"{path_manager.base_url}/images/{feed_id}/{download_id}.{ext}"


@pytest.mark.unit
def test_image_url_special_characters(path_manager: PathManager):
    """Tests image_url with special characters in identifiers."""
    feed_id = "feed-with_special.chars"
    download_id = "video.with.dots"
    ext = "jpg"

    # Test feed-level image
    feed_url = path_manager.image_url(feed_id, None, ext)
    assert feed_url == f"{path_manager.base_url}/images/{feed_id}.{ext}"

    # Test download-level image
    download_url = path_manager.image_url(feed_id, download_id, ext)
    assert (
        download_url == f"{path_manager.base_url}/images/{feed_id}/{download_id}.{ext}"
    )


@pytest.mark.unit
def test_image_url_different_extensions(path_manager: PathManager):
    """Tests image_url with various file extensions."""
    feed_id = "multi_format"
    download_id = "item_456"

    # Test different extensions
    jpg_url = path_manager.image_url(feed_id, download_id, "jpg")
    png_url = path_manager.image_url(feed_id, download_id, "png")
    webp_url = path_manager.image_url(feed_id, download_id, "webp")

    assert jpg_url == f"{path_manager.base_url}/images/{feed_id}/{download_id}.jpg"
    assert png_url == f"{path_manager.base_url}/images/{feed_id}/{download_id}.png"
    assert webp_url == f"{path_manager.base_url}/images/{feed_id}/{download_id}.webp"


# --- Tests for file path generation methods ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_media_file_path_generation(path_manager: PathManager):
    """Tests that media_file_path generates correct file system paths."""
    feed_id = "path_test"
    download_id = "video_789"
    ext = "mp4"

    file_path = await path_manager.media_file_path(feed_id, download_id, ext)

    expected_path = path_manager.base_data_dir / feed_id / f"{download_id}.{ext}"
    assert file_path == expected_path


@pytest.mark.unit
@pytest.mark.asyncio
async def test_media_file_path_creates_parent_dir(path_manager: PathManager):
    """Tests that media_file_path creates the parent directory."""
    feed_id = "new_feed_dir"
    download_id = "first_video"
    ext = "webm"

    file_path = await path_manager.media_file_path(feed_id, download_id, ext)

    # The parent directory should be created by the call to feed_data_dir
    assert file_path.parent.exists()
    assert file_path.parent.name == feed_id


@pytest.mark.unit
@pytest.mark.asyncio
async def test_media_file_path_different_extensions(path_manager: PathManager):
    """Tests media_file_path with various file extensions."""
    feed_id = "ext_test"
    download_id = "content_item"

    # Test different extensions
    mp4_path = await path_manager.media_file_path(feed_id, download_id, "mp4")
    mkv_path = await path_manager.media_file_path(feed_id, download_id, "mkv")
    flv_path = await path_manager.media_file_path(feed_id, download_id, "flv")

    expected_base = path_manager.base_data_dir / feed_id
    assert mp4_path == expected_base / "content_item.mp4"
    assert mkv_path == expected_base / "content_item.mkv"
    assert flv_path == expected_base / "content_item.flv"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_image_path_feed_level(path_manager: PathManager):
    """Tests that image_path generates correct feed-level image file system paths."""
    feed_id = "path_test_feed"
    ext = "jpg"

    file_path = await path_manager.image_path(feed_id, None, ext)

    expected_path = path_manager.base_images_dir / feed_id / f"{feed_id}.{ext}"
    assert file_path == expected_path


@pytest.mark.unit
@pytest.mark.asyncio
async def test_image_path_download_level(path_manager: PathManager):
    """Tests that image_path generates correct download-level image file system paths."""
    feed_id = "path_test_feed"
    download_id = "video_789"
    ext = "jpg"

    file_path = await path_manager.image_path(feed_id, download_id, ext)

    expected_path = (
        path_manager.base_images_dir / feed_id / "downloads" / f"{download_id}.{ext}"
    )
    assert file_path == expected_path


@pytest.mark.unit
@pytest.mark.asyncio
async def test_image_path_different_extensions(path_manager: PathManager):
    """Tests image_path with various file extensions."""
    feed_id = "ext_test_feed"
    download_id = "content_item"

    # Test different extensions
    jpg_path = await path_manager.image_path(feed_id, download_id, "jpg")
    png_path = await path_manager.image_path(feed_id, download_id, "png")
    webp_path = await path_manager.image_path(feed_id, download_id, "webp")

    expected_base = path_manager.base_images_dir / feed_id / "downloads"
    assert jpg_path == expected_base / "content_item.jpg"
    assert png_path == expected_base / "content_item.png"
    assert webp_path == expected_base / "content_item.webp"


# --- Integration tests for URL and path consistency ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_url_path_consistency(path_manager: PathManager):
    """Tests that URLs and file paths maintain consistent 1:1 mapping."""
    feed_id = "consistency_test"
    download_id = "test_video"
    ext = "mp4"

    # Get both URL and path
    file_url = path_manager.media_file_url(feed_id, download_id, ext)
    file_path = await path_manager.media_file_path(feed_id, download_id, ext)

    # URL should encode the same information as the path
    assert feed_id in file_url
    assert download_id in file_url
    assert ext in file_url
    assert feed_id in str(file_path)
    assert download_id in str(file_path)
    assert ext in str(file_path)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_special_characters_in_identifiers(path_manager: PathManager):
    """Tests handling of special characters in feed_id and download_id."""
    feed_id = "feed-with_underscores"
    download_id = "video.with.dots"
    ext = "mp4"

    # Test media files with special characters
    file_path = await path_manager.media_file_path(feed_id, download_id, ext)
    file_url = path_manager.media_file_url(feed_id, download_id, ext)

    assert file_path.parent.name == feed_id
    assert file_path.name == f"{download_id}.{ext}"
    assert feed_id in file_url
    assert download_id in file_url

    # Test image files with special characters
    image_path = await path_manager.image_path(feed_id, download_id, "jpg")
    image_url = path_manager.image_url(feed_id, download_id, "jpg")

    assert image_path.parent.parent.name == feed_id  # feed dir
    assert image_path.parent.name == "downloads"  # downloads subdir
    assert image_path.name == f"{download_id}.jpg"
    assert feed_id in image_url
    assert download_id in image_url


# --- Edge cases and error handling ---


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_feed_id",
    [
        "",
        "   ",
        "\t\n",
    ],
)
async def test_invalid_feed_id_raises_error(
    path_manager: PathManager, invalid_feed_id: str
):
    """Tests that an empty or whitespace-only feed_id raises ValueError."""
    download_id = "video_123"
    ext = "mp4"

    with pytest.raises(ValueError):
        await path_manager.feed_data_dir(invalid_feed_id)

    with pytest.raises(ValueError):
        await path_manager.feed_tmp_dir(invalid_feed_id)

    with pytest.raises(ValueError):
        await path_manager.feed_images_dir(invalid_feed_id)

    with pytest.raises(ValueError):
        path_manager.feed_url(invalid_feed_id)

    with pytest.raises(ValueError):
        path_manager.feed_media_url(invalid_feed_id)

    with pytest.raises(ValueError):
        await path_manager.media_file_path(invalid_feed_id, download_id, ext)

    with pytest.raises(ValueError):
        path_manager.media_file_url(invalid_feed_id, download_id, ext)

    with pytest.raises(ValueError):
        path_manager.image_url(invalid_feed_id, download_id, ext)

    with pytest.raises(ValueError):
        await path_manager.image_path(invalid_feed_id, download_id, ext)

    with pytest.raises(ValueError):
        await path_manager.download_images_dir(invalid_feed_id)


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_download_id",
    [
        "",
        "   ",
        "\t\n",
    ],
)
async def test_invalid_download_id_raises_error(
    path_manager: PathManager, invalid_download_id: str
):
    """Tests that an empty or whitespace-only download_id raises ValueError."""
    feed_id = "valid_feed"
    ext = "mp4"

    with pytest.raises(ValueError):
        await path_manager.media_file_path(feed_id, invalid_download_id, ext)

    with pytest.raises(ValueError):
        path_manager.media_file_url(feed_id, invalid_download_id, ext)

    # Test image methods with empty download_id
    with pytest.raises(ValueError):
        path_manager.image_url(feed_id, invalid_download_id, ext)

    with pytest.raises(ValueError):
        await path_manager.image_path(feed_id, invalid_download_id, ext)


@pytest.mark.unit
def test_url_base_without_trailing_slash():
    """Tests that base_url normalization removes trailing slashes."""
    data_dir = Path("/tmp/data")

    # Test various trailing slash scenarios
    pm1 = PathManager(data_dir, "http://example.com/")
    pm2 = PathManager(data_dir, "http://example.com")

    assert pm1.base_url == "http://example.com"
    assert pm2.base_url == "http://example.com"
    assert pm1.feed_url("test") == pm2.feed_url("test")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_multiple_directory_levels(path_manager: PathManager):
    """Tests that deep directory structures work correctly."""
    # This tests that mkdir(parents=True) works as expected
    feed_id = "deeply/nested/feed/structure"

    # Should create all necessary parent directories
    feed_dir = await path_manager.feed_data_dir(feed_id)
    tmp_dir = await path_manager.feed_tmp_dir(feed_id)

    assert feed_dir.exists()
    assert tmp_dir.exists()
    # The full path should include all the directory levels
    assert "deeply" in str(feed_dir)
    assert "nested" in str(feed_dir)
    assert "structure" in str(feed_dir)
