# pyright: reportPrivateUsage=false

"""Integration tests for ImageDownloader.

Tests that use real ffprobe/ffmpeg commands and actual image files.
"""

from pathlib import Path

import httpx
import pytest
import respx

from anypod.image_downloader import ImageDownloader
from anypod.path_manager import PathManager

# --- Integration Tests ---


@pytest.mark.integration
@pytest.mark.asyncio
async def test_is_jpg_format_detects_jpg(
    image_downloader: ImageDownloader, test_images: dict[str, Path]
) -> None:
    """Format detection correctly identifies JPG files using real ffprobe."""
    result = await image_downloader._is_jpg_format(
        test_images["jpg"], "test_feed", "http://example.com/test.jpg"
    )
    assert result is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_is_jpg_format_detects_non_jpg(
    image_downloader: ImageDownloader, test_images: dict[str, Path]
) -> None:
    """Format detection correctly identifies non-JPG files using real ffprobe."""
    result = await image_downloader._is_jpg_format(
        test_images["png"], "test_feed", "http://example.com/test.png"
    )
    assert result is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_convert_to_jpg_success(
    image_downloader: ImageDownloader, test_images: dict[str, Path], tmp_path: Path
) -> None:
    """Image conversion succeeds with real ffmpeg."""
    output_path = tmp_path / "converted.jpg"

    await image_downloader._convert_to_jpg(
        test_images["png"], output_path, "test_feed", "http://example.com/test.png"
    )

    # Verify output file was created and is a valid JPG
    assert output_path.exists()
    assert output_path.stat().st_size > 0

    # Verify it's actually a JPG by checking with our format detection
    result = await image_downloader._is_jpg_format(
        output_path, "test_feed", "http://example.com/test.png"
    )
    assert result is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_feed_image_direct_jpg_no_conversion(
    respx_mock: respx.Router,
    image_downloader: ImageDownloader,
    path_manager: PathManager,
    test_images: dict[str, Path],
) -> None:
    """Direct download of JPG file skips conversion and moves file using real ffprobe."""
    feed_id = "feed123"
    url = "https://example.com/image.jpg"
    jpg_content = test_images["jpg"].read_bytes()

    respx_mock.get(url).mock(return_value=httpx.Response(200, content=jpg_content))

    result = await image_downloader.download_feed_image_direct(feed_id, url)

    assert result == "jpg"

    # Verify file was saved correctly
    expected_path = await path_manager.image_path(feed_id, None, "jpg")
    assert expected_path.is_file()
    assert expected_path.read_bytes() == jpg_content


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_feed_image_direct_png_with_conversion(
    respx_mock: respx.Router,
    image_downloader: ImageDownloader,
    path_manager: PathManager,
    test_images: dict[str, Path],
) -> None:
    """Direct download of PNG file triggers conversion to JPG using real ffmpeg."""
    feed_id = "feed123"
    url = "https://example.com/image.png"
    png_content = test_images["png"].read_bytes()

    respx_mock.get(url).mock(return_value=httpx.Response(200, content=png_content))

    result = await image_downloader.download_feed_image_direct(feed_id, url)

    assert result == "jpg"

    # Verify file was converted and saved as JPG
    expected_path = await path_manager.image_path(feed_id, None, "jpg")
    assert expected_path.is_file()
    assert expected_path.stat().st_size > 0

    # Verify the final file is actually in JPG format
    is_jpg = await image_downloader._is_jpg_format(expected_path, feed_id, url)
    assert is_jpg is True
