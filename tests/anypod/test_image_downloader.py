# pyright: reportPrivateUsage=false

"""Unit tests for ImageDownloader.

Covers direct HTTP image downloads and yt-dlp-backed feed thumbnail downloads.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import respx

from anypod.db.types import SourceType
from anypod.exceptions import ImageDownloadError, YtdlpApiError
from anypod.image_downloader import ImageDownloader
from anypod.path_manager import PathManager
from anypod.ytdlp_wrapper import YtdlpWrapper

# --- Fixtures ---


@pytest.fixture
def path_manager(tmp_path_factory: pytest.TempPathFactory) -> PathManager:
    """Provide a PathManager rooted at a temporary directory."""
    base_dir = tmp_path_factory.mktemp("app_data")
    return PathManager(base_dir, "http://localhost")


@pytest.fixture
def ytdlp_wrapper_mock() -> MagicMock:
    """Provide a MagicMock for YtdlpWrapper with async methods."""
    wrapper = MagicMock(spec=YtdlpWrapper)
    wrapper.download_feed_thumbnail = AsyncMock()
    return wrapper


@pytest.fixture
def image_downloader(
    path_manager: PathManager, ytdlp_wrapper_mock: MagicMock
) -> ImageDownloader:
    """Provide an ImageDownloader instance using temp paths and mocked yt-dlp wrapper."""
    return ImageDownloader(path_manager, ytdlp_wrapper_mock)


# --- Tests: download_feed_image_direct ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_feed_image_direct_invalid_feed_id(
    image_downloader: ImageDownloader,
) -> None:
    """Invalid feed_id mapping failure is wrapped as ImageDownloadError."""
    with pytest.raises(ImageDownloadError) as exc:
        await image_downloader.download_feed_image_direct("   ", "https://x")
    assert exc.value.feed_id == "   "
    assert exc.value.url == "https://x"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_feed_image_direct_http_error(
    respx_mock: respx.Router,
    image_downloader: ImageDownloader,
) -> None:
    """HTTP client errors are wrapped as ImageDownloadError."""
    url = "https://bad.example/img.jpg"
    req = httpx.Request("GET", url)
    respx_mock.get(url).mock(
        side_effect=httpx.ConnectError("network-fail", request=req)
    )

    with pytest.raises(ImageDownloadError) as exc:
        await image_downloader.download_feed_image_direct("feed", url)
    assert exc.value.feed_id == "feed"
    assert exc.value.url == url


# --- Tests: download_feed_image_ytdlp ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_feed_image_ytdlp_success(
    image_downloader: ImageDownloader,
    ytdlp_wrapper_mock: MagicMock,
) -> None:
    """Delegates to wrapper and returns its result."""
    ytdlp_wrapper_mock.download_feed_thumbnail.return_value = "jpg"

    feed_id = "feed"
    src_type = SourceType.CHANNEL
    src_url = "https://www.youtube.com/@channel/videos"
    resolved_url = "https://www.youtube.com/@channel/videos"
    yt_args: list[str] = ["--format", "worst"]
    cookies = Path("/tmp/cookies.txt")

    res = await image_downloader.download_feed_image_ytdlp(
        feed_id=feed_id,
        source_type=src_type,
        source_url=src_url,
        resolved_url=resolved_url,
        user_yt_cli_args=yt_args,
        cookies_path=cookies,
    )

    assert res == "jpg"
    ytdlp_wrapper_mock.download_feed_thumbnail.assert_awaited_once_with(
        feed_id=feed_id,
        source_type=src_type,
        source_url=src_url,
        resolved_url=resolved_url,
        user_yt_cli_args=yt_args,
        cookies_path=cookies,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_feed_image_ytdlp_returns_none(
    image_downloader: ImageDownloader,
    ytdlp_wrapper_mock: MagicMock,
) -> None:
    """Propagates None returned by underlying wrapper."""
    ytdlp_wrapper_mock.download_feed_thumbnail.return_value = None

    res = await image_downloader.download_feed_image_ytdlp(
        feed_id="feed",
        source_type=SourceType.PLAYLIST,
        source_url="https://yt/playlist",
        resolved_url=None,
        user_yt_cli_args=[],
    )
    assert res is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_feed_image_ytdlp_wraps_error(
    image_downloader: ImageDownloader,
    ytdlp_wrapper_mock: MagicMock,
) -> None:
    """YtdlpApiError is wrapped into ImageDownloadError with context."""
    ytdlp_wrapper_mock.download_feed_thumbnail.side_effect = YtdlpApiError(
        "fail", feed_id="feed", url="https://yt"
    )

    with pytest.raises(ImageDownloadError) as exc:
        await image_downloader.download_feed_image_ytdlp(
            feed_id="feed",
            source_type=SourceType.SINGLE_VIDEO,
            source_url="https://yt/watch?v=abc",
            resolved_url=None,
            user_yt_cli_args=[],
        )
    assert exc.value.feed_id == "feed"
    assert exc.value.url == "https://yt/watch?v=abc"


# --- Tests: format detection and conversion ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_is_jpg_format_ffprobe_error(
    image_downloader: ImageDownloader,
    tmp_path: Path,
) -> None:
    """Format detection raises ImageDownloadError when ffprobe fails."""
    test_file = tmp_path / "test.jpg"
    test_file.write_bytes(b"fake image data")

    mock_process = AsyncMock()
    mock_process.returncode = 1
    mock_process.communicate.return_value = (b"", b"ffprobe error")

    with patch("asyncio.create_subprocess_exec", return_value=mock_process):
        with pytest.raises(ImageDownloadError) as exc:
            await image_downloader._is_jpg_format(
                test_file, "test_feed", "http://example.com/test.jpg"
            )

        assert exc.value.feed_id == "test_feed"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_convert_to_jpg_ffmpeg_not_found(
    image_downloader: ImageDownloader, tmp_path: Path
) -> None:
    """Conversion raises ImageDownloadError when ffmpeg not found."""
    input_path = tmp_path / "input.png"
    output_path = tmp_path / "converted.jpg"
    input_path.write_bytes(b"fake png data")

    with patch("asyncio.create_subprocess_exec") as mock_subprocess:
        mock_subprocess.side_effect = FileNotFoundError("ffmpeg not found")

        with pytest.raises(ImageDownloadError) as exc:
            await image_downloader._convert_to_jpg(
                input_path,
                output_path,
                "test_feed",
                "http://example.com/test.png",
            )

        assert "ffmpeg not found" in str(exc.value)
        assert exc.value.feed_id == "test_feed"
        assert exc.value.url == "http://example.com/test.png"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_convert_to_jpg_ffmpeg_error(
    image_downloader: ImageDownloader, tmp_path: Path
) -> None:
    """Conversion raises ImageDownloadError when ffmpeg fails."""
    input_path = tmp_path / "input.png"
    output_path = tmp_path / "converted.jpg"
    input_path.write_bytes(b"fake png data")

    mock_process = AsyncMock()
    mock_process.returncode = 1
    mock_process.communicate.return_value = (b"", b"conversion failed")

    with patch("asyncio.create_subprocess_exec", return_value=mock_process):
        with pytest.raises(ImageDownloadError) as exc:
            await image_downloader._convert_to_jpg(
                input_path,
                output_path,
                "test_feed",
                "http://example.com/test.png",
            )

        assert "Image conversion to JPG failed" in str(exc.value)
        assert exc.value.feed_id == "test_feed"
