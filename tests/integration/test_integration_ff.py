# pyright: reportPrivateUsage=false

"""Integration tests for FFProbe and FFmpeg utilities.

Covers real ffprobe format detection and ffmpeg image conversion, plus
duration detection from direct media URLs for individual YouTube videos.
"""

from pathlib import Path
from typing import Any

import pytest

from anypod.ffmpeg import FFmpeg
from anypod.ffprobe import FFProbe
from anypod.ytdlp_wrapper.core import YtdlpArgs, YtdlpCore

BIG_BUCK_BUNNY_SHORT_URL = "https://youtu.be/aqz-KE-bpKQ"
BIG_BUCK_BUNNY_EXPECTED_DURATION_SECONDS = 635  # Retrieved via yt-dlp --print duration


@pytest.mark.integration
@pytest.mark.asyncio
async def test_is_jpg_file_detects_jpg(
    ffprobe: FFProbe, test_images: dict[str, Path]
) -> None:
    """Format detection correctly identifies JPG files using real ffprobe."""
    result = await ffprobe.is_jpg_file(test_images["jpg"])
    assert result is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_is_jpg_file_detects_non_jpg(
    ffprobe: FFProbe, test_images: dict[str, Path]
) -> None:
    """Format detection correctly identifies non-JPG files using real ffprobe."""
    result = await ffprobe.is_jpg_file(test_images["png"])
    assert result is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ffmpeg_convert_image_to_jpg(
    ffmpeg: FFmpeg, ffprobe: FFProbe, test_images: dict[str, Path], tmp_path: Path
) -> None:
    """Image conversion succeeds with real ffmpeg and results in a valid JPG file."""
    output_path = tmp_path / "converted.jpg"
    await ffmpeg.convert_image_to_jpg(test_images["png"], output_path)

    assert output_path.exists()
    assert output_path.stat().st_size > 0

    # Verify output is actually JPG
    is_jpg = await ffprobe.is_jpg_file(output_path)
    assert is_jpg is True


@pytest.mark.integration
@pytest.mark.asyncio
async def _get_direct_media_url_for_video(video_url: str) -> str:
    """Return a direct media URL for the given YouTube video using yt-dlp.

    Uses skip-download + dump-json and extracts the first format URL, which is
    sufficient for duration probing purposes.
    """
    args = YtdlpArgs().skip_download().dump_json()
    infos = (await YtdlpCore.extract_downloads_info(args, video_url)).payload
    assert infos, "yt-dlp did not return any entries for the video URL"
    info = infos[0]
    formats = info.get("formats", list[dict[str, Any]])
    assert formats, "no formats found in yt-dlp output"

    candidates: list[str] = []
    for fmt in formats:  # type: ignore
        protocol = fmt.get("protocol")
        ext = fmt.get("ext")
        url = fmt.get("url")
        has_manifest = fmt.get("manifest_url") is not None
        # Prefer progressive HTTPS URLs (avoid HLS/DASH manifests)
        if (
            isinstance(url, str)
            and not has_manifest
            and protocol in ("https", "http")
            and isinstance(ext, str)
            and ext in ("mp4", "m4a", "webm")
        ):
            candidates.append(url)

    # Prefer MP4 if available for better container-level metadata
    mp4_candidates = [u for u in candidates if u.endswith(".mp4")]
    if mp4_candidates:
        return mp4_candidates[0]
    assert candidates, "no direct progressive media URL candidates found"
    return candidates[0]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ffprobe_get_duration_from_url_basic(ffprobe: FFProbe) -> None:
    """FFProbe can obtain duration (in seconds) from a direct media URL."""
    direct_url = await _get_direct_media_url_for_video(BIG_BUCK_BUNNY_SHORT_URL)
    duration = await ffprobe.get_duration_seconds_from_url(direct_url)
    # ffprobe can report fractional seconds. Allow 1s tolerance.
    assert abs(duration - BIG_BUCK_BUNNY_EXPECTED_DURATION_SECONDS) <= 1
