# pyright: reportPrivateUsage=false

"""Integration tests for FFProbe and FFmpeg utilities.

Covers real ffprobe format detection and ffmpeg image conversion, plus
duration detection from direct media URLs for individual YouTube videos.
"""

import json
from pathlib import Path
import subprocess
from typing import Any

import pytest

from anypod.exceptions import FFmpegError
from anypod.ffmpeg import FFmpeg
from anypod.ffprobe import FFProbe
from anypod.ytdlp_wrapper.core import YtdlpArgs, YtdlpCore


def _create_synthetic_mp4(path: Path) -> None:
    """Create a tiny valid H.264 + AAC MP4 file for real ffmpeg remux testing."""
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "color=c=black:s=160x120:d=1",
            "-f",
            "lavfi",
            "-i",
            "anullsrc=r=44100:cl=mono",
            "-t",
            "1",
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
            str(path),
        ],
        check=True,
        capture_output=True,
    )


def _probe_streams(path: Path) -> list[dict[str, object]]:
    """Return ffprobe stream dicts for a media file."""
    output = subprocess.check_output(
        [
            "ffprobe",
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_streams",
            str(path),
        ]
    )
    data = json.loads(output)
    return data["streams"]


BIG_BUCK_BUNNY_SHORT_URL = "https://youtu.be/aqz-KE-bpKQ"
BIG_BUCK_BUNNY_EXPECTED_DURATION_SECONDS = 635  # Retrieved via yt-dlp --print duration


@pytest.mark.asyncio
async def test_is_jpg_file_detects_jpg(
    ffprobe: FFProbe, test_images: dict[str, Path]
) -> None:
    """Format detection correctly identifies JPG files using real ffprobe."""
    result = await ffprobe.is_jpg_file(test_images["jpg"])
    assert result is True


@pytest.mark.asyncio
async def test_is_jpg_file_detects_non_jpg(
    ffprobe: FFProbe, test_images: dict[str, Path]
) -> None:
    """Format detection correctly identifies non-JPG files using real ffprobe."""
    result = await ffprobe.is_jpg_file(test_images["png"])
    assert result is False


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


@pytest.mark.asyncio
async def test_ffmpeg_embed_subtitles_muxes_mov_text_and_replaces_existing_track(
    ffmpeg: FFmpeg, tmp_path: Path
) -> None:
    """embed_subtitles adds a mov_text track with ISO 639-2/T lang and replaces on re-run."""
    mp4_path = tmp_path / "episode.mp4"
    en_vtt = tmp_path / "episode.en.vtt"
    de_vtt = tmp_path / "episode.de.vtt"

    _create_synthetic_mp4(mp4_path)
    en_vtt.write_text(
        "WEBVTT\n\n00:00:00.000 --> 00:00:00.900\nHello world\n",
        encoding="utf-8",
    )
    de_vtt.write_text(
        "WEBVTT\n\n00:00:00.000 --> 00:00:00.900\nHallo Welt\n",
        encoding="utf-8",
    )

    await ffmpeg.embed_subtitles(mp4_path, en_vtt, "en")

    streams = _probe_streams(mp4_path)
    assert [s["codec_type"] for s in streams] == ["video", "audio", "subtitle"]
    assert streams[2]["codec_name"] == "mov_text"
    tags = streams[2]["tags"]
    assert isinstance(tags, dict)
    assert tags["language"] == "eng"
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "episode.de.vtt",
        "episode.en.vtt",
        "episode.mp4",
    ]

    # Re-embedding replaces the existing subtitle stream instead of stacking a second one
    await ffmpeg.embed_subtitles(mp4_path, de_vtt, "de")
    streams_after = _probe_streams(mp4_path)
    assert [s["codec_type"] for s in streams_after] == ["video", "audio", "subtitle"]
    tags_after = streams_after[2]["tags"]
    assert isinstance(tags_after, dict)
    assert tags_after["language"] == "deu"


@pytest.mark.asyncio
async def test_ffmpeg_embed_subtitles_failure_preserves_original(
    ffmpeg: FFmpeg, tmp_path: Path
) -> None:
    """embed_subtitles raises FFmpegError and keeps original MP4 untouched on invalid input."""
    mp4_path = tmp_path / "episode.mp4"
    bad_sub = tmp_path / "episode.bin"

    _create_synthetic_mp4(mp4_path)
    original_bytes = mp4_path.read_bytes()
    bad_sub.write_bytes(b"\x00\x01\x02\x03not-subtitles")

    with pytest.raises(FFmpegError):
        await ffmpeg.embed_subtitles(mp4_path, bad_sub, "en")

    assert mp4_path.read_bytes() == original_bytes
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "episode.bin",
        "episode.mp4",
    ]


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


@pytest.mark.asyncio
async def test_ffprobe_get_duration_from_url_basic(ffprobe: FFProbe) -> None:
    """FFProbe can obtain duration (in seconds) from a direct media URL."""
    direct_url = await _get_direct_media_url_for_video(BIG_BUCK_BUNNY_SHORT_URL)
    duration = await ffprobe.get_duration_seconds_from_url(direct_url)
    # ffprobe can report fractional seconds. Allow 1s tolerance.
    assert abs(duration - BIG_BUCK_BUNNY_EXPECTED_DURATION_SECONDS) <= 1
