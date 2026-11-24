# pyright: reportPrivateUsage=false

"""Unit tests for FFmpeg helper."""

import json
from pathlib import Path
import shutil
import subprocess
from unittest.mock import AsyncMock, patch

import pytest

from anypod.exceptions import FFmpegError
from anypod.ffmpeg import FFmpeg, _to_iso639_2

REQUIRES_FFMPEG = pytest.mark.skipif(
    shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None,
    reason="ffmpeg and ffprobe executables are required",
)


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


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("en", "eng"),
        ("EN", "eng"),
        ("de", "deu"),
        ("fr", "fra"),
        ("zh", "zho"),
        ("eng", "eng"),
        ("", None),
        ("zz", None),
    ],
)
def test_to_iso639_2(code: str, expected: str | None) -> None:
    """Converts ISO 639-1 and ISO 639-2/T codes and returns None for unknown values."""
    assert _to_iso639_2(code) == expected


@pytest.mark.asyncio
@patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
async def test_ffmpeg_convert_image_to_jpg_success(
    mock_cse: AsyncMock, tmp_path: Path
) -> None:
    """convert_image_to_jpg succeeds when ffmpeg returns 0."""
    ffm = FFmpeg()
    mock_proc = AsyncMock()
    mock_proc.returncode = 0
    mock_proc.communicate.return_value = (b"", b"")
    mock_cse.return_value = mock_proc

    await ffm.convert_image_to_jpg(tmp_path / "in.png", tmp_path / "out.jpg")


@pytest.mark.asyncio
@patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
async def test_ffmpeg_convert_image_to_jpg_failure(
    mock_cse: AsyncMock, tmp_path: Path
) -> None:
    """convert_image_to_jpg raises FFmpegError when ffmpeg fails."""
    ffm = FFmpeg()
    mock_proc = AsyncMock()
    mock_proc.returncode = 1
    mock_proc.communicate.return_value = (b"", b"err")
    mock_cse.return_value = mock_proc

    with pytest.raises(FFmpegError):
        await ffm.convert_image_to_jpg(tmp_path / "in.png", tmp_path / "out.jpg")


@REQUIRES_FFMPEG
@pytest.mark.asyncio
async def test_embed_subtitles_muxes_mov_text_and_replaces_existing_track(
    tmp_path: Path,
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

    ffm = FFmpeg()
    await ffm.embed_subtitles(mp4_path, en_vtt, "en")

    streams = _probe_streams(mp4_path)
    codec_types = [s["codec_type"] for s in streams]
    assert codec_types == ["video", "audio", "subtitle"]
    sub_stream = streams[2]
    assert sub_stream["codec_name"] == "mov_text"
    tags = sub_stream["tags"]
    assert isinstance(tags, dict)
    assert tags["language"] == "eng"
    # No leftover temp files in directory
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "episode.de.vtt",
        "episode.en.vtt",
        "episode.mp4",
    ]

    # Re-embedding replaces the existing subtitle stream instead of stacking a second one
    await ffm.embed_subtitles(mp4_path, de_vtt, "de")
    streams_after = _probe_streams(mp4_path)
    assert [s["codec_type"] for s in streams_after] == ["video", "audio", "subtitle"]
    tags_after = streams_after[2]["tags"]
    assert isinstance(tags_after, dict)
    assert tags_after["language"] == "deu"


@REQUIRES_FFMPEG
@pytest.mark.asyncio
async def test_embed_subtitles_failure_preserves_original(
    tmp_path: Path,
) -> None:
    """embed_subtitles raises FFmpegError and keeps original MP4 untouched on invalid input."""
    mp4_path = tmp_path / "episode.mp4"
    bad_sub = tmp_path / "episode.bin"

    _create_synthetic_mp4(mp4_path)
    original_bytes = mp4_path.read_bytes()
    bad_sub.write_bytes(b"\x00\x01\x02\x03not-subtitles")

    ffm = FFmpeg()
    with pytest.raises(FFmpegError):
        await ffm.embed_subtitles(mp4_path, bad_sub, "en")

    assert mp4_path.read_bytes() == original_bytes
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "episode.bin",
        "episode.mp4",
    ]


@pytest.mark.asyncio
@patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
async def test_embed_subtitles_cleans_up_partial_temp_file_on_failure(
    mock_cse: AsyncMock,
    tmp_path: Path,
) -> None:
    """embed_subtitles removes any partial temp file if ffmpeg fails mid-remux."""
    mp4_path = tmp_path / "episode.mp4"
    vtt_path = tmp_path / "episode.en.vtt"
    mp4_path.write_bytes(b"original-mp4")
    vtt_path.write_text("WEBVTT\n\n", encoding="utf-8")

    async def write_partial_and_fail(*args: object, **_kwargs: object) -> AsyncMock:
        output_arg = Path(str(args[-1]))
        output_arg.write_bytes(b"partial-output")
        proc = AsyncMock()
        proc.returncode = 1
        proc.communicate.return_value = (b"", b"mux error")
        return proc

    mock_cse.side_effect = write_partial_and_fail

    ffm = FFmpeg()
    with pytest.raises(FFmpegError):
        await ffm.embed_subtitles(mp4_path, vtt_path, "en")

    assert mp4_path.read_bytes() == b"original-mp4"
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "episode.en.vtt",
        "episode.mp4",
    ]
