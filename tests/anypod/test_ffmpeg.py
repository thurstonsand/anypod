# pyright: reportPrivateUsage=false

"""Unit tests for FFmpeg helper."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from anypod.exceptions import FFmpegError
from anypod.ffmpeg import FFmpeg, _to_iso639_2


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


@pytest.mark.asyncio
@patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
async def test_embed_subtitles_replaces_media_with_remuxed_output(
    mock_cse: AsyncMock,
    tmp_path: Path,
) -> None:
    """embed_subtitles invokes ffmpeg with mov_text and ISO 639-2/T lang and swaps atomically."""
    mp4_path = tmp_path / "episode.mp4"
    vtt_path = tmp_path / "episode.en.vtt"
    mp4_path.write_bytes(b"original-mp4")
    vtt_path.write_text("WEBVTT\n\n", encoding="utf-8")

    async def write_remuxed_and_succeed(*args: object, **_kwargs: object) -> AsyncMock:
        output_arg = Path(str(args[-1]))
        output_arg.write_bytes(b"remuxed-mp4-with-subs")
        proc = AsyncMock()
        proc.returncode = 0
        proc.communicate.return_value = (b"", b"")
        return proc

    mock_cse.side_effect = write_remuxed_and_succeed

    ffm = FFmpeg()
    await ffm.embed_subtitles(mp4_path, vtt_path, "de")

    assert mp4_path.read_bytes() == b"remuxed-mp4-with-subs"
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "episode.en.vtt",
        "episode.mp4",
    ]
    argv = [str(arg) for arg in mock_cse.call_args.args]
    assert argv[0] == "ffmpeg"
    assert "-c:s" in argv and argv[argv.index("-c:s") + 1] == "mov_text"
    assert (
        "-metadata:s:s:0" in argv
        and argv[argv.index("-metadata:s:s:0") + 1] == "language=deu"
    )
    assert "-movflags" in argv and argv[argv.index("-movflags") + 1] == "+faststart"


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
