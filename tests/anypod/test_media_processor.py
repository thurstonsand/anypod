# pyright: reportPrivateUsage=false

"""Binary-free unit tests for MediaProcessor.

Real ffmpeg/ffprobe conversion tests live in
``tests/integration/test_integration_media_processor.py`` so the default unit
suite stays fast and requires no system ffmpeg binary in CI.
"""

from pathlib import Path
import stat
from unittest.mock import AsyncMock, MagicMock

import pytest

from anypod.config.types import MediaConversionPreset
from anypod.exceptions import MediaProcessingError
from anypod.ffmpeg import FFmpeg
from anypod.ffprobe import FFProbe, MediaStreamInfo
from anypod.media_processor import (
    MediaProcessor,
    _build_ffmpeg_args,
    default_format_for_preset,
)
from anypod.path_manager import PathManager


@pytest.fixture
def path_manager(tmp_path: Path) -> PathManager:
    """Provide a PathManager backed by a temporary directory."""
    return PathManager(tmp_path / "data", "https://podcasts.example.com")


def test_default_format_for_preset_modes() -> None:
    """Built-in presets emit distinct format selectors; off/custom defer to yt-dlp."""
    assert default_format_for_preset(MediaConversionPreset.OFF) is None
    assert default_format_for_preset(MediaConversionPreset.CUSTOM) is None

    light_fmt = default_format_for_preset(MediaConversionPreset.LIGHT)
    assert light_fmt is not None
    assert "avc1" in light_fmt

    opt_fmt = default_format_for_preset(MediaConversionPreset.OPTIMIZED)
    assert opt_fmt is not None
    assert opt_fmt.index("av01") < opt_fmt.index("vp9")

    audio_fmt = default_format_for_preset(
        MediaConversionPreset.LIGHT, prefer_audio=True
    )
    assert audio_fmt is not None
    assert "bestaudio" in audio_fmt


def test_build_ffmpeg_args_light_copies_h264_transcodes_opus_and_embeds_thumb(
    tmp_path: Path,
) -> None:
    """LIGHT stream-copies H.264, encodes Opus to 128k AAC, and attaches artwork."""
    args = _build_ffmpeg_args(
        input_path=tmp_path / "ep.mkv",
        output_path=tmp_path / "ep.mp4",
        streams=MediaStreamInfo(
            video_codec="h264",
            video_codec_tag="avc1",
            audio_codec="opus",
            has_attached_pic=False,
        ),
        preset=MediaConversionPreset.LIGHT,
        thumbnail_path=tmp_path / "thumb.jpg",
    )
    assert args is not None
    assert args[args.index("-c:v:0") : args.index("-c:v:0") + 2] == ["-c:v:0", "copy"]
    assert args[args.index("-c:a") : args.index("-c:a") + 4] == [
        "-c:a",
        "aac",
        "-b:a",
        "128k",
    ]
    assert "-disposition:v:1" in args
    assert "attached_pic" in args
    assert "+faststart" in args


def test_build_ffmpeg_args_optimized_encodes_hevc_hvc1_and_128k_aac(
    tmp_path: Path,
) -> None:
    """OPTIMIZED re-encodes H.264 to libx265 (hvc1) and Opus to 128k AAC."""
    args = _build_ffmpeg_args(
        input_path=tmp_path / "ep.mp4",
        output_path=tmp_path / "out.mp4",
        streams=MediaStreamInfo(
            video_codec="h264",
            video_codec_tag="avc1",
            audio_codec="opus",
            has_attached_pic=False,
        ),
        preset=MediaConversionPreset.OPTIMIZED,
        thumbnail_path=None,
    )
    assert args is not None
    assert "libx265" in args
    assert args[args.index("-tag:v:0") : args.index("-tag:v:0") + 2] == [
        "-tag:v:0",
        "hvc1",
    ]
    assert args[args.index("-c:a") : args.index("-c:a") + 4] == [
        "-c:a",
        "aac",
        "-b:a",
        "128k",
    ]


def test_build_ffmpeg_args_audio_only_preserves_existing_attached_pic(
    tmp_path: Path,
) -> None:
    """Audio-only conversion preserves pre-existing cover art when no new thumb is attached."""
    args = _build_ffmpeg_args(
        input_path=tmp_path / "ep.mp3",
        output_path=tmp_path / "ep.m4a",
        streams=MediaStreamInfo(
            video_codec=None,
            video_codec_tag=None,
            audio_codec="mp3",
            has_attached_pic=True,
        ),
        preset=MediaConversionPreset.LIGHT,
        thumbnail_path=None,
    )
    assert args is not None
    assert "0:v:0?" in args
    assert args[args.index("-c:v:0") : args.index("-c:v:0") + 4] == [
        "-c:v:0",
        "copy",
        "-disposition:v:0",
        "attached_pic",
    ]


def test_build_ffmpeg_args_noop_when_already_compliant(tmp_path: Path) -> None:
    """Returns None when container, codecs, tag, and artwork already match preset."""
    args = _build_ffmpeg_args(
        input_path=tmp_path / "ep.mp4",
        output_path=tmp_path / "out.mp4",
        streams=MediaStreamInfo(
            video_codec="hevc",
            video_codec_tag="hvc1",
            audio_codec="aac",
            has_attached_pic=True,
        ),
        preset=MediaConversionPreset.OPTIMIZED,
        thumbnail_path=tmp_path / "thumb.jpg",
    )
    assert args is None


@pytest.mark.asyncio
async def test_off_preset_leaves_media_untouched(
    path_manager: PathManager, tmp_path: Path
) -> None:
    """OFF preset without thumbnail embedding leaves downloaded media untouched."""
    processor = MediaProcessor(
        ffmpeg=MagicMock(spec=FFmpeg),
        ffprobe=MagicMock(spec=FFProbe),
        paths=path_manager,
        custom_script_path=tmp_path / "post_process.sh",
    )
    feed_dir = await path_manager.feed_data_dir("show")
    src = feed_dir / "ep1.mkv"
    src.write_bytes(b"raw-media")

    result = await processor.process(
        feed_id="show",
        download_id="ep1",
        source_url="https://example.com/ep1",
        media_path=src,
        preset=MediaConversionPreset.OFF,
        embed_thumbnail=False,
    )

    assert result.converted is False
    assert result.file_path == src
    assert src.read_bytes() == b"raw-media"


@pytest.mark.asyncio
async def test_custom_hook_renames_extension_and_passes_env_vars(
    path_manager: PathManager, tmp_path: Path
) -> None:
    """CUSTOM preset executes hook in isolated dir, passes context env, and adopts new ext."""
    hook = tmp_path / "post_process.sh"
    env_dump = tmp_path / "hook_env.txt"
    hook.write_text(
        f"""#!/bin/bash -e
INPUT="$1"
echo "$ANYPOD_FEED_ID|$ANYPOD_DOWNLOAD_ID|$ANYPOD_SOURCE_URL" > "{env_dump}"
OUT="${{INPUT%.*}}.mp4"
cp "$INPUT" "$OUT"
rm -f "$INPUT"
echo "converted to mp4"
""",
        encoding="utf-8",
    )
    hook.chmod(hook.stat().st_mode | stat.S_IXUSR)

    custom_proc = MediaProcessor(
        ffmpeg=MagicMock(spec=FFmpeg),
        ffprobe=MagicMock(spec=FFProbe),
        paths=path_manager,
        custom_script_path=hook,
    )

    feed_dir = await path_manager.feed_data_dir("custom_feed")
    src = feed_dir / "vid123.webm"
    src.write_bytes(b"fake-media-bytes")

    result = await custom_proc.process(
        feed_id="custom_feed",
        download_id="vid123",
        source_url="https://example.com/watch?v=vid123",
        media_path=src,
        preset=MediaConversionPreset.CUSTOM,
        embed_thumbnail=False,
    )

    assert result.converted is True
    assert result.file_path == feed_dir / "vid123.mp4"
    assert result.file_path.read_bytes() == b"fake-media-bytes"
    assert not src.exists()
    assert result.logs is not None and "converted to mp4" in result.logs
    assert env_dump.read_text(encoding="utf-8").strip() == (
        "custom_feed|vid123|https://example.com/watch?v=vid123"
    )


@pytest.mark.asyncio
async def test_custom_hook_nonzero_exit_raises_media_processing_error(
    path_manager: PathManager, tmp_path: Path
) -> None:
    """Non-zero custom hook exit raises MediaProcessingError with stderr logs."""
    hook = tmp_path / "fail_hook.sh"
    hook.write_text("#!/bin/bash\necho 'hook blew up' >&2\nexit 2\n", encoding="utf-8")
    hook.chmod(hook.stat().st_mode | stat.S_IXUSR)

    custom_proc = MediaProcessor(
        ffmpeg=MagicMock(spec=FFmpeg),
        ffprobe=MagicMock(spec=FFProbe),
        paths=path_manager,
        custom_script_path=hook,
    )

    feed_dir = await path_manager.feed_data_dir("custom_feed")
    src = feed_dir / "bad1.mp4"
    src.write_bytes(b"data")

    with pytest.raises(MediaProcessingError) as exc_info:
        await custom_proc.process(
            feed_id="custom_feed",
            download_id="bad1",
            source_url="https://example.com/bad1",
            media_path=src,
            preset=MediaConversionPreset.CUSTOM,
            embed_thumbnail=False,
        )

    assert exc_info.value.logs is not None
    assert "hook blew up" in exc_info.value.logs


@pytest.mark.asyncio
async def test_custom_hook_invokes_thumbnail_embed_when_enabled(
    path_manager: PathManager, tmp_path: Path
) -> None:
    """CUSTOM preset embeds artwork after hook finishes when embed_thumbnail=True."""
    hook = tmp_path / "noop_hook.sh"
    hook.write_text("#!/bin/bash -e\nexit 0\n", encoding="utf-8")
    hook.chmod(hook.stat().st_mode | stat.S_IXUSR)

    mock_ffprobe = MagicMock(spec=FFProbe)
    mock_ffprobe.probe_media_streams = AsyncMock(
        return_value=MediaStreamInfo(
            video_codec="h264",
            video_codec_tag="avc1",
            audio_codec="aac",
            has_attached_pic=False,
        )
    )
    mock_ffmpeg = MagicMock(spec=FFmpeg)

    async def fake_run_conversion(args: list[str]) -> str:
        Path(args[-1]).write_bytes(b"embedded-mp4")
        return ""

    mock_ffmpeg.run_conversion = AsyncMock(side_effect=fake_run_conversion)

    custom_proc = MediaProcessor(
        ffmpeg=mock_ffmpeg,
        ffprobe=mock_ffprobe,
        paths=path_manager,
        custom_script_path=hook,
    )

    feed_dir = await path_manager.feed_data_dir("custom_thumb")
    src = feed_dir / "ep1.mp4"
    src.write_bytes(b"original-mp4")
    thumb = tmp_path / "thumb.jpg"
    thumb.write_bytes(b"fake-jpg")

    result = await custom_proc.process(
        feed_id="custom_thumb",
        download_id="ep1",
        source_url="https://example.com/ep1",
        media_path=src,
        preset=MediaConversionPreset.CUSTOM,
        embed_thumbnail=True,
        thumbnail_path=thumb,
    )

    assert result.converted is True
    assert result.file_path.read_bytes() == b"embedded-mp4"
    mock_ffmpeg.run_conversion.assert_awaited_once()
