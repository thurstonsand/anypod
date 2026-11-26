# pyright: reportPrivateUsage=false

"""Behavioral tests for MediaProcessor using real ffmpeg/ffprobe on synthetic clips."""

import asyncio
from pathlib import Path
import stat

import pytest

from anypod.config.types import MediaConversionPreset
from anypod.exceptions import MediaProcessingError
from anypod.ffmpeg import FFmpeg
from anypod.ffprobe import FFProbe
from anypod.media_processor import MediaProcessor, default_format_for_preset
from anypod.path_manager import PathManager


async def _run_ffmpeg(*args: str) -> None:
    """Create a tiny synthetic media fixture with real ffmpeg."""
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    assert proc.returncode == 0, f"ffmpeg fixture generation failed: {stderr.decode()}"


async def _create_jpg(path: Path) -> Path:
    """Generate a tiny valid JPEG thumbnail file."""
    await _run_ffmpeg(
        "-f",
        "lavfi",
        "-i",
        "color=c=blue:s=64x64:d=0.1",
        "-frames:v",
        "1",
        str(path),
    )
    return path


@pytest.fixture
def path_manager(tmp_path: Path) -> PathManager:
    """Provide a PathManager backed by a temporary directory."""
    return PathManager(tmp_path / "data", "https://podcasts.example.com")


@pytest.fixture
def processor(path_manager: PathManager) -> MediaProcessor:
    """Provide a MediaProcessor wired to real FFmpeg and FFProbe."""
    return MediaProcessor(
        ffmpeg=FFmpeg(),
        ffprobe=FFProbe(),
        paths=path_manager,
    )


def test_default_format_for_preset_modes():
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


@pytest.mark.asyncio
async def test_off_preset_leaves_media_untouched(
    processor: MediaProcessor, path_manager: PathManager
):
    """OFF preset without thumbnail embedding leaves downloaded media untouched."""
    feed_dir = await path_manager.feed_data_dir("show")
    src = feed_dir / "ep1.mkv"
    await _run_ffmpeg(
        "-f",
        "lavfi",
        "-i",
        "color=c=black:s=64x64:d=0.5",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:duration=0.5",
        "-c:v",
        "libx264",
        "-c:a",
        "libopus",
        str(src),
    )

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
    assert src.exists()


@pytest.mark.asyncio
async def test_light_preset_remuxes_h264_opus_mkv_to_mp4_and_embeds_thumbnail(
    processor: MediaProcessor, path_manager: PathManager, tmp_path: Path
):
    """LIGHT copies H.264 video, transcodes Opus -> AAC, remuxes to MP4, and embeds artwork."""
    feed_dir = await path_manager.feed_data_dir("show")
    src = feed_dir / "ep_light.mkv"
    thumb = await _create_jpg(tmp_path / "thumb.jpg")
    await _run_ffmpeg(
        "-f",
        "lavfi",
        "-i",
        "color=c=red:s=64x64:d=0.5",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:duration=0.5",
        "-c:v",
        "libx264",
        "-c:a",
        "libopus",
        str(src),
    )

    result = await processor.process(
        feed_id="show",
        download_id="ep_light",
        source_url="https://example.com/ep_light",
        media_path=src,
        preset=MediaConversionPreset.LIGHT,
        embed_thumbnail=True,
        thumbnail_path=thumb,
    )

    assert result.converted is True
    assert result.file_path == feed_dir / "ep_light.mp4"
    assert result.file_path.exists()
    assert not src.exists()

    streams = await FFProbe().probe_media_streams(result.file_path)
    assert streams.video_codec == "h264"
    assert streams.audio_codec == "aac"
    assert streams.has_attached_pic is True


@pytest.mark.asyncio
async def test_optimized_preset_transcodes_h264_to_hevc_hvc1_and_embeds_thumbnail(
    processor: MediaProcessor, path_manager: PathManager, tmp_path: Path
):
    """OPTIMIZED transcodes H.264 to H.265 (hvc1), Opus to AAC, and preserves artwork."""
    feed_dir = await path_manager.feed_data_dir("show")
    src = feed_dir / "ep_opt.mp4"
    thumb = await _create_jpg(tmp_path / "thumb_opt.jpg")
    await _run_ffmpeg(
        "-f",
        "lavfi",
        "-i",
        "color=c=green:s=64x64:d=0.5",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:duration=0.5",
        "-c:v",
        "libx264",
        "-c:a",
        "libopus",
        str(src),
    )

    result = await processor.process(
        feed_id="show",
        download_id="ep_opt",
        source_url="https://example.com/ep_opt",
        media_path=src,
        preset=MediaConversionPreset.OPTIMIZED,
        embed_thumbnail=True,
        thumbnail_path=thumb,
    )

    assert result.converted is True
    assert result.file_path == feed_dir / "ep_opt.mp4"

    streams = await FFProbe().probe_media_streams(result.file_path)
    assert streams.video_codec == "hevc"
    assert streams.video_codec_tag == "hvc1"
    assert streams.audio_codec == "aac"
    assert streams.has_attached_pic is True

    # Running OPTIMIZED a second time on the already-optimized file is a no-op
    second = await processor.process(
        feed_id="show",
        download_id="ep_opt",
        source_url="https://example.com/ep_opt",
        media_path=result.file_path,
        preset=MediaConversionPreset.OPTIMIZED,
        embed_thumbnail=True,
        thumbnail_path=thumb,
    )
    assert second.converted is False


@pytest.mark.asyncio
async def test_audio_only_file_converts_to_m4a_with_artwork(
    processor: MediaProcessor, path_manager: PathManager, tmp_path: Path
):
    """Audio-only inputs convert to AAC .m4a (not .mp4) and attach cover art cleanly."""
    feed_dir = await path_manager.feed_data_dir("audio_show")
    src = feed_dir / "ep_audio.webm"
    thumb = await _create_jpg(tmp_path / "thumb_audio.jpg")
    await _run_ffmpeg(
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:duration=0.5",
        "-c:a",
        "libopus",
        str(src),
    )

    result = await processor.process(
        feed_id="audio_show",
        download_id="ep_audio",
        source_url="https://example.com/ep_audio",
        media_path=src,
        preset=MediaConversionPreset.OPTIMIZED,
        embed_thumbnail=True,
        thumbnail_path=thumb,
    )

    assert result.converted is True
    assert result.file_path == feed_dir / "ep_audio.m4a"
    assert result.file_path.exists()
    assert not src.exists()

    streams = await FFProbe().probe_media_streams(result.file_path)
    assert streams.video_codec is None
    assert streams.audio_codec == "aac"
    assert streams.has_attached_pic is True


@pytest.mark.asyncio
async def test_custom_hook_renames_extension_and_passes_env_vars(
    path_manager: PathManager, tmp_path: Path
):
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
        ffmpeg=FFmpeg(),
        ffprobe=FFProbe(),
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
):
    """Non-zero custom hook exit raises MediaProcessingError with stderr logs."""
    hook = tmp_path / "fail_hook.sh"
    hook.write_text("#!/bin/bash\necho 'hook blew up' >&2\nexit 2\n", encoding="utf-8")
    hook.chmod(hook.stat().st_mode | stat.S_IXUSR)

    custom_proc = MediaProcessor(
        ffmpeg=FFmpeg(),
        ffprobe=FFProbe(),
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
