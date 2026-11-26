# pyright: reportPrivateUsage=false

"""Integration tests for MediaProcessor using real ffmpeg/ffprobe on synthetic clips."""

import asyncio
from pathlib import Path
import stat

import pytest

from anypod.config.types import MediaConversionPreset
from anypod.ffmpeg import FFmpeg
from anypod.ffprobe import FFProbe
from anypod.media_processor import MediaProcessor
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
def processor(path_manager: PathManager, tmp_path: Path) -> MediaProcessor:
    """Provide a MediaProcessor wired to real FFmpeg and FFProbe."""
    return MediaProcessor(
        ffmpeg=FFmpeg(),
        ffprobe=FFProbe(),
        paths=path_manager,
        custom_script_path=tmp_path / "post_process.sh",
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_light_preset_remuxes_h264_opus_mkv_to_mp4_and_embeds_thumbnail(
    processor: MediaProcessor, path_manager: PathManager, tmp_path: Path
) -> None:
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_optimized_preset_transcodes_h264_to_hevc_hvc1_and_embeds_thumbnail(
    processor: MediaProcessor, path_manager: PathManager, tmp_path: Path
) -> None:
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_audio_only_file_converts_to_m4a_with_artwork(
    processor: MediaProcessor, path_manager: PathManager, tmp_path: Path
) -> None:
    """Audio-only inputs convert to AAC .m4a and attach cover art cleanly."""
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_audio_only_preserves_existing_attached_pic_during_conversion(
    processor: MediaProcessor, path_manager: PathManager, tmp_path: Path
) -> None:
    """Audio-only files that already contain cover art keep it when converted to .m4a."""
    feed_dir = await path_manager.feed_data_dir("audio_cover")
    src = feed_dir / "ep_mp3_art.mp3"
    thumb = await _create_jpg(tmp_path / "cover.jpg")
    await _run_ffmpeg(
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:duration=0.5",
        "-i",
        str(thumb),
        "-map",
        "0:a:0",
        "-map",
        "1:0",
        "-c:a",
        "libmp3lame",
        "-c:v",
        "mjpeg",
        "-disposition:v:0",
        "attached_pic",
        str(src),
    )

    result = await processor.process(
        feed_id="audio_cover",
        download_id="ep_mp3_art",
        source_url="https://example.com/ep_mp3_art",
        media_path=src,
        preset=MediaConversionPreset.LIGHT,
        embed_thumbnail=False,
    )

    assert result.converted is True
    assert result.file_path == feed_dir / "ep_mp3_art.m4a"
    streams = await FFProbe().probe_media_streams(result.file_path)
    assert streams.video_codec is None
    assert streams.audio_codec == "aac"
    assert streams.has_attached_pic is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_custom_hook_embeds_thumbnail_when_enabled(
    path_manager: PathManager, tmp_path: Path
) -> None:
    """CUSTOM preset honors embed_thumbnail=True by attaching cover art after the hook."""
    hook = tmp_path / "copy_hook.sh"
    hook.write_text("#!/bin/bash -e\nexit 0\n", encoding="utf-8")
    hook.chmod(hook.stat().st_mode | stat.S_IXUSR)

    custom_proc = MediaProcessor(
        ffmpeg=FFmpeg(),
        ffprobe=FFProbe(),
        paths=path_manager,
        custom_script_path=hook,
    )
    feed_dir = await path_manager.feed_data_dir("custom_thumb")
    src = feed_dir / "ep_custom_thumb.mp4"
    thumb = await _create_jpg(tmp_path / "custom_thumb.jpg")
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
        "aac",
        str(src),
    )

    result = await custom_proc.process(
        feed_id="custom_thumb",
        download_id="ep_custom_thumb",
        source_url="https://example.com/ep_custom_thumb",
        media_path=src,
        preset=MediaConversionPreset.CUSTOM,
        embed_thumbnail=True,
        thumbnail_path=thumb,
    )

    streams = await FFProbe().probe_media_streams(result.file_path)
    assert streams.has_attached_pic is True
