"""First-class media post-processing and conversion service for Anypod.

Provides built-in media optimization presets (off, light, optimized) using
in-process async FFmpeg/FFProbe calls, plus a sandboxed custom hook script
runner for power users.
"""

import asyncio
from dataclasses import dataclass
import logging
import os
from pathlib import Path
import shutil
import uuid

import aiofiles.os

from .config.types import MediaConversionPreset
from .exceptions import FFmpegError, FFProbeError, MediaProcessingError
from .ffmpeg import FFmpeg
from .ffprobe import FFProbe, MediaStreamInfo
from .path_manager import PathManager

logger = logging.getLogger(__name__)

# Format selectors applied by built-in presets when the user has not passed -f/--format.
_LIGHT_VIDEO_FORMAT = (
    "bestvideo*[height<=1080][vcodec^=avc1]+bestaudio[acodec^=mp4a]/"
    "bestvideo*[height<=1080][vcodec^=avc1]+bestaudio/"
    "bestvideo*[height<=1080][vcodec^=hvc1]+bestaudio/"
    "bestvideo*[height<=1080][vcodec^=hev1]+bestaudio/"
    "bestvideo*[height<=1080][vcodec^=av01]+bestaudio/"
    "bestvideo*[height<=1080]+bestaudio/"
    "best[height<=1080]/best"
)

_OPTIMIZED_VIDEO_FORMAT = (
    "bestvideo*[height<=1080][vcodec*=av01]+bestaudio/"
    "bestvideo*[height<=1080][vcodec*=av01]/"
    "bestvideo*[height<=1080][vcodec*=h265]+bestaudio/"
    "bestvideo*[height<=1080][vcodec*=h265]/"
    "bestvideo*[height<=1080][vcodec*=hevc]+bestaudio/"
    "bestvideo*[height<=1080][vcodec*=hevc]/"
    "bestvideo*[height<=1080][vcodec*=vp9]+bestaudio/"
    "bestvideo*[height<=1080][vcodec*=vp9]/"
    "bestvideo*[height<=1080]+bestaudio/"
    "bestvideo*[height<=1080]/"
    "best[height<=1080]/best"
)

_AUDIO_FORMAT = "bestaudio[ext=m4a]/bestaudio[acodec^=mp4a]/bestaudio/best"

_LIGHT_COPY_VIDEO_CODECS = frozenset({"h264", "avc1", "hevc", "h265", "av1", "av01"})
_OPTIMIZED_COPY_VIDEO_CODECS = frozenset({"hevc", "h265", "av1", "av01"})
_HEVC_CODECS = frozenset({"hevc", "h265"})
_THUMBNAIL_EMBED_EXTS = frozenset({"mp4", "m4a"})


def default_format_for_preset(
    preset: MediaConversionPreset,
    *,
    prefer_audio: bool = False,
) -> str | None:
    """Return the default yt-dlp format selector for a conversion preset.

    Args:
        preset: Active media conversion preset.
        prefer_audio: Whether the feed prefers audio-only streams.

    Returns:
        yt-dlp ``--format`` expression, or None for presets that defer to yt-dlp defaults.
    """
    if preset in (MediaConversionPreset.OFF, MediaConversionPreset.CUSTOM):
        return _AUDIO_FORMAT if prefer_audio else None
    if prefer_audio:
        return _AUDIO_FORMAT
    if preset == MediaConversionPreset.LIGHT:
        return _LIGHT_VIDEO_FORMAT
    return _OPTIMIZED_VIDEO_FORMAT


@dataclass(frozen=True, slots=True)
class ProcessedMediaResult:
    """Outcome of post-download media processing.

    Attributes:
        file_path: Final path to the media file on disk.
        converted: True if ffmpeg or a custom hook modified/remuxed the file.
        logs: Optional diagnostic log text from conversion or custom hook.
    """

    file_path: Path
    converted: bool
    logs: str | None = None


def _format_hook_logs(stdout: str, stderr: str) -> str | None:
    """Combine custom hook stdout/stderr into a readable log snippet."""
    parts: list[str] = []
    if stdout.strip():
        parts.append(f"HOOK STDOUT:\n{stdout.strip()}")
    if stderr.strip():
        parts.append(f"HOOK STDERR:\n{stderr.strip()}")
    return "\n\n".join(parts) if parts else None


def _build_ffmpeg_args(
    *,
    input_path: Path,
    output_path: Path,
    streams: MediaStreamInfo,
    preset: MediaConversionPreset,
    thumbnail_path: Path | None,
) -> list[str] | None:
    """Construct ffmpeg arguments for ``light`` or ``optimized`` processing.

    Returns None when the file already satisfies the preset and does not need
    remuxing, re-encoding, or thumbnail embedding.
    """
    ext = input_path.suffix.lstrip(".").lower()
    is_audio_only = streams.video_codec is None and streams.audio_codec is not None
    attach_thumb = (
        thumbnail_path is not None
        and not streams.has_attached_pic
        and (is_audio_only or streams.video_codec is not None)
    )

    if is_audio_only:
        target_ext = "m4a"
        needs_audio_encode = streams.audio_codec != "aac"
        needs_container = ext != target_ext
        if not (needs_audio_encode or needs_container or attach_thumb):
            return None

        args: list[str] = [
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(input_path),
        ]
        if attach_thumb and thumbnail_path is not None:
            args.extend(["-i", str(thumbnail_path), "-map", "0:a:0", "-map", "1:0"])
        elif streams.has_attached_pic:
            # Preserve an already-embedded cover image across remux/transcode
            args.extend(["-map", "0:a:0", "-map", "0:v:0?"])
        else:
            args.extend(["-map", "0:a:0"])

        if needs_audio_encode:
            args.extend(["-c:a", "aac", "-b:a", "128k"])
        else:
            args.extend(["-c:a", "copy"])

        if attach_thumb:
            args.extend(["-c:v:0", "mjpeg", "-disposition:v:0", "attached_pic"])
        elif streams.has_attached_pic:
            args.extend(["-c:v:0", "copy", "-disposition:v:0", "attached_pic"])

        args.extend(["-movflags", "+faststart", str(output_path)])
        return args

    if streams.video_codec is None:
        return None

    target_ext = "mp4"
    copy_codecs = (
        _OPTIMIZED_COPY_VIDEO_CODECS
        if preset == MediaConversionPreset.OPTIMIZED
        else _LIGHT_COPY_VIDEO_CODECS
    )
    needs_video_encode = streams.video_codec not in copy_codecs
    needs_hevc_tag = (
        not needs_video_encode
        and streams.video_codec in _HEVC_CODECS
        and streams.video_codec_tag != "hvc1"
    )
    needs_audio_encode = (
        streams.audio_codec is not None and streams.audio_codec != "aac"
    )
    needs_container = ext != target_ext

    if not (
        needs_video_encode
        or needs_hevc_tag
        or needs_audio_encode
        or needs_container
        or attach_thumb
    ):
        return None

    args = [
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(input_path),
    ]
    if attach_thumb and thumbnail_path is not None:
        args.extend(["-i", str(thumbnail_path)])

    # Explicitly map primary video + optional audio + optional attached picture
    args.extend(["-map", "0:v:0"])
    if streams.audio_codec is not None:
        args.extend(["-map", "0:a:0"])

    if attach_thumb:
        args.extend(["-map", "1:0"])
    elif streams.has_attached_pic:
        # Preserve an already-embedded cover image across remux/transcode
        args.extend(["-map", "0:v:1?"])

    # Video stream encoding/copy settings (scoped to v:0 so attached_pic isn't re-encoded as h265/h264)
    if needs_video_encode:
        if preset == MediaConversionPreset.OPTIMIZED:
            args.extend(
                [
                    "-c:v:0",
                    "libx265",
                    "-preset",
                    "medium",
                    "-crf",
                    "28",
                    "-pix_fmt",
                    "yuv420p",
                    "-tag:v:0",
                    "hvc1",
                ]
            )
        else:
            args.extend(
                [
                    "-c:v:0",
                    "libx264",
                    "-preset",
                    "fast",
                    "-crf",
                    "23",
                    "-pix_fmt",
                    "yuv420p",
                ]
            )
    else:
        args.extend(["-c:v:0", "copy"])
        if streams.video_codec in _HEVC_CODECS:
            args.extend(["-tag:v:0", "hvc1"])

    # Audio stream encoding/copy settings
    if streams.audio_codec is not None:
        if needs_audio_encode:
            args.extend(["-c:a", "aac", "-b:a", "128k"])
        else:
            args.extend(["-c:a", "copy"])

    # Attached artwork stream
    if attach_thumb:
        args.extend(["-c:v:1", "mjpeg", "-disposition:v:1", "attached_pic"])
    elif streams.has_attached_pic:
        args.extend(["-c:v:1", "copy", "-disposition:v:1", "attached_pic"])

    args.extend(["-movflags", "+faststart", str(output_path)])
    return args


class MediaProcessor:
    """Process downloaded media according to a feed's conversion preset.

    Handles built-in ``light`` and ``optimized`` conversions using FFmpeg/FFProbe
    inside ``tmp/`` with atomic replacement, as well as isolated execution of a
    user-provided hook script for ``custom``.
    """

    def __init__(
        self,
        ffmpeg: FFmpeg,
        ffprobe: FFProbe,
        paths: PathManager,
        custom_script_path: Path,
    ):
        self._ffmpeg = ffmpeg
        self._ffprobe = ffprobe
        self._paths = paths
        self._custom_script_path = custom_script_path

    async def _embed_thumbnail_only(
        self,
        *,
        feed_id: str,
        download_id: str,
        media_path: Path,
        thumbnail_path: Path,
    ) -> ProcessedMediaResult:
        """Stream-copy MP4/M4A to attach episode artwork when preset is OFF."""
        ext = media_path.suffix.lstrip(".").lower()
        if ext not in _THUMBNAIL_EMBED_EXTS:
            return ProcessedMediaResult(file_path=media_path, converted=False)

        try:
            streams = await self._ffprobe.probe_media_streams(media_path)
        except FFProbeError as e:
            raise MediaProcessingError(
                "Failed to probe media streams before thumbnail embedding.",
                feed_id=feed_id,
                download_id=download_id,
                logs=e.stderr,
            ) from e

        if streams.has_attached_pic:
            return ProcessedMediaResult(file_path=media_path, converted=False)

        tmp_dir = await self._paths.feed_tmp_dir(feed_id)
        tmp_output = tmp_dir / f"{download_id}.{uuid.uuid4().hex[:8]}.thumb.{ext}"

        is_audio_only = streams.video_codec is None and streams.audio_codec is not None
        args = [
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(media_path),
            "-i",
            str(thumbnail_path),
        ]
        if is_audio_only:
            args.extend(
                [
                    "-map",
                    "0:a:0",
                    "-map",
                    "1:0",
                    "-c:a",
                    "copy",
                    "-c:v:0",
                    "mjpeg",
                    "-disposition:v:0",
                    "attached_pic",
                    "-movflags",
                    "+faststart",
                    str(tmp_output),
                ]
            )
        else:
            args.extend(
                [
                    "-map",
                    "0:v:0",
                    "-map",
                    "0:a:0?",
                    "-map",
                    "1:0",
                    "-c:v:0",
                    "copy",
                    "-c:a",
                    "copy",
                    "-c:v:1",
                    "mjpeg",
                    "-disposition:v:1",
                    "attached_pic",
                    "-movflags",
                    "+faststart",
                    str(tmp_output),
                ]
            )

        try:
            stderr_text = await self._ffmpeg.run_conversion(args)
            await aiofiles.os.replace(tmp_output, media_path)
        except FFmpegError as e:
            if tmp_output.exists():
                tmp_output.unlink(missing_ok=True)
            raise MediaProcessingError(
                "Failed to embed thumbnail into media file.",
                feed_id=feed_id,
                download_id=download_id,
                logs=e.stderr,
            ) from e
        except BaseException:
            if tmp_output.exists():
                tmp_output.unlink(missing_ok=True)
            raise

        return ProcessedMediaResult(
            file_path=media_path,
            converted=True,
            logs=stderr_text or None,
        )

    async def _run_builtin_preset(
        self,
        *,
        feed_id: str,
        download_id: str,
        media_path: Path,
        preset: MediaConversionPreset,
        thumbnail_path: Path | None,
    ) -> ProcessedMediaResult:
        """Apply the built-in ``light`` or ``optimized`` FFmpeg preset."""
        log_params = {
            "feed_id": feed_id,
            "download_id": download_id,
            "preset": preset.value,
            "input_path": str(media_path),
        }

        try:
            streams = await self._ffprobe.probe_media_streams(media_path)
        except FFProbeError as e:
            raise MediaProcessingError(
                "Failed to probe media streams for conversion.",
                feed_id=feed_id,
                download_id=download_id,
                logs=e.stderr,
            ) from e

        is_audio_only = streams.video_codec is None and streams.audio_codec is not None
        target_ext = "m4a" if is_audio_only else "mp4"

        tmp_dir = await self._paths.feed_tmp_dir(feed_id)
        tmp_output = (
            tmp_dir / f"{download_id}.{uuid.uuid4().hex[:8]}.converting.{target_ext}"
        )

        ffmpeg_args = _build_ffmpeg_args(
            input_path=media_path,
            output_path=tmp_output,
            streams=streams,
            preset=preset,
            thumbnail_path=thumbnail_path,
        )

        if ffmpeg_args is None:
            logger.debug(
                "Media already matches preset; skipping ffmpeg.",
                extra=log_params,
            )
            return ProcessedMediaResult(file_path=media_path, converted=False)

        logger.info(
            "Converting media with built-in preset.",
            extra={
                **log_params,
                "video_codec": streams.video_codec,
                "audio_codec": streams.audio_codec,
                "target_ext": target_ext,
            },
        )

        try:
            stderr_text = await self._ffmpeg.run_conversion(ffmpeg_args)
        except FFmpegError as e:
            if tmp_output.exists():
                tmp_output.unlink(missing_ok=True)
            raise MediaProcessingError(
                f"FFmpeg media conversion failed for preset '{preset.value}'.",
                feed_id=feed_id,
                download_id=download_id,
                logs=e.stderr,
            ) from e
        except BaseException:
            if tmp_output.exists():
                tmp_output.unlink(missing_ok=True)
            raise

        final_path = media_path.with_name(f"{download_id}.{target_ext}")
        try:
            await aiofiles.os.replace(tmp_output, final_path)
            if final_path != media_path and media_path.exists():
                await aiofiles.os.remove(media_path)
        except OSError as e:
            if tmp_output.exists():
                tmp_output.unlink(missing_ok=True)
            raise MediaProcessingError(
                "Failed to move converted media into final storage.",
                feed_id=feed_id,
                download_id=download_id,
            ) from e

        logger.info(
            "Media conversion completed.",
            extra={**log_params, "output_path": str(final_path)},
        )
        return ProcessedMediaResult(
            file_path=final_path,
            converted=True,
            logs=stderr_text or None,
        )

    async def _run_custom_hook(
        self,
        *,
        feed_id: str,
        download_id: str,
        source_url: str,
        media_path: Path,
    ) -> ProcessedMediaResult:
        """Run a user-provided post-processing hook in an isolated staging directory."""
        script = self._custom_script_path
        if not script.is_file():
            raise MediaProcessingError(
                f"Custom conversion script not found at '{script}'.",
                feed_id=feed_id,
                download_id=download_id,
            )
        if not os.access(script, os.X_OK):
            raise MediaProcessingError(
                f"Custom conversion script '{script}' is not executable (run chmod +x).",
                feed_id=feed_id,
                download_id=download_id,
            )

        tmp_dir = await self._paths.feed_tmp_dir(feed_id)
        stage_dir = tmp_dir / f"hook_{download_id}_{uuid.uuid4().hex[:8]}"
        await aiofiles.os.makedirs(stage_dir, exist_ok=True)
        staged_input = stage_dir / media_path.name

        try:
            await aiofiles.os.replace(media_path, staged_input)

            env = {
                **os.environ,
                "ANYPOD_FEED_ID": feed_id,
                "ANYPOD_DOWNLOAD_ID": download_id,
                "ANYPOD_SOURCE_URL": source_url,
                "ANYPOD_TMP_DIR": str(stage_dir),
            }

            logger.info(
                "Running custom post-processing hook.",
                extra={
                    "feed_id": feed_id,
                    "download_id": download_id,
                    "script": str(script),
                },
            )

            try:
                proc = await asyncio.create_subprocess_exec(
                    str(script),
                    str(staged_input),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=str(stage_dir),
                    env=env,
                )
            except OSError as e:
                raise MediaProcessingError(
                    f"Failed to execute custom conversion script '{script}'.",
                    feed_id=feed_id,
                    download_id=download_id,
                ) from e

            try:
                stdout_b, stderr_b = await proc.communicate()
            except asyncio.CancelledError:
                proc.kill()
                await proc.wait()
                raise

            stdout_text = stdout_b.decode("utf-8", errors="replace") if stdout_b else ""
            stderr_text = stderr_b.decode("utf-8", errors="replace") if stderr_b else ""
            hook_logs = _format_hook_logs(stdout_text, stderr_text)

            if proc.returncode != 0:
                raise MediaProcessingError(
                    f"Custom conversion script exited with code {proc.returncode}.",
                    feed_id=feed_id,
                    download_id=download_id,
                    logs=hook_logs,
                )

            outputs = [p for p in stage_dir.iterdir() if p.is_file()]
            if len(outputs) != 1:
                raise MediaProcessingError(
                    "Custom conversion script must leave exactly one output file in the "
                    f"working directory, found {len(outputs)}: {[p.name for p in outputs]}",
                    feed_id=feed_id,
                    download_id=download_id,
                    logs=hook_logs,
                )

            produced = outputs[0]
            new_ext = produced.suffix.lstrip(".").lower()
            if not new_ext:
                raise MediaProcessingError(
                    f"Custom conversion script output '{produced.name}' has no file extension.",
                    feed_id=feed_id,
                    download_id=download_id,
                    logs=hook_logs,
                )

            final_path = media_path.parent / f"{download_id}.{new_ext}"
            await aiofiles.os.replace(produced, final_path)

            logger.info(
                "Custom post-processing hook completed.",
                extra={
                    "feed_id": feed_id,
                    "download_id": download_id,
                    "output_path": str(final_path),
                },
            )
            return ProcessedMediaResult(
                file_path=final_path,
                converted=True,
                logs=hook_logs,
            )
        finally:
            shutil.rmtree(stage_dir, ignore_errors=True)

    async def process(
        self,
        *,
        feed_id: str,
        download_id: str,
        source_url: str,
        media_path: Path,
        preset: MediaConversionPreset,
        embed_thumbnail: bool,
        thumbnail_path: Path | None = None,
    ) -> ProcessedMediaResult:
        """Process a downloaded media file according to the configured preset.

        Args:
            feed_id: Feed identifier.
            download_id: Download identifier.
            source_url: Original source URL of the episode.
            media_path: Path to the downloaded media file.
            preset: Conversion preset (off, light, optimized, custom).
            embed_thumbnail: Whether to embed episode thumbnail into MP4/M4A output.
            thumbnail_path: Path to the local JPG thumbnail if available.

        Returns:
            ProcessedMediaResult containing the final file path and any conversion logs.

        Raises:
            MediaProcessingError: If conversion or custom hook execution fails.
        """
        valid_thumb = (
            thumbnail_path
            if embed_thumbnail and thumbnail_path and thumbnail_path.is_file()
            else None
        )

        match preset:
            case MediaConversionPreset.OFF:
                if valid_thumb is not None:
                    return await self._embed_thumbnail_only(
                        feed_id=feed_id,
                        download_id=download_id,
                        media_path=media_path,
                        thumbnail_path=valid_thumb,
                    )
                return ProcessedMediaResult(file_path=media_path, converted=False)
            case MediaConversionPreset.LIGHT | MediaConversionPreset.OPTIMIZED:
                return await self._run_builtin_preset(
                    feed_id=feed_id,
                    download_id=download_id,
                    media_path=media_path,
                    preset=preset,
                    thumbnail_path=valid_thumb,
                )
            case MediaConversionPreset.CUSTOM:
                hook_result = await self._run_custom_hook(
                    feed_id=feed_id,
                    download_id=download_id,
                    source_url=source_url,
                    media_path=media_path,
                )
                if valid_thumb is not None:
                    embed_result = await self._embed_thumbnail_only(
                        feed_id=feed_id,
                        download_id=download_id,
                        media_path=hook_result.file_path,
                        thumbnail_path=valid_thumb,
                    )
                    combined_logs = "\n\n".join(
                        part for part in (hook_result.logs, embed_result.logs) if part
                    )
                    return ProcessedMediaResult(
                        file_path=embed_result.file_path,
                        converted=True,
                        logs=combined_logs or None,
                    )
                return hook_result
