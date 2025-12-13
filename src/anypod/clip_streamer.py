"""Stream media clips using FFmpeg without writing to disk.

This module provides functionality to extract and stream portions of media files
on-the-fly using FFmpeg's pipe protocol. It supports both audio and video files
and produces streamable fragmented MP4 output.
"""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
import logging
from pathlib import Path
import re

from .exceptions import FFmpegError

logger = logging.getLogger(__name__)

# Maximum clip duration in seconds (1 hour)
MAX_CLIP_DURATION_SECONDS = 3600

# Chunk size for reading FFmpeg output (64KB)
FFMPEG_CHUNK_SIZE = 65536


@dataclass(frozen=True, slots=True)
class ClipRange:
    """Represents a time range for a media clip.

    Attributes:
        start_seconds: Start time in seconds from the beginning of the media.
        end_seconds: End time in seconds from the beginning of the media.
    """

    start_seconds: float
    end_seconds: float

    @property
    def duration_seconds(self) -> float:
        """Calculate the duration of the clip in seconds."""
        return self.end_seconds - self.start_seconds


def parse_timestamp(value: str) -> float:
    """Parse a timestamp string into seconds.

    Supports formats:
        - Seconds: "90", "90.5"
        - MM:SS: "1:30", "01:30.5"
        - HH:MM:SS: "1:30:00", "01:30:00.5"

    Args:
        value: Timestamp string to parse.

    Returns:
        Time in seconds as a float.

    Raises:
        ValueError: If the timestamp format is invalid.
    """
    value = value.strip()

    # Try parsing as plain seconds first
    try:
        seconds = float(value)
        if seconds < 0:
            raise ValueError("Timestamp cannot be negative")
        return seconds
    except ValueError:
        pass

    # Try MM:SS or HH:MM:SS format
    parts = value.split(":")
    if len(parts) == 2:
        # MM:SS
        try:
            minutes = int(parts[0])
            seconds = float(parts[1])
            if minutes < 0 or seconds < 0:
                raise ValueError("Timestamp components cannot be negative")
            return minutes * 60 + seconds
        except ValueError as e:
            raise ValueError(f"Invalid MM:SS timestamp format: {value}") from e
    elif len(parts) == 3:
        # HH:MM:SS
        try:
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = float(parts[2])
            if hours < 0 or minutes < 0 or seconds < 0:
                raise ValueError("Timestamp components cannot be negative")
            return hours * 3600 + minutes * 60 + seconds
        except ValueError as e:
            raise ValueError(f"Invalid HH:MM:SS timestamp format: {value}") from e

    raise ValueError(f"Invalid timestamp format: {value}")


def validate_clip_range(
    start_seconds: float,
    end_seconds: float,
    media_duration: float | None = None,
) -> ClipRange:
    """Validate and create a ClipRange.

    Args:
        start_seconds: Start time in seconds.
        end_seconds: End time in seconds.
        media_duration: Total duration of the source media in seconds (optional).

    Returns:
        Validated ClipRange instance.

    Raises:
        ValueError: If the range is invalid.
    """
    if start_seconds < 0:
        raise ValueError("Start time cannot be negative")
    if end_seconds < 0:
        raise ValueError("End time cannot be negative")
    if end_seconds <= start_seconds:
        raise ValueError("End time must be greater than start time")

    duration = end_seconds - start_seconds
    if duration > MAX_CLIP_DURATION_SECONDS:
        raise ValueError(
            f"Clip duration ({duration:.1f}s) exceeds maximum "
            f"allowed ({MAX_CLIP_DURATION_SECONDS}s)"
        )

    if media_duration is not None and start_seconds >= media_duration:
        raise ValueError(
            f"Start time ({start_seconds:.1f}s) is beyond "
            f"media duration ({media_duration:.1f}s)"
        )

    return ClipRange(start_seconds=start_seconds, end_seconds=end_seconds)


def _format_timestamp_for_ffmpeg(seconds: float) -> str:
    """Format seconds as HH:MM:SS.mmm for FFmpeg.

    Args:
        seconds: Time in seconds.

    Returns:
        Formatted timestamp string.
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:06.3f}"


def _get_output_format_for_extension(ext: str) -> tuple[str, str]:
    """Determine FFmpeg output format and additional flags based on extension.

    Args:
        ext: File extension (without dot).

    Returns:
        Tuple of (format_name, additional_movflags or empty string).
    """
    ext_lower = ext.lower()

    # Audio-only formats that don't need movflags
    audio_only_formats = {"mp3", "ogg", "opus", "flac", "wav"}
    if ext_lower in audio_only_formats:
        return ext_lower, ""

    # Formats that need fragmented MP4 for streaming
    return "mp4", "frag_keyframe+empty_moov+default_base_moof"


def _build_ffmpeg_clip_command(
    input_path: Path,
    clip_range: ClipRange,
    output_format: str,
    movflags: str,
) -> list[str]:
    """Build FFmpeg command for extracting a clip.

    Args:
        input_path: Path to source media file.
        clip_range: Time range to extract.
        output_format: FFmpeg output format (e.g., "mp4", "mp3").
        movflags: Additional movflags for MP4 streaming (empty for non-MP4).

    Returns:
        List of command arguments for FFmpeg.
    """
    start_ts = _format_timestamp_for_ffmpeg(clip_range.start_seconds)
    duration = clip_range.duration_seconds

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        # Input seeking (fast, seeks to nearest keyframe)
        "-ss",
        start_ts,
        "-i",
        str(input_path),
        # Duration limit
        "-t",
        str(duration),
        # Re-encode for frame-accurate cuts
        # Audio settings (AAC is widely compatible)
        "-c:a",
        "aac",
        "-b:a",
        "128k",
    ]

    # Add video settings if this might be a video file
    if output_format == "mp4":
        cmd.extend(
            [
                # Video settings (H.264 for compatibility)
                "-c:v",
                "libx264",
                "-preset",
                "fast",
                "-crf",
                "23",
                # Handle case where input is audio-only
                "-vn" if False else "-map",
                "0",
            ]
        )
        # Remove the placeholder -vn/-map we just added
        cmd = cmd[:-2]
        # Use map to include all streams, let FFmpeg figure out what exists
        cmd.extend(["-map", "0"])

    # Add movflags for streamable MP4
    if movflags:
        cmd.extend(["-movflags", movflags])

    # Output to stdout
    cmd.extend(["-f", output_format, "pipe:1"])

    return cmd


async def stream_clip(
    input_path: Path,
    clip_range: ClipRange,
    ext: str,
) -> AsyncIterator[bytes]:
    """Stream a clip from a media file using FFmpeg.

    This function spawns FFmpeg as a subprocess and yields chunks of the
    transcoded clip. The output is a streamable format (fragmented MP4 for
    video, or the appropriate format for audio).

    Args:
        input_path: Path to the source media file.
        clip_range: Time range to extract.
        ext: Original file extension (used to determine output format).

    Yields:
        Bytes chunks of the transcoded clip.

    Raises:
        FFmpegError: If FFmpeg fails to process the media.
        FileNotFoundError: If the input file doesn't exist.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_format, movflags = _get_output_format_for_extension(ext)
    cmd = _build_ffmpeg_clip_command(input_path, clip_range, output_format, movflags)

    logger.debug(
        "Starting FFmpeg clip extraction",
        extra={
            "input_path": str(input_path),
            "start": clip_range.start_seconds,
            "end": clip_range.end_seconds,
            "output_format": output_format,
        },
    )

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as e:
        raise FFmpegError("ffmpeg executable not found") from e
    except OSError as e:
        raise FFmpegError("Failed to execute ffmpeg") from e

    stderr_data = b""

    try:
        # Stream stdout while process runs
        while True:
            assert process.stdout is not None
            chunk = await process.stdout.read(FFMPEG_CHUNK_SIZE)
            if not chunk:
                break
            yield chunk

        # Wait for process to complete
        _, stderr_data = await process.communicate()

        if process.returncode != 0:
            stderr_text = stderr_data.decode("utf-8", errors="replace")
            logger.error(
                "FFmpeg clip extraction failed",
                extra={
                    "returncode": process.returncode,
                    "stderr": stderr_text,
                    "input_path": str(input_path),
                },
            )
            raise FFmpegError(
                "FFmpeg clip extraction failed",
                stderr=stderr_text,
            )

    except asyncio.CancelledError:
        # Client disconnected, clean up FFmpeg process
        logger.debug("Clip stream cancelled, terminating FFmpeg")
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5.0)
        except TimeoutError:
            process.kill()
            await process.wait()
        raise

    logger.debug(
        "FFmpeg clip extraction completed",
        extra={
            "input_path": str(input_path),
            "start": clip_range.start_seconds,
            "end": clip_range.end_seconds,
        },
    )


def get_clip_content_type(ext: str) -> str:
    """Determine the content type for a clipped file.

    For video files, clips are always output as MP4.
    For audio-only formats, the original format is preserved where possible.

    Args:
        ext: Original file extension.

    Returns:
        MIME type string for the clip.
    """
    ext_lower = ext.lower()

    # Audio formats that keep their original type
    audio_type_map = {
        "mp3": "audio/mpeg",
        "ogg": "audio/ogg",
        "opus": "audio/opus",
        "flac": "audio/flac",
        "wav": "audio/wav",
    }

    if ext_lower in audio_type_map:
        return audio_type_map[ext_lower]

    # Video and other audio (m4a, aac) become MP4
    if ext_lower in {"m4a", "aac", "m4b"}:
        return "audio/mp4"

    # Default to video/mp4 for video files
    return "video/mp4"


def generate_clip_filename(original_filename: str, clip_range: ClipRange) -> str:
    """Generate a filename for the clip based on the original and time range.

    Args:
        original_filename: Original media filename (with extension).
        clip_range: Time range of the clip.

    Returns:
        Suggested filename for the clip.
    """
    # Remove extension from original
    name_match = re.match(r"^(.+)\.([^.]+)$", original_filename)
    if name_match:
        base_name = name_match.group(1)
        ext = name_match.group(2)
    else:
        base_name = original_filename
        ext = "mp4"

    # Format times for filename (use underscores, no colons)
    start_str = f"{int(clip_range.start_seconds)}"
    end_str = f"{int(clip_range.end_seconds)}"

    # Determine output extension
    output_format, _ = _get_output_format_for_extension(ext)
    if output_format == "mp4" and ext.lower() not in {
        "mp3",
        "ogg",
        "opus",
        "flac",
        "wav",
    }:
        output_ext = "mp4"
    else:
        output_ext = ext

    return f"{base_name}_clip_{start_str}-{end_str}.{output_ext}"
