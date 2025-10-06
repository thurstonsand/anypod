"""Thin async wrapper around ffmpeg for simple media conversions.

Currently used to convert arbitrary images to JPG using MJPEG encoder.
"""

import asyncio
from pathlib import Path

from .exceptions import FFmpegError


class FFmpeg:
    """Run ffmpeg commands for media processing.

    Provides minimal helpers for specific conversions needed by the codebase.
    """

    async def _run(self, *args: str) -> tuple[int, bytes, bytes]:
        try:
            process = await asyncio.create_subprocess_exec(
                "ffmpeg",
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as e:
            raise FFmpegError("ffmpeg executable not found") from e
        except OSError as e:
            raise FFmpegError("Failed to execute ffmpeg") from e

        try:
            stdout, stderr = await process.communicate()
        except asyncio.CancelledError:
            # Ensure subprocess cleanup on cancellation
            process.kill()
            raise

        return process.returncode or 0, stdout or b"", stderr or b""

    async def convert_image_to_jpg(self, input_path: Path, output_path: Path) -> None:
        """Convert an image file to a JPG using ffmpeg (MJPEG).

        Raises:
            FFmpegError: When conversion fails.
        """
        rc, _, stderr = await self._run(
            "-i",
            str(input_path),
            "-f",
            "mjpeg",
            "-y",
            str(output_path),
        )
        if rc != 0:
            raise FFmpegError(
                "Image conversion to JPG failed",
                stderr=stderr.decode() if stderr else None,
            )
