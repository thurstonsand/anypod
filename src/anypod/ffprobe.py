"""Thin async wrapper around ffprobe for media probing.

Provides centralized helpers for:
- Determining if an image file is JPG (MJPEG) using local file paths
- Extracting media duration (in seconds) from local files or remote URLs

All methods are implemented using asyncio subprocess execution to avoid
blocking the event loop.
"""

import asyncio
import json
from pathlib import Path
from typing import Any

from .exceptions import FFProbeError


class FFProbe:
    """Run ffprobe commands to gather media metadata.

    This class provides minimal, focused helpers that wrap common ffprobe
    invocations used throughout the codebase. It does not attempt to be an
    exhaustive interface to ffprobe.
    """

    async def _run(self, *args: str) -> tuple[int, bytes, bytes]:
        """Execute ffprobe with the given arguments.

        Args:
            *args: Arguments passed directly to the ffprobe executable.

        Returns:
            Tuple of (returncode, stdout, stderr).

        Raises:
            FileNotFoundError: When ffprobe is not installed/available in PATH.
            OSError: When the subprocess fails to execute.
        """
        try:
            process = await asyncio.create_subprocess_exec(
                "ffprobe",
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as e:
            raise FFProbeError("ffprobe executable not found") from e
        except OSError as e:
            raise FFProbeError("Failed to execute ffprobe") from e

        try:
            stdout, stderr = await process.communicate()
        except asyncio.CancelledError:
            # Ensure subprocess cleanup on cancellation
            process.kill()
            raise

        return process.returncode or 0, stdout or b"", stderr or b""

    async def is_jpg_file(self, file_path: Path) -> bool:
        """Return True if the file's first stream is MJPEG (JPG).

        Args:
            file_path: Local filesystem path to an image file.

        Returns:
            True if the first stream codec is ``mjpeg``; False otherwise.

        Raises:
            FileNotFoundError: If ffprobe is not installed.
            OSError: If the subprocess fails to execute.
            RuntimeError: If ffprobe returns a non-zero status or JSON cannot be parsed.
        """
        rc, stdout, stderr = await self._run(
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_streams",
            str(file_path),
        )
        if rc != 0:
            raise FFProbeError(
                "ffprobe failed (is_jpg_file)",
                stderr=stderr.decode() if stderr else None,
            )
        try:
            data: dict[str, Any] = json.loads(stdout.decode())
        except json.JSONDecodeError as e:
            raise FFProbeError(
                "Failed to parse ffprobe JSON output (is_jpg_file)",
                stderr=stdout.decode(),
            ) from e

        try:
            streams = data.get("streams", [])
            if streams:
                codec_name = streams[0].get("codec_name", "").lower()
                return codec_name == "mjpeg"
        except Exception as e:
            raise FFProbeError(
                "Unexpected ffprobe output", stderr=stdout.decode()
            ) from e
        return False

    async def _duration_seconds(
        self, probe_target: str, headers: dict[str, str] | None
    ) -> int:
        """Core helper to extract duration seconds; raises on failure.

        Args:
            probe_target: Local file path or remote URL string.
            headers: Optional HTTP headers (only used for remote URLs).

        Returns:
            Integer duration in seconds.

        Raises:
            FFProbeError: When ffprobe fails or output is unparsable/empty.
        """
        args: list[str] = [
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
        ]
        if headers:
            for k, v in headers.items():
                args.extend(["-headers", f"{k}: {v}"])
        args.append(probe_target)

        rc, stdout, stderr = await self._run(*args)
        if rc != 0:
            raise FFProbeError(
                "ffprobe failed (duration)",
                stderr=stderr.decode() if stderr else None,
            )
        text = stdout.decode().strip()
        if not text:
            raise FFProbeError(
                "ffprobe returned empty duration output",
                stderr=stderr.decode() if stderr else None,
            )
        try:
            return int(float(text))
        except ValueError as e:
            raise FFProbeError("Failed to parse duration output", stderr=text) from e

    async def get_duration_seconds_from_file(self, file_path: Path) -> int:
        """Return media duration in seconds from a local file; raises on failure."""
        return await self._duration_seconds(str(file_path), headers=None)

    async def get_duration_seconds_from_url(
        self, url: str, headers: dict[str, str] | None = None
    ) -> int:
        """Return media duration in seconds by probing a remote URL; raises on failure."""
        return await self._duration_seconds(url, headers=headers)
