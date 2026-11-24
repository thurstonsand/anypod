"""Thin async wrapper around ffmpeg for simple media conversions.

Provides helpers for image conversion and soft-subtitle muxing into MP4 containers.
"""

import asyncio
import contextlib
from pathlib import Path
from typing import cast
import uuid

import aiofiles.os
import pycountry

from .exceptions import FFmpegError


def _to_iso639_2(lang: str) -> str | None:
    """Convert an ISO 639-1 or ISO 639-2/3 language code to an ISO 639-2/T code.

    MP4 container language metadata (ISO 14496-12) requires 3-letter ISO 639-2/T
    codes (e.g., ``eng``, ``deu``, ``fra``) rather than 2-letter ISO 639-1 codes.

    Args:
        lang: Language code string (e.g., ``"en"``, ``"de"``, ``"eng"``).

    Returns:
        Lowercase 3-letter ISO 639-2/T code if recognized, or None otherwise.
    """
    normalized = lang.strip().lower()
    if not normalized:
        return None
    language = cast(
        object,
        pycountry.languages.get(  # pyright: ignore[reportUnknownMemberType]
            alpha_2=normalized
        )
        or pycountry.languages.get(  # pyright: ignore[reportUnknownMemberType]
            alpha_3=normalized
        ),
    )
    alpha_3: object = getattr(language, "alpha_3", None)
    return alpha_3 if isinstance(alpha_3, str) and len(alpha_3) == 3 else None


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

    async def embed_subtitles(
        self,
        media_path: Path,
        subtitle_path: Path,
        lang: str,
    ) -> None:
        """Soft-mux a subtitle file into an MP4-family container in place.

        Stream-copies video and audio without re-encoding, converts subtitles to
        ``mov_text``, replaces any existing subtitle stream, preserves faststart
        atom placement, and atomically swaps the remuxed file into ``media_path``.

        Args:
            media_path: Path to the target MP4/M4V/MOV file to update in place.
            subtitle_path: Path to the subtitle file (e.g., WebVTT).
            lang: Language code for the subtitle track (ISO 639-1 or 639-2/T).

        Raises:
            FFmpegError: If ffmpeg fails or the atomic file swap fails.
        """
        temp_path = media_path.with_name(
            f".{media_path.stem}.{uuid.uuid4().hex[:8]}.tmp{media_path.suffix}"
        )
        args: list[str] = [
            "-i",
            str(media_path),
            "-i",
            str(subtitle_path),
            "-map",
            "0:v?",
            "-map",
            "0:a?",
            "-map",
            "1:s:0",
            "-c:v",
            "copy",
            "-c:a",
            "copy",
            "-c:s",
            "mov_text",
        ]
        iso_lang = _to_iso639_2(lang)
        if iso_lang:
            args.extend(["-metadata:s:s:0", f"language={iso_lang}"])
        args.extend(["-movflags", "+faststart", "-y", str(temp_path)])

        try:
            rc, _, stderr = await self._run(*args)
            if rc != 0:
                raise FFmpegError(
                    "Embedding subtitles into media container failed",
                    stderr=stderr.decode(errors="replace") if stderr else None,
                )
            try:
                await aiofiles.os.replace(temp_path, media_path)
            except OSError as e:
                raise FFmpegError(
                    "Failed to replace media file after embedding subtitles"
                ) from e
        except BaseException:
            if await aiofiles.os.path.exists(temp_path):
                with contextlib.suppress(OSError):
                    await aiofiles.os.remove(temp_path)
            raise
