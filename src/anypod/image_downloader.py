"""Image downloading functionality for feed and download thumbnails."""

import asyncio
import json
import logging
from pathlib import Path

import aiofiles
import aiofiles.os
import httpx

from .db.types import SourceType
from .exceptions import ImageDownloadError, YtdlpApiError
from .path_manager import PathManager
from .ytdlp_wrapper import YtdlpWrapper

logger = logging.getLogger(__name__)


class ImageDownloader:
    """Handle downloading and storing images for feeds and downloads.

    This class provides a unified interface for downloading images either
    directly via HTTP or through yt-dlp, always storing them as JPG format
    in the appropriate directory structure.

    Attributes:
        _paths: PathManager instance for coordinating file paths.
        _ytdlp_wrapper: YtdlpWrapper for yt-dlp based downloads.
    """

    def __init__(self, paths: PathManager, ytdlp_wrapper: YtdlpWrapper):
        self._paths = paths
        self._ytdlp_wrapper = ytdlp_wrapper
        logger.debug("ImageDownloader initialized.")

    async def _is_jpg_format(self, file_path: Path, feed_id: str, url: str) -> bool:
        """Check if a file is already in JPG format using ffprobe.

        Args:
            file_path: Path to the file to check.
            feed_id: Feed identifier for error context.
            url: Image URL for error context.

        Returns:
            True if the file is already in JPG format, False otherwise.

        Raises:
            ImageDownloadError: If ffprobe check fails.
        """
        try:
            process = await asyncio.create_subprocess_exec(
                "ffprobe",
                "-v",
                "quiet",
                "-print_format",
                "json",
                "-show_streams",
                str(file_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
        except FileNotFoundError as e:
            raise ImageDownloadError(
                "ffprobe not found - required for format detection",
                feed_id=feed_id,
                url=url,
            ) from e
        except OSError as e:
            raise ImageDownloadError(
                "Failed to execute format detection",
                feed_id=feed_id,
                url=url,
            ) from e

        if process.returncode != 0:
            error_msg = stderr.decode() if stderr else "Unknown ffprobe error"
            raise ImageDownloadError(
                f"Format detection failed: {error_msg}",
                feed_id=feed_id,
                url=url,
            )

        try:
            probe_data = json.loads(stdout.decode())
        except json.JSONDecodeError as e:
            raise ImageDownloadError(
                "Failed to parse format detection output",
                feed_id=feed_id,
                url=url,
            ) from e

        streams = probe_data.get("streams", [])
        if streams:
            codec_name = streams[0].get("codec_name", "").lower()
            return codec_name == "mjpeg"
        return False

    async def _convert_to_jpg(
        self, input_path: Path, output_path: Path, feed_id: str, url: str
    ) -> None:
        """Convert an image file to JPG format using ffmpeg.

        Args:
            input_path: Path to the input image file.
            output_path: Path where the JPG output should be saved.
            feed_id: Feed identifier for error context.
            url: Image URL for error context.

        Raises:
            ImageDownloadError: If conversion fails.
        """
        try:
            process = await asyncio.create_subprocess_exec(
                "ffmpeg",
                "-i",
                str(input_path),
                "-f",
                "mjpeg",
                "-y",  # Overwrite output file
                str(output_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await process.communicate()
        except FileNotFoundError as e:
            raise ImageDownloadError(
                "ffmpeg not found - required for image conversion",
                feed_id=feed_id,
                url=url,
            ) from e
        except OSError as e:
            raise ImageDownloadError(
                "Failed to execute image conversion",
                feed_id=feed_id,
                url=url,
            ) from e

        if process.returncode != 0:
            error_msg = stderr.decode() if stderr else "Unknown ffmpeg error"
            raise ImageDownloadError(
                f"Image conversion to JPG failed: {error_msg}",
                feed_id=feed_id,
                url=url,
            )

    async def download_feed_image_direct(self, feed_id: str, url: str) -> str | None:
        """Download feed image directly via HTTP.

        Args:
            feed_id: Feed identifier for storage path.
            url: Image URL to download.

        Returns:
            Extension string (e.g., "jpg") if successful, None if failed.
        """
        log_params = {"feed_id": feed_id, "url": url}
        logger.debug("Starting direct HTTP feed image download.", extra=log_params)

        # Get the target file path
        try:
            final_path = await self._paths.image_path(feed_id, None, "jpg")
        except ValueError as e:
            raise ImageDownloadError(
                "Invalid feed identifier for image path.",
                feed_id=feed_id,
                url=url,
            ) from e

        # Create temporary file path for initial download
        tmp_path = await self._paths.tmp_file(feed_id)

        # Download the image
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, follow_redirects=True)
                response.raise_for_status()
            except httpx.HTTPError as e:
                raise ImageDownloadError(
                    "HTTP request failed for image download",
                    feed_id=feed_id,
                    url=url,
                ) from e

            # Write the image data to temporary file
            try:
                async with aiofiles.open(tmp_path, "wb") as file:
                    await file.write(response.content)
            except OSError as e:
                raise ImageDownloadError(
                    "Failed to write temporary image file",
                    feed_id=feed_id,
                    url=url,
                ) from e

        try:
            # Check if already JPG format
            if await self._is_jpg_format(tmp_path, feed_id, url):
                # Already JPG, just move the file
                try:
                    await aiofiles.os.rename(tmp_path, final_path)
                except OSError as e:
                    raise ImageDownloadError(
                        "Failed to move JPG file to final location",
                        feed_id=feed_id,
                        url=url,
                    ) from e
            else:
                # Convert to JPG format
                await self._convert_to_jpg(tmp_path, final_path, feed_id, url)
        finally:
            # Clean up temporary file
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                logger.warning(
                    "Failed to clean up temporary image file",
                    extra={"feed_id": feed_id, "tmp_path": str(tmp_path)},
                )

        return "jpg"

    async def download_feed_image_ytdlp(
        self,
        feed_id: str,
        source_type: SourceType,
        source_url: str,
        resolved_url: str | None,
        user_yt_cli_args: list[str],
        cookies_path: Path | None = None,
    ) -> str | None:
        """Download feed image using yt-dlp wrapper.

        Args:
            feed_id: Feed identifier for storage path.
            source_type: Source type for ytdlp.
            source_url: Source URL for ytdlp.
            resolved_url: Resolved URL for ytdlp.
            user_yt_cli_args: User CLI args for ytdlp.
            cookies_path: Optional cookies file path.

        Returns:
            Extension string (e.g., "jpg") if successful, None if failed.
        """
        try:
            return await self._ytdlp_wrapper.download_feed_thumbnail(
                feed_id=feed_id,
                source_type=source_type,
                source_url=source_url,
                resolved_url=resolved_url,
                user_yt_cli_args=user_yt_cli_args,
                cookies_path=cookies_path,
            )
        except YtdlpApiError as e:
            raise ImageDownloadError(
                "Failed to download feed image using yt-dlp",
                feed_id=feed_id,
                url=source_url,
            ) from e
