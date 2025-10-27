"""Image downloading functionality for feed and download thumbnails."""

import logging
from pathlib import Path

import aiofiles
import aiofiles.os
import httpx

from .db.types import SourceType
from .exceptions import FFmpegError, FFProbeError, ImageDownloadError, YtdlpApiError
from .ffmpeg import FFmpeg
from .ffprobe import FFProbe
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

    def __init__(
        self,
        paths: PathManager,
        ytdlp_wrapper: YtdlpWrapper,
        ffprobe: FFProbe,
        ffmpeg: FFmpeg,
    ):
        self._paths = paths
        self._ytdlp_wrapper = ytdlp_wrapper
        self._ffprobe = ffprobe
        self._ffmpeg = ffmpeg
        logger.debug("ImageDownloader initialized.")

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
            await self._ffmpeg.convert_image_to_jpg(input_path, output_path)
        except FFmpegError as e:
            raise ImageDownloadError(
                "Image conversion to JPG failed",
                feed_id=feed_id,
                url=url,
            ) from e

    async def download_feed_image_direct(self, feed_id: str, url: str) -> str:
        """Download feed image directly via HTTP.

        Args:
            feed_id: Feed identifier for storage path.
            url: Image URL to download.

        Returns:
            Extension string (e.g., "jpg").

        Raises:
            ImageDownloadError: If the image cannot be downloaded or stored.
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
            try:
                is_jpg = await self._ffprobe.is_jpg_file(tmp_path)
            except FFProbeError as e:
                raise ImageDownloadError(
                    "Format detection failed",
                    feed_id=feed_id,
                    url=url,
                ) from e

            if is_jpg:
                # Already JPG, just move the file
                try:
                    await aiofiles.os.replace(tmp_path, final_path)
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
