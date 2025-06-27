"""Core yt-dlp wrapper functionality and typed data access."""

import asyncio
import json

from ...exceptions import YtdlpApiError
from .args import YtdlpArgs
from .info import YtdlpInfo


class YtdlpCore:
    """Static methods for core yt-dlp operations.

    Provides a clean interface to yt-dlp functionality including option
    parsing, metadata extraction, and media downloading with proper
    error handling and conversion to application-specific exceptions.
    """

    @staticmethod
    async def extract_info(args: YtdlpArgs, url: str) -> YtdlpInfo | None:
        """Extract metadata information from a URL using yt-dlp subprocess.

        Must have yt-dlp binary installed and in PATH.

        Args:
            args: YtdlpArgs object containing command-line arguments for yt-dlp.
            url: URL to extract information from.

        Returns:
            YtdlpInfo object with extracted metadata, or None if extraction failed.

        Raises:
            YtdlpApiError: If extraction fails or an unexpected error occurs.
        """
        # Build subprocess command
        cli_args = args.dump_single_json().no_download().to_list()
        cmd = ["yt-dlp", *cli_args, url]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as e:
            raise YtdlpApiError(
                message="yt-dlp executable not found. Please ensure yt-dlp is installed and in PATH.",
                url=url,
            ) from e

        try:
            stdout, stderr = await proc.communicate()
        except asyncio.CancelledError:
            # Ensure subprocess cleanup on cancellation
            proc.kill()
            await proc.wait()
            raise

        if proc.returncode != 0:
            raise YtdlpApiError(
                message=f"yt-dlp failed with exit code {proc.returncode}: {stderr.decode('utf-8', errors='replace')}",
                url=url,
            )

        # Parse JSON output
        if not stdout:
            return None

        try:
            extracted_info = json.loads(stdout.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise YtdlpApiError(
                message="Failed to parse yt-dlp JSON output",
                url=url,
            ) from e

        return YtdlpInfo(extracted_info)  # type: ignore

    @staticmethod
    async def download(args: YtdlpArgs, url: str) -> None:
        """Download media from a URL using yt-dlp subprocess.

        Args:
            args: YtdlpArgs object containing command-line arguments for yt-dlp.
            url: URL to download media from.

        Raises:
            YtdlpApiError: If download fails or returns a non-zero exit code.
        """
        # Build subprocess command: yt-dlp + args + url
        cmd = ["yt-dlp", *args.to_list(), url]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as e:
            raise YtdlpApiError(
                message="yt-dlp executable not found. Please ensure yt-dlp is installed and in PATH.",
                url=url,
            ) from e

        try:
            _, stderr = await proc.communicate()
        except asyncio.CancelledError:
            # Ensure subprocess cleanup on cancellation
            proc.kill()
            await proc.wait()
            raise

        if proc.returncode != 0:
            raise YtdlpApiError(
                message=f"Download failed with exit code {proc.returncode}: {stderr.decode('utf-8', errors='replace')}",
                url=url,
            )
