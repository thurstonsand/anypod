"""Core yt-dlp wrapper functionality and typed data access."""

import asyncio
import json
import logging

from ...exceptions import YtdlpApiError
from .args import YtdlpArgs
from .info import YtdlpInfo

logger = logging.getLogger(__name__)


class YtdlpCore:
    """Static methods for core yt-dlp operations.

    Provides a clean interface to yt-dlp functionality including option
    parsing, metadata extraction, and media downloading with proper
    error handling and conversion to application-specific exceptions.
    """

    @staticmethod
    async def extract_playlist_info(args: YtdlpArgs, url: str) -> YtdlpInfo:
        """Extract playlist metadata only -- no download metadata.

        Args:
            args: YtdlpArgs object containing command-line arguments for yt-dlp.
            url: URL to extract information from.

        Returns:
            YtdlpInfo object with extracted playlist metadata.

        Raises:
            YtdlpApiError: If extraction fails or an unexpected error occurs.
        """
        # Build subprocess command
        cli_cmd_prefix = (
            args.quiet()
            .no_warnings()
            .dump_single_json()
            .flat_playlist()
            .skip_download()
            .to_list()
        )
        cmd = [*cli_cmd_prefix, url]

        logger.debug(
            "Running yt-dlp for playlist metadata extraction", extra={"cmd": cmd}
        )

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
            raise
        finally:
            await proc.wait()

        # Parse JSON output first - yt-dlp can produce valid output even with non-zero exit codes
        logger.debug(
            "yt-dlp process completed.",
            extra={
                "exit_code": proc.returncode,
                "stdout_length": len(stdout) if stdout else 0,
                "stderr_length": len(stderr) if stderr else 0,
                "has_stdout": bool(stdout),
            },
        )

        if proc.returncode != 0:
            raise YtdlpApiError(
                message=f"yt-dlp completed with error {proc.returncode}: {stderr.decode('utf-8', errors='replace')}",
                url=url,
            )
        elif not stdout:
            raise YtdlpApiError(
                message="yt-dlp did not produce any output",
                url=url,
            )

        try:
            extracted_info = json.loads(stdout.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise YtdlpApiError(
                message="Failed to parse yt-dlp JSON output",
                url=url,
            ) from e

        return YtdlpInfo(extracted_info)  # type: ignore

    @staticmethod
    async def extract_downloads_info(args: YtdlpArgs, url: str) -> list[YtdlpInfo]:
        """Extract download metadata only -- no playlist or channel metadata.

        Args:
            args: YtdlpArgs object containing command-line arguments for yt-dlp.
            url: URL to extract information from.

        Returns:
            List of YtdlpInfo objects, each representing a download.

        Raises:
            YtdlpApiError: If extraction fails or an unexpected error occurs.
        """
        # Build subprocess command
        cli_cmd_prefix = (
            args.quiet().no_warnings().dump_json().skip_download().to_list()
        )
        cmd = [*cli_cmd_prefix, url]

        logger.debug(
            "Running yt-dlp for filtered downloads extraction", extra={"cmd": cmd}
        )

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
            proc.kill()
            raise
        finally:
            await proc.wait()

        logger.debug(
            "yt-dlp process completed.",
            extra={
                "exit_code": proc.returncode,
                "stdout_length": len(stdout) if stdout else 0,
                "stderr_length": len(stderr) if stderr else 0,
                "has_stdout": bool(stdout),
            },
        )

        # 101 means "Download cancelled by some flag, e.g. --break-match-filter etc" - treat as success
        if proc.returncode != 0 and proc.returncode != 101:
            raise YtdlpApiError(
                message=f"yt-dlp completed with error {proc.returncode}: {stderr.decode('utf-8', errors='replace')}",
                url=url,
            )

        stdout_text = stdout.decode("utf-8").strip()

        # Parse multiple JSON objects (one per video)
        entries: list[YtdlpInfo] = []
        for line in stdout_text.split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                entries.append(YtdlpInfo(entry))  # type: ignore
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON line: {line[:100]}")
                continue

        return entries

    @staticmethod
    async def download(args: YtdlpArgs, url: str) -> None:
        """Download media from a URL using yt-dlp subprocess.

        Args:
            args: YtdlpArgs object containing command-line arguments for yt-dlp.
            url: URL to download media from.

        Raises:
            YtdlpApiError: If download fails or returns a non-zero exit code.
        """
        # Build subprocess command:
        cli_cmd_prefix = args.quiet().no_warnings().to_list()
        cmd = [*cli_cmd_prefix, url]

        logger.debug("Running yt-dlp for download", extra={"cmd": cmd})

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
            raise
        finally:
            await proc.wait()

        if proc.returncode != 0:
            raise YtdlpApiError(
                message=f"Download failed with exit code {proc.returncode}: {stderr.decode('utf-8', errors='replace')}",
                url=url,
            )
