"""Core yt-dlp wrapper functionality and typed data access."""

import asyncio
from dataclasses import dataclass
import json
import logging

from ...exceptions import YtdlpApiError
from .args import YtdlpArgs
from .info import YtdlpInfo

logger = logging.getLogger(__name__)


def _format_run_output(stdout: str, stderr: str) -> str:
    """Format stdout and stderr content with section headers."""
    sections: list[str] = []
    if stdout:
        sections.append(f"STDOUT:\n{stdout}")
    if stderr:
        sections.append(f"STDERR:\n{stderr}")
    return "\n\n".join(sections)


@dataclass(frozen=True, slots=True)
class YtdlpRunResult[T]:
    """Container for yt-dlp subprocess payloads and raw log output."""

    payload: T
    logs: str | None


class YtdlpCore:
    """Static methods for core yt-dlp operations.

    Provides a clean interface to yt-dlp functionality including option
    parsing, metadata extraction, and media downloading with proper
    error handling and conversion to application-specific exceptions.
    """

    @staticmethod
    async def extract_playlist_info(
        args: YtdlpArgs, url: str
    ) -> YtdlpRunResult[YtdlpInfo]:
        """Extract playlist metadata without downloading media.

        Args:
            args: YtdlpArgs object containing command-line arguments for yt-dlp.
            url: URL to extract information from.

        Returns:
            YtdlpRunResult containing playlist metadata and raw yt-dlp logs.

        Raises:
            YtdlpApiError: If extraction fails or an unexpected error occurs.
        """
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

        stdout_text = stdout.decode("utf-8", errors="replace") if stdout else ""
        stderr_text = stderr.decode("utf-8", errors="replace") if stderr else ""
        combined_logs = _format_run_output(stdout_text, stderr_text)

        if proc.returncode != 0 and proc.returncode != 101:
            raise YtdlpApiError(
                message=f"yt-dlp completed with error {proc.returncode}: {stderr_text}",
                url=url,
                logs=combined_logs,
            )
        if not stdout_text:
            raise YtdlpApiError(
                message="yt-dlp did not produce any output",
                url=url,
                logs=combined_logs,
            )

        try:
            extracted_info = json.loads(stdout_text)
        except json.JSONDecodeError as e:
            raise YtdlpApiError(
                message="Failed to parse yt-dlp JSON output",
                url=url,
                logs=combined_logs,
            ) from e

        return YtdlpRunResult(
            payload=YtdlpInfo(extracted_info),
            logs=combined_logs,
        )

    @staticmethod
    async def extract_downloads_info(
        args: YtdlpArgs, url: str
    ) -> YtdlpRunResult[list[YtdlpInfo]]:
        """Extract download metadata without downloading media content.

        Args:
            args: YtdlpArgs object containing command-line arguments for yt-dlp.
            url: URL to extract information from.

        Returns:
            YtdlpRunResult containing downloads metadata and raw yt-dlp logs.

        Raises:
            YtdlpApiError: If extraction fails or an unexpected error occurs.
        """
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

        stdout_text = stdout.decode("utf-8", errors="replace").strip()
        stderr_text = stderr.decode("utf-8", errors="replace") if stderr else ""
        combined_logs = _format_run_output(stdout_text, stderr_text)

        entries: list[YtdlpInfo] = []
        for line in stdout_text.splitlines():
            stripped_line = line.strip()
            if not stripped_line:
                continue
            try:
                entry = json.loads(stripped_line)
                entries.append(YtdlpInfo(entry))
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON line: {stripped_line[:100]}")
                continue

        if proc.returncode != 0:
            if stderr_text.strip():
                for line in stderr_text.strip().splitlines():
                    clean_line = line.strip()
                    if clean_line:
                        logger.warning(
                            f"yt-dlp error: ${clean_line}",
                            extra={
                                "exit_code": proc.returncode,
                            },
                        )
            if not entries and proc.returncode != 101:  # 101 == filtered out
                logger.warning(
                    "yt-dlp completed with errors and extracted no entries.",
                    extra={
                        "exit_code": proc.returncode,
                        "url": url,
                    },
                )

        return YtdlpRunResult(payload=entries, logs=combined_logs)

    @staticmethod
    async def download(args: YtdlpArgs, url: str) -> str:
        """Download media from a URL using yt-dlp subprocess.

        Args:
            args: YtdlpArgs object containing command-line arguments for yt-dlp.
            url: URL to download media from.

        Returns:
            Combined stdout/stderr log text emitted by yt-dlp.

        Raises:
            YtdlpApiError: If download fails or returns a non-zero exit code.
        """
        # Build subprocess command:
        cli_cmd_prefix = args.no_warnings().no_progress().to_list()
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
            stdout, stderr = await proc.communicate()
        except asyncio.CancelledError:
            # Ensure subprocess cleanup on cancellation
            proc.kill()
            raise
        finally:
            await proc.wait()

        stdout_text = stdout.decode("utf-8", errors="replace") if stdout else ""
        stderr_text = stderr.decode("utf-8", errors="replace") if stderr else ""
        combined_logs = _format_run_output(stdout_text, stderr_text)

        if proc.returncode != 0:
            raise YtdlpApiError(
                message=(
                    f"Download failed with exit code {proc.returncode}: {stderr_text}"
                ),
                url=url,
                logs=combined_logs or None,
            )
        return combined_logs
