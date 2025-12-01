"""Downloaded media result type."""

from dataclasses import dataclass
from pathlib import Path

from .transcript_info import TranscriptInfo


@dataclass(frozen=True, slots=True)
class DownloadedMedia:
    """Result of downloading media via yt-dlp.

    Attributes:
        file_path: Path to the downloaded media file.
        logs: Combined stdout/stderr logs from the download process.
        transcript: Transcript metadata if a transcript was downloaded, None otherwise.
    """

    file_path: Path
    logs: str
    transcript: TranscriptInfo | None
