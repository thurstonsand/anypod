"""Types for selective artifact downloads.

This module defines types for controlling which artifacts are downloaded
during download operations and tracking results per artifact type.
"""

from dataclasses import dataclass, field
from enum import Flag, auto

from anypod.exceptions import AnypodError


class DownloadArtifact(Flag):
    """Flag enum controlling which artifacts to download.

    Combinable via bitwise operations for flexible artifact selection.

    Attributes:
        NONE: No artifacts (useful for initialization).
        MEDIA: Download media file (audio/video).
        THUMBNAIL: Download thumbnail image.
        TRANSCRIPT: Download transcript/subtitles.
        ALL: Download all artifacts (media + thumbnail + transcript).
    """

    NONE = 0
    MEDIA = auto()
    THUMBNAIL = auto()
    TRANSCRIPT = auto()
    ALL = MEDIA | THUMBNAIL | TRANSCRIPT


@dataclass
class ArtifactDownloadResult:
    """Track success/failure for each requested artifact type.

    Each field is tri-state:
        - True: Artifact was downloaded successfully
        - False: Artifact download was attempted but failed
        - None: Artifact was not requested

    Attributes:
        media_downloaded: Media download result.
        thumbnail_downloaded: Thumbnail download result.
        transcript_downloaded: Transcript download result.
        errors: List of errors that occurred during download attempts.
    """

    media_downloaded: bool | None = None
    thumbnail_downloaded: bool | None = None
    transcript_downloaded: bool | None = None
    errors: list[AnypodError] = field(default_factory=list)

    @property
    def all_succeeded(self) -> bool:
        """Return True if all requested artifacts were downloaded successfully.

        Unrequested artifacts (None) do not affect this result.
        """
        requested_results = [
            result
            for result in [
                self.media_downloaded,
                self.thumbnail_downloaded,
                self.transcript_downloaded,
            ]
            if result is not None
        ]
        return all(requested_results) if requested_results else True
