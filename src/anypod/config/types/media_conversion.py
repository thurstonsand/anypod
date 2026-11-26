"""Media conversion preset enum for Anypod feed configuration."""

from enum import StrEnum


class MediaConversionPreset(StrEnum):
    """Built-in media post-processing presets for downloaded episodes.

    Attributes:
        OFF: Leave downloaded media untouched (raw yt-dlp output).
        LIGHT: Fast, low-CPU podcast-compatible MP4/M4A (stream-copy compatible codecs,
            re-encode non-AAC audio, remux with faststart).
        OPTIMIZED: Compact modern codecs (prefer AV1/HEVC; transcode other video to
            H.265 hvc1 and non-AAC audio to 96k AAC in faststart MP4).
        CUSTOM: Run a user-provided post-processing hook script.
    """

    OFF = "off"
    LIGHT = "light"
    OPTIMIZED = "optimized"
    CUSTOM = "custom"
