# pyright: reportPrivateUsage=false

"""Tests for the clip_streamer module."""

from pathlib import Path

import pytest

from anypod.clip_streamer import (
    ClipRange,
    _build_ffmpeg_clip_command,
    _format_timestamp_for_ffmpeg,
    _get_output_format_for_extension,
    generate_clip_filename,
    get_clip_content_type,
    parse_timestamp,
    validate_clip_range,
)


class TestParseTimestamp:
    """Tests for parse_timestamp function."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "input_value,expected",
        [
            # Plain seconds
            ("0", 0.0),
            ("30", 30.0),
            ("90", 90.0),
            ("90.5", 90.5),
            ("123.456", 123.456),
            # MM:SS format
            ("1:30", 90.0),
            ("01:30", 90.0),
            ("10:00", 600.0),
            ("1:30.5", 90.5),
            # HH:MM:SS format
            ("1:00:00", 3600.0),
            ("01:30:00", 5400.0),
            ("0:01:30", 90.0),
            ("1:30:00.5", 5400.5),
        ],
    )
    def test_valid_timestamps(self, input_value: str, expected: float) -> None:
        """Test parsing valid timestamp formats."""
        result = parse_timestamp(input_value)
        assert abs(result - expected) < 0.001

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "input_value",
        [
            "-1",  # Negative
            "abc",  # Not a number
            "1:2:3:4",  # Too many parts
            "",  # Empty
            ":",  # Just colon
            "1:-30",  # Negative component
        ],
    )
    def test_invalid_timestamps(self, input_value: str) -> None:
        """Test that invalid timestamps raise ValueError."""
        with pytest.raises(ValueError):
            parse_timestamp(input_value)


class TestValidateClipRange:
    """Tests for validate_clip_range function."""

    @pytest.mark.unit
    def test_valid_range(self) -> None:
        """Test creating a valid clip range."""
        clip = validate_clip_range(10.0, 60.0)
        assert clip.start_seconds == 10.0
        assert clip.end_seconds == 60.0
        assert clip.duration_seconds == 50.0

    @pytest.mark.unit
    def test_end_before_start(self) -> None:
        """Test that end before start raises ValueError."""
        with pytest.raises(ValueError, match="End time must be greater"):
            validate_clip_range(60.0, 30.0)

    @pytest.mark.unit
    def test_equal_start_end(self) -> None:
        """Test that equal start and end raises ValueError."""
        with pytest.raises(ValueError, match="End time must be greater"):
            validate_clip_range(30.0, 30.0)

    @pytest.mark.unit
    def test_negative_start(self) -> None:
        """Test that negative start raises ValueError."""
        with pytest.raises(ValueError, match="Start time cannot be negative"):
            validate_clip_range(-10.0, 30.0)

    @pytest.mark.unit
    def test_negative_end(self) -> None:
        """Test that negative end raises ValueError."""
        with pytest.raises(ValueError, match="End time cannot be negative"):
            validate_clip_range(10.0, -30.0)

    @pytest.mark.unit
    def test_exceeds_max_duration(self) -> None:
        """Test that clip exceeding max duration raises ValueError."""
        # MAX_CLIP_DURATION_SECONDS is 3600 (1 hour)
        with pytest.raises(ValueError, match="exceeds maximum"):
            validate_clip_range(0, 4000)

    @pytest.mark.unit
    def test_start_beyond_media_duration(self) -> None:
        """Test that start beyond media duration raises ValueError."""
        with pytest.raises(ValueError, match="is beyond media duration"):
            validate_clip_range(100.0, 150.0, media_duration=90.0)


class TestClipRange:
    """Tests for ClipRange dataclass."""

    @pytest.mark.unit
    def test_duration_calculation(self) -> None:
        """Test duration property calculation."""
        clip = ClipRange(start_seconds=10.0, end_seconds=70.0)
        assert clip.duration_seconds == 60.0

    @pytest.mark.unit
    def test_frozen(self) -> None:
        """Test that ClipRange is immutable."""
        clip = ClipRange(start_seconds=10.0, end_seconds=60.0)
        with pytest.raises(AttributeError):
            clip.start_seconds = 20.0  # type: ignore[misc]


class TestFormatTimestampForFfmpeg:
    """Tests for _format_timestamp_for_ffmpeg function."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "seconds,expected",
        [
            (0, "00:00:00.000"),
            (30, "00:00:30.000"),
            (90, "00:01:30.000"),
            (3600, "01:00:00.000"),
            (3661.5, "01:01:01.500"),
            (7321.123, "02:02:01.123"),
        ],
    )
    def test_formatting(self, seconds: float, expected: str) -> None:
        """Test timestamp formatting for FFmpeg."""
        result = _format_timestamp_for_ffmpeg(seconds)
        assert result == expected


class TestGetOutputFormatForExtension:
    """Tests for _get_output_format_for_extension function."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "ext,expected_format,has_movflags",
        [
            # Audio-only formats
            ("mp3", "mp3", False),
            ("ogg", "ogg", False),
            ("opus", "opus", False),
            ("flac", "flac", False),
            ("wav", "wav", False),
            # Video/container formats
            ("mp4", "mp4", True),
            ("m4a", "mp4", True),
            ("mkv", "mp4", True),
            ("webm", "mp4", True),
        ],
    )
    def test_format_detection(
        self, ext: str, expected_format: str, has_movflags: bool
    ) -> None:
        """Test output format detection."""
        fmt, movflags = _get_output_format_for_extension(ext)
        assert fmt == expected_format
        assert (len(movflags) > 0) == has_movflags


class TestBuildFfmpegClipCommand:
    """Tests for _build_ffmpeg_clip_command function."""

    @pytest.mark.unit
    def test_basic_command_structure(self) -> None:
        """Test basic FFmpeg command structure."""
        clip = ClipRange(start_seconds=30, end_seconds=60)
        cmd = _build_ffmpeg_clip_command(
            Path("/test/file.mp4"), clip, "mp4", "frag_keyframe+empty_moov"
        )

        assert cmd[0] == "ffmpeg"
        assert "-hide_banner" in cmd
        assert "-loglevel" in cmd
        assert "-ss" in cmd
        assert "-i" in cmd
        assert "/test/file.mp4" in cmd
        assert "-t" in cmd
        assert "30" in cmd  # Duration
        assert "-movflags" in cmd
        assert "pipe:1" in cmd

    @pytest.mark.unit
    def test_audio_only_command(self) -> None:
        """Test FFmpeg command for audio-only format."""
        clip = ClipRange(start_seconds=0, end_seconds=30)
        cmd = _build_ffmpeg_clip_command(Path("/test/file.mp3"), clip, "mp3", "")

        assert cmd[0] == "ffmpeg"
        assert "-movflags" not in cmd
        assert "-f" in cmd
        assert "mp3" in cmd


class TestGetClipContentType:
    """Tests for get_clip_content_type function."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "ext,expected_type",
        [
            # Audio formats
            ("mp3", "audio/mpeg"),
            ("ogg", "audio/ogg"),
            ("opus", "audio/opus"),
            ("flac", "audio/flac"),
            ("wav", "audio/wav"),
            # MP4-based audio
            ("m4a", "audio/mp4"),
            ("aac", "audio/mp4"),
            # Video formats
            ("mp4", "video/mp4"),
            ("mkv", "video/mp4"),
            ("webm", "video/mp4"),
        ],
    )
    def test_content_type_detection(self, ext: str, expected_type: str) -> None:
        """Test content type detection for clips."""
        result = get_clip_content_type(ext)
        assert result == expected_type


class TestGenerateClipFilename:
    """Tests for generate_clip_filename function."""

    @pytest.mark.unit
    def test_basic_filename(self) -> None:
        """Test basic clip filename generation."""
        clip = ClipRange(start_seconds=30, end_seconds=90)
        result = generate_clip_filename("video.mp4", clip)
        assert result == "video_clip_30-90.mp4"

    @pytest.mark.unit
    def test_audio_filename(self) -> None:
        """Test audio clip filename generation."""
        clip = ClipRange(start_seconds=0, end_seconds=60)
        result = generate_clip_filename("audio.mp3", clip)
        assert result == "audio_clip_0-60.mp3"

    @pytest.mark.unit
    def test_m4a_becomes_mp4(self) -> None:
        """Test that m4a clips become mp4."""
        clip = ClipRange(start_seconds=10, end_seconds=30)
        result = generate_clip_filename("podcast.m4a", clip)
        # m4a is processed as mp4 but output extension stays mp4
        assert result == "podcast_clip_10-30.mp4"

    @pytest.mark.unit
    def test_fractional_seconds_truncated(self) -> None:
        """Test that fractional seconds are truncated in filename."""
        clip = ClipRange(start_seconds=10.7, end_seconds=30.2)
        result = generate_clip_filename("video.mp4", clip)
        assert result == "video_clip_10-30.mp4"

    @pytest.mark.unit
    def test_filename_without_extension(self) -> None:
        """Test handling filename without extension."""
        clip = ClipRange(start_seconds=0, end_seconds=30)
        result = generate_clip_filename("video", clip)
        # Should default to mp4
        assert result == "video_clip_0-30.mp4"
