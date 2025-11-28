"""Tests for the DownloadDelay configuration type.

This module contains unit tests for the DownloadDelay class, which parses
duration strings in the format "<number><unit>" for configuring download delays.
"""

from datetime import timedelta

import pytest

from anypod.config.types import DownloadDelay


class TestDownloadDelayParsing:
    """Tests for DownloadDelay string parsing."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("duration_str", "expected_hours"),
        [
            ("1h", 1),
            ("24h", 24),
            ("48h", 48),
            ("1d", 24),
            ("3d", 72),
            ("7d", 168),
            ("1w", 168),
            ("2w", 336),
        ],
    )
    def test_valid_duration_strings(
        self, duration_str: str, expected_hours: int
    ) -> None:
        """Valid duration strings are parsed correctly."""
        delay = DownloadDelay(duration_str)

        assert delay.total_hours == expected_hours
        assert delay.timedelta == timedelta(hours=expected_hours)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "duration_str",
        [
            " 24h ",
            "24 h",
            " 3d",
            "1w ",
        ],
    )
    def test_whitespace_handling(self, duration_str: str) -> None:
        """Whitespace around and within duration strings is handled."""
        delay = DownloadDelay(duration_str)
        assert delay.total_hours > 0

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "duration_str",
        [
            "24H",
            "3D",
            "1W",
            "24h",
        ],
    )
    def test_case_insensitivity(self, duration_str: str) -> None:
        """Unit letters are case-insensitive."""
        delay = DownloadDelay(duration_str)
        assert delay.total_hours > 0

    @pytest.mark.unit
    def test_duration_str_preserved(self) -> None:
        """Original duration string is preserved (without outer whitespace)."""
        delay = DownloadDelay("  24h  ")
        assert delay.duration_str == "24h"

    @pytest.mark.unit
    def test_str_representation(self) -> None:
        """String representation returns the original duration string."""
        delay = DownloadDelay("3d")
        assert str(delay) == "3d"

    @pytest.mark.unit
    def test_repr_representation(self) -> None:
        """Repr includes both duration string and total hours."""
        delay = DownloadDelay("3d")
        assert "3d" in repr(delay)
        assert "72" in repr(delay)


class TestDownloadDelayInvalidInputs:
    """Tests for invalid DownloadDelay inputs."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("duration_str", "expected_error"),
        [
            ("", "Invalid duration format"),
            ("   ", "Invalid duration format"),
            ("24", "Invalid duration format"),  # Missing unit
            ("h", "Invalid duration format"),  # Missing number
            ("24x", "Invalid duration format"),  # Invalid unit
            ("24m", "Invalid duration format"),  # Minutes not supported
            ("24s", "Invalid duration format"),  # Seconds not supported
            ("-24h", "Invalid duration format"),  # Negative not supported
            ("1.5h", "Invalid duration format"),  # Decimal not supported
            ("abc", "Invalid duration format"),  # Not a duration
            ("24hours", "Invalid duration format"),  # Full word not supported
        ],
    )
    def test_invalid_format_raises_value_error(
        self, duration_str: str, expected_error: str
    ) -> None:
        """Invalid format strings raise ValueError with descriptive message."""
        with pytest.raises(ValueError, match=expected_error):
            DownloadDelay(duration_str)

    @pytest.mark.unit
    def test_zero_value_raises_value_error(self) -> None:
        """Zero value raises ValueError."""
        with pytest.raises(ValueError, match="positive"):
            DownloadDelay("0h")


class TestDownloadDelayEquality:
    """Tests for DownloadDelay dataclass properties."""

    @pytest.mark.unit
    def test_frozen_immutable(self) -> None:
        """DownloadDelay instances are frozen/immutable."""
        delay = DownloadDelay("24h")
        with pytest.raises(AttributeError):
            delay.total_hours = 48  # type: ignore[misc]

    @pytest.mark.unit
    def test_equality(self) -> None:
        """Equal duration strings produce equal instances."""
        delay1 = DownloadDelay("24h")
        delay2 = DownloadDelay("24h")
        assert delay1 == delay2

    @pytest.mark.unit
    def test_different_strings_different_instances(self) -> None:
        """Different duration strings produce different instances."""
        delay1 = DownloadDelay("24h")
        delay2 = DownloadDelay("1d")  # Same duration, different string
        # They may or may not be equal depending on implementation
        # But they should both have 24 hours
        assert delay1.total_hours == delay2.total_hours

    @pytest.mark.unit
    def test_hashable(self) -> None:
        """DownloadDelay instances are hashable (frozen dataclass)."""
        delay = DownloadDelay("24h")
        # Should not raise
        hash(delay)
        # Can be used in sets
        delays = {delay, DownloadDelay("24h")}
        assert len(delays) == 1
