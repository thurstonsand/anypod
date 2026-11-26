# pyright: reportPrivateUsage=false

"""Tests for the configuration loading and validation functionality."""

# tests/test_config.py
from datetime import date, timedelta
import os
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

from pydantic import ValidationError
import pytest
import yaml

from anypod.config.config import (
    AppSettings,
    DynamicYamlConfigSettingsSource,
    FeedConfig,
)
from anypod.config.types import FeedMetadataOverrides, MediaConversionPreset
from anypod.exceptions import ConfigLoadError

# --- Tests for AppSettings configuration loading ---

# Sample valid feed configuration data for testing
SAMPLE_FEEDS_DATA = {
    "feeds": {
        "podcast1": {
            "url": "https://example.com/feed1.xml",
            "schedule": "0 0 * * *",
            "keep_last": 10,
            "max_errors": 5,  # Explicitly set for testing
        },
        "podcast2": {
            "url": "https://example.com/feed2.xml",
            "schedule": "0 12 * * *",
            "yt_args": "--format bestaudio",
            "keep_last": 3,
            # max_errors will use default (3) for this one
        },
    }
}

# Expected parsed yt_args for podcast2
EXPECTED_PODCAST2_YT_ARGS = ["--format", "bestaudio"]

# Example POT provider URL used for environment override tests
POT_PROVIDER_URL_VALUE = "http://pot.example:4416"


@pytest.fixture
def sample_config_file(tmp_path: Path) -> Path:
    """Creates a sample YAML config file in a temporary directory."""
    config_path = tmp_path / "test_feeds.yaml"
    with Path.open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(SAMPLE_FEEDS_DATA, f)
    return config_path


@patch.object(DynamicYamlConfigSettingsSource, "_resolve_config_path")
def test_load_from_default_location(mock_get_yaml_path: Mock, tmp_path: Path):
    """Tests if AppSettings loads configuration from the default file path when no overrides are provided."""
    default_config_path = tmp_path / "default_feeds.yaml"
    with Path.open(default_config_path, "w", encoding="utf-8") as f:
        yaml.dump(SAMPLE_FEEDS_DATA, f)

    # Configure the mock to return our test config path
    mock_get_yaml_path.return_value = default_config_path

    settings = AppSettings()  # type: ignore

    assert len(settings.feeds) == len(SAMPLE_FEEDS_DATA["feeds"])
    assert settings.pot_provider_url is None, "pot_provider_url should default to None"
    assert "podcast1" in settings.feeds
    assert (
        settings.feeds["podcast1"].url == SAMPLE_FEEDS_DATA["feeds"]["podcast1"]["url"]
    )
    assert (
        str(settings.feeds["podcast1"].schedule)
        == SAMPLE_FEEDS_DATA["feeds"]["podcast1"]["schedule"]
    )
    assert (
        settings.feeds["podcast1"].keep_last
        == SAMPLE_FEEDS_DATA["feeds"]["podcast1"]["keep_last"]
    )
    assert (
        settings.feeds["podcast1"].max_errors
        == SAMPLE_FEEDS_DATA["feeds"]["podcast1"]["max_errors"]
    ), "'max_errors' for 'podcast1' should match sample data (5)"

    assert "podcast2" in settings.feeds
    assert (
        settings.feeds["podcast2"].url == SAMPLE_FEEDS_DATA["feeds"]["podcast2"]["url"]
    )
    assert settings.feeds["podcast2"].yt_args == EXPECTED_PODCAST2_YT_ARGS
    assert (
        str(settings.feeds["podcast2"].schedule)
        == SAMPLE_FEEDS_DATA["feeds"]["podcast2"]["schedule"]
    )
    assert settings.feeds["podcast2"].max_errors == 3, (  # Asserting default value
        "'max_errors' for 'podcast2' should be the default value (3)"
    )
    assert (
        settings.feeds["podcast2"].keep_last
        == SAMPLE_FEEDS_DATA["feeds"]["podcast2"]["keep_last"]
    )

    dumped_settings = settings.model_dump()
    assert "config_file" in dumped_settings, (
        "'config_file' should be present in model_dump even tho it is not used anywhere"
    )
    assert "data_dir" in dumped_settings, "'data_dir' should be present in model_dump"
    assert "feeds" in dumped_settings
    assert "pot_provider_url" in dumped_settings, (
        "'pot_provider_url' should be present in model_dump to reflect default None"
    )


@patch.dict(os.environ, {"CONFIG_FILE": ""})
def test_override_location_with_env_var(sample_config_file: Path):
    """Tests if AppSettings loads configuration from the path specified by the CONFIG_FILE environment variable."""
    # Set the CONFIG_FILE environment variable to our test config file path
    os.environ["CONFIG_FILE"] = str(sample_config_file)
    # Set POT provider URL via environment
    os.environ["POT_PROVIDER_URL"] = POT_PROVIDER_URL_VALUE

    settings = AppSettings()  # type: ignore

    assert len(settings.feeds) == len(SAMPLE_FEEDS_DATA["feeds"])
    assert "podcast1" in settings.feeds, (
        "Feed 'podcast1' should be loaded when overridden by env var"
    )
    assert (
        settings.feeds["podcast1"].url == SAMPLE_FEEDS_DATA["feeds"]["podcast1"]["url"]
    )

    assert "podcast2" in settings.feeds, (
        "Feed 'podcast2' should be loaded when overridden by env var"
    )
    assert (
        settings.feeds["podcast2"].url == SAMPLE_FEEDS_DATA["feeds"]["podcast2"]["url"]
    )
    assert settings.pot_provider_url == POT_PROVIDER_URL_VALUE, (
        "pot_provider_url should be sourced from POT_PROVIDER_URL env var"
    )


@patch.dict(os.environ, {"CONFIG_FILE": "/path/to/nonexistent/file.yaml"})
def test_override_location_with_init_arg(sample_config_file: Path):
    """Tests if AppSettings loads configuration from the path specified via an initialization argument, overriding defaults and env vars."""
    settings = AppSettings(config_file=sample_config_file)

    assert len(settings.feeds) == len(SAMPLE_FEEDS_DATA["feeds"]), (
        "Number of loaded feeds should match sample data when overridden by init arg"
    )
    assert settings.pot_provider_url is None, (
        "pot_provider_url should default to None when not provided via env or YAML"
    )

    assert "podcast1" in settings.feeds, (
        "Feed 'podcast1' should be loaded when overridden by init arg"
    )
    assert (
        settings.feeds["podcast1"].url == SAMPLE_FEEDS_DATA["feeds"]["podcast1"]["url"]
    )
    assert (
        str(settings.feeds["podcast1"].schedule)
        == SAMPLE_FEEDS_DATA["feeds"]["podcast1"]["schedule"]
    )
    assert (
        settings.feeds["podcast1"].keep_last
        == SAMPLE_FEEDS_DATA["feeds"]["podcast1"]["keep_last"]
    )
    assert (
        settings.feeds["podcast1"].max_errors
        == SAMPLE_FEEDS_DATA["feeds"]["podcast1"]["max_errors"]
    ), (
        "'max_errors' for 'podcast1' should match sample data (5) when overridden by init arg"
    )

    assert "podcast2" in settings.feeds, (
        "Feed 'podcast2' should be loaded when overridden by init arg"
    )
    assert (
        settings.feeds["podcast2"].url == SAMPLE_FEEDS_DATA["feeds"]["podcast2"]["url"]
    )
    assert settings.feeds["podcast2"].yt_args == EXPECTED_PODCAST2_YT_ARGS
    assert (
        str(settings.feeds["podcast2"].schedule)
        == SAMPLE_FEEDS_DATA["feeds"]["podcast2"]["schedule"]
    )
    assert settings.feeds["podcast2"].max_errors == 3, (  # Asserting default value
        "'max_errors' for 'podcast2' should be the default value (3) when overridden by init arg"
    )
    assert (
        settings.feeds["podcast2"].keep_last
        == SAMPLE_FEEDS_DATA["feeds"]["podcast2"]["keep_last"]
    )


def test_manual_feed_requires_title():
    """Schedule 'manual' enforces metadata title override."""
    with pytest.raises(ValidationError):
        FeedConfig(
            url="https://example.com/manual",
            schedule="manual",  # type: ignore[arg-type]
            metadata=None,
        )


def test_manual_feed_parses_successfully_when_title_present():
    """Manual feeds accept schedule sentinel and expose helper flag."""
    cfg = FeedConfig(
        url="https://example.com/manual",
        schedule="manual",  # type: ignore[arg-type]
        metadata=FeedMetadataOverrides(title="Manual"),
    )
    assert cfg.is_manual is True
    assert cfg.metadata is not None
    assert cfg.metadata.title == "Manual"


def test_manual_feed_url_optional_stays_none():
    """Manual feeds may omit the URL entirely."""
    cfg = FeedConfig(
        url=None,
        schedule="manual",  # type: ignore[arg-type]
        metadata=FeedMetadataOverrides(title="Manual"),
    )
    assert cfg.url is None


def test_scheduled_feed_requires_url():
    """Non-manual feeds must define `url`."""
    with pytest.raises(ValidationError):
        FeedConfig(
            url=None,
            schedule="0 3 * * *",  # type: ignore[arg-type]
            yt_args=None,  # type: ignore[arg-type]
            metadata=None,
        )


@patch.dict(os.environ, {"CONFIG_FILE": "/path/to/hopefully/nonexistent/feeds.yaml"})
def test_nonexistent_config_file_raises_error():
    """Tests that instantiating AppSettings raises an error if the specified config file does not exist, and that the cause is FileNotFoundError."""
    with pytest.raises(
        ConfigLoadError, match="Failed to load or parse YAML configuration file"
    ) as exc_info:
        AppSettings()  # type: ignore

    assert isinstance(exc_info.value.__cause__, FileNotFoundError), (
        f"Cause of OSError should be FileNotFoundError, got {type(exc_info.value.__cause__).__name__}"
    )


def test_invalid_yaml_format_raises_error(tmp_path: Path):
    """Tests that instantiating AppSettings raises an error if the specified config file contains invalid YAML, and that the cause is a yaml.YAMLError."""
    invalid_yaml_path = tmp_path / "invalid_feeds.yaml"
    # This specific invalid YAML causes a ScannerError from PyYAML
    invalid_content = "this: is: not: valid: yaml:"
    with Path.open(invalid_yaml_path, "w", encoding="utf-8") as f:
        f.write(invalid_content)

    with pytest.raises(
        ConfigLoadError, match="Failed to load or parse YAML configuration file"
    ) as exc_info:
        AppSettings(config_file=invalid_yaml_path)

    assert isinstance(exc_info.value.__cause__, yaml.YAMLError), (
        f"Top-level OSError cause should be a yaml.YAMLError, got {type(exc_info.value.__cause__).__name__}"
    )


def test_empty_yaml_file_loads_defaults(tmp_path: Path):
    """Tests that an empty YAML file results in default settings values."""
    empty_yaml_path = tmp_path / "empty_feeds.yaml"
    with Path.open(empty_yaml_path, "w", encoding="utf-8") as f:
        f.write("")  # Empty file

    settings = AppSettings(config_file=empty_yaml_path)

    assert settings.feeds == {}, "Feeds should be empty for an empty YAML file"
    assert settings.pot_provider_url is None, (
        "pot_provider_url should default to None for an empty YAML file"
    )


def test_yaml_file_with_only_other_keys(tmp_path: Path):
    """Tests that a YAML file with keys not defined in AppSettings is ignored for the main settings, but the 'feeds' key is still processed if present."""
    config_path = tmp_path / "other_keys.yaml"
    data = {
        "some_other_key": "value",
        "another_setting": 123,
        "feeds": {
            "podcast3": {
                "url": "https://example.com/feed3.xml",
                "schedule": "0 6 * * *",
            }
        },
    }
    with Path.open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(data, f)

    settings = AppSettings(config_file=config_path)

    assert len(settings.feeds) == 1, (
        "Should load 1 feed even when other top-level keys are present"
    )
    assert "podcast3" in settings.feeds, "Feed 'podcast3' should be loaded"
    assert settings.feeds["podcast3"].url == "https://example.com/feed3.xml"
    assert settings.pot_provider_url is None, (
        "pot_provider_url should default to None when not provided"
    )
    assert not hasattr(settings, "some_other_key"), (
        "Extra key 'some_other_key' should not be an attribute on settings (extra='ignore')"
    )
    assert not hasattr(settings, "another_setting"), (
        "Extra key 'another_setting' should not be an attribute on settings (extra='ignore')"
    )


# --- Tests for AppSettings loading from YAML vs env vars ---


def test_yaml_settings_override_defaults(tmp_path: Path):
    """Settings defined in YAML override default values."""
    config_path = tmp_path / "settings.yaml"
    yaml_data: dict[str, Any] = {
        "log_level": "DEBUG",
        "log_format": "human",
        "server_port": 9999,
        "base_url": "https://custom.example.com",
        "feeds": {},
    }
    with Path.open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(yaml_data, f)

    settings = AppSettings(config_file=config_path)

    assert settings.log_level == "DEBUG"
    assert settings.log_format == "human"
    assert settings.server_port == 9999
    assert settings.base_url == "https://custom.example.com"


@patch.dict(
    os.environ,
    {
        "LOG_LEVEL": "ERROR",
        "LOG_FORMAT": "json",
        "SERVER_PORT": "8888",
        "BASE_URL": "https://env.example.com",
    },
    clear=False,
)
def test_env_vars_override_defaults(tmp_path: Path):
    """Environment variables override default values."""
    config_path = tmp_path / "empty.yaml"
    with Path.open(config_path, "w", encoding="utf-8") as f:
        yaml.dump({"feeds": {}}, f)

    settings = AppSettings(config_file=config_path)

    assert settings.log_level == "ERROR"
    assert settings.log_format == "json"
    assert settings.server_port == 8888
    assert settings.base_url == "https://env.example.com"


@patch.dict(
    os.environ,
    {
        "LOG_LEVEL": "ERROR",
        "SERVER_PORT": "7777",
    },
    clear=False,
)
def test_env_vars_take_priority_over_yaml(tmp_path: Path):
    """Environment variables override YAML settings (env has higher priority)."""
    config_path = tmp_path / "settings.yaml"
    yaml_data: dict[str, Any] = {
        "log_level": "DEBUG",
        "log_format": "human",
        "server_port": 9999,
        "feeds": {},
    }
    with Path.open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(yaml_data, f)

    settings = AppSettings(config_file=config_path)

    assert settings.log_level == "ERROR", "Env var should override YAML"
    assert settings.log_format == "human", "YAML should be used when no env var"
    assert settings.server_port == 7777, "Env var should override YAML"


def test_yaml_complex_types_load_correctly(tmp_path: Path):
    """Complex types like lists and timedeltas load correctly from YAML."""
    config_path = tmp_path / "complex.yaml"
    yaml_data: dict[str, Any] = {
        "trusted_proxies": ["192.168.1.0/24", "10.0.0.1"],
        "yt_dlp_update_freq": 21600,  # seconds
        "tz": "America/Los_Angeles",
        "feeds": {},
    }
    with Path.open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(yaml_data, f)

    settings = AppSettings(config_file=config_path)

    assert settings.trusted_proxies == ["192.168.1.0/24", "10.0.0.1"]
    assert settings.yt_dlp_update_freq == timedelta(hours=6)
    assert settings.tz is not None
    assert str(settings.tz) == "America/Los_Angeles"


# --- Tests for FeedConfig.yt_args validator ---


def test_feed_config_yt_args_valid_string():
    """Tests that a valid yt_args string is correctly parsed."""
    feed = FeedConfig(  # type: ignore
        url="http://example.com",
        schedule="* * * * *",
        yt_args="-f best --verbose --retries 5",
    )
    expected_args = ["-f", "best", "--verbose", "--retries", "5"]

    assert feed.yt_args == expected_args


def test_feed_config_yt_args_empty_string():
    """Tests that an empty yt_args string results in an empty list."""
    feed = FeedConfig(url="http://example.com", schedule="* * * * *", yt_args="")  # type: ignore
    assert feed.yt_args == []


def test_feed_config_yt_args_invalid_string_shlex_raises_validation_error():
    """Tests that a malformed yt_args string (shlex error) raises ValidationError."""
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig(  # type: ignore
            url="http://example.com",
            schedule="* * * * *",
            yt_args="--format 'incomplete quote",
        )
    assert len(exc_info.value.errors()) == 1
    assert exc_info.value.errors()[0]["type"] == "value_error"
    assert "no closing quotation" in exc_info.value.errors()[0]["msg"].lower()


def test_feed_config_yt_args_invalid_type_raises_type_error():
    """Tests that a non-string/non-None yt_args type raises ValidationError (wrapping TypeError)."""
    with pytest.raises(TypeError):
        FeedConfig(url="http://example.com", schedule="* * * * *", yt_args=123)  # type: ignore


def test_feed_config_no_yt_args_uses_default_factory():
    """Tests that if yt_args is not provided, it defaults to an empty list."""
    feed = FeedConfig(url="http://example.com", schedule="* * * * *")  # type: ignore
    assert feed.yt_args == []


# --- Tests for FeedConfig.since validator ---


@pytest.mark.parametrize(
    "since_value,expected_year,expected_month,expected_day,expected_hour",
    [
        ("20240115", 2024, 1, 15, 5),  # January 15, 2024 (EST, no DST) -> 05:00 UTC
        ("2024-01-15", 2024, 1, 15, 5),  # ISO YYYY-MM-DD string
        (20240115, 2024, 1, 15, 5),  # Unquoted YAML int
        (date(2025, 1, 15), 2025, 1, 15, 5),  # Unquoted YAML native date
        ("2024031", 2024, 3, 1, 5),  # March 1, 2024 (strptime parses 7 digits as this)
    ],
)
@patch.dict(os.environ, {"TZ": "America/New_York"})
def test_feed_config_since_valid_values(
    since_value: Any,
    expected_year: int,
    expected_month: int,
    expected_day: int,
    expected_hour: int,
):
    """Tests that valid since values are correctly parsed."""
    feed = FeedConfig(  # type: ignore
        url="http://example.com",
        schedule="* * * * *",
        since=since_value,
    )
    assert feed.since is not None
    assert feed.since.year == expected_year
    assert feed.since.month == expected_month
    assert feed.since.day == expected_day
    assert feed.since.hour == expected_hour  # 00:00 EST = 05:00 UTC
    assert feed.since.minute == 0
    assert feed.since.second == 0


@pytest.mark.parametrize(
    "since_value,expected_exception",
    [
        ("202403155", ValidationError),  # Too long - has unconverted data
        ("2024/03/15", ValidationError),  # Slashes not supported
        ("20240230", ValidationError),  # February 30 doesn't exist
        (True, TypeError),  # Bool rejected
    ],
)
def test_feed_config_since_invalid_values(
    since_value: Any, expected_exception: type[Exception]
):
    """Tests that invalid since values raise appropriate exceptions."""
    with pytest.raises(expected_exception):
        FeedConfig(
            url="http://example.com",
            schedule="* * * * *",
            since=since_value,
        )  # type: ignore


# --- Tests for FeedConfig.download_delay validator ---


def test_feed_config_download_delay_valid_string():
    """Valid duration string is parsed into timedelta."""
    feed = FeedConfig(  # type: ignore[call-arg]
        url="http://example.com",
        schedule="* * * * *",
        download_delay="24h",
    )
    assert feed.download_delay == timedelta(hours=24)


def test_feed_config_download_delay_none_preserved():
    """None value is preserved."""
    feed = FeedConfig(  # type: ignore[call-arg]
        url="http://example.com",
        schedule="* * * * *",
        download_delay=None,
    )
    assert feed.download_delay is None


def test_feed_config_download_delay_empty_string_becomes_none():
    """Empty string is treated as None."""
    feed = FeedConfig(  # type: ignore[call-arg]
        url="http://example.com",
        schedule="* * * * *",
        download_delay="",
    )
    assert feed.download_delay is None


def test_feed_config_download_delay_whitespace_becomes_none():
    """Whitespace-only string is treated as None."""
    feed = FeedConfig(  # type: ignore[call-arg]
        url="http://example.com",
        schedule="* * * * *",
        download_delay="   ",
    )
    assert feed.download_delay is None


def test_feed_config_download_delay_timedelta_passthrough():
    """Timedelta values pass through unchanged."""
    td = timedelta(hours=24)
    feed = FeedConfig(  # type: ignore[call-arg]
        url="http://example.com",
        schedule="* * * * *",
        download_delay=td,
    )
    assert feed.download_delay == td


def test_feed_config_download_delay_invalid_format_raises_value_error():
    """Invalid format strings raise ValueError."""
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig(  # type: ignore[call-arg]
            url="http://example.com",
            schedule="* * * * *",
            download_delay="not a duration",
        )
    assert "Invalid duration format" in str(exc_info.value)


def test_feed_config_download_delay_negative_raises_value_error():
    """Negative duration strings raise ValueError."""
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig(  # type: ignore[call-arg]
            url="http://example.com",
            schedule="* * * * *",
            download_delay="-24h",
        )
    assert "non-negative" in str(exc_info.value)


def test_feed_config_download_delay_invalid_type_raises_type_error():
    """Invalid types raise TypeError."""
    with pytest.raises(TypeError, match="duration string"):
        FeedConfig(  # type: ignore[call-arg]
            url="http://example.com",
            schedule="* * * * *",
            download_delay=123,
        )


# --- Tests for Batch 1 onboarding/config hardening ---


@pytest.mark.parametrize(
    "raw_value,expected",
    [
        ("12h", timedelta(hours=12)),
        ("1d", timedelta(days=1)),
        ("30m", timedelta(minutes=30)),
        ("0", timedelta(0)),
        ("PT6H", timedelta(hours=6)),
    ],
)
def test_yt_dlp_update_freq_human_durations(
    tmp_path: Path, raw_value: str, expected: timedelta
):
    """YT_DLP_UPDATE_FREQ accepts human duration strings, zero, and ISO-8601."""
    config_path = tmp_path / "feeds.yaml"
    config_path.write_text("feeds: {}\n", encoding="utf-8")

    with patch.dict(os.environ, {"YT_DLP_UPDATE_FREQ": raw_value}, clear=False):
        settings = AppSettings(config_file=config_path)

    assert settings.yt_dlp_update_freq == expected


@pytest.mark.parametrize(
    "raw_value,expected",
    [
        ('["192.168.1.0/24", "10.0.0.1"]', ["192.168.1.0/24", "10.0.0.1"]),
        ("192.168.1.0/24, 10.0.0.1", ["192.168.1.0/24", "10.0.0.1"]),
        ("*", []),
        ("   ", None),
    ],
)
def test_trusted_proxies_env_formats(
    tmp_path: Path, raw_value: str, expected: list[str] | None
):
    """TRUSTED_PROXIES accepts JSON arrays, comma-separated strings, wildcard, and blank."""
    config_path = tmp_path / "feeds.yaml"
    config_path.write_text("feeds: {}\n", encoding="utf-8")

    with patch.dict(os.environ, {"TRUSTED_PROXIES": raw_value}, clear=False):
        settings = AppSettings(config_file=config_path)

    assert settings.trusted_proxies == expected


@pytest.mark.parametrize("bad_id", ["has space", "has.dot", "..", "a/b"])
def test_invalid_feed_id_rejected_at_config_load(tmp_path: Path, bad_id: str):
    """Feed IDs incompatible with HTTP routes fail fast during config validation."""
    config_path = tmp_path / "feeds.yaml"
    with config_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(
            {
                "feeds": {
                    bad_id: {
                        "url": "https://example.com/channel",
                        "schedule": "@daily",
                        "keep_last": 5,
                    }
                }
            },
            f,
        )

    with pytest.raises(ValidationError):
        AppSettings(config_file=config_path)


def test_unknown_yaml_keys_and_unbounded_feed_produce_warnings(tmp_path: Path):
    """Unknown keys and unbounded feeds emit warnings without blocking startup."""
    config_path = tmp_path / "feeds.yaml"
    with config_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(
            {
                "bogus_top": 1,
                "feeds": {
                    "unbounded_feed": {
                        "url": "https://example.com/channel",
                        "schedule": "@daily",
                        "keep_latest": 5,  # typo for keep_last
                        "metadata": {
                            "title": "Show",
                            "artwork": "http://example.com/a.jpg",
                        },
                    }
                },
            },
            f,
        )

    settings = AppSettings(config_file=config_path)

    warnings_joined = "\n".join(settings.config_warnings)
    assert "bogus_top" in warnings_joined
    assert "keep_latest" in warnings_joined
    assert "artwork" in warnings_joined
    assert "neither 'since' nor 'keep_last'" in warnings_joined


def test_example_feeds_yaml_loads_cleanly():
    """Shipped example_feeds.yaml is valid and loads without warnings."""
    repo_root = Path(__file__).resolve().parents[2]
    settings = AppSettings(config_file=repo_root / "example_feeds.yaml")

    assert "example_channel" in settings.feeds
    assert "manual_drops" in settings.feeds
    assert settings.config_warnings == []


def test_yaml_defaults_merge_and_allow_per_feed_overrides(tmp_path: Path):
    """Top-level YAML defaults apply to all feeds while per-feed keys override them."""
    config_path = tmp_path / "feeds.yaml"
    with config_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(
            {
                "defaults": {
                    "media_conversion": "optimized",
                    "transcript_lang": "en",
                    "embed_thumbnail": True,
                    "keep_last": 10,
                    "metadata": {
                        "language": "en",
                        "explicit": "no",
                    },
                },
                "feeds": {
                    "inherited": {
                        "url": "https://example.com/a",
                        "schedule": "@daily",
                    },
                    "overridden": {
                        "url": "https://example.com/b",
                        "schedule": "@hourly",
                        "media_conversion": "off",
                        "transcript_lang": None,
                        "embed_thumbnail": False,
                        "metadata": {
                            "explicit": "yes",
                            "title": "Custom Title",
                        },
                    },
                },
            },
            f,
        )

    settings = AppSettings(config_file=config_path)

    inherited = settings.feeds["inherited"]
    assert inherited.media_conversion == MediaConversionPreset.OPTIMIZED
    assert inherited.transcript_lang == "en"
    assert inherited.effective_embed_thumbnail is True
    assert inherited.keep_last == 10
    assert inherited.metadata is not None
    assert inherited.metadata.language == "en"
    assert inherited.metadata.explicit is False

    overridden = settings.feeds["overridden"]
    assert overridden.media_conversion == MediaConversionPreset.OFF
    assert overridden.transcript_lang is None
    assert overridden.effective_embed_thumbnail is False
    assert overridden.keep_last == 10
    assert overridden.metadata is not None
    assert overridden.metadata.language == "en"
    assert overridden.metadata.explicit is True
    assert overridden.metadata.title == "Custom Title"
    assert settings.config_warnings == []


def test_media_conversion_defaults_and_custom_script_resolution(tmp_path: Path):
    """media_conversion defaults to light; custom script defaults beside config_file."""
    config_path = tmp_path / "feeds.yaml"
    with config_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(
            {
                "feeds": {
                    "default_show": {
                        "url": "https://example.com/show",
                        "schedule": "@daily",
                        "keep_last": 5,
                    }
                }
            },
            f,
        )

    settings = AppSettings(config_file=config_path)
    feed = settings.feeds["default_show"]
    assert feed.media_conversion == MediaConversionPreset.LIGHT
    assert feed.effective_embed_thumbnail is True
    assert settings.effective_custom_conversion_script == tmp_path / "post_process.sh"
