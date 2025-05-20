# tests/test_config.py
from pathlib import Path

from pydantic import ValidationError
import pytest
from pytest import MonkeyPatch
import yaml

from anypod.config import AppSettings, FeedConfig, YamlFileFromFieldSource
from anypod.exceptions import ConfigLoadError
from anypod.ytdlp_wrapper.ytdlp_core import YtdlpCore

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
            "yt_args": "--format bestaudio --playlist-items 1-3",
            # max_errors will use default (3) for this one
        },
    }
}

# Expected parsed yt_args for podcast2
EXPECTED_PODCAST2_YT_ARGS = YtdlpCore.parse_options(
    ["--format", "bestaudio", "--playlist-items", "1-3"]
)


@pytest.fixture
def sample_config_file(tmp_path: Path) -> Path:
    """Creates a sample YAML config file in a temporary directory."""
    config_path = tmp_path / "test_feeds.yaml"
    with Path.open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(SAMPLE_FEEDS_DATA, f)
    return config_path


@pytest.mark.unit
def test_load_from_default_location(monkeypatch: MonkeyPatch, tmp_path: Path):
    """
    Tests if AppSettings loads configuration from the default file path
    when no overrides are provided. We monkeypatch _get_yaml_path.
    """
    default_config_path = tmp_path / "default_feeds.yaml"
    with Path.open(default_config_path, "w", encoding="utf-8") as f:
        yaml.dump(SAMPLE_FEEDS_DATA, f)

    # Define a properly typed mock function for _get_yaml_path
    def mock_get_yaml_path(self_source: YamlFileFromFieldSource) -> Path:
        return default_config_path

    # Monkeypatch _get_yaml_path to use our typed mock function
    monkeypatch.setattr(YamlFileFromFieldSource, "_get_yaml_path", mock_get_yaml_path)

    # Clear environment variables that might interfere
    monkeypatch.delenv("CONFIG_FILE", raising=False)

    settings = AppSettings()  # type: ignore

    assert len(settings.feeds) == len(SAMPLE_FEEDS_DATA["feeds"])
    assert "podcast1" in settings.feeds
    assert (
        settings.feeds["podcast1"].url == SAMPLE_FEEDS_DATA["feeds"]["podcast1"]["url"]
    )
    assert (
        settings.feeds["podcast1"].schedule
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
        settings.feeds["podcast2"].schedule
        == SAMPLE_FEEDS_DATA["feeds"]["podcast2"]["schedule"]
    )
    assert settings.feeds["podcast2"].max_errors == 3, (  # Asserting default value
        "'max_errors' for 'podcast2' should be the default value (3)"
    )

    dumped_settings = settings.model_dump()
    assert "config_file" in dumped_settings, (
        "'config_file' should be present in model_dump even tho it is not used anywhere"
    )
    assert "feeds" in dumped_settings


@pytest.mark.unit
def test_override_location_with_env_var(
    monkeypatch: MonkeyPatch, sample_config_file: Path
):
    """
    Tests if AppSettings loads configuration from the path specified
    by the CONFIG_FILE environment variable.
    """
    monkeypatch.setenv("CONFIG_FILE", str(sample_config_file))

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


@pytest.mark.unit
def test_override_location_with_init_arg(
    monkeypatch: MonkeyPatch, sample_config_file: Path
):
    """
    Tests if AppSettings loads configuration from the path specified
    via an initialization argument, overriding defaults and env vars.
    """
    # Set an env var to ensure the init arg takes precedence
    monkeypatch.setenv("CONFIG_FILE", "/path/to/nonexistent/file.yaml")

    settings = AppSettings(config_file=sample_config_file)

    # Clean up env var after test
    monkeypatch.delenv("CONFIG_FILE", raising=False)

    assert len(settings.feeds) == len(SAMPLE_FEEDS_DATA["feeds"]), (
        "Number of loaded feeds should match sample data when overridden by init arg"
    )

    assert "podcast1" in settings.feeds, (
        "Feed 'podcast1' should be loaded when overridden by init arg"
    )
    assert (
        settings.feeds["podcast1"].url == SAMPLE_FEEDS_DATA["feeds"]["podcast1"]["url"]
    )
    assert (
        settings.feeds["podcast1"].schedule
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
        settings.feeds["podcast2"].schedule
        == SAMPLE_FEEDS_DATA["feeds"]["podcast2"]["schedule"]
    )
    assert settings.feeds["podcast2"].max_errors == 3, (  # Asserting default value
        "'max_errors' for 'podcast2' should be the default value (3) when overridden by init arg"
    )


@pytest.mark.unit
def test_nonexistent_config_file_raises_error(monkeypatch: MonkeyPatch):
    """
    Tests that instantiating AppSettings raises an error if the
    specified config file does not exist, and that the cause is FileNotFoundError.
    """
    non_existent_path = "/path/to/hopefully/nonexistent/feeds.yaml"
    monkeypatch.setenv("CONFIG_FILE", non_existent_path)

    with pytest.raises(
        ConfigLoadError, match="Failed to load or parse YAML configuration file"
    ) as exc_info:
        AppSettings()  # type: ignore

    assert isinstance(exc_info.value.__cause__, FileNotFoundError), (
        f"Cause of OSError should be FileNotFoundError, got {type(exc_info.value.__cause__).__name__}"
    )


@pytest.mark.unit
def test_invalid_yaml_format_raises_error(tmp_path: Path):
    """
    Tests that instantiating AppSettings raises an error if the
    config file contains invalid YAML, checking the chain of causes.
    """
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


@pytest.mark.unit
def test_empty_yaml_file_loads_defaults(tmp_path: Path):
    """
    Tests that an empty YAML file results in default settings values.
    """
    empty_yaml_path = tmp_path / "empty_feeds.yaml"
    with Path.open(empty_yaml_path, "w", encoding="utf-8") as f:
        f.write("")  # Empty file

    settings = AppSettings(config_file=empty_yaml_path)

    assert settings.feeds == {}, "Feeds should be empty for an empty YAML file"


@pytest.mark.unit
def test_yaml_file_with_only_other_keys(tmp_path: Path):
    """
    Tests that a YAML file with keys not defined in AppSettings is ignored
    for the main settings, but the 'feeds' key is still processed if present.
    """
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
    assert not hasattr(settings, "some_other_key"), (
        "Extra key 'some_other_key' should not be an attribute on settings (extra='ignore')"
    )
    assert not hasattr(settings, "another_setting"), (
        "Extra key 'another_setting' should not be an attribute on settings (extra='ignore')"
    )


@pytest.mark.unit
def test_invalid_yaml_returns_non_dict_type_raises_error(tmp_path: Path):
    """
    Tests that AppSettings raises an OSError with a TypeError cause if the
    YAML content is valid YAML but not a dictionary (e.g., a list).
    """
    invalid_type_yaml_path = tmp_path / "invalid_type.yaml"
    # YAML content that is a list, not a dictionary
    list_content = "- download1\n- download2"
    with Path.open(invalid_type_yaml_path, "w", encoding="utf-8") as f:
        f.write(list_content)

    with pytest.raises(
        ConfigLoadError, match="Failed to load or parse YAML configuration file"
    ) as exc_info:
        AppSettings(config_file=invalid_type_yaml_path)

    assert isinstance(exc_info.value.__cause__, TypeError), (
        f"Cause of OSError should be TypeError, got {type(exc_info.value.__cause__).__name__}"
    )
    assert "Invalid YAML config format: expected dict, got list" in str(
        exc_info.value.__cause__
    ), "TypeError message did not match expected format for list input"


# New tests for FeedConfig.yt_args validator
@pytest.mark.unit
def test_feed_config_yt_args_valid_string():
    """Tests that a valid yt_args string is correctly parsed."""
    feed = FeedConfig(  # type: ignore
        url="http://example.com",
        schedule="* * * * *",
        yt_args="-f best --verbose --retries 5",
    )
    expected_args = YtdlpCore.parse_options(
        ["-f", "best", "--verbose", "--retries", "5"]
    )
    assert feed.yt_args == expected_args


@pytest.mark.unit
def test_feed_config_yt_args_empty_string():
    """Tests that an empty yt_args string results in an empty dict."""
    feed = FeedConfig(url="http://example.com", schedule="* * * * *", yt_args="")  # type: ignore
    assert feed.yt_args == {}


@pytest.mark.unit
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
    assert "invalid yt_args string" in exc_info.value.errors()[0]["msg"].lower()
    assert "failed to parse" in exc_info.value.errors()[0]["msg"].lower()


@pytest.mark.unit
def test_feed_config_yt_args_unsupported_ytdlp_option_raises_validation_error():
    """
    Tests that a yt_args string with an option not recognized by yt-dlp
    raises a ValidationError. This assumes YtdlpCore.parse_options will fail.
    """
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig(  # type: ignore
            url="http://example.com",
            schedule="* * * * *",
            yt_args="--this-is-definitely-not-a-real-yt-dlp-option",
        )
    assert len(exc_info.value.errors()) == 1
    assert exc_info.value.errors()[0]["type"] == "value_error"
    assert "invalid yt_args string" in exc_info.value.errors()[0]["msg"].lower()
    assert "failed to parse" in exc_info.value.errors()[0]["msg"].lower()


@pytest.mark.unit
def test_feed_config_yt_args_invalid_type_raises_type_error():
    """Tests that a non-string/non-None yt_args type raises ValidationError (wrapping TypeError)."""
    with pytest.raises(TypeError):
        FeedConfig(url="http://example.com", schedule="* * * * *", yt_args=123)  # type: ignore


@pytest.mark.unit
def test_feed_config_no_yt_args_uses_default_factory():
    """Tests that if yt_args is not provided, it defaults to an empty dict."""
    feed = FeedConfig(url="http://example.com", schedule="* * * * *")  # type: ignore
    assert feed.yt_args == {}
