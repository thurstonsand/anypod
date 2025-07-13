"""Application configuration management for Anypod.

This module defines configuration models and settings sources for the Anypod
application, including feed configurations, application settings, and custom
YAML file loading capabilities.
"""

from enum import Enum
import logging
from pathlib import Path
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)
import yaml

from ..exceptions import ConfigLoadError
from .feed_config import FeedConfig

logger = logging.getLogger(__name__)


class DebugMode(str, Enum):
    """Represent available debug modes for the application.

    Debug modes provide isolated testing of specific application components
    without running the full application workflow.
    """

    YTDLP = "ytdlp"
    ENQUEUER = "enqueuer"
    DOWNLOADER = "downloader"


class YamlFileFromFieldSource(PydanticBaseSettingsSource):
    """Load configuration from a YAML file specified by a field.

    A settings source that loads configuration from a YAML file specified
    by a field within the settings model itself. This source should be run
    after all other sources that might populate the path field.

    Attributes:
        yaml_file_encoding: Encoding to use when reading the YAML file.
        yaml_data: Cached YAML data loaded from the file.
    """

    def _get_current_state_of(self, field_name: str) -> Any:
        """Get the current state of a field from the settings model."""
        value = self.current_state.get(field_name)
        if value not in (None, PydanticUndefined):
            return value

        field_info = self.settings_cls.model_fields[field_name]
        if isinstance(field_info.validation_alias, str):
            value = self.current_state.get(field_info.validation_alias)
            if value not in (None, PydanticUndefined):
                return value
        return field_info.get_default()

    def __init__(
        self,
        settings_cls: type[BaseSettings],
        yaml_file_encoding: str | None = None,
    ):
        super().__init__(settings_cls)
        self.yaml_file_encoding = yaml_file_encoding or "utf-8"
        self.yaml_data: dict[str, Any] = {}

    def _get_yaml_path(self) -> Path | None:
        """Determines the YAML path from the already processed settings state."""
        path_value = self._get_current_state_of("config_file")
        logger.debug(
            "Attempting to resolve YAML configuration file path.",
            extra={
                "current_path_value_type": type(path_value).__name__,
                "current_path_value": "None" if path_value is None else str(path_value),
            },
        )

        if isinstance(path_value, Path):
            return path_value.expanduser()
        elif isinstance(path_value, str):
            return Path(path_value).expanduser()
        elif path_value is not None:
            raise TypeError(
                f"Field 'config_file' must resolve to a Path or string, "
                f"received type '{type(path_value).__name__}'"
            )
        else:  # path_value is None
            return None

    def _read_yaml_file(self, file_path: Path) -> dict[str, Any]:
        """Reads and parses the YAML file."""
        logger.debug(
            "Attempting to read and parse YAML file.",
            extra={"file_path": str(file_path)},
        )
        with Path.open(file_path, encoding=self.yaml_file_encoding) as f:
            loaded_yaml = yaml.safe_load(f)  # This can raise yaml.YAMLError

        # Process loaded_yaml after successful loading
        if isinstance(loaded_yaml, dict):
            logger.debug(
                "Successfully parsed YAML configuration file.",
                extra={"file_path": str(file_path)},
            )
            return cast(dict[str, Any], loaded_yaml)
        elif loaded_yaml is None:  # Empty YAML file often loads as None
            logger.info(
                "YAML configuration file is empty.",
                extra={"file_path": str(file_path)},
            )
            return {}
        else:
            # Valid YAML, but not a dictionary (e.g., a list or a scalar)
            raise TypeError(
                f"Invalid YAML config format: expected dict, got {type(loaded_yaml).__name__}"
            )

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> tuple[Any, str, bool]:
        """Get value from loaded YAML data."""
        field_value = self.yaml_data.get(field_name)
        return field_value, field_name, self.field_is_complex(field)

    def __call__(self) -> dict[str, Any]:
        """Load YAML data from file specified in the config_file field."""
        try:
            yaml_path = self._get_yaml_path()
        except TypeError as e:
            raise ConfigLoadError(
                "Failed to resolve YAML configuration file path.",
            ) from e

        if yaml_path:
            logger.debug(
                "Loading YAML configuration.", extra={"file_path": str(yaml_path)}
            )
            try:
                self.yaml_data = self._read_yaml_file(yaml_path)
            except (TypeError, FileNotFoundError, OSError, yaml.YAMLError) as e:
                raise ConfigLoadError(
                    "Failed to load or parse YAML configuration file.",
                    config_file=str(yaml_path),
                ) from e
        else:
            logger.debug(
                "No YAML configuration file specified or resolved; skipping YAML loading.",
            )
            self.yaml_data = {}

        return self.yaml_data.copy()


class AppSettings(BaseSettings):
    """Application settings and feed configurations.

    Holds all feed configurations and global application settings.
    Configuration is loaded from environment variables, CLI arguments,
    and YAML files.

    Attributes:
        debug_mode: Debug mode to run (ytdlp, enqueuer, downloader, or None).
        log_format: Format for application logs (human or json).
        log_level: Logging level for the application.
        log_include_stacktrace: Include full stack traces in error logs.
        base_url: Base URL for RSS feeds and media files.
        data_dir: Root directory for all application data.
        server_host: Host address for the HTTP server to bind to.
        server_port: Port number for the HTTP server to listen on.
        tz: Timezone for date parsing in config files.
        config_file: Path to the YAML config file.
        cookies_path: Path to the cookies.txt file for yt-dlp authentication.
        feeds: Configuration for all podcast feeds.
    """

    # Global settings
    debug_mode: DebugMode | None = Field(
        default=None,
        validation_alias="DEBUG_MODE",
        description="Specifies the debug mode to run ('ytdlp', 'enqueuer', or None for default).",
    )
    log_format: Literal["human", "json"] = Field(
        default="json",
        validation_alias="LOG_FORMAT",
        description="Format for application logs ('human' or 'json').",
    )
    log_level: str = Field(
        default="INFO",
        validation_alias="LOG_LEVEL",
        description="Logging level for the application (e.g., DEBUG, INFO, WARNING, ERROR). Case-insensitive.",
    )
    log_include_stacktrace: bool = Field(
        default=False,
        validation_alias="LOG_INCLUDE_STACKTRACE",
        description="Include full stack traces in error logs (true/false).",
    )
    base_url: str = Field(
        default="http://localhost:8024",
        validation_alias="BASE_URL",
        description="Base URL for RSS feeds and media files (e.g., 'https://podcasts.example.com').",
    )
    data_dir: Path = Field(
        default=Path("/data"),
        validation_alias="DATA_DIR",
        description="Root directory for all application data (database, media files, temp files).",
    )

    # Server configuration
    server_host: str = Field(
        default="0.0.0.0",
        validation_alias="SERVER_HOST",
        description="Host address for the HTTP server to bind to (e.g., '0.0.0.0' for all interfaces, '127.0.0.1' for localhost only).",
    )
    server_port: int = Field(
        default=8024,
        validation_alias="SERVER_PORT",
        description="Port number for the HTTP server to listen on.",
    )
    trusted_proxies: list[str] | None = Field(
        default=None,
        validation_alias="TRUSTED_PROXIES",
        description="List of trusted proxy IP addresses or networks. When set, enables proxy header processing. Set to null/empty to trust all proxies. Format: ['192.168.1.0/24', '10.0.0.1'].",
    )
    tz: ZoneInfo | None = Field(
        default=None,
        validation_alias="TZ",
        description="Timezone for date parsing in config files (e.g., 'America/New_York', 'Europe/London'). Must be explicitly set.",
    )

    # Feeds config
    config_file: Path = Field(
        default=Path("/config/feeds.yaml"),
        validation_alias="CONFIG_FILE",
        description="Path to the YAML config file.",
    )
    cookies_path: Path | None = Field(
        default=None,
        validation_alias="COOKIES_PATH",
        description="Optional path to the cookies.txt file for yt-dlp authentication.",
    )

    feeds: dict[str, FeedConfig] = Field(
        default_factory=dict[str, FeedConfig],
        description="Configuration for all podcast feeds. Must be read from a YAML file.",
    )

    model_config = SettingsConfigDict(
        env_prefix="",
        env_nested_delimiter="__",
        # yaml_file=None, # Removed as we handle YAML via YamlFileFromFieldSource
        yaml_file_encoding="utf-8",
        cli_parse_args=True,
        cli_ignore_unknown_args=True,
        cli_kebab_case=True,
        extra="ignore",
    )

    @field_validator("tz", mode="before")
    @classmethod
    def parse_timezone_string(cls, v: Any) -> ZoneInfo | None:
        """Parse timezone string into a ZoneInfo object.

        Args:
            v: Value to parse, can be string or None.

        Returns:
            ZoneInfo object for the timezone, or None if not provided.

        Raises:
            ValueError: If the timezone string is invalid.
            TypeError: If the value is not a string or None.
        """
        match v:
            case None:
                return None
            case str() as s if not s.strip():  # Handle empty string
                return None
            case str() as s:
                try:
                    return ZoneInfo(s.strip())
                except ZoneInfoNotFoundError as e:
                    raise ValueError(
                        f"Invalid timezone string '{s}'. Must be a valid timezone name (e.g., 'America/New_York', 'UTC')."
                    ) from e
            case _:
                raise TypeError(f"tz must be a string, got {type(v).__name__}")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Customize the order and sources for settings loading.

        This method customizes the order in which settings sources are processed.
        Environment variables and initialization parameters are processed first to
        potentially set the `config_file`. After that, the `YamlFileFromFieldSource`
        reads the `config_file` field and loads the YAML configuration.

        Args:
            settings_cls: The settings class being configured.
            init_settings: Settings from initialization parameters.
            env_settings: Settings from environment variables.
            dotenv_settings: Settings from .env files.
            file_secret_settings: Settings from secret files.

        Returns:
            Tuple of settings sources in the order they should be processed.
        """
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlFileFromFieldSource(settings_cls=settings_cls),
            file_secret_settings,
        )
