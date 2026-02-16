"""Application configuration management for Anypod.

This module defines configuration models and settings sources for the Anypod
application, including feed configurations, application settings, and custom
YAML file loading capabilities.
"""

from datetime import timedelta
from enum import StrEnum
import logging
from pathlib import Path
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)
from pydantic_settings.sources import InitSettingsSource, YamlConfigSettingsSource
import yaml

from ..exceptions import ConfigLoadError
from .feed_config import FeedConfig

logger = logging.getLogger(__name__)


class DebugMode(StrEnum):
    """Represent available debug modes for the application.

    Debug modes provide isolated testing of specific application components
    without running the full application workflow.
    """

    YTDLP = "ytdlp"
    ENQUEUER = "enqueuer"
    DOWNLOADER = "downloader"


class DynamicYamlConfigSettingsSource(YamlConfigSettingsSource):
    """YAML settings source that resolves its path from earlier sources.

    Extends the built-in YamlConfigSettingsSource to dynamically determine
    the YAML file path from the `config_file` field, which may be set via
    environment variables or CLI arguments processed by earlier sources.
    """

    CONFIG_PATH_FIELD = "config_file"

    def __init__(self, settings_cls: type[BaseSettings]):
        """Initialize without loading YAML yet (path comes from current_state)."""
        PydanticBaseSettingsSource.__init__(self, settings_cls)
        self._initialized = False
        self.yaml_file_path: Path | None = None
        self.yaml_file_encoding = "utf-8"
        self.yaml_data: dict[str, Any] = {}

    def _resolve_config_path(self) -> Path:
        """Resolve the config file path from current_state or field default."""
        field_info = self.settings_cls.model_fields[self.CONFIG_PATH_FIELD]
        alias = field_info.validation_alias
        if not isinstance(alias, str):
            raise TypeError(
                f"{self.CONFIG_PATH_FIELD} validation_alias must be a string"
            )

        value = self.current_state.get(
            self.CONFIG_PATH_FIELD
        ) or self.current_state.get(alias)
        if value is None:
            value = field_info.default

        path = Path(value) if isinstance(value, str) else cast(Path, value)
        return path.expanduser()

    def __call__(self) -> dict[str, Any]:
        """Load YAML and delegate to parent for alias-aware processing."""
        if self._initialized:
            return super().__call__()

        self._initialized = True
        path = self._resolve_config_path()

        logger.debug("Loading YAML configuration.", extra={"file_path": str(path)})

        self.yaml_file_path = path
        try:
            self.yaml_data = self._read_file(path)
        except (FileNotFoundError, OSError, yaml.YAMLError) as e:
            raise ConfigLoadError(
                "Failed to load or parse YAML configuration file.",
                config_file=str(path),
            ) from e
        InitSettingsSource.__init__(self, self.settings_cls, self.yaml_data)
        return super().__call__()


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
        pot_provider_url: URL for bgutil POT provider HTTP server used by yt-dlp.
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
        description="Port number for the static HTTP server (default: 8024).",
    )
    admin_server_port: int = Field(
        default=8025,
        validation_alias="ADMIN_SERVER_PORT",
        description="Port number for the admin HTTP server (default: 8025). Should not be exposed to the public.",
    )
    single_server_mode: bool = Field(
        default=False,
        validation_alias="SINGLE_SERVER_MODE",
        description=(
            "When enabled, mounts admin routes on the main server under /admin/ instead of running "
            "a separate admin server. Use only when admin access is protected at the infrastructure level "
            "(e.g., Cloudflare Access). WARNING: Admin APIs will be exposed on the public port."
        ),
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

    # yt-dlp POT provider configuration
    pot_provider_url: str | None = Field(
        default=None,
        validation_alias="POT_PROVIDER_URL",
        description=(
            "URL for bgutil POT provider HTTP server (e.g., 'http://bgutil-provider:4416'). "
            "If unset, POT fetching is disabled."
        ),
    )

    # yt-dlp update configuration
    yt_channel: str = Field(
        default="stable",
        validation_alias="YT_CHANNEL",
        description=(
            "yt-dlp update channel: stable, nightly, master, or specific version/repository."
        ),
    )
    yt_dlp_update_freq: timedelta = Field(
        default=timedelta(hours=12),
        validation_alias="YT_DLP_UPDATE_FREQ",
        description=(
            "Minimum interval between yt-dlp --update-to invocations (e.g., '12h', '1d')."
        ),
    )

    feeds: dict[str, FeedConfig] = Field(
        default_factory=dict[str, FeedConfig],
        description="Configuration for all podcast feeds. Must be read from a YAML file.",
    )

    model_config = SettingsConfigDict(
        env_prefix="",
        env_nested_delimiter="__",
        validate_by_name=True,
        validate_by_alias=True,
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

    @field_validator("cookies_path", mode="before")
    @classmethod
    def normalize_cookies_path(cls, v: Any) -> Any:
        """Treat blank cookie paths as unset.

        Args:
            v: Value to parse, can be string or None.

        Returns:
            Path to the cookies.txt file, or None if not provided.
        """
        match v:
            case None:
                return None
            case str() as s if not s.strip():
                return None
            case _:
                return v

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
        potentially set the `config_file`. After that, the `DynamicYamlConfigSettingsSource`
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
            DynamicYamlConfigSettingsSource(settings_cls=settings_cls),
            file_secret_settings,
        )
