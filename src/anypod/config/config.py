"""Application configuration management for Anypod.

This module defines configuration models and settings sources for the Anypod
application, including feed configurations, application settings, and custom
YAML file loading capabilities.
"""

from __future__ import annotations

from enum import Enum
import logging
from pathlib import Path
from typing import Any, Literal, cast

from pydantic import Field
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
            logger.info(
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
        """Load YAML data from file specified in the config_file field, unless debug_mode is 'ytdlp'."""
        current_debug_mode_value = self._get_current_state_of("debug_mode")
        # Check against the enum member or its string value
        if (
            current_debug_mode_value == DebugMode.YTDLP
            or current_debug_mode_value == DebugMode.YTDLP.value
        ):
            logger.info(
                "YAML configuration loading skipped due to debug_mode='ytdlp'.",
                extra={
                    "debug_mode_status": f"({type(current_debug_mode_value).__name__}) {current_debug_mode_value}"
                },
            )
            return {}

        try:
            yaml_path = self._get_yaml_path()
        except TypeError as e:
            raise ConfigLoadError(
                "Failed to resolve YAML configuration file path.",
            ) from e

        if yaml_path:
            logger.info(
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
            logger.info(
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
        config_file: Path to the YAML config file.
        feeds: Configuration for all podcast feeds.
    """

    # Global settings
    debug_mode: DebugMode | None = Field(
        default=None,
        validation_alias="DEBUG_MODE",
        description="Specifies the debug mode to run ('ytdlp', 'enqueuer', or None for default).",
    )
    log_format: Literal["human", "json"] = Field(
        default="human",
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

    # Feeds config
    config_file: Path = Field(
        default=Path("/config/feeds.yaml"),
        validation_alias="CONFIG_FILE",
        description="Path to the YAML config file.",
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
