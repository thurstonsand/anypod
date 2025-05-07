from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)
import yaml


class FeedConfig(BaseModel):
    """Configuration for a single podcast feed."""

    url: str = Field(..., min_length=1, description="Feed source URL")
    yt_args: str | None = Field(None, description="Arguments passed verbatim to yt-dlp")
    schedule: str = Field(..., min_length=1, description="Cron schedule string")
    keep_last: int | None = Field(
        None, ge=1, description="Prune policy - number of latest items to keep"
    )
    since: datetime | None = Field(
        None, description="ISO8601 timestamp to ignore older items"
    )


class YamlFileFromFieldSource(PydanticBaseSettingsSource):
    """
    A settings source that loads configuration from a YAML file specified
    by a field within the settings model itself.

    This source should be run after all other sources that might populate the path field.
    """

    path_field_name: str = "config_file"

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
        # check for the regular field name ("config_file") in the current state
        path_value = self.current_state.get(self.path_field_name)

        field_info = self.settings_cls.model_fields[self.path_field_name]

        # check for the alternate field name ("CONFIG_FILE") via validation_alias
        if path_value in (None, PydanticUndefined) and isinstance(
            field_info.validation_alias, str
        ):
            path_value = self.current_state.get(field_info.validation_alias)

        # check for the default value (e.g., "/config/feeds.yaml")
        if path_value in (None, PydanticUndefined) and field_info:
            default_value = field_info.get_default()
            if default_value not in (None, PydanticUndefined):
                path_value = default_value

        if isinstance(path_value, Path):
            return path_value.expanduser()
        elif isinstance(path_value, str):
            return Path(path_value).expanduser()
        elif path_value is not None:
            raise TypeError(
                f"Field '{self.path_field_name}' (or its string alias) must resolve to a Path or string, "
                f"received type '{type(path_value).__name__}'"
            )
        else:  # path_value is None
            return None

    def _read_yaml_file(self, file_path: Path) -> dict[str, Any]:
        """Reads and parses the YAML file."""
        with Path.open(file_path, encoding=self.yaml_file_encoding) as f:
            loaded_yaml = yaml.safe_load(f)  # This can raise yaml.YAMLError

        # Process loaded_yaml after successful loading
        if isinstance(loaded_yaml, dict):
            return cast(dict[str, Any], loaded_yaml)
        elif loaded_yaml is None:  # Empty YAML file often loads as None
            return {}
        else:
            # Valid YAML, but not a dictionary (e.g., a list or a scalar)
            raise TypeError(
                f"Invalid YAML config format: expected dict, got {type(loaded_yaml).__name__}"
            )

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> tuple[Any, str, bool]:
        """Get value from loaded YAML data"""
        field_value = self.yaml_data.get(field_name)
        return field_value, field_name, self.field_is_complex(field)

    def __call__(self) -> dict[str, Any]:
        """Load YAML data from file specified in the config_file field"""
        try:
            yaml_path = self._get_yaml_path()
            if yaml_path:
                self.yaml_data = self._read_yaml_file(yaml_path)
            else:
                self.yaml_data = {}
        except (TypeError, FileNotFoundError, OSError, yaml.YAMLError) as e:
            raise OSError("Failed to load YAML config file") from e

        return self.yaml_data.copy()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(path_field_name={self.path_field_name})"


class AppSettings(BaseSettings):
    """Holds all feed configurations."""

    config_file: Path = Field(
        Path("/config/feeds.yaml"),
        validation_alias="CONFIG_FILE",
        description="Path to the YAML config file.",
    )

    feeds: dict[str, FeedConfig] = Field(default_factory=dict)

    model_config = SettingsConfigDict(
        env_prefix="",
        env_nested_delimiter="__",
        yaml_file=None,
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
        # Order matters: Env vars/init args should be processed first to potentially set config_file
        # Then YamlFileFromFieldSource reads the config_file field and loads the YAML
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlFileFromFieldSource(settings_cls=settings_cls),
            file_secret_settings,
        )
