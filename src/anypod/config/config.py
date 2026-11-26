"""Application configuration management for Anypod.

This module defines configuration models and settings sources for the Anypod
application, including feed configurations, application settings, and custom
YAML file loading capabilities.
"""

from contextvars import ContextVar
from datetime import datetime, timedelta, tzinfo
from enum import StrEnum
import json
import logging
import os
from pathlib import Path
import re
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, PrivateAttr, field_validator, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)
from pydantic_settings.sources import InitSettingsSource, YamlConfigSettingsSource
import pytimeparse2  # pyright: ignore[reportMissingTypeStubs]
import yaml

from ..exceptions import ConfigLoadError
from .feed_config import FeedConfig
from .types import FeedMetadataOverrides

logger = logging.getLogger(__name__)
SAFE_FEED_ID_PATTERN = r"^[a-zA-Z0-9_-]{1,255}$"
_FEED_ID_RE = re.compile(SAFE_FEED_ID_PATTERN)
_PENDING_CONFIG_WARNINGS: ContextVar[list[str] | None] = ContextVar(
    "_PENDING_CONFIG_WARNINGS", default=None
)


class DebugMode(StrEnum):
    """Represent available debug modes for the application.

    Debug modes provide isolated testing of specific application components
    without running the full application workflow.
    """

    YTDLP = "ytdlp"
    ENQUEUER = "enqueuer"
    DOWNLOADER = "downloader"


def _collect_yaml_unknown_key_warnings(
    settings_cls: type[BaseSettings], yaml_data: dict[str, Any]
) -> None:
    """Inspect raw YAML data for unrecognized keys and stash warnings for startup logging."""
    warnings: list[str] = []

    valid_top_keys: set[str] = {"defaults"}
    for name, field_info in settings_cls.model_fields.items():
        valid_top_keys.add(name)
        if isinstance(field_info.validation_alias, str):
            valid_top_keys.add(field_info.validation_alias)
            valid_top_keys.add(field_info.validation_alias.lower())

    for key in sorted(yaml_data.keys()):
        if key not in valid_top_keys:
            warnings.append(f"Unrecognized top-level config key '{key}' (ignored).")

    valid_feed_keys = set(FeedConfig.model_fields.keys())
    valid_meta_keys = set(FeedMetadataOverrides.model_fields.keys())

    raw_defaults = yaml_data.get("defaults")
    if isinstance(raw_defaults, dict):
        defaults_dict = cast(dict[str, Any], raw_defaults)
        for dkey in sorted(defaults_dict.keys()):
            if dkey not in valid_feed_keys or dkey == "url":
                warnings.append(
                    f"Unrecognized key '{dkey}' in top-level 'defaults' (ignored)."
                )
        raw_default_meta = defaults_dict.get("metadata")
        if isinstance(raw_default_meta, dict):
            for mkey in sorted(cast(dict[str, Any], raw_default_meta).keys()):
                if mkey not in valid_meta_keys:
                    warnings.append(
                        f"Unrecognized metadata key '{mkey}' in top-level 'defaults' (ignored)."
                    )

    raw_feeds = yaml_data.get("feeds")
    if isinstance(raw_feeds, dict):
        for feed_id, feed_val in cast(dict[str, Any], raw_feeds).items():
            if not isinstance(feed_val, dict):
                continue
            feed_dict = cast(dict[str, Any], feed_val)
            for fkey in sorted(feed_dict.keys()):
                if fkey not in valid_feed_keys:
                    warnings.append(
                        f"Unrecognized config key '{fkey}' in feed '{feed_id}' (ignored)."
                    )
            raw_meta = feed_dict.get("metadata")
            if isinstance(raw_meta, dict):
                for mkey in sorted(cast(dict[str, Any], raw_meta).keys()):
                    if mkey not in valid_meta_keys:
                        warnings.append(
                            f"Unrecognized metadata key '{mkey}' in feed '{feed_id}' (ignored)."
                        )

    _PENDING_CONFIG_WARNINGS.set(warnings or None)


def _apply_feed_defaults(yaml_data: dict[str, Any]) -> dict[str, Any]:
    """Merge top-level YAML ``defaults`` into each entry under ``feeds``.

    Feed-specific keys (including explicit ``None`` values) take precedence over
    defaults. Nested ``metadata`` dicts are merged one level deep.
    """
    raw_defaults = yaml_data.get("defaults")
    raw_feeds = yaml_data.get("feeds")
    if not isinstance(raw_defaults, dict) or not isinstance(raw_feeds, dict):
        return yaml_data

    defaults_dict = {
        k: v for k, v in cast(dict[str, Any], raw_defaults).items() if k != "url"
    }
    default_meta = (
        cast(dict[str, Any], defaults_dict["metadata"])
        if isinstance(defaults_dict.get("metadata"), dict)
        else None
    )

    merged_feeds: dict[str, Any] = {}
    for feed_id, feed_val in cast(dict[str, Any], raw_feeds).items():
        if not isinstance(feed_val, dict):
            merged_feeds[feed_id] = feed_val
            continue

        feed_dict = cast(dict[str, Any], feed_val)
        merged_feed = {**defaults_dict, **feed_dict}

        if default_meta is not None:
            feed_meta = feed_dict.get("metadata")
            if isinstance(feed_meta, dict):
                merged_feed["metadata"] = {
                    **default_meta,
                    **cast(dict[str, Any], feed_meta),
                }
            elif "metadata" not in feed_dict:
                merged_feed["metadata"] = dict(default_meta)

        merged_feeds[feed_id] = merged_feed

    return {**yaml_data, "feeds": merged_feeds}


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

        _collect_yaml_unknown_key_warnings(self.settings_cls, self.yaml_data)
        self.yaml_data = _apply_feed_defaults(self.yaml_data)
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
        description=(
            "List of trusted proxy IP addresses or CIDR networks. When unset/null, "
            "proxy header processing is disabled. Set to '[]' or '*' to trust all proxies. "
            "Accepts JSON array ('[\"192.168.1.0/24\"]') or comma-separated string ('192.168.1.0/24,10.0.0.1')."
        ),
    )
    tz: ZoneInfo | None = Field(
        default=None,
        validation_alias="TZ",
        description="Timezone for cron schedules and date parsing (e.g., 'America/New_York', 'UTC'). Defaults to system timezone.",
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
    media_conversion_script: Path | None = Field(
        default=None,
        validation_alias="MEDIA_CONVERSION_SCRIPT",
        description=(
            "Path to the executable hook script used when a feed has "
            "media_conversion: custom. Defaults to '<config_dir>/post_process.sh'."
        ),
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
            "yt-dlp update channel: stable, nightly, master, specific version/repository, "
            "or 'disabled' to turn off self-updates."
        ),
    )
    yt_dlp_update_freq: timedelta = Field(
        default=timedelta(hours=12),
        validation_alias="YT_DLP_UPDATE_FREQ",
        description=(
            "Minimum interval between yt-dlp self-updates (e.g., '12h', '1d', or '0' to disable)."
        ),
    )

    feeds: dict[str, FeedConfig] = Field(
        default_factory=dict[str, FeedConfig],
        description="Configuration for all podcast feeds. Must be read from a YAML file.",
    )

    _config_warnings: list[str] = PrivateAttr(default_factory=list)

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

    @field_validator("trusted_proxies", mode="before")
    @classmethod
    def parse_trusted_proxies(cls, v: Any) -> list[str] | None:
        """Parse trusted_proxies from JSON array, comma-separated string, or list.

        Args:
            v: Raw value from env, CLI, or YAML.

        Returns:
            List of trusted proxy IPs/CIDRs, empty list to trust all, or None if disabled.
        """
        match v:
            case None:
                return None
            case list():
                raw_items = cast(list[Any], v)
                if not all(isinstance(item, str) for item in raw_items):
                    raise TypeError("trusted_proxies list items must be strings")
                cleaned = [item.strip() for item in raw_items if item.strip()]
                return [] if cleaned == ["*"] else cleaned
            case str() as s:
                stripped = s.strip()
                if not stripped:
                    return None
                if stripped == "*":
                    return []
                if stripped.startswith("["):
                    try:
                        parsed: Any = json.loads(stripped)
                    except json.JSONDecodeError as e:
                        raise ValueError(
                            f"Invalid JSON array for TRUSTED_PROXIES: {s!r}"
                        ) from e
                    if not isinstance(parsed, list):
                        raise ValueError(
                            "TRUSTED_PROXIES JSON value must be a list of strings"
                        )
                    parsed_items = cast(list[Any], parsed)
                    if not all(isinstance(item, str) for item in parsed_items):
                        raise ValueError(
                            "TRUSTED_PROXIES JSON value must be a list of strings"
                        )
                    cleaned_json = [
                        item.strip() for item in parsed_items if item.strip()
                    ]
                    return [] if cleaned_json == ["*"] else cleaned_json
                cleaned_csv = [
                    part.strip() for part in stripped.split(",") if part.strip()
                ]
                return [] if cleaned_csv == ["*"] else cleaned_csv
            case _:
                raise TypeError(
                    f"trusted_proxies must be a list of strings or string, got {type(v).__name__}"
                )

    @field_validator("yt_dlp_update_freq", mode="before")
    @classmethod
    def parse_yt_dlp_update_freq(cls, v: Any) -> Any:
        """Parse yt_dlp_update_freq from duration strings (e.g., '12h', '1d'), seconds, or timedelta.

        Args:
            v: Value to parse.

        Returns:
            Parsed timedelta or value delegated to Pydantic's built-in ISO-8601 parser.
        """
        match v:
            case timedelta() as td:
                if td < timedelta(0):
                    raise ValueError(
                        f"yt_dlp_update_freq must be non-negative, got {td}"
                    )
                return td
            case bool():
                raise TypeError("yt_dlp_update_freq cannot be a boolean")
            case int() | float() as num:
                if num < 0:
                    raise ValueError(
                        f"yt_dlp_update_freq must be non-negative, got {num}"
                    )
                return timedelta(seconds=num)
            case str() as s:
                stripped = s.strip()
                if not stripped:
                    raise ValueError("yt_dlp_update_freq cannot be empty")
                if stripped == "0":
                    return timedelta(0)
                seconds = cast(
                    int | float | None,
                    pytimeparse2.parse(stripped),  # pyright: ignore[reportUnknownMemberType]
                )
                if seconds is not None:
                    if seconds < 0:
                        raise ValueError(
                            f"yt_dlp_update_freq must be non-negative, got '{s}'"
                        )
                    return timedelta(seconds=seconds)
                # Delegate ISO-8601 (e.g. 'PT12H') or HH:MM:SS to Pydantic
                return stripped
            case _:
                raise TypeError(
                    f"yt_dlp_update_freq must be a duration string or timedelta, got {type(v).__name__}"
                )

    @field_validator("yt_dlp_update_freq", mode="after")
    @classmethod
    def validate_yt_dlp_update_freq_non_negative(cls, v: timedelta) -> timedelta:
        """Ensure parsed yt_dlp_update_freq is non-negative."""
        if v < timedelta(0):
            raise ValueError(f"yt_dlp_update_freq must be non-negative, got {v}")
        return v

    @field_validator("feeds", mode="after")
    @classmethod
    def validate_feed_ids(cls, feeds: dict[str, FeedConfig]) -> dict[str, FeedConfig]:
        """Validate that all feed IDs are URL-safe and match HTTP route constraints."""
        for feed_id in feeds:
            if feed_id in (".", "..") or not _FEED_ID_RE.match(feed_id):
                raise ValueError(
                    f"Invalid feed ID '{feed_id}'. Feed IDs must be 1-255 characters "
                    "containing only letters, numbers, hyphens, and underscores "
                    "(matching ^[a-zA-Z0-9_-]{1,255}$)."
                )
        return feeds

    @model_validator(mode="after")
    def finalize_config_warnings(self) -> AppSettings:
        """Finalize collected warnings including unbounded-feed checks."""
        pending = _PENDING_CONFIG_WARNINGS.get()
        _PENDING_CONFIG_WARNINGS.set(None)
        if pending:
            self._config_warnings.extend(pending)

        for feed_id, feed_cfg in self.feeds.items():
            if (
                feed_cfg.enabled
                and not feed_cfg.is_manual
                and feed_cfg.since is None
                and feed_cfg.keep_last is None
            ):
                self._config_warnings.append(
                    f"Feed '{feed_id}' has neither 'since' nor 'keep_last' set; "
                    "Anypod will attempt to download every item in the source."
                )
        return self

    @property
    def config_warnings(self) -> list[str]:
        """Return non-fatal configuration warnings detected during loading."""
        return list(self._config_warnings)

    @property
    def effective_timezone(self) -> tzinfo:
        """Resolve the effective timezone for scheduling and date parsing.

        Precedence: explicit ``tz`` setting -> ``TZ`` env var -> local system timezone -> UTC.
        """
        if self.tz is not None:
            return self.tz
        tz_env = os.environ.get("TZ")
        if tz_env:
            try:
                return ZoneInfo(tz_env.strip())
            except ZoneInfoNotFoundError:
                pass
        return datetime.now().astimezone().tzinfo or ZoneInfo("UTC")

    @property
    def effective_custom_conversion_script(self) -> Path:
        """Resolve the hook script path for feeds using ``media_conversion: custom``."""
        if self.media_conversion_script is not None:
            return self.media_conversion_script.expanduser()
        return self.config_file.expanduser().parent / "post_process.sh"

    @field_validator("cookies_path", "media_conversion_script", mode="before")
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
