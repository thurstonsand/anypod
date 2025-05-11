import logging
import pathlib

from ..config import AppSettings
from ..logging_config import setup_logging
from .debug_ytdlp import run_debug_ytdlp_mode
from .default import default as run_default_mode

DEBUG_YAML_PATH = pathlib.Path(__file__).resolve().parents[3] / "debug.yaml"


def main_cli():
    settings = AppSettings()  # type: ignore[call-arg]

    setup_logging(
        log_format_type=settings.log_format,
        app_log_level_name=settings.log_level,
        include_stacktrace=settings.log_include_stacktrace,
    )

    logger = logging.getLogger(__name__)

    logger.info(
        "Application logging configured.",
        extra={
            "log_format": settings.log_format,
            "log_level": settings.log_level,
            "include_stacktrace": settings.log_include_stacktrace,
        },
    )
    logger.debug(
        "Application settings loaded.",
        extra={
            "config_file": str(settings.config_file),
            "debug_ytdlp_mode_enabled": settings.debug_ytdlp,
        },
    )

    if settings.debug_ytdlp:
        logger.info(
            "Initializing Anypod in yt-dlp debug mode.",
            extra={"debug_config_file_used": str(settings.config_file)},
        )
        run_debug_ytdlp_mode(settings.config_file)
    else:
        logger.info("Initializing Anypod in default mode.")
        run_default_mode(settings)

    logger.debug("main_cli execution finished.")
