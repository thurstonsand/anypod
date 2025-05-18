import logging
import pathlib

from ..config import AppSettings, DebugMode
from ..logging_config import setup_logging
from .debug_enqueuer import run_debug_enqueuer_mode
from .debug_ytdlp import run_debug_ytdlp_mode
from .default import default as run_default_mode

DEBUG_YTDLP_CONFIG_FILENAME = "debug.yaml"


def main_cli():
    settings = AppSettings()  # type: ignore

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
            "active_debug_mode": settings.debug_mode,
        },
    )

    match settings.debug_mode:
        case DebugMode.YTDLP:
            debug_ytdlp_config_path = pathlib.Path.cwd() / DEBUG_YTDLP_CONFIG_FILENAME
            logger.info(
                "Initializing Anypod in 'ytdlp' debug mode.",
                extra={"debug_config_file_path": str(debug_ytdlp_config_path)},
            )
            run_debug_ytdlp_mode(debug_ytdlp_config_path)
        case DebugMode.ENQUEUER:
            logger.info(
                "Initializing Anypod in 'enqueuer' debug mode.",
                extra={"feeds_config_file_path": str(settings.config_file)},
            )
            run_debug_enqueuer_mode(settings)
        case None:
            logger.info("Initializing Anypod in default mode.")
            run_default_mode(settings)

    logger.debug("main_cli execution finished.")
