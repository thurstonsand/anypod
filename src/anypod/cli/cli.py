"""Command-line interface entry points for Anypod.

This module provides the main CLI function that handles application
initialization, logging setup, and routing to different execution modes
based on configuration settings.
"""

import logging
from pathlib import Path

from ..config import AppSettings, DebugMode
from ..logging_config import setup_logging
from ..path_manager import PathManager
from .debug_downloader import run_debug_downloader_mode
from .debug_enqueuer import run_debug_enqueuer_mode
from .debug_ytdlp import run_debug_ytdlp_mode
from .default import default as run_default_mode

DEBUG_DB_FILE = Path.cwd() / "debug.db"
DEBUG_DOWNLOADS_DIR = Path.cwd() / "debug_downloads"


def main_cli():
    """Initialize and run the Anypod application based on configuration.

    Sets up logging, loads application settings, and routes execution
    to the appropriate mode (default, ytdlp debug, enqueuer debug, or
    downloader debug) based on the DEBUG_MODE setting.
    """
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

    paths = PathManager(
        base_data_dir=DEBUG_DOWNLOADS_DIR,
        base_tmp_dir=DEBUG_DOWNLOADS_DIR / "tmp",
        base_url=settings.base_url,
    )

    match settings.debug_mode:
        case DebugMode.YTDLP:
            logger.info(
                "Initializing Anypod in 'ytdlp' debug mode.",
                extra={"debug_config_file_path": str(settings.config_file)},
            )
            run_debug_ytdlp_mode(
                settings.config_file,
                paths,
            )
        case DebugMode.ENQUEUER:
            logger.info(
                "Initializing Anypod in 'enqueuer' debug mode.",
                extra={"feeds_config_file_path": str(settings.config_file)},
            )
            run_debug_enqueuer_mode(
                settings,
                DEBUG_DB_FILE,
                paths,
            )
        case DebugMode.DOWNLOADER:
            logger.info(
                "Initializing Anypod in 'downloader' debug mode.",
                extra={"feeds_config_file_path": str(settings.config_file)},
            )
            run_debug_downloader_mode(
                settings,
                DEBUG_DB_FILE,
                paths,
            )
        case None:
            logger.info("Initializing Anypod in default mode.")
            run_default_mode(settings)

    logger.debug("main_cli execution finished.")
