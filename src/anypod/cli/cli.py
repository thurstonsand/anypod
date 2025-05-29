import logging
from pathlib import Path

from ..config import AppSettings, DebugMode
from ..logging_config import setup_logging
from .debug_downloader import run_debug_downloader_mode
from .debug_enqueuer import run_debug_enqueuer_mode
from .debug_ytdlp import run_debug_ytdlp_mode
from .default import default as run_default_mode

DEBUG_YTDLP_CONFIG_FILENAME = "debug.yaml"
DEBUG_DB_FILE = Path.cwd() / "debug.db"
DEBUG_DOWNLOADS_DIR = Path.cwd() / "debug_downloads"


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
            debug_ytdlp_config_path = Path.cwd() / DEBUG_YTDLP_CONFIG_FILENAME
            logger.info(
                "Initializing Anypod in 'ytdlp' debug mode.",
                extra={"debug_config_file_path": str(debug_ytdlp_config_path)},
            )
            app_tmp_dir = DEBUG_DOWNLOADS_DIR / "tmp"
            app_tmp_dir.mkdir(parents=True, exist_ok=True)
            run_debug_ytdlp_mode(
                debug_ytdlp_config_path,
                DEBUG_DOWNLOADS_DIR,
                app_tmp_dir,
            )
        case DebugMode.ENQUEUER:
            logger.info(
                "Initializing Anypod in 'enqueuer' debug mode.",
                extra={"feeds_config_file_path": str(settings.config_file)},
            )
            app_tmp_dir = DEBUG_DOWNLOADS_DIR / "tmp"
            app_tmp_dir.mkdir(parents=True, exist_ok=True)
            run_debug_enqueuer_mode(
                settings,
                DEBUG_DB_FILE,
                DEBUG_DOWNLOADS_DIR,
                app_tmp_dir,
            )
        case DebugMode.DOWNLOADER:
            logger.info(
                "Initializing Anypod in 'downloader' debug mode.",
                extra={"feeds_config_file_path": str(settings.config_file)},
            )
            app_tmp_dir = DEBUG_DOWNLOADS_DIR / "tmp"
            app_tmp_dir.mkdir(parents=True, exist_ok=True)
            run_debug_downloader_mode(
                settings,
                DEBUG_DB_FILE,
                DEBUG_DOWNLOADS_DIR,
                app_tmp_dir,
            )
        case None:
            logger.info("Initializing Anypod in default mode.")
            run_default_mode(settings)

    logger.debug("main_cli execution finished.")
