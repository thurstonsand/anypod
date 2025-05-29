"""Default mode implementation for Anypod.

This module provides the default execution mode that loads and validates
application configuration without performing any processing operations.
"""

import logging

from ..config import AppSettings

logger = logging.getLogger(__name__)


def default(settings: AppSettings) -> None:
    """Load and validate application configuration in default mode.

    Loads the Anypod application configuration and logs the settings
    for verification purposes without performing any feed processing.

    Args:
        settings: Application settings object containing configuration.
    """
    logger.debug(
        "Entered default mode execution.", extra={"settings_object_id": id(settings)}
    )

    logger.info(
        "Loading Anypod application configuration.",
        extra={"config_file_path": settings.config_file},
    )

    settings_json = settings.model_dump_json(indent=2)
    logger.info(
        "Anypod application configuration loaded successfully.",
        extra={
            "config_file_path": str(settings.config_file),
            "configuration": settings_json,
        },
    )
