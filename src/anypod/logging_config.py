"""Logging configuration and custom formatters for Anypod.

This module provides custom logging formatters, filters, and configuration
setup for the Anypod application, supporting both human-readable and JSON
output formats with enhanced error information and context tracking.
"""

from collections.abc import Mapping
from contextvars import ContextVar
import json
import logging
from logging.config import dictConfig
import sys
from typing import Any, Literal

_original_log_record_factory = logging.getLogRecordFactory()


def custom_record_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
    """Create a custom log record with enhanced exception information.

    Extends the default log record with custom exception attributes and
    semantic trace information for better error reporting.

    Args:
        *args: Arguments passed to the original log record factory.
        **kwargs: Keyword arguments passed to the original log record factory.

    Returns:
        Enhanced LogRecord with additional exception information.
    """
    record = _original_log_record_factory(*args, **kwargs)

    if record.exc_info and record.exc_info[1]:
        exception_instance = record.exc_info[1]

        collected_attrs: dict[str, Any] = {}
        semantic_chain_messages: list[str] = []

        current_exc: BaseException | None = exception_instance
        while current_exc:
            for name, val in vars(current_exc).items():
                if not name.startswith("_") and name not in collected_attrs:
                    collected_attrs[name] = val

            semantic_chain_messages.append(str(current_exc))

            current_exc = current_exc.__cause__ or current_exc.__context__

        # Only set if there are actual values
        if collected_attrs:
            record.exc_custom_attrs = collected_attrs
        if semantic_chain_messages:
            record.semantic_trace = semantic_chain_messages

    return record


_context_id_var: ContextVar[str | None] = ContextVar("context_id", default=None)


def set_context_id(context_id: str) -> None:
    """Set the context ID for the current async context.

    This function sets the context ID in the current async context,
    which will then be automatically included in all log messages
    within that context via the ContextIdFilter.

    Args:
        context_id: The context identifier to set (e.g., "feed_123-1640995200").
    """
    _context_id_var.set(context_id)


class ContextIdFilter(logging.Filter):
    """A logging filter that injects the current context_id into log records.

    Retrieves the context_id from a context variable and adds it to each
    log record for correlation across related operations.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Inject the current context_id into the log record.

        Args:
            record: The log record to modify.

        Returns:
            Always True to allow the record to be processed.
        """
        context_id = _context_id_var.get()
        if context_id is not None:
            record.context_id = context_id
        return True


_should_include_stacktrace: bool = False


class HumanReadableExtrasFormatter(logging.Formatter):
    """A custom formatter for human-readable logs with extra fields.

    Includes a base human-readable format and dynamically appends any 'extra'
    fields passed to the logger. Also includes the context_id if set and
    provides enhanced exception formatting with semantic trace information.

    Attributes:
        Standard logging.Formatter attributes plus custom formatting logic.
    """

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: Literal["%", "{", "$"] = "%",
        validate: bool = True,
        *,
        defaults: Mapping[str, Any] | None = None,
    ):
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record with human-readable output and extra fields.

        Creates a formatted log message that includes timestamp, level, logger name,
        context ID, extra fields, and the main message, with enhanced exception
        formatting when available.

        Args:
            record: The log record to format.

        Returns:
            Formatted log message string.
        """
        prefix_parts: list[str] = []
        prefix_parts.append(self.formatTime(record, self.datefmt))
        prefix_parts.append(record.levelname)
        prefix_parts.append(f"[{record.name}]")

        ctx_id = getattr(record, "context_id", None)
        if ctx_id is not None:
            prefix_parts.append(f"CtxID:{ctx_id}")

        log_string_parts: list[str] = [" ".join(prefix_parts)]

        standard_attrs = {
            "args",
            "asctime",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "message",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "thread",
            "threadName",
            "context_id",
            "taskName",
            "exc_custom_attrs",
            "semantic_trace",
        }

        # Combine exc_custom_attrs and other extras from record.__dict__
        combined_extras: dict[str, Any] = {}
        exc_custom_attributes = getattr(record, "exc_custom_attrs", None)
        if isinstance(exc_custom_attributes, dict):
            combined_extras.update(exc_custom_attributes)  # type: ignore

        for key, value in record.__dict__.items():
            if key not in standard_attrs and not key.startswith("_"):
                # Overwrites if key was in exc_custom_attrs
                combined_extras[key] = value

        extra_kv_pairs: list[str] = []
        if combined_extras:
            for key, value in combined_extras.items():
                try:
                    if isinstance(value, dict | list | tuple):
                        formatted_value = json.dumps(
                            value, sort_keys=True, separators=(", ", ":")
                        )
                    else:
                        formatted_value = str(value)  # type: ignore
                    extra_kv_pairs.append(f"{key}:{formatted_value}")
                except TypeError:
                    extra_kv_pairs.append(
                        f"{key}=[Unserializable Value: {type(value)}]"  # type: ignore
                    )

        if extra_kv_pairs:
            log_string_parts.append(" ".join(extra_kv_pairs))

        main_message = record.getMessage()
        if main_message:
            log_string_parts.append(f"- {main_message}")
        else:
            log_string_parts.append("-")

        final_log_string = " ".join(filter(None, log_string_parts))

        if record.exc_info:
            if _should_include_stacktrace:
                if not record.exc_text:
                    record.exc_text = self.formatException(record.exc_info)
                if record.exc_text:
                    final_log_string += "\n" + record.exc_text
            else:  # Stack trace suppressed, print semantic_trace if available
                semantic_trace_list: list[str] | None = getattr(
                    record, "semantic_trace", None
                )
                if semantic_trace_list:
                    final_log_string += "\n"
                    for i, msg in enumerate(semantic_trace_list):
                        if i == 0:
                            final_log_string += f"Error: {msg}"
                        else:
                            final_log_string += f"\n  Caused by: {msg}"

        if record.stack_info:
            final_log_string += "\n" + self.formatStack(record.stack_info)

        return final_log_string


LOGGING_CONFIG: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "filters": {
        "context_id_filter": {
            "()": ContextIdFilter,
        },
    },
    "formatters": {
        "human_readable_formatter": {
            "()": HumanReadableExtrasFormatter,
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "json_formatter": {
            "()": "pythonjsonlogger.json.JsonFormatter",
            "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
        },
    },
    "handlers": {
        "console_handler": {
            "class": "logging.StreamHandler",
            "formatter": "human_readable_formatter",
            "stream": "ext://sys.stdout",
            "filters": ["context_id_filter"],
        },
    },
    "loggers": {
        "anypod": {
            "handlers": ["console_handler"],
            "level": "INFO",
            "propagate": False,
        },
        # "uvicorn.access": {
        #     "handlers": ["console_handler"],
        #     "level": "INFO",
        #     "propagate": False,
        # },
        # "uvicorn.error": {
        #     "handlers": ["console_handler"],
        #     "level": "INFO",
        #     "propagate": False,
        # },
    },
    "root": {
        "handlers": ["console_handler"],
        "level": "WARNING",  # Default for libraries, etc.
    },
}


def setup_logging(
    log_format_type: Literal["human", "json"],
    app_log_level_name: str,
    include_stacktrace: bool,
) -> None:
    """Configure logging for the application based on provided settings.

    Sets up the logging system with the specified format, log level, and
    stacktrace inclusion preferences. Configures both custom formatters
    and the overall logging hierarchy.

    Args:
        log_format_type: Format for logs ('human' or 'json').
        app_log_level_name: Logging level name (e.g., 'INFO', 'DEBUG').
        include_stacktrace: Whether to include full stack traces in error logs.
    """
    global _should_include_stacktrace
    _should_include_stacktrace = include_stacktrace

    logging.setLogRecordFactory(custom_record_factory)

    log_level_upper = app_log_level_name.upper()
    log_level_val = getattr(logging, log_level_upper, None)
    if not isinstance(log_level_val, int):
        print(
            f"Warning: Invalid LOG_LEVEL '{app_log_level_name}'. Defaulting to INFO.",
            file=sys.stderr,
        )
        LOGGING_CONFIG["loggers"]["anypod"]["level"] = "INFO"
    else:
        LOGGING_CONFIG["loggers"]["anypod"]["level"] = log_level_upper

    actual_log_format = log_format_type.lower()
    if actual_log_format == "json":
        LOGGING_CONFIG["handlers"]["console_handler"]["formatter"] = "json_formatter"
    elif actual_log_format == "human":
        LOGGING_CONFIG["handlers"]["console_handler"]["formatter"] = (
            "human_readable_formatter"
        )

    dictConfig(LOGGING_CONFIG)
