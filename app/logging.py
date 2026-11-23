"""Structured JSON logging."""

import logging
import sys

import structlog


def configure_logging(level: str = "INFO") -> None:
    lvl = logging.getLevelName(level.upper())
    if not isinstance(lvl, int):
        lvl = logging.INFO
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=lvl, force=True)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(lvl),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str = "taskforge"):
    return structlog.get_logger(name)
