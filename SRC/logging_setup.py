"""
This module provides logging utilities for setting up and configuring loggers.
It includes functions for creating loggers with various logging levels and 
handlers.
"""

import logging
from logging import (
    BASIC_FORMAT,
    CRITICAL,
    DEBUG,
    ERROR,
    FATAL,
    Formatter,
    Handler,
    INFO,
    LogRecord,
    WARN,
)
from logging.handlers import RotatingFileHandler

__all__ = [
    "BASIC_FORMAT",
    "CRITICAL",
    "DEBUG",
    "ERROR",
    "FATAL",
    "Formatter",
    "Handler",
    "INFO",
    "LogRecord",
    "WARN",
    "critical",
    "debug",
    "exception",
    "warn",
    "warning",
    "raise_exception",
    "log",
    "info",
    "capture_warnings",
]

def get_logger(name, log_level="INFO", log_file=None):
    """
    Create and configure a logger.

    :param name: Name of the logger.
    :param log_level: Logging level.
    :param log_file: Optional log file path.
    :return: Configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)

    if not logger.hasHandlers():
        logger.addHandler(ch)

    if log_file:
        fh = RotatingFileHandler(log_file, maxBytes=10 ** 6, backupCount=3)
        fh.setLevel(log_level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger

def critical(logger, msg):
    """Log a critical message."""
    logger.critical(msg)

def debug(logger, msg):
    """Log a debug message."""
    logger.debug(msg)

def exception(logger, msg):
    """Log an exception message."""
    logger.exception(msg)

def warn(logger, msg):
    """Log a warning message."""
    logger.warn(msg)

def warning(logger, msg):
    """Log a warning message."""
    logger.warning(msg)

def raise_exception(logger, exc):
    """Log an exception."""
    logger.exception(exc)

def log(logger, level, msg):
    """Log a message with a specified level."""
    logger.log(level, msg)

def info(logger, msg):
    """Log an info message."""
    logger.info(msg)

def capture_warnings(capture):
    """Capture warnings with logging."""
    logging.captureWarnings(capture)

# Example usage
if __name__ == "__main__":
    logger = get_logger(__name__, "DEBUG", "example.log")
    info(logger, "This is an info message")
    debug(logger, "This is a debug message")
    warning(logger, "This is a warning message")
    logger.error("This is an error message")
    critical(logger, "This is a critical message")
