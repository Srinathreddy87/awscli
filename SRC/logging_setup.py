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
    "raiseException",
    "log",
    "info",
    "captureWarnings",
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
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
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
    logger.critical(msg)

def debug(logger, msg):
    logger.debug(msg)

def exception(logger, msg):
    logger.exception(msg)

def warn(logger, msg):
    logger.warn(msg)

def warning(logger, msg):
    logger.warning(msg)

def raiseException(logger, exc):
    logger.exception(exc)

def log(logger, level, msg):
    logger.log(level, msg)

def info(logger, msg):
    logger.info(msg)

def captureWarnings(capture):
    logging.captureWarnings(capture)

# Example usage
if __name__ == "__main__":
    logger = get_logger(__name__, "DEBUG", "example.log")
    info(logger, "This is an info message")
    debug(logger, "This is a debug message")
    warning(logger, "This is a warning message")
    error(logger, "This is an error message")
    critical(logger, "This is a critical message")
