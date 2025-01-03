import logging
from enum import Enum
from threading import Lock
from typing import Dict

# ----------------------- #


class LogLevel(Enum):
    DEBUG = logging.DEBUG  # 10
    INFO = logging.INFO  # 20
    WARNING = logging.WARNING  # 30
    ERROR = logging.ERROR  # 40
    CRITICAL = logging.CRITICAL  # 50


# ....................... #


class LogManager:
    """Manager for loggers."""

    _loggers: Dict[str, logging.Logger] = {}
    _lock = Lock()

    # ....................... #

    @classmethod
    def get_logger(cls, name: str, level: LogLevel = LogLevel.INFO) -> logging.Logger:
        """
        Create or retrieve a logger with the given name and level.
        Thread-safe implementation to ensure only one logger is created per name.

        Args:
            name (str): The name of the logger.
            level (LogLevel): The log level.

        Returns:
            logger (logging.Logger): The logger.
        """

        with cls._lock:
            if name not in cls._loggers:
                # Create a new logger
                logger = logging.getLogger(name)
                logger.setLevel(level.value)

                # Add console handler
                console_handler = logging.StreamHandler()
                console_handler.setLevel(level.value)

                formatter = logging.Formatter(
                    "%(asctime)s - [%(levelname)s] :: %(name)s ::  %(message)s",
                    datefmt="%m/%d/%Y %I:%M:%S%p",
                )
                console_handler.setFormatter(formatter)
                logger.addHandler(console_handler)

                # Register the logger
                cls._loggers[name] = logger

        return cls._loggers[name]

    # ....................... #

    @classmethod
    def update_log_level(cls, level: LogLevel) -> None:
        """
        Update the log level of all registered loggers.
        Thread-safe implementation to update loggers in the registry.

        Args:
            level (LogLevel): The new log level.
        """

        with cls._lock:
            for logger in cls._loggers.values():
                logger.setLevel(level.value)
                for handler in logger.handlers:
                    handler.setLevel(level.value)
