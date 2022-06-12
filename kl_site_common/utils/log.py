import logging
import os.path
from logging.handlers import TimedRotatingFileHandler
from typing import Callable, Literal, TypeAlias

from kl_site_common.const import LOG_TO_DIR

_logger = logging.getLogger("KL-Site-Back")

_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_TO_DIR, "server.log"), when="D", interval=1,
    backupCount=7, encoding="utf-8", delay=False
)

_formatter = logging.Formatter(
    fmt="%(asctime)s.%(msecs)03d: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_handler.setFormatter(_formatter)

_logger.addHandler(_handler)
_logger.setLevel(logging.INFO)

LogLevels: TypeAlias = Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]

_log_func_map: dict[LogLevels, Callable[[str], None]] = {
    "CRITICAL": _logger.critical,
    "ERROR": _logger.error,
    "WARNING": _logger.warning,
    "INFO": _logger.info,
    "DEBUG": _logger.debug,
}


def log_message_to_file(level: LogLevels, message: str):
    if level not in _log_func_map:
        raise ValueError(f"Invalid log level: {level}")

    _log_func_map[level](message)
