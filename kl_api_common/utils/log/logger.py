import json
import logging
from typing import Callable

from kl_api_common.const import LOG_TO_DIR, py_logger
from kl_api_common.utils import JSONEncoder
from .attach_handlers import attach_file_handler
from .types import LogData, LogLevels

# Configure loggers

py_logger.setLevel(logging.DEBUG)  # Set to `lowest` to allow handlers to get logs of lower levels
if LOG_TO_DIR:
    attach_file_handler(py_logger)

# Map functions

_py_log_func_map: dict[LogLevels, Callable[[str], None]] = {
    "CRITICAL": py_logger.critical,
    "ERROR": py_logger.error,
    "WARNING": py_logger.warning,
    "INFO": py_logger.info,
    "DEBUG": py_logger.debug,
}


def log_message_via_logger(level: LogLevels, log_data: LogData):
    if level not in _py_log_func_map:
        raise ValueError(f"Invalid log level: {level}")

    _py_log_func_map[level](json.dumps(log_data, cls=JSONEncoder))
