import logging
import os

from kl_api_common.const import LOG_TO_DIR
from kl_api_common.env import APP_NAME
from .handlers import ParallelTimedRotatingFileHandler


def attach_file_handler(logger: logging.Logger):
    handler = ParallelTimedRotatingFileHandler(
        filename=os.path.join(LOG_TO_DIR, APP_NAME),
        encoding="utf-8",
        when="D",
        backup_count=14,
    )
    handler.setFormatter(logging.Formatter(fmt="%(message)s"))

    logger.addHandler(handler)  # All messages should be logged
