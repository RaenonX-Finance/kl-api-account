import threading
import time
from datetime import datetime

from rich.console import Console, Text

from kl_api_common.const import rich_console
from kl_api_common.env import APP_NAME, DEVELOPMENT_MODE
from .logger import log_message_via_logger
from .types import LogData, LogLevels


def _get_current_timestamp() -> str:
    return datetime.now().isoformat()[:-3]


def _print_console(
    rich_console: Console, level: LogLevels, message: str, *,
    timestamp_color: str, identifier: str | None, **data
):
    if level == "DEBUG" and not DEVELOPMENT_MODE:
        return

    epoch_ms = int(time.time() * 1000)
    log_data: LogData = {
        "application": APP_NAME,
        "level": level,
        "timestamp": epoch_ms,
        "threadId": threading.get_ident(),
        "message": Text.from_markup(message).plain,
        **data
    }

    if identifier:
        log_data["identifier"] = identifier

    log_message_via_logger(level, log_data)

    if not DEVELOPMENT_MODE:
        return

    message = f"{level:>8} [{timestamp_color}]{datetime.fromtimestamp(epoch_ms / 1000).isoformat()[:-3]}[/] " \
              f"\[{log_data['threadId']:>6}]: {f'[{identifier}]' if identifier else ''} {message}"  # noqa: W605

    rich_console.print(message, soft_wrap=True)  # Disable soft wrapping


def print_log(message: str, *, identifier: str | None = None, **data):
    _print_console(rich_console, "INFO", message, timestamp_color="green", identifier=identifier, **data)


def print_socket_event(event: str, *, session_id: str, **data):
    message = f"Received `[purple]{event}[/]`"

    if session_id:
        message += f" - SID: [yellow]{session_id}[/]"

    data["socket"] = {
        "event": event,
        "id": session_id
    }

    print_log(message, **data)
