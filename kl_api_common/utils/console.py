import inspect
from datetime import datetime
import threading

from rich.console import Console, Text

from kl_api_common.const import LOG_SUPPRESS_WARNINGS, LOG_TO_DIR, console, console_error
from kl_api_common.env import DEVELOPMENT_MODE
from .log import LogLevels, log_message_to_file


def _get_current_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _print_console(
    rich_console: Console, level: LogLevels, message: str, *,
    timestamp_color: str, identifier: str | None,
):
    if level == "DEBUG" and not DEVELOPMENT_MODE:
        return

    info = f"\[{threading.get_ident():>6}] {inspect.getmodule(inspect.stack()[2][0]).__name__:45}"  # noqa: W605

    if identifier:
        message = f"\[{identifier}] {message}"  # noqa: W605

    if LOG_TO_DIR:
        log_message_to_file(level, Text.from_markup(f"{info}: {message}").plain)

        if not DEVELOPMENT_MODE:
            return

    message = f"[{timestamp_color}]{_get_current_timestamp()}[/] {info}: {message}"

    if DEVELOPMENT_MODE:
        message = f"[bold yellow]-DEV-[/] {message}"

    rich_console.print(message, soft_wrap=True)  # Disable soft wrapping


def print_log(message: str, *, identifier: str | None = None):
    _print_console(console, "INFO", message, timestamp_color="green", identifier=identifier)


def print_warning(message: str, *, force: bool = False, identifier: str | None = None):
    if LOG_SUPPRESS_WARNINGS and not force:
        return

    _print_console(console, "WARNING", f"[yellow]{message}[/]", timestamp_color="yellow", identifier=identifier)


def print_debug(message: str, identifier: str | None = None):
    _print_console(console, "DEBUG", f"[grey50]{message}[/]", timestamp_color="grey50", identifier=identifier)


def print_error(message: str, identifier: str | None = None):
    _print_console(console_error, "ERROR", message, timestamp_color="red", identifier=identifier)


def print_socket_event(event: str, *, session_id: str, additional: str = ""):
    message = f"Received `[purple]{event}[/]`"

    if session_id:
        message += f" - SID: [yellow]{session_id}[/]"

    if additional:
        message += f" - {additional}"

    print_log(message)
