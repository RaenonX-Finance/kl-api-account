from datetime import datetime

from rich.console import Console, Text

from kl_site_common.const import LOG_SUPPRESS_WARNINGS, LOG_TO_DIR, console, console_error
from kl_site_common.env import DEVELOPMENT_MODE
from .log import LogLevels, log_message_to_file


def _get_current_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def print_console(rich_console: Console, level: LogLevels, message: str, *, timestamp_color: str):
    if LOG_TO_DIR:
        log_message_to_file(level, Text.from_markup(message).plain)

        if not DEVELOPMENT_MODE:
            return

    message = f"[{timestamp_color}]{_get_current_timestamp()}[/{timestamp_color}]: {message}"

    if DEVELOPMENT_MODE:
        message = f"[bold][yellow]-DEV-[/yellow][/bold] {message}"

    rich_console.print(message, soft_wrap=True)  # Disable soft wrapping


def print_log(message: str):
    print_console(console, "INFO", message, timestamp_color="green")


def print_warning(message: str, *, force: bool = False):
    if LOG_SUPPRESS_WARNINGS and not force:
        return

    print_console(console, "WARNING", f"[yellow]{message}[/yellow]", timestamp_color="yellow")


def print_error(message: str):
    print_console(console_error, "INFO", message, timestamp_color="red")


def print_socket_event(event: str, *, session_id: str, additional: str = ""):
    message = f"[Socket] Received `[purple]{event}[/purple]`"

    if session_id:
        message += f" - SID: [yellow]{session_id}[/yellow]"

    if additional:
        message += f" - {additional}"

    print_log(message)
