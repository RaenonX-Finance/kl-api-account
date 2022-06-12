from datetime import datetime

from rich.console import Console, Text

from kl_site_common.const import LOG_SUPPRESS_WARNINGS, LOG_TO_DIR, console, console_error
from .log import LogLevels, log_message_to_file


def print_console(rich_console: Console, level: LogLevels, message: str):
    if LOG_TO_DIR:
        log_message_to_file(level, Text.from_markup(message).plain)
    else:
        rich_console.print(message, soft_wrap=True)  # Disable soft wrapping


def print_log(message: str, *, timestamp_color: str = "green"):
    print_console(
        console,
        "INFO",
        f"[{timestamp_color}]{datetime.now().strftime('%H:%M:%S.%f')[:-3]}[/{timestamp_color}]: "
        f"{message}"
    )


def print_warning(message: str, *, force: bool = False):
    if LOG_SUPPRESS_WARNINGS and not force:
        return

    print_console(console, "WARNING", f"[yellow]{datetime.now().strftime('%H:%M:%S.%f')[:-3]}: {message}[/yellow]")


def print_error(message: str):
    print_console(console_error, "ERROR", f"[red]{datetime.now().strftime('%H:%M:%S.%f')[:-3]}[/red]: {message}")


def print_socket_event(event: str, additional: str = ""):
    message = f"[Socket] Received `[purple]{event}[/purple]`"

    if additional:
        message += f" - {additional}"

    print_log(message)
