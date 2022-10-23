import inspect
from datetime import datetime
import threading
from typing import TYPE_CHECKING

from rich.console import Console, Text

from kl_site_common.const import LOG_SUPPRESS_WARNINGS, LOG_TO_DIR, console, console_error
from kl_site_common.env import DEVELOPMENT_MODE
from .log import LogLevels, log_message_to_file

if TYPE_CHECKING:
    from kl_site_server.socket import SocketNamespace


def _get_current_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _print_console(rich_console: Console, level: LogLevels, message: str, *, timestamp_color: str):
    info = f"\[{threading.get_ident():>6}] {inspect.getmodule(inspect.stack()[2][0]).__name__:45}"

    if LOG_TO_DIR:
        log_message_to_file(level, Text.from_markup(f"{info}: {message}").plain)

        if not DEVELOPMENT_MODE:
            return

    message = f"[{timestamp_color}]{_get_current_timestamp()}[/] {info}: {message}"  # noqa: W605

    if DEVELOPMENT_MODE:
        message = f"[bold yellow]-DEV-[/] {message}"

    rich_console.print(message, soft_wrap=True)  # Disable soft wrapping


def print_log(message: str):
    _print_console(console, "INFO", message, timestamp_color="green")


def print_warning(message: str, *, force: bool = False):
    if LOG_SUPPRESS_WARNINGS and not force:
        return

    _print_console(console, "WARNING", f"[yellow]{message}[/]", timestamp_color="yellow")


def print_error(message: str):
    _print_console(console_error, "ERROR", message, timestamp_color="red")


def print_socket_event(event: str, *, session_id: str, additional: str = "", namespace: "SocketNamespace" = "/"):
    message = f"Received `[purple]{event}[/] @ [blue]{namespace}[/]`"

    if session_id:
        message += f" - SID: [yellow]{session_id}[/]"

    if additional:
        message += f" - {additional}"

    print_log(message)
