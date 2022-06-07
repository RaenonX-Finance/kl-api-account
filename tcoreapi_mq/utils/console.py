from datetime import datetime

from rich.console import Console

from tcoreapi_mq.const import LOG_SUPPRESS_WARNINGS, console, console_error


def print_console(rich_console: Console, message: str):
    rich_console.print(message, soft_wrap=True)  # Disable soft wrapping


def print_log(message: str, *, timestamp_color: str = "green"):
    print_console(
        console,
        f"[{timestamp_color}]{datetime.now().strftime('%H:%M:%S.%f')[:-3]}[/{timestamp_color}]: "
        f"{message}"
    )


def print_warning(message: str, *, force: bool = False):
    if LOG_SUPPRESS_WARNINGS and not force:
        return

    print_console(console, f"[yellow]{datetime.now().strftime('%H:%M:%S.%f')[:-3]}: {message}[/yellow]")


def print_error(message: str):
    print_console(console_error, f"[red]{datetime.now().strftime('%H:%M:%S.%f')[:-3]}[/red]: {message}")


def print_socket_event(event: str, additional: str = ""):
    message = f"[Socket] Received `[purple]{event}[/purple]`"

    if additional:
        message += f" - {additional}"

    print_log(message)
