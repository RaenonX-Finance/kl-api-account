import logging

import yaml
from rich.console import Console

from .config import get_config

rich_console = Console()

py_logger = logging.getLogger("KL.Api.Account")

config = get_config()


def print_configs():
    # Print current config
    rich_console.print("[cyan]--- Config content ---[/]")
    rich_console.print(yaml.dump(config, default_flow_style=False))


# region Log

_CONFIG_LOG = config.get("log", {})

LOG_TO_DIR = _CONFIG_LOG.get("output-directory")

# endregion

# region Account

_CONFIG_ACCOUNT = config["account"]

ACCOUNT_SIGNUP_KEY_EXPIRY_SEC = _CONFIG_ACCOUNT["sign-up-key-expiry-sec"]
JWT_LEEWAY_SEC = _CONFIG_ACCOUNT["token-auto-refresh-leeway-sec"]

# endregion
