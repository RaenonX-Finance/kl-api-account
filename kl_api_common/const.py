import yaml
from rich.console import Console

from .config import get_config

console = Console()
console_error = Console(stderr=True, style="bold red")

config = get_config()


def print_configs():
    # Print current config
    console.print("[cyan]--- Config content ---[/]")
    console.print(yaml.dump(config, default_flow_style=False))


# region Log

_CONFIG_LOG = config["log"]

LOG_SUPPRESS_WARNINGS = _CONFIG_LOG["suppress-warnings"]
LOG_TO_DIR = _CONFIG_LOG.get("output-directory")

# endregion

# region Account

_CONFIG_ACCOUNT = config["account"]

ACCOUNT_SIGNUP_KEY_EXPIRY_SEC = _CONFIG_ACCOUNT["sign-up-key-expiry-sec"]
JWT_LEEWAY_SEC = _CONFIG_ACCOUNT["token-auto-refresh-leeway-sec"]

# endregion
