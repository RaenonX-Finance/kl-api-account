import yaml
from rich.console import Console

console = Console()
console_error = Console(stderr=True, style="bold red")

with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)


def print_configs():
    # Print current config
    console.print("[cyan]--- Config content ---[/cyan]")
    console.print(yaml.dump(config, default_flow_style=False))


# region System

_CONFIG_SYS = config["system"]

SYS_APP_ID = _CONFIG_SYS["app-id"]
SYS_PORT_QUOTE = _CONFIG_SYS["port"]["quote"]
SYS_SERVICE_KEY = _CONFIG_SYS["service-key"]

# endregion

# region Log

_CONFIG_LOG = config["log"]

LOG_SUPPRESS_WARNINGS = _CONFIG_LOG["suppress-warnings"]
LOG_TO_DIR = _CONFIG_LOG.get("output-directory")

# endregion

# region Data

_CONFIG_DATA = config["data"]

# region Data / Stream

_CONFIG_DATA_STREAM = _CONFIG_DATA["stream"]

DATA_TIMEOUT_SEC = _CONFIG_DATA_STREAM["timeout-sec"]
DATA_PX_UPDATE_SEC = _CONFIG_DATA_STREAM["px-update-sec"]
DATA_PX_UPDATE_MARKET_SEC = _CONFIG_DATA_STREAM["px-update-market-sec"]

# endregion

SMA_PERIODS: list[int] = _CONFIG_DATA["sma"]

DATA_SOURCES = _CONFIG_DATA["source"]

# region Data / SR levels

_CONFIG_DATA_SR = _CONFIG_DATA["sr-level"]
SR_STRONG_THRESHOLD = _CONFIG_DATA_SR["strong-threshold"]
SR_CUSTOM_LEVELS = _CONFIG_DATA_SR["custom"]

# endregion

# endregion
