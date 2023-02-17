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


# region System

_CONFIG_SYS = config["system"]

# region System / Touchance

_SYS_TOUCHANCE = _CONFIG_SYS["touchance"]

SYS_APP_ID = _SYS_TOUCHANCE["app-id"]
SYS_PORT_QUOTE = _SYS_TOUCHANCE["port"]["quote"]
SYS_SERVICE_KEY = _SYS_TOUCHANCE["service-key"]

# endregion

# region System / Touchance

_SYS_GRPC = _CONFIG_SYS["grpc"]

GRPC_PX_CALC = _SYS_GRPC["px-calc"]

# endregion

# endregion

# region Log

_CONFIG_LOG = config["log"]

LOG_SUPPRESS_WARNINGS = _CONFIG_LOG["suppress-warnings"]
LOG_TO_DIR = _CONFIG_LOG.get("output-directory")

# endregion

# region Data

_CONFIG_DATA = config["data"]

# region Data / Download

_CONFIG_DATA_DOWNLOAD = _CONFIG_DATA["download"]

DATA_DOWNLOAD_1K = _CONFIG_DATA_DOWNLOAD["1k"]
DATA_DOWNLOAD_DK = _CONFIG_DATA_DOWNLOAD["dk"]

# endregion

# region Data / Cache

_CONFIG_DATA_CACHE = _CONFIG_DATA["cache"]

CACHE_PX_REQUEST_TTL_SEC = _CONFIG_DATA_CACHE["px-request-ttl-sec"]
CACHE_PX_REQUEST_TTL_SIZE = _CONFIG_DATA_CACHE["px-request-ttl-size"]

# endregion

# region Data / Stream

_CONFIG_DATA_STREAM = _CONFIG_DATA["stream"]

DATA_SEGMENT_COUNT = _CONFIG_DATA_STREAM["segment-count"]

DATA_TIMEOUT_SEC = _CONFIG_DATA_STREAM["timeout-sec"]

_CONFIG_DATA_PX_UPDATE = _CONFIG_DATA_STREAM["px-update"]
MARKET_PX_TIME_GATE_SEC = _CONFIG_DATA_PX_UPDATE["market-time-gate-sec"]
MARKET_DELAY_WARNING_SEC = _CONFIG_DATA_PX_UPDATE["market-delay-warning-sec"]

_CONFIG_DATA_PX_REFETCH = _CONFIG_DATA_STREAM["px-history-refetch"]
DATA_PX_REFETCH_BACKWARD_HOUR = _CONFIG_DATA_PX_REFETCH["backward-hour"]
DATA_PX_REFETCH_STORE_LIMIT = _CONFIG_DATA_PX_REFETCH["limit"]

# endregion

DATA_SOURCES = _CONFIG_DATA["source"]

# region Data / SR levels

_CONFIG_DATA_SR = _CONFIG_DATA["sr-level"]
SR_CUSTOM_LEVELS = _CONFIG_DATA_SR["custom"]

# endregion

# endregion

# region Account

_CONFIG_ACCOUNT = config["account"]

ACCOUNT_SIGNUP_KEY_EXPIRY_SEC = _CONFIG_ACCOUNT["sign-up-key-expiry-sec"]
JWT_LEEWAY_SEC = _CONFIG_ACCOUNT["token-auto-refresh-leeway-sec"]

# endregion
