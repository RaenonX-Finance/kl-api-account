from rich.console import Console
import yaml

console = Console()
console_error = Console(stderr=True, style="bold red")


with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)
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

# endregion
