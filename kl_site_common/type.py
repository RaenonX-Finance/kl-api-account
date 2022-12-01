from typing import Literal, TypeAlias, get_args

AppName: TypeAlias = Literal["web-rtc", "app"]

ENV_VAR_APP_NAME: str = "APP_NAME"

VALID_APP_NAMES: tuple[AppName, ...] = get_args(AppName)
