import os
from typing import Literal, TypeAlias, cast, get_args

AppName: TypeAlias = Literal["web-rtc", "app"]

ENV_VAR_APP_NAME: str = "APP_NAME"

VALID_APP_NAMES: tuple[AppName, ...] = get_args(AppName)


def get_app_name() -> AppName:
    app_name = os.environ.get(ENV_VAR_APP_NAME)

    if not app_name:
        raise ValueError(f"Specify the app name (one of {VALID_APP_NAMES}) as env var `{ENV_VAR_APP_NAME}`")

    if app_name not in VALID_APP_NAMES:
        raise ValueError(f"`{app_name}` is not a valid app name (valid ones: {VALID_APP_NAMES})")

    return cast(AppName, app_name)
