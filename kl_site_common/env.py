from environs import Env
from jose.constants import Algorithms

from .type import AppName, VALID_APP_NAMES

env = Env(expand_vars=True)
env.read_env()


@env.parser_for("app_name")
def choice_parser(value):
    if value not in VALID_APP_NAMES:
        raise ValueError(f"`{value}` is not a valid app name (valid ones: {VALID_APP_NAMES})")

    return value


with env.prefixed("FASTAPI_"):
    with env.prefixed("AUTH_"):
        FASTAPI_AUTH_SECRET: str = env.str("SECRET")
        FASTAPI_AUTH_ALGORITHM: str = env.str("ALGORITHM", Algorithms.HS256)
        FASTAPI_AUTH_TOKEN_EXPIRY_MINS: int = env.int("TOKEN_EXPIRY_MINS", 15)
        FASTAPI_AUTH_CALLBACK: str = env.str("CALLBACK")

MONGO_URL: str = env.str("MONGO_URL")

DEVELOPMENT_MODE: bool = env.bool("DEV", False)

APP_NAME: AppName = env.app_name("APP_NAME")
