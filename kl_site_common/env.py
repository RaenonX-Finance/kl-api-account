from environs import Env
from jose.constants import Algorithms

env = Env(expand_vars=True)
env.read_env()

with env.prefixed("FASTAPI_"):
    with env.prefixed("AUTH_"):
        FASTAPI_AUTH_SECRET: str = env.str("SECRET")
        FASTAPI_AUTH_ALGORITHM: str = env.str("ALGORITHM", Algorithms.HS256)
        FASTAPI_AUTH_TOKEN_EXPIRY_MINS: int = env.int("TOKEN_EXPIRY_MINS", 15)
        FASTAPI_AUTH_CALLBACK: str = env.str("CALLBACK")

MONGO_URL: str = env.str("MONGO_URL")

DEVELOPMENT_MODE: bool = env.bool("DEV", False)
