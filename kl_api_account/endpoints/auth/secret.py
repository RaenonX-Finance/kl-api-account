from datetime import datetime, timedelta

from jose import jwt

from kl_api_common.const import JWT_LEEWAY_SEC
from kl_api_common.env import FASTAPI_AUTH_SECRET, FASTAPI_AUTH_ALGORITHM, FASTAPI_AUTH_TOKEN_EXPIRY_MINS
from .const import auth_crypto_ctx
from .type import JwtDataDict


def is_password_match(plain_password: str, hashed_password: str) -> bool:
    return auth_crypto_ctx.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return auth_crypto_ctx.hash(password)


def make_jwt_dict(username: str, expiry: datetime) -> JwtDataDict:
    return {
        "sub": username,
        "exp": expiry,
    }


def create_access_token(*, username: str, expiry_delta: timedelta | None = None) -> str:
    jwt_dict = make_jwt_dict(
        username,
        datetime.utcnow() + (expiry_delta or timedelta(minutes=FASTAPI_AUTH_TOKEN_EXPIRY_MINS))
    )

    return jwt.encode(jwt_dict, FASTAPI_AUTH_SECRET, algorithm=FASTAPI_AUTH_ALGORITHM)


def decode_access_token(token: str) -> JwtDataDict:
    return jwt.decode(
        token,
        FASTAPI_AUTH_SECRET,
        algorithms=[FASTAPI_AUTH_ALGORITHM],
        options={"leeway": JWT_LEEWAY_SEC}
    )
