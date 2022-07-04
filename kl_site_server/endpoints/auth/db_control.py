import secrets
from datetime import timedelta

import pymongo.errors
from fastapi import Depends
from fastapi.security import OAuth2PasswordRequestForm
from jose import JWTError

from kl_site_common.env import FAST_API_AUTH_TOKEN_EXPIRY_MINS
from .const import auth_db_users, auth_db_validation, auth_oauth2_scheme, auth_crypto_ctx
from .exceptions import generate_bad_request_exception, generate_blocked_exception, generate_unauthorized_exception
from .model import (
    DbUserModel, OAuthTokenData, UserDataModel, UserSignupModel,
    ValidationSecretsModel,
)
from .secret import create_access_token, decode_access_token, is_password_match


async def get_user_by_account_id(account_id: str) -> DbUserModel | None:
    find_one_result = auth_db_users.find_one({"account_id": account_id})

    if not find_one_result:
        return None

    return DbUserModel(**find_one_result)


async def get_user_data_by_account_id(account_id: str) -> UserDataModel | None:
    find_one_result = auth_db_users.find_one({"account_id": account_id})

    if not find_one_result:
        return None

    # Remove secrets from the returned model
    # > This is exactly the prop difference between `DbUserModel` and `UserDataModel`
    find_one_result.pop("hashed_password")

    return UserDataModel(**find_one_result)


async def get_user_by_oauth2_token(token: str = Depends(auth_oauth2_scheme)) -> UserDataModel:
    try:
        payload = decode_access_token(token)
        account_id: str = payload.get("sub")

        if account_id is None:
            raise generate_unauthorized_exception("Invalid token - no user name")

        token_data = OAuthTokenData(account_id=account_id)
    except JWTError:
        raise generate_unauthorized_exception("Invalid token - JWT decode error")

    user = await get_user_data_by_account_id(token_data.account_id)
    if user is None:
        raise generate_unauthorized_exception("Invalid token - user not exists")

    return user


async def get_active_user_by_oauth2_token(
    current_user: UserDataModel = Depends(get_user_by_oauth2_token)
) -> UserDataModel:
    if current_user.blocked:
        raise generate_blocked_exception()

    return current_user


async def get_admin_user_by_oauth2_token(
    current_user: UserDataModel = Depends(get_active_user_by_oauth2_token)
) -> UserDataModel:
    if not current_user.admin:
        raise generate_unauthorized_exception("Insufficient permission")

    return current_user


async def authenticate_user_by_credentials(form_data: OAuth2PasswordRequestForm = Depends()) -> DbUserModel:
    user = await get_user_by_account_id(form_data.username)

    if not user:
        raise generate_unauthorized_exception("User not exists")

    if not is_password_match(form_data.password, user.hashed_password):
        raise generate_unauthorized_exception(f"Incorrect password")

    return user


async def generate_access_token(user: DbUserModel = Depends(authenticate_user_by_credentials)) -> str:
    return create_access_token(
        account_id=user.account_id,
        expiry_delta=timedelta(minutes=FAST_API_AUTH_TOKEN_EXPIRY_MINS)
    )


async def generate_validation_secrets(
    _: UserDataModel = Depends(get_admin_user_by_oauth2_token)
) -> ValidationSecretsModel:
    model = ValidationSecretsModel(
        client_id=auth_crypto_ctx.hash(secrets.token_hex(32)),
        client_secret=auth_crypto_ctx.hash(secrets.token_urlsafe(32)),
    )
    auth_db_validation.insert_one(model.dict())

    return model


async def signup_user_ensure_unique(user: UserSignupModel = Depends()) -> UserSignupModel:
    if auth_db_users.count_documents({}) == 0:
        auth_db_users.insert_one(user.to_db_user_model(admin=True).dict())

        return user

    try:
        auth_db_users.insert_one(user.to_db_user_model(admin=False).dict())
    except pymongo.errors.DuplicateKeyError as ex:
        raise generate_bad_request_exception("Duplicated account ID") from ex

    return user


async def signup_user(user: UserSignupModel = Depends(signup_user_ensure_unique)) -> UserDataModel:
    return await get_user_data_by_account_id(user.account_id)
