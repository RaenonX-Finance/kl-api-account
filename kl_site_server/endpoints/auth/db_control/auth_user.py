from datetime import timedelta

from fastapi import Body, Depends
from fastapi.security import OAuth2PasswordRequestForm
from jose import JWTError

from kl_site_common.env import FASTAPI_AUTH_CALLBACK, FAST_API_AUTH_TOKEN_EXPIRY_MINS
from ..const import auth_db_users, auth_db_validation, auth_oauth2_scheme
from ..exceptions import generate_bad_request_exception, generate_blocked_exception, generate_unauthorized_exception
from ..model import DbUserModel, OAuthTokenData, UserDataModel
from ..secret import create_access_token, decode_access_token, is_password_match


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


async def get_user_data_by_oauth2_token(token: str = Depends(auth_oauth2_scheme)) -> UserDataModel:
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
    current_user: UserDataModel = Depends(get_user_data_by_oauth2_token)
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


async def authenticate_user_by_credentials(
    form: OAuth2PasswordRequestForm = Depends()
) -> DbUserModel:
    user = await get_user_by_account_id(form.username)

    if not user:
        raise generate_unauthorized_exception("User not exists")

    if not is_password_match(form.password, user.hashed_password):
        raise generate_unauthorized_exception("Incorrect password")

    return user


async def generate_access_token_on_doc(user: DbUserModel = Depends(authenticate_user_by_credentials)) -> str:
    return create_access_token(
        account_id=user.account_id,
        expiry_delta=timedelta(minutes=FAST_API_AUTH_TOKEN_EXPIRY_MINS)
    )


async def authenticate_user_with_callback(
    form: OAuth2PasswordRequestForm = Depends(),
    redirect_uri: str = Body(...),
) -> DbUserModel:
    if not auth_db_validation.find_one({"client_id": form.client_id}):
        raise generate_bad_request_exception("Invalid client ID")

    if FASTAPI_AUTH_CALLBACK != redirect_uri:
        raise generate_bad_request_exception("Callback URI mismatch")

    return await authenticate_user_by_credentials(form)


async def generate_access_token(user: DbUserModel = Depends(authenticate_user_with_callback)) -> str:
    return create_access_token(
        account_id=user.account_id,
        expiry_delta=timedelta(minutes=FAST_API_AUTH_TOKEN_EXPIRY_MINS)
    )
